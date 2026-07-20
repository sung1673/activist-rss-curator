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
   editorial review + DeliveryOutbox
                 |
  /api/v1 + Pages UI + RSS/CSV/JSON
```

주요 구현은 다음 위치에 있다.

- `curator/governance.py`: 회사, 문서, 사건, 캠페인, 근거, 투표, 약속·이행 모델
- `curator/official_ingest.py`, `curator/official_sources.py`: DART·KIND 증분 수집
- `curator/source_rights.py`: 수집·AI·재배포 권한의 fail-closed 판정
- `curator/governance_publisher.py`, `curator/publish_outbox.py`: 공개 승인 사건의 멱등 발송과 재시도
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
DART_API_KEY
KIND_API_KEY
CURATOR_FEEDS
TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID
STORY_REVIEW_ACCESS_TOKEN
TELEGRAM_ADMIN_ACCESS_TOKEN
TELEGRAM_API_ID
TELEGRAM_API_HASH
TELEGRAM_SESSION_STRING
```

`KIND_API_KEY`는 내부 KIND 어댑터가 인증을 요구할 때만 추가하는 선택 Secret이다.

`TELEGRAM_CHAT_ID`는 공개 콘텐츠 발송 목적지로 계속 사용한다. 비공개 Telegram 관리자 채팅과 `TELEGRAM_ADMIN_CHAT_ID`는 사용하지 않으며, 관리자 token을 Telegram 메시지나 URL에 넣어 전달하지 않는다. 관리자는 고정 URL `https://news.bside.ai/feed/telegram-admin.html`을 열어 `TELEGRAM_ADMIN_ACCESS_TOKEN`을 직접 입력한다. 정적 `story-review.html`·메타데이터는 인증된 서버 측 편집 UI가 마련될 때까지 공개 Pages artifact에 포함하지 않는다.

주요 Repository variable은 `ACTIVIST_PUBLIC_API_URL`, `GOVERNANCE_API_BASE_URL`, `KIND_DISCLOSURE_ENDPOINT`, `ENABLE_PAGES`와 단계별 전환 플래그다. 신규 파이프라인은 `ENABLE_GOVERNANCE_SHADOW`, `ENABLE_GOVERNANCE_PAGES`, `ENABLE_GOVERNANCE_DELIVERY`를 각각 명시적으로 `true`로 바꾸기 전에는 예약 실행·공개 배포·발송을 하지 않는다. 전체 목록과 예약 시각은 [운영 자동화 문서](docs/operations-automation.md)를 따른다.

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
- [KIND 연동 결정과 외부 선행조건](docs/kind-integration-decision-2026-07-16.md)
- [KRX KIND 공시 데이터 이용 문의 초안](docs/krx-kind-data-inquiry-ko.md)
- [Shadow 비교와 공개 전환 게이트](docs/release-transition-gate.md)
- [Governance API v1 운영 계약](docs/governance-api-v1.md)
- [Telegram 공개 채널 운영 정책](docs/telegram-public-channels.md)
- [2026-07-16 운영 기반 반영 기록](docs/production-foundation-deployment-2026-07-16.md)

## 안전 경계

이 서비스는 사실·근거·상태를 기록하며 매수·매도 권고, 목표가, 종목 추천을 제공하지 않는다. 공개 전 법률 검토가 필요한 투자 권고 경계, 이해상충, 저작권·이용허가, 정정·당사자 답변 절차는 별도 운영 정책과 함께 승인해야 한다.
