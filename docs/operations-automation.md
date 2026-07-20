# 운영 자동화 / Operations automation

이 문서는 BSIDE 거버넌스 인텔리전스의 GitHub Actions 운영 계약을 설명한다. 모든 생성 HTML, `state.json`, 아카이브는 더 이상 `main` 브랜치에 커밋하지 않는다. 운영 데이터는 MySQL을 기준으로 읽고 쓰며, 공개 페이지는 GitHub Pages artifact로만 배포한다.

## 워크플로

| 파일 | 역할 | 주기 |
|---|---|---|
| `ci.yml` | Python 테스트, PHP 구문 검사, 린트·타입·의존성 보안 검사 | PR, `main` 코드 push |
| `ingest-official.yml` | DART·KIND·회사/행동주주 공식 자료 수집 | KST 07:00~23:45 15분, KST 00:00~06:00 1시간 |
| `ingest-media.yml` | 허가된 Telegram·뉴스 발견 큐 수집 | 30분 |
| `resolve-links.yml` | Google News 발견 URL 후처리 | 1시간 |
| `publish.yml` | 원격 `DeliveryOutbox` claim·발송·ack/fail | 수집 완료 직후, 10분 재시도 |
| `daily.yml` | 일일 페이지 생성·Pages 배포, 일일 Telegram 발송 | KST 05:45, 06:05 |
| `watchdog.yml` | 수집 최신성·outbox·dead letter 감시 | 5분 |
| `pages-deployment-incident.yml` | Pages 최종 검증 실패·회복 이슈 조정 | Pages workflow 완료 직후 |
| `release-gate.yml` | production 증빙 artifact의 14일 shadow·7일 운영·성능·benchmark 전환 판정 | 운영자 수동 실행 |

`ci.yml`의 테스트와 품질 job은 모두 필수다. 린트, 신규 거버넌스 핵심 모듈 타입 검사, `requirements.txt` 의존성 취약점 감사 중 하나라도 실패하면 CI가 실패한다. 기존 수집기 전체에 일괄 예외를 두지 않고 typed-core 범위를 점진적으로 넓힌다.

GitHub cron은 UTC로 해석된다. 일일 생성은 `45 20 * * *`(KST 05:45), 발송은 `5 21 * * *`(KST 06:05)이다. GitHub Actions 예약 실행은 지연될 수 있으므로 애플리케이션은 실행 시각이 아니라 DB cursor와 idempotency key를 기준으로 처리해야 한다.

## 필수 설정

운영 Secret:

- `ACTIVIST_API_URL`, `ACTIVIST_API_SECRET`: 서명된 운영 API
- `DART_API_KEY`: OpenDART 수집
- `CURATOR_FEEDS`: 비공개 보조 발견 피드. 운영 범위 정책이 켜져 있으므로 단순 URL 문자열이 아니라 `name`, `url`, `scope`, `enabled`를 담은 JSON 배열로 등록한다. 세부 형식은 [미디어 발견 피드 범위 정책](media-source-scope-policy.md)을 따른다.
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`: 공개 발송
- `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELEGRAM_SESSION_STRING`: 허가 채널 수집
- `BSIDE_API_BASE_URL`, `BSIDE_OPS_TOKEN`: watchdog의 `/api/v1/ops/health` 호출
- `STORY_REVIEW_ACCESS_TOKEN`, `TELEGRAM_ADMIN_ACCESS_TOKEN`: 명시적으로 생성·등록하는 편집 검수 token. Telegram 메시지나 URL에는 넣지 않고 관리자가 고정된 관리자 URL에서 직접 입력

Repository variable:

- `ENABLE_LEGACY_PIPELINE=true`: 90일 호환 기간 동안 기존 수집·발송 workflow 유지
- `ENABLE_PAGES=true`: 기존 workflow의 Pages artifact 배포 유지
- `ENABLE_GOVERNANCE_SHADOW=false`: 신규 수집·링크 해결·watchdog 예약 실행 차단. 검증된 KIND 어댑터와 수동 smoke test 이후에만 `true`로 변경
- `ENABLE_GOVERNANCE_PAGES=false`: 신규 일일 Pages 공개 배포 차단
- `ENABLE_GOVERNANCE_DELIVERY=false`: 신규 outbox와 일일 Telegram 발송 차단
- `ACTIVIST_PUBLIC_API_URL`: 브라우저에서 읽는 공개 API URL
- `GOVERNANCE_API_BASE_URL`: 공개 거버넌스 UI의 `/api/v1` 기준 URL. 비어 있으면 `ACTIVIST_PUBLIC_API_URL` 뒤에 `/api/v1`을 붙여 사용
- `KIND_DISCLOSURE_ENDPOINT`: 이 저장소가 정의한 JSON·pagination 계약을 충족하는 검증된 KIND 어댑터 URL. 일반 KIND HTML 화면이나 임의 자리표시자 URL을 넣지 않으며, 값이 없거나 계약 검증에 실패하면 공식 수집 workflow가 fail-closed로 종료

수동 `ingest-official`은 `include_kind=false`로 DART-only smoke/shadow를 실행할 수 있다. 예약 실행은 항상 KIND를 필수로 요구하므로 검증된 어댑터가 없는 상태에서 `ENABLE_GOVERNANCE_SHADOW=true`로 바꾸면 성공한 것처럼 건너뛰지 않고 실패한다.

2026-07-16 안전 기본값은 `ENABLE_LEGACY_PIPELINE=true`, 나머지 세 신규 전환 플래그는 `false`다. 전환은 shadow → Pages → delivery 순서로 각각 별도 승인하고, 기존 pipeline 종료는 90일 호환 관측 뒤 결정한다.

`ENABLE_PAGES`와 `ENABLE_GOVERNANCE_PAGES`는 동시에 `true`일 수 없다. 신규 Pages를 켜기 전에 기존 `ENABLE_PAGES=false`를 먼저 적용하며, 두 값이 모두 `true`이면 legacy와 신규 workflow가 모두 fail-closed한다. 코드/API만 바뀐 push에서 생성 단계가 하나도 선택되지 않으면 legacy workflow도 Pages artifact를 배포하지 않는다. 두 Pages 경로 모두 artifact 업로드 전에 `telegram-admin.html` 셸을 생성하고 `TELEGRAM_ADMIN_ACCESS_TOKEN`과 `ACTIVIST_PUBLIC_API_URL`을 검증한다.

전달 경로도 상호 배타적이다. 기존 호환 기간에는 legacy workflow가 MySQL 상태를 복원한 뒤 `legacy-direct`로 공개 채널에 발송하고 신규 ingest는 `disabled`로 수집만 한다. 신규 전달을 승인하면 legacy 직접 발송과 수동 smoke·resend를 중단하고 일일 briefing과 공개 승인 사건을 `DeliveryOutbox`에 넣은 뒤 `publish.yml`만 `TELEGRAM_CHAT_ID` 공개 채널로 전송한다. 비공개 1:1 관리자 채팅, 관리자 링크 발송, token이 포함된 Telegram 메시지는 사용하지 않으며 공개 콘텐츠 outbox에도 관리자 token을 저장하지 않는다. 관리자는 고정 URL에서 명시적으로 등록된 token을 직접 입력한다. 정적 `story-review.html`과 검수 메타데이터는 인증된 서버 측 편집 UI가 준비될 때까지 Pages artifact에서 제외한다. publisher는 한 번에 1건을 900초 lease로 claim하고 한 실행에서 최대 5건만 처리한다. Telegram 응답이 불명확하거나 외부 message ID 저장 전후의 ACK가 실패하면 자동 재전송하지 않고 조정 대상 dead-letter로 격리한다. 실행이 중단되어 `processing` lease가 만료된 경우도 결과 불명으로 간주해 자동 재claim하지 않는다. 사건 enqueue 일부가 거절되더라도 이미 대기 중인 안전한 발송 건을 굶기지 않도록 outbox consumer는 별도로 계속 실행한다.

## 소스 이용권한

운영 이용권한의 단일 기준은 MySQL `SourceRight`이며 `rights` 또는 `admin` 역할의 `/api/v1/admin/source-rights`로만 승인·변경한다. `config.yaml`에 있는 `telegram:activistkorea` 항목은 fail-closed 동작을 보여 주는 `pending` 자리표시자이고 증빙이 없으므로 수집·AI·재배포 권한이 아니다. 운영자는 이를 직접 `active`로 고치는 대신 권한 범위, 증빙 참조 또는 해시, 유효기간, 철회일을 관리자 API에 등록해야 한다.

권한이 만료되거나 철회되면 다음 실행부터 수집과 AI 입력을 중단하고 공개 API에서도 연결 문서와 파생 신호를 제외한다. 철회 상태는 Pages artifact 롤백으로 되돌리지 않는다.

`DeliveryOutbox.payload_json`은 `rights_lineage_complete: true`와 `source_right_ids` 배열을 반드시 포함한다. 새 계약 배포 전에 기존 `pending`·`retry`·`remote_queued` row의 계보를 점검하고, 입증 가능한 건만 보강한다. 계보를 입증할 수 없는 기존 row는 추정해 복구하지 않고 다음 claim에서 `source_right_inactive_or_missing` dead-letter로 격리한다.

예약 실행에서 필수 Secret이 빠지면 해당 작업은 명시적으로 실패한다. PR CI는 운영 Secret을 읽거나 요구하지 않는다.

`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`는 로컬 유지보수용 `google_news_repair` 또는 직접 MySQL 동기화를 선택한 경우에만 로컬 비공개 환경 파일에서 사용한다. 현재 GitHub Actions 경로는 서명된 `ACTIVIST_API_URL`을 사용하므로 repository Secret으로 요구하지 않는다.

## 장애와 복구

Watchdog은 `/api/v1/ops/health` 응답의 `last_success_at`, `pending_outbox`, `oldest_pending_at`, `dead_letter_count`를 확인한다.

- 마지막 정상 수집이 90분을 넘으면 incident
- 가장 오래된 발송 대기가 5분을 넘으면 incident. 5분 감시 주기와 합쳐 설계상 실패 탐지를 10분 이내로 제한
- dead-letter가 한 건 이상이면 즉시 incident
- endpoint, 인증, 응답 형식이 유효하지 않아도 incident

Incident가 발생하면 `[ops/incident] Governance pipeline unhealthy` 이슈를 만들거나 기존 열린 이슈 본문을 최신 진단으로 갱신한다. 정상 회복을 확인하면 같은 이슈에 회복 기록을 남기고 닫는다.

Pages artifact는 한 번만 업로드한 뒤 같은 immutable artifact를 최대 세 번 순차 배포한다. 첫 실패 후 180초, 두 번째 실패 후 300초를 기다리며, 성공한 시도의 URL만 최종 Pages environment URL로 확정한다. 세 번 모두 실패하면 workflow가 실패하고 `[ops/incident] GitHub Pages deployment unhealthy` 이슈를 별도로 생성·갱신한다. 다음 실제 Pages 검증 성공 때 회복 기록을 남기고 닫으며, Pages를 실행하지 않은 workflow 성공은 이 이슈를 닫지 않는다. Incident listener는 기본 브랜치의 완료된 workflow에서 최종 검증 step만 읽고, triggering revision을 checkout하거나 artifact·운영 Secret을 실행하지 않는다.

Governance Pages 생성 결과는 `pages-<run_id>-<attempt>` artifact로 30일 보존한다. Legacy Pages는 최종 배포 실패본을 `pages-failed-<run_id>-<attempt>` artifact로 7일 보존한다. 배포 문제가 발생하면 GitHub Actions의 정상 artifact를 내려받아 `daily.yml`을 수동 실행해 재배포한다. DB의 신규 데이터와 outbox는 롤백하지 않는다.

운영 Pages 배포는 저장소 기본 브랜치에서만 허용한다. `github-pages` environment의 branch policy와 workflow 내부 기본 브랜치 gate를 함께 유지하며, 기능 브랜치 수동 실행은 페이지 생성·검증 artifact까지만 만들 수 있다. 05:45 생성이 Pages 재시도로 늦어질 수 있으므로 06:05 발송 검증은 당일 05:40~07:00 KST에 생성된 성공 marker를 허용한다. workflow 경로, 실행 성공 여부, 당일 artifact 검증은 그대로 fail-closed로 유지한다.

`daily.yml`의 생성 단계는 `python -m curator.governance_ui`를 실행해 `public/governance/config.js`에 공개 API 기준 URL만 기록하고 HTML·JS·CSS 성능 예산을 검사한다. 인증값이나 운영 Secret은 브라우저 자산에 포함하지 않는다.

## 배포 전 점검

1. PR의 `CI` 필수 테스트가 통과했는지 확인한다.
2. 수동 `Ingest official sources`와 `Ingest media sources`를 한 번씩 실행한다.
3. `Publish delivery outbox` 로그에서 external message ID가 저장된 ack를 확인한다.
4. `Daily pages and briefing`을 `generate`로 실행해 Pages artifact와 실제 페이지를 확인한다.
5. `Operations watchdog`을 실행해 건강 상태와 incident 자동 회복을 확인한다.
6. 14일 shadow와 최근 7일 production 증빙 artifact를 준비해 `Governance release transition gate`를 실행하고, 통과 보고서와 사람 승인을 보존한다.
