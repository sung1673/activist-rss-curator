# 운영 자동화 / Operations automation

이 문서는 BSIDE 거버넌스 인텔리전스의 GitHub Actions 운영 계약을 설명한다. 모든 생성 HTML, `state.json`, 아카이브는 더 이상 `main` 브랜치에 커밋하지 않는다. 운영 데이터는 MySQL을 기준으로 읽고 쓰며, 공개 페이지는 GitHub Pages artifact로만 배포한다.

## 워크플로

| 파일 | 역할 | 주기 |
|---|---|---|
| `ci.yml` | Python 테스트, PHP 구문 검사, 린트·타입·의존성 보안 검사 | PR, `main` 코드 push |
| `ingest-official.yml` | DART·KIND 공식 공시 자동 수집 | KST 07:00~23:45 15분, KST 00:00~06:30 30분 |
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

`ci.yml`의 테스트와 품질 job은 모두 필수다. 린트, 신규 거버넌스 핵심 모듈 타입 검사, `requirements.txt` 의존성 취약점 감사 중 하나라도 실패하면 CI가 실패한다. 기존 수집기 전체에 일괄 예외를 두지 않고 typed-core 범위를 점진적으로 넓힌다.

공식 JavaScript action은 GitHub-hosted runner의 Node.js 24 계열 major를 사용한다. `checkout@v7`, `setup-python@v7`, `setup-node@v7`, Pages action v5/v6, artifact action v7/v8, `github-script@v9`보다 오래된 Node.js 20 계열 major를 새 workflow에 추가하지 않는다.

GitHub cron은 UTC로 해석된다. 일일 생성은 `45 20 * * *`(KST 05:45), 발송은 `5 21 * * *`(KST 06:05)이다. GitHub Actions 예약 실행은 지연될 수 있으므로 애플리케이션은 실행 시각이 아니라 DB cursor와 idempotency key를 기준으로 처리해야 한다.

모바일 운영 성능 수집은 KST 23:00에 시작해 다음 날 KST 00:35 evidence input 수집 전에 완료한다. `/today`, `/events`, `/companies`, `/calendar`를 각각 5회 실제 Chromium journey로 측정하며, 상세 fail-closed 계약은 [운영 모바일 Web Vitals 수집](web-vitals-production-probe.md)을 따른다.

배포 workflow는 Pages/API 결과를 `/api/v1/ops/web-distribution-observations`에 최대 50건씩 적재한다. full 40자 build SHA와 GitHub run ID·run attempt·target 조합을 사용하고, 같은 조합의 재전송만 멱등이다. 실패 관측에는 실제 탐지 시각을 기록하며 시도 0건을 성공으로 계산하지 않는다. KIND 지연은 공시 문서의 실제 `published_at`과 최초 `EventObservation.first_observed_at`이 모두 있는 경우에만 서버가 계산한다. 날짜만 있는 자료에 자정을 합성하지 않는다. 실제 관측이 0건인 무공시일만 표본 0·지연 `null`의 N/A를 허용하고, 최근 7일 창 전체에는 실제 KIND 관측과 완전한 시각 표본이 1건 이상 있어야 한다.

## 필수 설정

운영 Secret:

- `ACTIVIST_API_URL`, `ACTIVIST_API_SECRET`: 서명된 운영 API
- `DART_API_KEY`: OpenDART 수집
- `KIND_API_KEY`: 승인 직후 수동 adapter preflight에서는 필수. 일반 수집에서는 승인된 adapter 인증 계약에 따라 사용
- `CURATOR_FEEDS`: 비공개 보조 발견 피드. 운영 범위 정책이 켜져 있으므로 단순 URL 문자열이 아니라 `name`, `url`, `scope`, `enabled`를 담은 JSON 배열로 등록한다. 세부 형식은 [미디어 발견 피드 범위 정책](media-source-scope-policy.md)을 따른다.
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`: 등록하지 않는다. 과거 값이 남아 있으면 삭제하며, 현재 코드와 workflow에서는 outbound Telegram을 재활성화할 수 없다.
- `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELEGRAM_SESSION_STRING`: 허가 채널 수집
- `BSIDE_API_BASE_URL`, `BSIDE_OPS_TOKEN`: watchdog의 `/api/v1/ops/health` 호출
- `BSIDE_ADMIN_TOKEN`: `/api/v1/admin/release-state` 전환과 최종 공개·긴급 차단에만 사용하는 32바이트 이상 token. PHP에는 평문 대신 SHA-256을 admin 역할 hash로 등록
- `BSIDE_EDITOR_TOKEN`: 사건·캠페인·정정·shadow discrepancy 검수에 사용하는 32바이트 이상 token. PHP에는 평문 대신 SHA-256을 editor 역할 hash로 등록
- `GOVERNANCE_PREVIEW_TOKEN`: 비공개 preview 조회용 32바이트 이상 token. PHP에는 평문 대신 `governance_preview_token_hash` SHA-256만 등록하고 Pages artifact·URL query에는 포함하지 않음
- `STORY_REVIEW_ACCESS_TOKEN`, `TELEGRAM_ADMIN_ACCESS_TOKEN`: 명시적으로 생성·등록하는 편집 검수 token. Telegram 메시지나 URL에는 넣지 않고 관리자가 고정된 관리자 URL에서 직접 입력

Repository variable의 단일 기준:

- `PAGES_OWNER=legacy|governance`: 한 번에 하나의 Pages 소유자만 선택
- `GOVERNANCE_PIPELINE_MODE=off|dart_canary|shadow|live`: 공식 수집과 후보 파이프라인 단계. KIND 없이 허용되는 값은 `dart_canary`까지이며 정식 증빙·공개는 `shadow|live`에서 KIND를 필수로 요구
- `ENABLE_TELEGRAM_DELIVERY=false`, `ENABLE_GOVERNANCE_DELIVERY=false`: 영구 비활성. `true`이면 workflow가 fail-closed
- `ACTIVIST_PUBLIC_API_URL`: 브라우저에서 읽는 공개 API URL
- `GOVERNANCE_API_BASE_URL`: 공개 거버넌스 UI의 `/api/v1` 기준 URL. 비어 있으면 `ACTIVIST_PUBLIC_API_URL` 뒤에 `/api/v1`을 붙여 사용
- `KIND_DISCLOSURE_ENDPOINT`: 이 저장소가 정의한 JSON·pagination 계약을 충족하는 검증된 KIND 어댑터 URL. 일반 KIND HTML 화면이나 임의 자리표시자 URL을 넣지 않으며, 값이 없거나 계약 검증에 실패하면 공식 수집 workflow가 fail-closed로 종료

아래 기존 boolean은 90일 전환 기간의 입력 어댑터일 뿐 새 운영값으로 사용하지 않는다. 새 값과 충돌하면 항상 fail-closed한다.

- `ENABLE_LEGACY_PIPELINE=true`: 90일 호환 기간 동안 기존 수집·Pages workflow 유지
- `ENABLE_PAGES=true`: 기존 workflow의 Pages artifact 배포 유지
- `GOVERNANCE_PIPELINE_MODE=off`: 신규 governance 예약 실행 차단. DART canary는 `dart_canary`, KIND가 준비된 비교 운영은 `shadow`, 공개 후에는 `live`
- `PAGES_OWNER=legacy`: 기존 Pages만 배포. 보호된 전환 직전 `governance`로 바꾸며 workflow가 직접 Repository Variables를 수정하지 않음

거버넌스 v1 공개 데이터 경로는 repository variable이 아니라 MySQL의 서버 측 release state로 최종 제어한다. migration 001~010을 순서대로 적용한 요구 schema version은 10이며, 공개 상태는 006이 만든 `closed`를 유지한다. 서버는 최댓값이 아니라 1~10의 정확한 버전·이름·체크섬 manifest를 검증한다. DART canary와 백필은 `closed`에서도 HMAC writer로 적재할 수 있지만 API 안내 루트를 포함한 공개 데이터 조회는 503이다. 검수자는 관리자가 `preview`로 전환한 동안에만 preview Bearer token으로 접근하고, 실제 승인 전에는 `closed`로 되돌린다. `live` 전환과 `live → closed` 긴급 차단은 모두 관리자 API의 optimistic version과 감사 로그를 사용한다.

수동 `ingest-official`과 backfill은 `GOVERNANCE_PIPELINE_MODE=dart_canary`, `include_kind=false`로 DART-only smoke를 실행할 수 있다. `shadow|live` 예약 실행은 항상 KIND를 필수로 요구하므로 검증된 어댑터가 없으면 성공한 것처럼 건너뛰지 않고 실패한다.

모든 DART list·회사 master HTTP 시도는 전역 MySQL 쿼터 ledger의 `POST /api/v1/ops/dart-quota` consume ACK를 먼저 받아야 한다. 서버 기준 KST 날짜, 10,000건 한도, `used_count + remaining_count = limit_count`와 attempt ID 일치를 클라이언트가 검증하지 못하면 외부 요청을 보내지 않는다. OpenDART `020`은 같은 attempt로 `block_020`을 기록하며 그 KST 날짜의 다른 workflow도 즉시 멈춘다. 사용하지 않은 예약량을 반환하거나 완료 처리하는 별도 모델은 두지 않는다.

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
