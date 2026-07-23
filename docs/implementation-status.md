# 구현 상태와 전환 체크리스트

기준일: 2026-07-22

이 문서는 6개월 계획 중 저장소에 구현된 기반과 운영 환경에서 추가로 완료해야 하는 작업을 구분한다. 코드가 존재한다는 사실은 운영 목표 달성을 의미하지 않는다. 수집률, 지연, 정확도, 가용성은 실제 API·DB·Telegram·Pages 환경에서 관측한 뒤 승인한다.

## 현재 구현된 기반

| 영역 | 저장소 구현 | 운영 활성화 전 확인 |
|---|---|---|
| 전달 신뢰성 | canonical channel identity, cursor 기반 증분 동기화, 과거 `DeliveryOutbox` 감사 보존, 신규 enqueue·claim 서버 측 HTTP 410 차단 | Pages/API 배포 성공률과 실패 탐지 7일 연속 검증 |
| 공식 수집 | DART·KIND 자동 커넥터, 회사 마스터, KST 당일+2일 중첩 실행, 재시작 가능 백필, 정정·취소 정규화, 페이지·부분수집 fail-closed, 서버 전역 DART 일 10,000회 quota | 운영 key·KIND endpoint, contract 표본, 2021년 백필, 회사·행동주주 공식자료 allowlist 활성화 |
| 데이터 모델 | Company, Actor, SourceRight, Document, GovernanceEvent, EventObservation, Campaign, ClaimEvidence, ProposalVote, CommitmentOutcome, TimelineEntry, EditorialRevision, 서버 계산 canonical identity와 운영 관측·증빙 | migration 001~010 운영 적용, 실제 데이터 대조와 보존 정책 검증 |
| 이용권한 | 수집·AI·재배포 목적별 차단, 유효기간·철회·증빙 검사, KIND 사전 eligibility와 transaction 재검증, 관리자 API | 모든 소스의 실제 증빙 등록과 법률 검토 |
| 사건 발행 | 공식/확인 사건 후보와 편집 승인, 불완전·충돌 identity 검수 큐, 고위험 자동 공개 차단 | 상위·시장 민감 사건 100% 편집 절차 훈련 |
| 공개 제품 | `/api/v1`, OpenAPI, Today 서버 정렬, 회사·actor·사건·캠페인·캘린더·검색 UI, Atom·CSV·JSON, `closed|preview|live` 서버 release guard | migration 006~009, preview/admin/editor token hash, 운영 rewrite·CORS, 브라우저·접근성·성능 실측 |
| 운영 자동화 | CI, 공식/미디어 수집, 링크 해결, 발행, 일일 Pages, watchdog 분리 | GitHub Environment·Secret·Variable, incident issue 권한 |
| 품질 평가 | 사람 라벨 JSONL 스키마, benchmark CLI, 표본 수와 precision/recall 게이트 | 실제 article pair 500개와 사건 300개 라벨링 |
| 전환 판정 | 14일 shadow 비교 보고서, 7일 운영·성능 증빙, 실제 KIND lag, benchmark를 같은 코드 리비전으로 검증하는 fail-closed API·CLI와 보호 workflow | 실제 production export artifact와 사람 승인 |

## 이번 고도화 변경의 구현 범위

- schema version 7에서 사건 canonical identity와 문서 관측을 분리하고, 비교 키가 불완전하거나 충돌하면 자동 병합하지 않고 검수 대상으로 남긴다. version 8은 공식사이트 snapshot·문서 버전 receipt를, version 9는 모든 DART 실행이 공유하는 KST 일자별 quota 원장을 추가한다.
- `closed` 상태에서는 `/api/v1/` 안내 루트를 포함한 모든 공개 데이터 API를 503으로 막고 health·OpenAPI·인증된 ops/admin만 허용한다. GET 요청은 DDL을 실행하지 않는다.
- `/today`는 전체 공개 사건에서 signal을 제외한 Top 5·Watch를 서버에서 결정적으로 선정한다. 캘린더의 사건과 주총 의안에는 동일한 공통 필터를 적용한다.
- 실제 official evidence가 있는 회사 20곳·승인 actor 10곳 후보 API, SourceRight allowlist 공식사이트 connector, durable backfill checkpoint, shadow snapshot, availability·Web Vitals·Pages/API 배포 관측, immutable 사람 검수 bundle과 production evidence exporter를 추가한다.
- KIND 지연은 실제 공시시각과 최초 관측시각의 차이만 사용하며, same-event 정답은 일별 AI 품질값이 아니라 사람 라벨 benchmark만 사용한다.
- 운영 콘텐츠 품질은 2021년 이후 누적 corpus를 각 KST 일자 종료 시점으로 고정한다. shadow는 같은 SHA의 전일 receipt와 DB 원장을 재대조해 실제 corpus를 누적하므로 무공시일에도 실제 분모를 유지하고, receipt 누락·SHA 변경·chain 단절 시 14일 창을 다시 시작한다.
- Telegram outbound는 계속 영구 비활성이고 PHP의 신규 enqueue·claim도 HTTP 410으로 차단한다. 정식 분배 게이트는 `distribution_mode=web_only`의 Pages/API 성공률과 실패 탐지로 판정한다.
- cutover와 rollback은 원본 artifact ID·digest·파일 inventory를 검증한 전체 legacy 복구 bundle을 사용한다. 성공한 기본 브랜치 workflow가 같은 bundle을 매일 90일 artifact로 carry-forward해 전환 후에도 호환 피드와 전체 rollback 자산이 만료되지 않게 한다.

2026-07-22 기준 이 기능 브랜치의 전체 로컬 Python 회귀 테스트는 1,036개 통과, 3개 건너뜀이고 Ruff·엄격 MyPy·compileall, OpenAPI·workflow·JSON Schema, Node 계약 10개, Playwright 사용자 여정 8개·접근성·모바일 성능 예산, npm high 취약점 감사를 통과했다. PHP 7.3/MySQL 8의 실제 migration·API 통합 계약은 draft PR의 GitHub Actions에서 최종 검증한다. 이전 운영 기반인 [PR #8](https://github.com/sung1673/activist-rss-curator/pull/8)의 필수 CI [run 29877517421](https://github.com/sung1673/activist-rss-curator/actions/runs/29877517421)과 병합 SHA `1f8c2acda354d006f15f927a3d9ab31d464ca831`의 [main CI run 29877648961](https://github.com/sung1673/activist-rss-curator/actions/runs/29877648961)은 각각 모든 job을 통과했다. 이 결과는 코드·배포 계약 검증이며 아래 장기 운영 게이트를 대체하지 않는다.

## 2026-07-22 운영 반영 현황

- 운영 MySQL을 92개 테이블·1,940,943행 기준으로 전체 백업했다. 압축본은 498,420,434바이트이고 SHA-256은 `e851085b65060f4bb169e7032dc52ca9674299564d16e9ca46b797625844ea72`다. 이어 migration 005를 작업 터미널 관측 9.141초에 적용해 97개 채널의 identity marker 컬럼과 1,524,369개 메시지 테이블의 `(telegram_channel_id, telegram_message_id)` 인덱스 형태를 검증했다.
- PHP 7.3 운영 서버에는 release `telegram-timeout-fix-1f8c2ac-20260722T091300KST`를 후보 검증 뒤 원자 배포했다. 후보·운영 smoke test 12/12가 통과했고 레거시 health·reports와 `/api/v1/health` fallback이 HTTP 200, 잘못된 역할 token은 403을 반환했다.
- 운영 배포 백업은 `/www_root/activist/_private/deployment-backups/`에만 보관하며 공개 경로의 `.bak`·`.bak.*` 요청이 거부되는 것을 확인했다.
- 실제 Telegram 수집 목록 97개를 하나의 물리 증빙 문서 번호에 연결해 `SourceRight`에 등록했다. 내부 수집·AI·사건 맥락 분석은 허용하되 Telegram 원문·파생 콘텐츠 재배포는 보수적으로 비활성화했다.
- Telegram 365일 이력 복구는 동일한 97채널 fingerprint 아래 97/97개 canonical 채널을 모두 완료했다. 분할·재시도를 포함한 durable ACK 처리량은 1,468,220건이며 실패·대기·잘림이 남은 구간은 0개다. 후속 signal-only run 29872608749도 최근 72시간 메시지 21,317건·매치 693건에서 signal 40건을 재구축하고 누락 17건을 삭제해 완료했다.
- marker 적용 전 전체 1,524,369개 메시지와 10,726개 match를 감사했다. canonical identity 누락·중복·mapping mismatch·현재 메시지를 잘못 가리키는 match·collision은 모두 0건이었다. 기존 orphan match 164건은 모두 원본 메시지가 없는 `truly_missing` 레거시 행이고 재연결 가능한 행은 0건이므로 DB row는 삭제하지 않았고 JSON 감사 증빙을 별도 보존했다. 조건부 승인 뒤 97/97개 채널 marker가 version 1이고 불일치가 0건임을 다시 확인했다.
- 첫 safe-full 실패 원인을 보강한 뒤 [재시도 run 29880780637](https://github.com/sung1673/activist-rss-curator/actions/runs/29880780637)이 성공했다. 23분 19초 동안 메시지 1,175건·match 32건을 원격 ACK했고 remote/channel/metadata 실패·대기 0건, 최대 요청 456,875바이트, signal 40건, 발송 0건을 기록했다. Google News 미해결 255건은 발행 경로를 막지 않고 별도 해결 큐에 넣었다.
- `ENABLE_LEGACY_PIPELINE=true`, `ENABLE_PAGES=true` 복구 뒤 [Pages run 29882176705](https://github.com/sung1673/activist-rss-curator/actions/runs/29882176705)이 11분 26초에 성공했다. 페이지 생성은 10분 27초, artifact ID는 `8515364933`이고 첫 배포 시도에서 [news.bside.ai](https://news.bside.ai/)가 갱신됐다. 배포 직후 공개 페이지와 레거시 health, `/api/v1/health` fallback은 모두 HTTP 200이었다.
- 최종 변수는 `ENABLE_LEGACY_PIPELINE=true`, `ENABLE_PAGES=true`, `ENABLE_TELEGRAM_DELIVERY=false`, 세 거버넌스 전환 플래그 `false`다. 따라서 허가 채널 읽기 수집과 기존 Pages만 재개됐고 모든 outbound Telegram 및 신규 거버넌스 예약·공개 전환은 차단된 상태다. 검증된 KIND JSON 어댑터, 장기 운영 증빙과 사람 승인이 준비되기 전에는 신규 거버넌스 예약 실행이나 공개 전환이 일어나지 않는다.
- 상세 백업 해시, 서버 로그 위치와 다음 활성화 순서는 [운영 기반 반영 기록](production-foundation-deployment-2026-07-16.md)에 남겼다.

## 운영 데이터 전환 순서

1. 현재 DB와 PHP 비공개 설정을 백업한다.
2. `deploy/activist/migrations`의 001~010을 staging에 순서대로 적용하고 `POST ?action=schema`로 레거시 writer용 누락 컬럼을 확인한다. migration manifest가 1~10의 정확한 버전·이름·체크섬과 일치하지 않으면 `/api/v1` 데이터 경로는 DDL을 실행하지 않고 503이어야 한다.
3. role token hash, preview token hash, HMAC secret, CORS allowlist를 운영 환경별로 분리한다. release state가 기본값 `closed`인지 관리자 API와 DB 감사 로그로 확인한다.
4. `/api/v1/admin/source-rights`에 소스별 증빙과 범위를 등록한다. `config.yaml`의 pending 자리표시자는 운영 승인으로 간주하지 않는다.
5. `closed`를 유지한 채 DART 회사 마스터와 짧은 공식 수집 구간을 staging에 적재해 공개 노출이 0인지 확인하고, `preview` token으로만 접수번호·정정 연결·사건 상태를 표본 검수한다.
6. `official_backfill`을 작은 청크부터 실행하고 실패한 청크를 해결한 뒤 전체 범위로 확장한다.
7. 신규 사건 엔진을 기존 결과와 최소 2주 shadow 실행하고 [`curator.release_gate`](release-transition-gate.md) 입력 형식으로 일별 결과를 내보낸다.
8. 사람 라벨 benchmark와 전달·근거·성능 게이트를 7일 연속 충족했는지 CLI와 수동 workflow로 판정한 뒤 사람이 공개 UI 전환을 승인한다.
9. 기존 API와 `feed.xml` 어댑터를 90일 유지하고 이용 현황과 오류를 확인한 뒤 종료 여부를 결정한다.

## 정식 전환 게이트

- DART·KIND 대상 공시 수집 성공률 99% 이상
- 주요 공식 소스 지연 p95 45분 이하
- 핵심 거버넌스 사건 recall 95% 이상
- 동일 사건 묶음 precision 97% 이상
- 핵심 사건 공식 근거 연결률 95% 이상
- Pages/API 배포 성공률 99.5%, 실패 탐지 p95 10분 이내(시도 0건 제외)
- 주요 페이지 가용성 99.9%
- 모바일 LCP p75 2.5초, INP 200ms, CLS 0.1 이하
- 상위·시장 민감 사건 사람 검수율 100%
- 원문 언어 보존률과 유효 이용권한 등록률 100%
- 목표 평가자 15명 중 12명 이상이 3분 안에 사건·당사자·근거·현재 상태 확인

게이트는 CI 성공만으로 충족 처리하지 않는다. 수집 run metric, web distribution·availability 관측, 편집 로그, 성능 측정, 사람 라벨 보고서와 사용자 평가 결과를 함께 보존한다.

## 롤백

공개 전환 후 장애가 발생하면 `release state=closed` 뒤 최근 정상 legacy Pages artifact와 기존 API 어댑터로 되돌리고 `PAGES_OWNER=legacy`로 복구한다. 신규 MySQL 데이터는 삭제하거나 되감지 않으며 source-right 철회 상태는 롤백 대상이 아니다.

## 남은 6개월 작업

- 실제 2021년 이후 DART·KIND 백필과 표본 검수
- 백필 결과에서 회사 20곳·행동주주 10곳을 선정하고 공식 홈페이지/문서 SourceRight와 실제 allowlist를 등록해 connector를 활성화
- 500 article-pair·300 사건 사람 라벨 구축과 이견 조정
- 2주 shadow 비교, 7일 연속 운영 게이트 관측
- 기관·고액자산가·해외기관 15명 사용성 평가
- 법률 검토를 거친 투자 권고 경계, 이해상충, 정정·답변 공개 정책 확정
- 최초 cutover 전에 실제 연속 90일 이상을 포함하는 legacy seed artifact를 고정하고 rollback 훈련 완료
- 기존 Python 모듈의 타입 검사 범위를 신규 typed-core 기준으로 점진 확대
