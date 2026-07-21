# 구현 상태와 전환 체크리스트

기준일: 2026-07-21

이 문서는 6개월 계획 중 저장소에 구현된 기반과 운영 환경에서 추가로 완료해야 하는 작업을 구분한다. 코드가 존재한다는 사실은 운영 목표 달성을 의미하지 않는다. 수집률, 지연, 정확도, 가용성은 실제 API·DB·Telegram·Pages 환경에서 관측한 뒤 승인한다.

## 현재 구현된 기반

| 영역 | 저장소 구현 | 운영 활성화 전 확인 |
|---|---|---|
| 전달 신뢰성 | canonical channel identity, cursor 기반 증분 동기화, `DeliveryOutbox`, lease·재시도·dead letter, 외부 메시지 ID ack | 실제 채널 `getChat`, 429·권한 오류 주입, 7일 연속 성공률 |
| 공식 수집 | DART·KIND 자동 커넥터, 회사 마스터, KST 당일+2일 중첩 실행, 재시작 가능 백필, 정정·취소 정규화, 페이지·부분수집 fail-closed | 운영 key·endpoint, contract 표본, 2021년 백필, 회사·행동주주 공식자료 connector 구현 |
| 데이터 모델 | Company, Actor, SourceRight, Document, GovernanceEvent, Campaign, ClaimEvidence, ProposalVote, CommitmentOutcome, TimelineEntry, EditorialRevision, DeliveryOutbox | migration 001~004 운영 적용 완료, 실제 데이터 대조와 보존 정책 검증 |
| 이용권한 | 수집·AI·재배포 목적별 차단, 유효기간·철회·증빙 검사, 관리자 API | 모든 소스의 실제 증빙 등록과 법률 검토 |
| 사건 발행 | 공식/확인 사건과 편집 승인 사건만 outbox producer가 큐잉, 멱등 revision key | 상위·시장 민감 사건 100% 편집 절차 훈련 |
| 공개 제품 | `/api/v1`, OpenAPI, 회사·사건·캠페인·캘린더·검색 UI, Atom·CSV·JSON | 운영 rewrite·CORS·role token, 브라우저·접근성·성능 실측 |
| 운영 자동화 | CI, 공식/미디어 수집, 링크 해결, 발행, 일일 Pages, watchdog 분리 | GitHub Environment·Secret·Variable, incident issue 권한 |
| 품질 평가 | 사람 라벨 JSONL 스키마, benchmark CLI, 표본 수와 precision/recall 게이트 | 실제 article pair 500개와 사건 300개 라벨링 |
| 전환 판정 | 14일 shadow 비교 보고서, 7일 운영·성능 증빙, benchmark를 같은 코드 리비전으로 검증하는 fail-closed CLI와 수동 workflow | 실제 production export artifact와 사람 승인 |

2026-07-21 기준 전체 로컬 Python 회귀 테스트는 682개 통과, 2개 건너뜀이고 Ruff·엄격 MyPy·compileall, OpenAPI·workflow·JSON Schema, Playwright 사용자 여정·접근성·모바일 성능 예산을 통과했다. PHP 7.3/MySQL 8 통합 계약은 GitHub Actions에서 검증한다. PR #5의 필수 CI와 병합 뒤 `main` CI도 모두 통과했다. 운영 서버에는 병합 커밋 `199737f1279426fd45c3205bb45cfb16fdfa917c`의 PHP API와 접근 차단 설정을 비공개 백업·후보 검증·원자 교체 절차로 배포했다. 이 결과는 코드·배포 계약 검증이며 아래 장기 운영 게이트를 대체하지 않는다.

## 2026-07-21 운영 반영 현황

- 운영 MySQL 전체 백업 뒤 migration 001~004를 적용하고 Telegram signal staging·revision fence 스키마를 확인했다.
- PHP 7.3 운영 서버에 PR #5 병합본의 `/api/v1`, OpenAPI, 역할 토큰 인증, Telegram 복구 staging 계약과 접근 차단 설정을 원자 배포하고 레거시 API를 함께 smoke test했다.
- 운영 배포 백업은 `/www_root/activist/_private/deployment-backups/`에만 보관하며 공개 경로의 `.bak`·`.bak.*` 요청이 거부되는 것을 확인했다.
- 실제 Telegram 수집 목록 97개를 하나의 물리 증빙 문서 번호에 연결해 `SourceRight`에 등록했다. 내부 수집·AI·사건 맥락 분석은 허용하되 Telegram 원문·파생 콘텐츠 재배포는 보수적으로 비활성화했다.
- Telegram 365일 이력 복구는 단일 채널 카나리에서 완료 metrics를 확인했지만 전체 허가 채널 복구는 진행 중이다. 전체 완료 metrics와 후속 safe-full 성공 전에는 복구 완료로 판정하지 않는다.
- 신규 shadow·Pages·발송 플래그는 모두 `false`다. 검증된 KIND JSON 어댑터, 운영 증빙과 사람 승인이 준비되기 전에는 신규 예약 실행이나 공개 전환이 일어나지 않는다.
- 상세 백업 해시, 서버 로그 위치와 다음 활성화 순서는 [운영 기반 반영 기록](production-foundation-deployment-2026-07-16.md)에 남겼다.

## 운영 데이터 전환 순서

1. 현재 DB와 PHP 비공개 설정을 백업한다.
2. `deploy/activist/migrations`를 staging에 적용하고 `POST ?action=schema`로 누락 컬럼을 확인한다.
3. role token hash, HMAC secret, CORS allowlist를 운영 환경별로 분리한다.
4. `/api/v1/admin/source-rights`에 소스별 증빙과 범위를 등록한다. `config.yaml`의 pending 자리표시자는 운영 승인으로 간주하지 않는다.
5. DART 회사 마스터와 짧은 공식 수집 구간을 staging에 적재해 접수번호·정정 연결·사건 상태를 표본 검수한다.
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
- 발송 성공률 99.5%, 실패 탐지 10분 이내
- 주요 페이지 가용성 99.9%
- 모바일 LCP p75 2.5초, INP 200ms, CLS 0.1 이하
- 상위·시장 민감 사건 사람 검수율 100%
- 원문 언어 보존률과 유효 이용권한 등록률 100%
- 목표 평가자 15명 중 12명 이상이 3분 안에 사건·당사자·근거·현재 상태 확인

게이트는 CI 성공만으로 충족 처리하지 않는다. 수집 run metric, outbox 이력, 편집 로그, 성능 측정, 사람 라벨 보고서와 사용자 평가 결과를 함께 보존한다.

## 롤백

공개 전환 후 장애가 발생하면 최근 정상 Pages artifact와 기존 API 어댑터로 되돌린다. 신규 MySQL 데이터는 삭제하거나 되감지 않는다. outbox는 멱등 키와 외부 메시지 ID를 확인한 뒤 재개하며, source-right 철회 상태는 롤백 대상이 아니다.

## 남은 6개월 작업

- 실제 2021년 이후 DART·KIND 백필과 표본 검수
- 회사 IR·행동주주 공식 홈페이지/문서용 허용목록 connector와 변경·삭제 contract 테스트 연결(현재 공식 자동수집은 DART·KIND만 구현)
- 500 article-pair·300 사건 사람 라벨 구축과 이견 조정
- 2주 shadow 비교, 7일 연속 운영 게이트 관측
- 기관·고액자산가·해외기관 15명 사용성 평가
- 법률 검토를 거친 투자 권고 경계, 이해상충, 정정·답변 공개 정책 확정
- 기존 Python 모듈의 타입 검사 범위를 신규 typed-core 기준으로 점진 확대
