# 2026-07-16 운영 기반 반영 기록

이 문서는 BSIDE 거버넌스 인텔리전스의 첫 운영 기반 반영 범위와 아직 활성화하지 않은 항목을 기록한다. 비밀값, 개인정보, Telegram 채널별 목록은 포함하지 않는다.

## 반영 완료

### MySQL

- 운영 데이터베이스 전체를 71개 테이블, 591,696행 기준으로 백업했다.
- 압축 백업: `C:\BSIDE\codex\260525_텔레그램_행동주의_채널\_preserved_pre_governance_20260716\dbalignpe-pre-governance-20260716T134533Z.sql.gz`
- 압축 파일 SHA-256: `b54ceaef4b155a71690c90df15f0b236a3c1297900ffad1ce1c0deaac483650b`
- 압축 해제 SQL SHA-256: `83e0c943ac8360c995bc481433c3e33ee41d54ffcbb41937c908ee1f43b2104a`
- `deploy/activist/migrations/001_governance_intelligence.sql`부터 `003`까지 순서대로 적용했다.
- 서버 적용 기록: `/www_root/activist/_private/migrations/governance-schema-20260716T134748Z.json`

### PHP API

- 기존 PHP/MySQL 구조를 유지하면서 `/api/v1` 라우터, OpenAPI 문서와 서버 인증 보정을 배포했다.
- Gabia 환경에서 제거되던 `Authorization` 헤더를 `.htaccess`에서 PHP로 전달하도록 보정했다.
- 공개 health·회사 조회, 기존 reports 어댑터, OpenAPI, 잘못된 역할 토큰의 403, 정상 역할 토큰의 200, CORS를 staging과 운영에서 확인했다.
- 배포 전 백업: `/www_root/activist/api.php.bak.20260716T135614Z`, `/www_root/activist/.htaccess.bak.20260716T135614Z`
- 서버 배포 기록: `/www_root/activist/_private/migrations/governance-api-20260716T135614Z.json`

#### Outbox 안전성 보강 재배포

- 원격 claim을 1건으로 제한하고 lease를 900초(허용 300~1800초)로 보강했다.
- 전송 결과가 불명확하거나 Telegram 성공 뒤 ACK가 실패하면 자동 재발송하지 않고 외부 메시지 ID와 함께 격리한다.
- 서버에 PHP CLI가 없어 임시 무작위 web probe가 staging 파일을 `require`해 HTTP 200을 반환하는 방식으로 PHP 7.3 parser 검증 후 즉시 probe를 삭제했다.
- 운영 교체 뒤 health, 회사 목록, 레거시 reports, OpenAPI가 모두 HTTP 200인지 확인했다.
- `governance_v1.php` SHA-256: `a050a0982af8f9854cc2585984596cdfcd5edb8bbe87f71154ce4561d828335e`
- `openapi.yaml` SHA-256: `383bcb3fadf717c8f989198e7d1427f344a45ed154be3d5faf3ea320d45027f7`
- 서버 배포 기록: `/www_root/activist/_private/migrations/governance-outbox-safety-20260716T145755Z.json`

#### 최종 감사 후 배포 보류

- 로컬 `governance_v1.php`는 만료된 `processing` lease를 자동 재claim하지 않고 `delivery_lease_expired_outcome_unknown` dead-letter로 격리하도록 추가 보강했다. SHA-256은 `fcecce0b5ce1fe7d7942096f4a15c49c0863833b0c65e0450b4a0adbdb38ef57`이다.
- 최초 감사 시 운영 SSH host key의 독립 검증 지문이 로컬 `known_hosts`에 없었고, 서버 FTP도 `AUTH TLS`를 제공하지 않아 추가 PHP 배포를 보류했다.
- 2026-07-17 `alignpartnerscap.com:22`의 SSH 비밀번호 인증에 성공했다. 서버가 제시한 `ssh-rsa` 2048-bit host key 지문은 `SHA256:4Y2J13Nis0NOKupLJCOnr2w5X2UdBZH78TkZMVJCVLo`이다.
- 동일 SSH 세션으로 무작위 일회성 파일을 `/www_root/activist`에 생성하고, 유효한 TLS 연결의 `https://alignpe.gabia.io/activist/`에서 정확히 같은 난수를 읽은 뒤 원격 파일을 즉시 삭제했다. 이 교차 프로토콜 검증으로 접속한 SSH endpoint가 실제 운영 HTTPS 문서 루트를 제어함을 확인했다.
- SSH에서 다시 읽은 운영 `governance_v1.php` SHA-256은 `a050a0982af8f9854cc2585984596cdfcd5edb8bbe87f71154ce4561d828335e`로 기존 배포 기록과 일치했다. 검증 과정에서는 운영 PHP를 변경하지 않았다.
- 현재 운영에는 위에 기록한 이전 검증본 `a050a0...`이 유지된다. `ENABLE_GOVERNANCE_DELIVERY=false`이므로 신규 outbox consumer는 실행되지 않는다.
- 명시적인 운영 배포 승인 후 위 지문을 고정해 로컬 최종본을 백업·임시 업로드·PHP 7.3 parser 검증·원자 교체하고, smoke test와 새 배포 로그를 남겨야 한다. `ENABLE_GOVERNANCE_DELIVERY=true` 전환은 이 배포와 분리해 품질 게이트 통과 후에만 수행한다.

### Telegram 이용권한

- 실제 실행 목록과 일치하는 공개 채널 97개를 canonical numeric ID 기준으로 등록했다.
- 목록 SHA-256: `c6f7dade2ad9ea2f4275e0aa760124e0a7fa5a2f9d9befad9ea5b34dd2302f67`
- 물리 보관 증빙 참조: `physical://bside/telegram-permissions/BSIDE-TG-PERM-20260716-001`
- 허용 범위: 수집, 내부 저장, AI 분석, 사건 맥락·사실·원문 링크 추출, 내부 분석
- 공개 재배포: `false`. 공식 공시나 독립 근거로 확인된 사실만 별도 근거 계보를 통해 공개하고 Telegram 원문·파생 콘텐츠는 자동 공개하지 않는다.
- 서버 등록 기록: `/www_root/activist/_private/migrations/telegram-source-rights-20260716T140316Z.json`

### GitHub 전환 설정

| 변수 | 값 | 의미 |
|---|---|---|
| `ENABLE_LEGACY_PIPELINE` | `true` | 기존 운영 유지 |
| `ENABLE_GOVERNANCE_SHADOW` | `false` | 신규 예약 수집·감시 중지 |
| `ENABLE_GOVERNANCE_PAGES` | `false` | 신규 Pages 공개 배포 중지 |
| `ENABLE_GOVERNANCE_DELIVERY` | `false` | 신규 outbox·Telegram 발송 중지 |

기존 필수 Secret 이름이 등록된 사실은 확인했지만 그 값은 이 문서나 저장소에 복사하지 않았다. 최종 보안 감사에서 새로 도입한 `TELEGRAM_ADMIN_CHAT_ID`는 추가 등록이 필요하다. `KIND_DISCLOSURE_ENDPOINT`는 검증된 JSON 어댑터가 없으므로 자리표시자를 넣지 않았다.

검수 토큰을 공개 발송 채널과 분리하기 위해 `TELEGRAM_ADMIN_CHAT_ID`를 추가했다. 값은 bot과 먼저 1:1 대화를 시작한 관리자의 양수 numeric user ID여야 하며 `TELEGRAM_CHAT_ID`와 같으면 발송이 차단된다. 신규 daily 경로에서는 story-review 직접 발송을 제거했고, 수동 관리자 링크와 legacy 검수 알림만 이 전용 목적지를 사용한다. token은 query string이 아니라 URL fragment로 전달한다.

현재 `ENABLE_PAGES=true`이므로 `ENABLE_GOVERNANCE_PAGES`를 켜기 전에는 반드시 `ENABLE_PAGES=false`로 먼저 전환한다. 두 Pages 소유권이 동시에 켜지면 workflow가 실패한다.

## 검증 결과

- Python 회귀 테스트: 527개 통과
- Ruff와 신규 typed-core MyPy: 통과
- workflow·설정 validator·일일 배포 marker 계약 테스트: 통과
- Playwright 주요 사용자 여정: 데스크톱·모바일 4개 통과
- `git diff --check`: 오류 없음
- Python 및 npm 의존성 감사: 알려진 취약점 없음

## 의도적으로 미실행

- 2021년 이후 DART·KIND 전체 백필
- 검증되지 않은 KIND HTML/비공식 endpoint 연결
- 신규 예약 shadow 실행
- 신규 Pages 전환과 신규 Telegram 발송
- 500 article-pair·300 사건 사람 라벨 benchmark
- 14일 shadow·최근 7일 운영/성능 증빙 수집
- 운영 원시 데이터에서 네 release evidence 파일을 만드는 production exporter
- 15명 사용자 검증과 최종 사람 승인

## 다음 활성화 순서

1. 수동 `ingest-official`을 `include_kind=false`로 실행해 DART-only 적재와 무발송을 확인한다.
2. KRX에 역사·증분 KIND 데이터와 내부 AI·사실/메타데이터 공개 범위를 확인하고, 검증된 KIND JSON 어댑터를 준비한다.
3. `include_kind=true` 수동 실행에서 connector preflight와 DART·KIND 동시 적재를 확인한다.
4. `ENABLE_GOVERNANCE_SHADOW=true`로 바꾸고 14일 비교와 7일 운영·성능 원시 증빙을 수집한다.
5. 실제 원시 증빙 exporter를 연결하고 사람 라벨 benchmark, 편집 검수, 법률 검토와 사용자 평가를 마친다.
6. 전환 게이트 통과 후 `ENABLE_GOVERNANCE_PAGES=true`를 먼저 승인한다.
7. 공개 화면과 rollback을 확인한 뒤 `ENABLE_GOVERNANCE_DELIVERY=true`를 별도로 승인한다.
8. 기존 pipeline은 90일 호환 관측 뒤에만 종료한다.
