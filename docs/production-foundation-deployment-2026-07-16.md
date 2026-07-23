# 2026-07-16 운영 기반 반영 기록

이 문서는 BSIDE 거버넌스 인텔리전스의 첫 운영 기반 반영 범위와 아직 활성화하지 않은 항목을 기록한다. 비밀값, 개인정보, Telegram 채널별 목록은 포함하지 않는다.

## 반영 완료

### MySQL

- 운영 데이터베이스 전체를 71개 테이블, 591,696행 기준으로 백업했다.
- 압축 백업: `C:\BSIDE\codex\260525_텔레그램_행동주의_채널\_preserved_pre_governance_20260716\dbalignpe-pre-governance-20260716T134533Z.sql.gz`
- 압축 파일 SHA-256: `b54ceaef4b155a71690c90df15f0b236a3c1297900ffad1ce1c0deaac483650b`
- 압축 해제 SQL SHA-256: `83e0c943ac8360c995bc481433c3e33ee41d54ffcbb41937c908ee1f43b2104a`
- 2026-07-16에 `001_governance_v1.sql`부터 `003_editorial_governance.sql`까지 순서대로 적용했다.
- 001~003 서버 적용 기록: `/www_root/activist/_private/migrations/governance-schema-20260716T134748Z.json`
- 2026-07-21에 `004_telegram_signal_rebuild_staging.sql`을 추가 적용하고 staging·revision fence 스키마를 운영에서 확인했다.

### PHP API

- 기존 PHP/MySQL 구조를 유지하면서 `/api/v1` 라우터, OpenAPI 문서와 서버 인증 보정을 배포했다.
- Gabia 환경에서 제거되던 `Authorization` 헤더를 `.htaccess`에서 PHP로 전달하도록 보정했다.
- 공개 health·회사 조회, 기존 reports 어댑터, OpenAPI, 잘못된 역할 토큰의 403, 정상 역할 토큰의 200, CORS를 staging과 운영에서 확인했다.
- 당시 배포 전 백업은 문서 루트에 생성했으나, 이후 보안 점검에서 `_private/deployment-backups/`로 이전했다. 후속 배포는 비공개 디렉터리에만 백업한다.
- 서버 배포 기록: `/www_root/activist/_private/migrations/governance-api-20260716T135614Z.json`

#### Outbox 안전성 보강 재배포

- 원격 claim을 1건으로 제한하고 lease를 900초(허용 300~1800초)로 보강했다.
- 전송 결과가 불명확하거나 Telegram 성공 뒤 ACK가 실패하면 자동 재발송하지 않고 외부 메시지 ID와 함께 격리한다.
- 서버에 PHP CLI가 없어 임시 무작위 web probe가 staging 파일을 `require`해 HTTP 200을 반환하는 방식으로 PHP 7.3 parser 검증 후 즉시 probe를 삭제했다.
- 운영 교체 뒤 health, 회사 목록, 레거시 reports, OpenAPI가 모두 HTTP 200인지 확인했다.
- `governance_v1.php` SHA-256: `a050a0982af8f9854cc2585984596cdfcd5edb8bbe87f71154ce4561d828335e`
- `openapi.yaml` SHA-256: `383bcb3fadf717c8f989198e7d1427f344a45ed154be3d5faf3ea320d45027f7`
- 서버 배포 기록: `/www_root/activist/_private/migrations/governance-outbox-safety-20260716T145755Z.json`

#### 최종 감사 후 일시 배포 보류(2026-07-21 해소)

- 로컬 `governance_v1.php`는 만료된 `processing` lease를 자동 재claim하지 않고 `delivery_lease_expired_outcome_unknown` dead-letter로 격리하도록 추가 보강했다. SHA-256은 `fcecce0b5ce1fe7d7942096f4a15c49c0863833b0c65e0450b4a0adbdb38ef57`이다.
- 최초 감사 시 운영 SSH host key의 독립 검증 지문이 로컬 `known_hosts`에 없었고, 서버 FTP도 `AUTH TLS`를 제공하지 않아 추가 PHP 배포를 보류했다.
- 2026-07-17 `alignpartnerscap.com:22`의 SSH 비밀번호 인증에 성공했다. 서버가 제시한 `ssh-rsa` 2048-bit host key 지문은 `SHA256:4Y2J13Nis0NOKupLJCOnr2w5X2UdBZH78TkZMVJCVLo`이다.
- 동일 SSH 세션으로 무작위 일회성 파일을 `/www_root/activist`에 생성하고, 유효한 TLS 연결의 `https://alignpe.gabia.io/activist/`에서 정확히 같은 난수를 읽은 뒤 원격 파일을 즉시 삭제했다. 이 교차 프로토콜 검증으로 접속한 SSH endpoint가 실제 운영 HTTPS 문서 루트를 제어함을 확인했다.
- SSH에서 다시 읽은 운영 `governance_v1.php` SHA-256은 `a050a0982af8f9854cc2585984596cdfcd5edb8bbe87f71154ce4561d828335e`로 기존 배포 기록과 일치했다. 검증 과정에서는 운영 PHP를 변경하지 않았다.
- 이 시점에는 위에 기록한 이전 검증본 `a050a0...`이 운영에 유지됐다. 아래 2026-07-21 배포가 이 상태를 대체했다.

#### 2026-07-21 이전 원자 배포

- PR [#5](https://github.com/sung1673/activist-rss-curator/pull/5)를 병합한 `main` 커밋 `199737f1279426fd45c3205bb45cfb16fdfa917c`를 운영 배포 기준으로 확정했다.
- `origin/main`과 정확히 일치하는 `api.php`와 `.htaccess`를 후보 경로에서 PHP 7.3 구문·서명 인증·health·권한·staging/revision fence·finalize 멱등성을 검증한 뒤 같은 파일시스템에서 원자 교체했다.
- 배포 전 사본은 `/www_root/activist/_private/deployment-backups/`에만 저장했다. 운영 HTTPS에서 공개 경로의 `.bak`·`.bak.*` 접근 거부와 최종 파일 hash를 확인했다.
- migration 004를 비공개 migration 경로에 보관하고 운영 스키마의 Telegram signal staging·heartbeat·live revision 필드를 확인했다.
- `ENABLE_TELEGRAM_DELIVERY=false`는 유지했으며, 이 배포는 outbound Telegram 발송을 활성화하지 않았다.

### Telegram 이용권한

- 실제 실행 목록과 일치하는 공개 채널 97개를 canonical numeric ID 기준으로 등록했다.
- 목록 SHA-256: `c6f7dade2ad9ea2f4275e0aa760124e0a7fa5a2f9d9befad9ea5b34dd2302f67`
- 물리 보관 증빙 참조: `physical://bside/telegram-permissions/BSIDE-TG-PERM-20260716-001`
- 허용 범위: 수집, 내부 저장, AI 분석, 사건 맥락·사실·원문 링크 추출, 내부 분석
- 공개 재배포: `false`. 공식 공시나 독립 근거로 확인된 사실만 별도 근거 계보를 통해 공개하고 Telegram 원문·파생 콘텐츠는 자동 공개하지 않는다.
- 서버 등록 기록: `/www_root/activist/_private/migrations/telegram-source-rights-20260716T140316Z.json`

### 현재 GitHub 전환 설정(2026-07-22)

| 변수 | 값 | 의미 |
|---|---|---|
| `ENABLE_LEGACY_PIPELINE` | `true` | 전체 이력 복구·signal 최종화·safe-full 성공 뒤 기존 정규 실행 재개 |
| `ENABLE_PAGES` | `true` | safe-full과 Pages 전용 배포 성공 뒤 기존 Pages artifact 배포 재개 |
| `ENABLE_TELEGRAM_DELIVERY` | `false` | 모든 outbound Telegram 경로 차단 |
| `ENABLE_GOVERNANCE_SHADOW` | `false` | 신규 예약 수집·감시 중지 |
| `ENABLE_GOVERNANCE_PAGES` | `false` | 신규 Pages 공개 배포 중지 |
| `ENABLE_GOVERNANCE_DELIVERY` | `false` | 신규 outbox·Telegram 발송 중지 |

기존 필수 Secret 이름이 등록된 사실은 확인했지만 그 값은 이 문서나 저장소에 복사하지 않았다. `KIND_DISCLOSURE_ENDPOINT`는 검증된 JSON 어댑터가 없으므로 자리표시자를 넣지 않았다.

### 2026-07-20 관리자 접근 정책 변경

- 비공개 Telegram 관리자 채팅을 사용하지 않기로 결정했으며 `TELEGRAM_ADMIN_CHAT_ID`를 등록하거나 요구하지 않는다.
- `TELEGRAM_CHAT_ID`와 `TELEGRAM_BOT_TOKEN`은 공개 콘텐츠 채널 발송에만 계속 사용한다.
- Telegram을 통한 관리자 링크·검수 알림·token 전달 경로를 제거한다. 관리자 대시보드는 고정 URL로 접속하고, 명시적으로 생성해 GitHub Secret과 PHP hash 설정에 등록한 `TELEGRAM_ADMIN_ACCESS_TOKEN`을 관리자가 직접 입력한다.
- token은 query string이나 URL fragment에 넣지 않으며 Telegram 메시지, Actions 로그, artifact, job summary에도 기록하지 않는다.
- 정적 `story-review.html`과 검수 메타데이터는 페이지 소스만으로 후보 내용이 노출될 수 있으므로, 인증된 서버 측 편집 UI가 마련될 때까지 공개 Pages artifact에 배포하지 않는다.
- 이 변경은 공개 채널 발송을 중단하는 조치가 아니다. 기존 공개 `TELEGRAM_CHAT_ID` 발송과 `DeliveryOutbox`의 성공 응답·외부 message ID 확인 규칙은 유지한다.

### 2026-07-21 outbound Telegram delivery 중단

이 절은 위 2026-07-20 기록 중 “공개 채널 발송을 계속 유지한다”는 문구를 대체한다. 기존 실행 기록은 변경 이력으로 보존하되, 2026-07-21 이후 운영 판단에는 아래 정책을 우선 적용한다.

- 공개 콘텐츠를 포함해 Telegram 채팅으로 outbound 메시지를 발송하지 않는다.
- `ENABLE_TELEGRAM_DELIVERY=false`, `config.yaml` `telegram.enabled=false`, 빈 `telegram.chat_id`를 함께 유지한다.
- `TELEGRAM_API_ID`·`TELEGRAM_API_HASH`·`TELEGRAM_SESSION_STRING`을 사용하는 허가 공개 채널 읽기 수집은 outbound 발송과 분리해 계속한다.

2026-07-22 safe-full과 Pages 전용 배포가 성공해 현재 `ENABLE_LEGACY_PIPELINE=true`, `ENABLE_PAGES=true`다. 향후 `ENABLE_GOVERNANCE_PAGES`를 켜기 전에는 반드시 기존 `ENABLE_PAGES=false`를 먼저 적용한다. 두 Pages 소유권이 동시에 켜지면 workflow가 실패한다.

safe-full 수정본 배포에서는 일반·복구·거버넌스 Telegram writer가 모두 정지했고 관련 Actions가 0건인 상태를 먼저 고정했다. 전체 DB 백업 뒤 migration 005와 marker 무효화 로직이 포함된 병합 SHA의 PHP를 순서대로 배포했다. 감사 결과 중복 channel ID, 비정규 message identity, canonical mapping mismatch, 현재 메시지를 잘못 가리키는 match는 모두 0건이었다. 기존 orphan match 164건은 모두 원본이 없는 `truly_missing` 레거시 행이고 재연결 가능한 행·collision은 0건이었다. DB row는 삭제하지 않았고 JSON 감사 증빙을 별도 보존했다. 불일치가 없는 현재 권위 채널 97개만 하나의 조건부 UPDATE로 marker 1을 승인하고 사후 수·불일치를 재확인했다.

### 2026-07-22 timeout fix 운영 완료

#### 운영 DB 백업과 migration 005

- 전체 백업 기준: 92개 테이블, 1,940,943행
- 압축 백업: `C:\BSIDE\codex\260525_텔레그램_행동주의_채널\_preserved_pre_timeout_fix_20260722\dbalignpe-pre-timeout-fix-20260722T083900KST.sql.gz`
- 압축 크기: 498,420,434바이트
- 압축 파일 SHA-256: `e851085b65060f4bb169e7032dc52ca9674299564d16e9ca46b797625844ea72`
- 압축 해제 SQL 크기: 1,880,782,826바이트
- 압축 해제 SQL SHA-256: `fdb14f16e660e4727d9c9cfe69d98171a45ba3bfc95ecc4ecffdfd2240857da6`
- migration `005_telegram_channel_identity_index.sql` SHA-256: `84a40e57dc6461660d5c62de16e582725c7fa02dfb81c3466e41b76cafd4bfe1`
- MySQL 8.0.44에서 작업 터미널 관측 9.141초에 적용했으며 97개 채널의 identity marker 컬럼과 1,524,369개 메시지 테이블의 `(telegram_channel_id, telegram_message_id)` 인덱스가 정의와 정확히 일치했다.

#### PHP 원자 배포

- 배포 기준: PR [#8](https://github.com/sung1673/activist-rss-curator/pull/8) 병합 SHA `1f8c2acda354d006f15f927a3d9ab31d464ca831`
- PR CI: [run 29877517421](https://github.com/sung1673/activist-rss-curator/actions/runs/29877517421) 전 job 성공
- main CI: [run 29877648961](https://github.com/sung1673/activist-rss-curator/actions/runs/29877648961) 전 job 성공
- release: `telegram-timeout-fix-1f8c2ac-20260722T091300KST`
- 비공개 백업: `/www_root/activist/_private/deployment-backups/telegram-timeout-fix-1f8c2ac-20260722T091300KST/`
- 배포 기록: `/www_root/activist/_private/migrations/telegram-timeout-fix-1f8c2ac-20260722T091300KST.json`
- 배포 기록 SHA-256: `b93c00eecb4bfc6e459ac25b5f2455cce78b2006f4fc35c27fef7eead2de02fa`
- 운영 `api.php` SHA-256: `b358469da0a96861bed8b25fed5d67d9066d2f519f164227f53d70210832fed5`
- 운영 `governance_v1.php` SHA-256: `57269dabe60a8b7ea86941ae5994a81d4069ab5707ba06a744ccfa9e73096e89`
- 후보와 운영에서 각각 6개 smoke test, 합계 12/12를 통과했다. health·companies·reports·capabilities·v1 health fallback은 HTTP 200이고 잘못된 역할 token은 403이었다. Telegram outbound는 실행하지 않았다.

#### marker 감사와 승인

- 감사 대상: 메시지 1,524,369건, match 10,726건, canonical 채널 97개
- canonical identity 누락·중복·mapping mismatch·bad live match·collision: 모두 0건
- 기존 orphan match: 164건. 모두 `truly_missing`, 재연결 가능 0건, identity field 누락 0건
- 증빙: `C:\BSIDE\codex\260525_텔레그램_행동주의_채널\_preserved_pre_timeout_fix_20260722\telegram-orphans-pre-marker-20260722T092629KST.json`
- 증빙 SHA-256: `c3252f70a88e7e1d4ed4468adcbddc6ded55fd74a321a766c7fa6f8845673898`
- 조건부 승인 결과: marker version 0인 채널 `97 → 0`, version 1인 채널 `0 → 97`; 사후 불일치 0건. 작업 터미널 관측 총 소요 18.125초
- 일회성 helper는 작업 뒤 제거했다. DB transport의 종단 서버 인증은 PHP/PDO 운영 경로까지 포함해 별도 증빙이 필요한 hardening 항목이므로 같은 helper를 반복 사용하지 않는다. 향후 직접 DB 유지보수나 PHP DB 설정 변경 전에는 공급자 CA·고정 인증서 또는 공급자가 보장하는 private route를 확인하고 실제 연결의 TLS 협상·서버 인증을 검증한다. 상세 endpoint와 검증 자료는 비공개 운영 기록에만 보존한다.

#### safe-full과 Pages 복구

- [safe-full run 29880780637](https://github.com/sung1673/activist-rss-curator/actions/runs/29880780637): `success`, 실행 23분 19초, build metrics 735.537초
- 처리량: fetched 298, accepted 17, 메시지 1,175건·match 32건 원격 ACK, signal 40건
- 안정성: channel/remote/metadata failed 0, pending 0, 최대 요청 456,875바이트, outbound sent 0
- Google News: discovery 267건 중 unresolved 255건을 별도 큐에 넣었고 핵심 발행 경로는 fail-open으로 완료
- metrics artifact: `C:\BSIDE\codex\260525_텔레그램_행동주의_채널\_preserved_pre_timeout_fix_20260722\safe-full-29880780637\curator-run-metrics.json`
- metrics JSON 파일 SHA-256: `7bf1a148e82df9fb35c273d6a45c4d1e93543688ef632e3d8b8c6f537d0e2e23`
- GitHub metrics artifact: ID `8514897680`, digest `sha256:19f487c023c41315d6ffe8c28d9e883e9f1db2977521c331f68c300737a8cf95`
- [Pages run 29882176705](https://github.com/sung1673/activist-rss-curator/actions/runs/29882176705): `success`, 총 11분 26초, 페이지 생성 10분 27초
- `github-pages` artifact ID `8515364933`을 첫 시도에 [https://news.bside.ai/](https://news.bside.ai/)로 배포했다. 배포 직후 루트·최신 보고서(111,481바이트)·RSS(49,840바이트)·`feed/telegram-admin.html` 셸(36,370바이트), 레거시 health와 `/api/v1/health` fallback이 모두 HTTP 200이었다.
- 최종 변수: legacy pipeline·기존 Pages `true`, Telegram delivery·governance shadow·governance Pages·governance delivery `false`

## 검증 결과

- 전체 로컬 Python 회귀 테스트: 684개 통과, 2개 건너뜀
- Ruff와 신규 typed-core MyPy: 통과
- PHP 7.3/MySQL 8 통합 계약과 workflow·설정 validator·일일 배포 marker 계약 테스트: 통과
- Playwright 주요 사용자 여정: 데스크톱·모바일 4개 통과
- `git diff --check`: 오류 없음
- Python 및 npm 의존성 감사: 알려진 취약점 없음

### Telegram 이력 복구 진행 기록

- 2026-07-21 `Yeouido_Lab` 단일 채널의 365일 이력을 resume cursor로 나눠 복구했다. 앞선 3,000건 상한 run 두 건은 의도한 fail-closed 상태로 다음 cursor를 남겼고, [완료 run 29827367590](https://github.com/sung1673/activist-rss-curator/actions/runs/29827367590)에서 `ok=true`, `status=complete`를 확인했다.
- 세 구간에서 메시지 9,685건을 처리했으며 마지막 완료 run은 3,685건이었다. 최종 metrics는 `telegram_channel_failed=0`, `telegram_remote_failed=0`, `telegram_remote_pending=0`, `telegram_backfill_truncated_channels=0`이었다. 당시 최근 72시간 signal 40건도 staging/finalize됐지만, 후속 검토에서 단일 채널·부분 실행의 signal 결과는 전체 universe의 권위 있는 결과가 될 수 없음을 확인했다. 해당 결과는 카나리 증빙으로만 보존하고, 권위 있는 전체-universe signal 결과는 run 29872608749의 signal-only 최종화로 교체했다.
- PR #6 병합 뒤 [전체 복구 재시작 run 29835701573](https://github.com/sung1673/activist-rss-curator/actions/runs/29835701573)은 새 97채널 fingerprint를 고정하고 `activistkorea` 35,371건을 12개 page checkpoint로 ACK 완료했다. 다음 `anyoungjin`의 메시지 없는 완료 checkpoint에서 97채널 metadata 344,321바이트를 다시 처리하던 API가 `ReadTimeout`을 내면서 workflow는 의도대로 실패했고, `telegram_remote_pending=0`이며 signal rebuild는 실행되지 않았다. 1차 후속 수정은 메시지 없는 checkpoint를 canonical identity의 현재 채널 1개로 제한하고, 부분 실행 말미의 전체 metadata 재전송을 생략하며, 일반 metadata를 최대 20채널씩 분할했다. 첫 safe-full 분석 뒤 최종 상한은 메시지·metadata 모두 5채널로 낮췄다.
- [실패 지점 재시도 run 29839021277](https://github.com/sung1673/activist-rss-curator/actions/runs/29839021277)은 PR #7 병합 커밋 `a586b699d1b408d5173c99330a3312b332846aa7`에서 `only_handles=anyoungjin`, `before_message_id=0`, 동일한 97채널 fingerprint로 실행했다. 메시지 없는 채널의 완료 checkpoint는 현재 채널 metadata 1건·619바이트만 전송해 성공했다. artifact는 `ok=true`, `status=complete`, 선택·완료 채널 1개, `telegram_channel_failed=0`, `telegram_remote_failed=0`, `telegram_remote_pending=0`, `telegram_backfill_truncated_channels=0`, `telegram_remote_metadata_synced=1`, `telegram_remote_metadata_failed=0`, `telegram_repair_remote_checkpoint_complete=1`을 기록했다. 부분 실행이므로 signal 재구축은 의도대로 생략됐다(`telegram_signal_rebuild_skipped_partial=1`).
- run 29835701573의 `activistkorea` 완료 checkpoint와 run 29839021277의 `anyoungjin` 재시도부터 마지막 `YuantaKoreaTech`까지 아래 체인이 동일한 fingerprint로 이어졌다. 97/97개 canonical 채널, durable ACK 처리량 1,468,220건과 signal-only 최종화를 완료했고, 무배포·무발송 safe-full run 29880780637 및 후속 Pages run 29882176705의 성공까지 확인해 기존 운영 경로 복구를 확정했다.

| 단계 | 실행/체인 경계 | 필수 완료 증빙 | 상태 |
|---|---|---|---|
| 후속 복구 구간 | [run 29839617110](https://github.com/sung1673/activist-rss-curator/actions/runs/29839617110): `anyoungjin` 이후, `companyreport` 완료 후 `dada_news2` cursor `3666236` | 300,000건 ACK, canonical 채널 11개 완료, remote/metadata failed·pending 0, checkpoint complete 1; 전역 상한으로 fail-closed | `dada_news2` 재개 |
| 대형 채널 재개 | [run 29845058461](https://github.com/sung1673/activist-rss-curator/actions/runs/29845058461): `dada_news2` cursor `3666236` → `3366236` | 300,000건 ACK, remote/metadata failed·pending 0, checkpoint complete 1; 전역 상한으로 fail-closed | cursor 재개 |
| 대형 채널 완료 | [run 29850951380](https://github.com/sung1673/activist-rss-curator/actions/runs/29850951380): `dada_news2` cursor `3366236` → 0 | 226,560건·76 page ACK, `ok=true`, truncated/remote/metadata failed·pending 0, checkpoint complete 1 | 누적 782,392건, 완료 |
| 후속 복구 구간 | [run 29855338208](https://github.com/sung1673/activist-rss-curator/actions/runs/29855338208): `dada_news2` 이후 → `hyundaiindustirial` | 20채널·182,593건·71 page ACK, `ok=true`, truncated/remote/metadata failed·pending 0, checkpoint complete 1 | 완료 |
| 후속 복구 구간 | [run 29859906644](https://github.com/sung1673/activist-rss-curator/actions/runs/29859906644): `hyundaiindustirial` 이후 → `kisthemacro` 완료, `kiwoom_semibat` 첫 checkpoint | 앞 10채널 durable ACK; `kiwoom_semibat` 313건 요청 `ReadTimeout`, pending 313·checkpoint complete 0으로 fail-closed | 단일 채널 재시도 |
| 실패 지점 재시도 | [run 29862978883](https://github.com/sung1673/activist-rss-curator/actions/runs/29862978883): `kiwoom_semibat` | 1,213건·1 page ACK, `ok=true`, truncated/remote/metadata failed·pending 0, checkpoint complete 1 | 완료 |
| 후속 복구 구간 | [run 29863762961](https://github.com/sung1673/activist-rss-curator/actions/runs/29863762961): `kiwoom_semibat` 이후 → `rassiro_channel` | 20채널·192,950건·77 page ACK, `ok=true`, truncated/remote/metadata failed·pending 0, checkpoint complete 1 | 완료 |
| 후속 복구 구간 | [run 29868715163](https://github.com/sung1673/activist-rss-curator/actions/runs/29868715163): `rassiro_channel` 이후 → `stockinfo7` | 20채널·93,569건·45 page ACK, `ok=true`, truncated/remote/metadata failed·pending 0, checkpoint complete 1 | 완료, 85/97 |
| 최종 복구 구간 | [run 29871166348](https://github.com/sung1673/activist-rss-curator/actions/runs/29871166348): `stockinfo7` 이후 → `YuantaKoreaTech` | 12채널·37,323건·20 page ACK, `ok=true`, truncated/remote/metadata failed·pending 0, checkpoint complete 1 | 완료, 97/97 |
| signal-only 최종화 | [run 29872608749](https://github.com/sung1673/activist-rss-curator/actions/runs/29872608749): `YuantaKoreaTech` 이후 zero-channel tail, 동일한 97채널 fingerprint | 72시간 메시지 21,317건·매치 693건 → signal 40건, 삭제 17건; authorized/finalize/window_rebuilt 1, remote failed/pending 0 | 완료 |
| 첫 safe-full | [run 29873829199](https://github.com/sung1673/activist-rss-curator/actions/runs/29873829199): `run_mode=full`, Pages/Telegram 모두 false | 14분 04초 뒤 실패; 530건 첫 ACK 전 `ReadTimeout`, pending 530, remote failed 2, metadata failed 1, sent 0; cursor·prune 전진 0, Pages·delivery 미실행 | fail-closed; legacy·Pages false 복구 |
| safe-full 재시도 | [run 29880780637](https://github.com/sung1673/activist-rss-curator/actions/runs/29880780637): migration 005와 bounded transaction 코드 배포 뒤 같은 무배포·무발송 입력 | `ok=true`, `status=complete`, 메시지 1,175건·match 32건, failed/pending 0, sent 0, Pages·delivery 미실행 | 완료 |
| 기존 Pages 복구 | [run 29882176705](https://github.com/sung1673/activist-rss-curator/actions/runs/29882176705): `page_only`, Pages 허용, Telegram false | 총 11분 26초, artifact `8515364933`, 첫 시도 배포·검증 성공 | 완료 |

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
7. 공개 화면과 rollback을 확인한 뒤에도 `ENABLE_GOVERNANCE_DELIVERY=false`를 유지한다. 배포는 web-only이며 outbound delivery는 제품 범위에서 영구 제외됐다.
8. 기존 pipeline은 90일 호환 관측 뒤에만 종료한다.
