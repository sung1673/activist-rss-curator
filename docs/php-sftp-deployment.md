# Gabia PHP SFTP 배포·롤백

`scripts/deploy_php_sftp.py`는 최종 Git SHA의 PHP API 묶음을 pinned SFTP로
배포한다. FTP와 trust-on-first-use는 사용하지 않는다. 이 실행기는 DB 백업이나
migration 실행을 대신하지 않는다. 운영 DB 백업, migration 001~012 검증·적용,
release state `closed` 확인이 먼저 완료돼야 한다.

기존 v2 manifest가 있는 재배포에서는 실행기가 현재 manifest·파일 hash·API SHA와
보호된 release state `closed`를 배포 시작 전과 첫 commit 직전에 다시 검증한다.
최초 v2 배포에는 검증할 기존 v2 보호 경로가 없으므로, 이 시점의 `closed` 보장은
사전에 완료한 migration 011·012 및 운영 DB release-state 검증에 의존한다. 신규 파일
설치 후에는 동일한 보호 API를 통해 다시 `closed`를 검증하며 실패하면 자동
rollback한다.

실제 운영 연결은 승인된 배포 창에서만 실행한다. `plan`은 로컬 전용이고,
`deploy --dry-run`과 `rollback --dry-run`은 pinned SFTP에 연결해 읽기만 하며
원격 파일·디렉터리·lock을 만들지 않는다.

## 배포 대상과 commit 순서

deployment manifest가 검증하는 핵심 파일은 정확히 다음 9개다.

1. `.htaccess`
2. `api.php`
3. `governance_v1.php`
4. `governance_v2.php`
5. `governance_v2_write.php`
6. `openapi.yaml`
7. `openapi-v2.yaml`
8. `migrations/011_global_terminal_v2.sql`
9. `migrations/012_dart_credential_pool.sql`

실제 전송에는 위 9개와 생성된 `deployment-manifest.json`이 포함된다. 설치는
의존성을 먼저 두고 다음 순서로 수행한다.

```text
.htaccess
→ migrations/011_global_terminal_v2.sql
→ migrations/012_dart_credential_pool.sql
→ openapi-v2.yaml
→ openapi.yaml
→ governance_v2_write.php
→ governance_v2.php
→ governance_v1.php
→ api.php
→ deployment-manifest.json
```

`api.php`는 마지막 실행 의존성이고 `deployment-manifest.json`은 전체 transaction의
마지막 commit marker다. 각 교체는 SFTP `posix-rename@openssh.com`을 사용한다.
서버가 원자 덮어쓰기를 지원하지 않으면 실제 파일을 바꾸기 전에 중단한다.

## 필요한 로컬 환경변수

비밀번호는 명령행 인자로 받지 않는다. 다음 값을 승인된 로컬 환경에만 둔다.

```text
SSH_HOST
SSH_PORT
SSH_USER
SSH_PASSWORD
SSH_HOST_KEY_SHA256
BSIDE_OPS_TOKEN
```

fingerprint는 독립적으로 확인한 `SHA256:...` 값을 정확히 사용한다. 변경되거나
누락된 host key는 비밀번호 인증 전에 차단된다.

일반 연결에서는 `ssh-rsa/SHA-1` host signature를 명시적으로 비활성화한다.
해당 구형 알고리즘이 필요한 단 하나의 검증된 서버에만 다음 두 값을 함께
설정한다.

```text
SSH_ALLOW_LEGACY_RSA_SHA1=true
SSH_LEGACY_RSA_SHA1_HOST=<SSH_HOST와 정확히 같은 host>
```

opt-in만 있거나 대상 host가 다르면 연결 전에 실패한다. 이 예외는 Paramiko 5의
해당 transport 인스턴스에만 적용되며 pinned SHA-256 fingerprint 검증은 그대로
수행한다.

## 1. 최종 묶음 생성과 로컬 plan

최종 병합된 깨끗한 `main` checkout에서 실행한다.

```powershell
$releaseSha = (git rev-parse HEAD).Trim()
python -m curator.deployment_manifest `
  --root deploy/activist `
  --code-revision $releaseSha `
  --output deploy/activist/deployment-manifest.json

python scripts/deploy_php_sftp.py plan `
  --local-root deploy/activist `
  --expected-sha $releaseSha
```

최초 schema 11→12 전환에서는 migration 012를 PHP보다 먼저 적용하지 않는다.
구 PHP는 일 한도가 10,000이라고 가정하므로 DB만 먼저 40,000으로 바꾸면 새 PHP
배포 실패 후 파일 롤백이 불가능해진다. 아래의 명시적 pending 전환 순서를
사용한다. schema 12가 이미 검증된 이후의 일반 배포에는 이 예외가 없다.

`plan`은 다음을 검증한다.

- SHA가 현재 Git `HEAD`와 정확히 일치
- 배포 대상 tracked file에 staged·unstaged 변경이 없음
- symlink가 아닌 정규 파일
- manifest의 9개 파일 집합과 각 SHA-256
- `api.php`와 manifest-last 순서

JSON 출력에는 파일 경로·크기·hash·mode만 있고 비밀번호나 token은 없다.

## 2. 원격 read-only dry-run

```powershell
python scripts/deploy_php_sftp.py deploy `
  --dry-run `
  --local-root deploy/activist `
  --expected-sha $releaseSha `
  --remote-root /www_root/activist
```

dry-run은 SFTP host key와 인증을 검증한 뒤 현재 각 대상이
`create|replace|unchanged`인지, 현재 mode와 hash가 무엇인지 출력한다. 원격
stage, backup, lock, OPcache probe를 만들지 않는다.

## 3. schema 11→12 최초 전환과 closed 배포

먼저 `DART_OFFICIAL_INGEST_ENABLED=false`를 설정하고 `ingest-official`과
`official-backfill`의 queued·running 실행이 모두 0건인지 확인한다. 기존
Pages와 레거시 수집은 계속 제공할 수 있지만 이 게이트는 migration과 smoke가
끝날 때까지 다시 열지 않는다.

새 PHP bundle을 schema 11 DB 위에 먼저 배포할 때만 다음 one-time bridge를
사용한다.

```powershell
python scripts/deploy_php_sftp.py deploy `
  --local-root deploy/activist `
  --expected-sha $releaseSha `
  --confirm-production-write $releaseSha `
  --schema-upgrade-from 11 `
  --remote-root /www_root/activist `
  --public-url-root https://alignpe.gabia.io/activist `
  --api-v2-base-url https://alignpe.gabia.io/activist/api.php/api/v2 `
  --rollback-health-url 'https://alignpe.gabia.io/activist/api.php?action=health' `
  --protected-token-env BSIDE_OPS_TOKEN
```

이 단계는 기존 release가 schema 11·`closed`임을 먼저 검증한다. 교체 뒤에는
새 health와 OpenAPI가 schema 12·exact SHA이고 공개·보호 데이터 경로가 모두
HTTP 503 `schema_version_mismatch`(`expected=12`, `actual=11`)인지 확인한다.
성공 JSON의 `deployment_smoke_mode`가
`pending_schema_upgrade_11_to_12`가 아니면 migration을 시작하지 않는다.

그 다음에만 exact source bytes로 migration 012를 apply·replay한다.

```powershell
python scripts/apply_migration_012.py `
  --migration deploy/activist/migrations/012_dart_credential_pool.sql
```

도구는 같은 MySQL 세션에서 `@bside_migration_012_sha256`을 설정하고 SQL 원본
bytes를 적용한다. version 12 checksum과 manifest·서버 파일 hash가 모두
일치해야 한다. 적용 직후 strict closed smoke를 실행한다.

```powershell
python .github/scripts/smoke-global-v2.py `
  --base-url https://alignpe.gabia.io/activist/api.php/api/v2 `
  --expected-sha $releaseSha `
  --release-state closed `
  --privileged-token-env BSIDE_OPS_TOKEN
```

strict smoke까지 성공한 뒤에만 `DART_OFFICIAL_INGEST_ENABLED=true`로 복원한다.
PHP-first 단계가 실패하면 DB는 여전히 schema 11이므로 자동 파일 rollback이
안전하다. migration 012가 적용된 뒤에는 구 PHP 파일만 단독 복원하지 않는다.
구 release로 돌아가야 한다면 DART writer가 정지된 상태에서 pre-migration DB
backup을 먼저 복원한 뒤 해당 PHP bundle을 복원한다.

schema 12가 이미 운영 중인 이후의 일반 closed 배포는 bridge 인자 없이 다음과
같이 실행한다.

```powershell
python scripts/deploy_php_sftp.py deploy `
  --local-root deploy/activist `
  --expected-sha $releaseSha `
  --confirm-production-write $releaseSha `
  --remote-root /www_root/activist `
  --public-url-root https://alignpe.gabia.io/activist `
  --api-v2-base-url https://alignpe.gabia.io/activist/api.php/api/v2 `
  --rollback-health-url 'https://alignpe.gabia.io/activist/api.php?action=health' `
  --protected-token-env BSIDE_OPS_TOKEN
```

필요할 때만 `--release-id`를 지정한다. 지정하지 않으면 SHA·UTC 시각·난수를
결합한 ID가 생성된다. 성공 JSON의 `release_id`와 `backup_manifest`를 보호된
배포 기록에 저장한다.

실행기는 다음 transaction을 강제한다.

1. `/www_root/activist/_private/.htaccess`가 exact deny-all 정책인지 확인한다.
   없으면 웹서버가 읽을 수 있는 mode `0644`로 exclusive-create하며, 다른 내용이나
   symlink이면 덮어쓰지 않고 중단한다.
2. 같은 random sentinel로 document root의 mode `0644` public canary가 정확히
   HTTP 200과 동일 bytes를 반환하는지 먼저 확인해 URL↔document-root 매핑을
   증명한다. 이어 `_private`의 mode `0600` canary가 redirect 없이 HTTP 403/404이고
   sentinel bytes를 노출하지 않는지 확인한 뒤 두 canary를 모두 즉시 삭제한다.
3. `/www_root/activist/_private/deployment-lock`을 원자 생성한다.
4. 고유 stage를 mode `0700`으로 만들고 deny-all `.htaccess`를 먼저 쓴다.
5. 후보 파일은 `.php`가 아닌 고유 `.blob` 이름과 mode `0600`으로 올린다.
6. 모든 blob을 다시 읽어 로컬 SHA-256과 비교한다.
7. POSIX rename 덮어쓰기를 두 개의 임시 blob으로 사전 검증한다.
8. 현재 대상 각각의 존재 여부, 실제 bytes, mode, 크기, SHA-256을 비공개
   backup에 보존하고 backup blob도 다시 읽어 검증한다.
9. 첫 commit 직전에 모든 대상의 존재·bytes·mode·크기·hash가 backup 당시와
   여전히 같은지 재검증해 비협조 writer의 동시 변경을 차단한다.
10. 기존 v2 재배포이면 현재 exact SHA와 protected `closed`를 다시 확인하고
    대상 snapshot을 한 번 더 확인한다.
11. 위 commit 순서로 원자 교체하고, 매 파일을 다시 읽어 설치 hash를 확인한다.
12. manifest를 마지막에 설치한다.
13. 일회성 OPcache probe로 cache reset을 검증한다.
14. v2 health·OpenAPI·404·admin 인증·closed events 응답을 확인한다.
15. stage와 lock을 제거한다. backup은 rollback을 위해 보존한다.

API smoke는 다음을 모두 요구한다.

- `/health`: HTTP 200, `bside-global-market-terminal`, schema 12, exact SHA
- `/openapi.yaml`: YAML, v2 header, schema 12
- 존재하지 않는 v2 경로: HTTP 404 `not_found`
- `/events`: HTTP 503 `global_terminal_release_closed`
- 인증 없는 `/admin/release-state`: HTTP 401 `bearer_token_required`
- 유효한 Bearer를 전달한 `/ops/release-state`: HTTP 200,
  `release_state=closed`

`BSIDE_OPS_TOKEN` 원문은 로컬 환경변수에만 두고
`--protected-token-env BSIDE_OPS_TOKEN`처럼 환경변수 이름만 인수로 전달한다.
유효한 보호 경로가 200을 반환하지 않으면 `.htaccess`의 Authorization 전달
실패까지 포함해 배포를 실패 처리한다. `public-url-root`, `api-v2-base-url`,
`rollback-health-url`은 정확히 같은 scheme·host·port를 사용하고, API와
rollback 경로는 public root의 path subtree 안에 있어야 한다. HTTP redirect는
따라가지 않는다.

## OPcache 일회성 probe

서버에 PHP CLI가 없으므로 실행기는 다음 방식만 사용한다.

- 32바이트 무작위 ID의 `.bside-opcache-<64 hex>.php` 경로
- 48바이트 이상의 무작위 token을 POST header로만 전송
- PHP 파일에는 token 원문이 아니라 SHA-256만 포함
- POST와 `hash_equals` 검증 후에만 `opcache_reset()` 실행
- HTTP 200과 exact `probe_id`, `opcache_reset=true`를 검증
- 성공·HTTP 실패·예외 모두 `finally`에서 probe 삭제
- SFTP `lstat`으로 삭제 완료를 다시 확인

token은 URL, JSON 출력, backup, 저장소에 남지 않는다. reset 또는 probe 삭제를
증명하지 못하면 배포 성공으로 처리하지 않는다.

## 자동 rollback

파일 교체가 한 건이라도 시작된 뒤 OPcache 또는 closed smoke가 실패하면 실행기는
즉시 같은 lock 안에서 기존 backup을 복원한다.

- 현재 manifest를 먼저 제거해 혼합 배포가 200으로 보이지 않게 한다.
- 이전에도 v2가 있었다면 의존성→`api.php`→이전 manifest 순서로 복원한다.
- 최초 v2 배포였다면 이전 `api.php`를 먼저 복원해 새 v2 require를 끊는다.
- backup에 `existed=false`였던 v2 파일·manifest는 복원하지 않고 삭제한다.
- 기존 파일은 정확한 bytes와 mode로 복원한다.
- 새 일회성 probe로 OPcache를 다시 reset한다.
- legacy rollback health가 HTTP 200·`ok=true`인지 확인한다.

자동 rollback까지 성공해도 deploy 명령은 0이 아닌 종료값을 반환한다. 자동
rollback이 완결되지 않으면 별도의 심각 오류로 종료하고 비공개 backup과 stage를
보존해 수동 복구 근거로 사용한다.

## 명시적 rollback

먼저 backup을 읽기 전용으로 검증한다.

```powershell
python scripts/deploy_php_sftp.py rollback `
  --dry-run `
  --release-id <성공 배포에서 기록한 release_id> `
  --remote-root /www_root/activist
```

실제 rollback은 다음과 같다.

```powershell
python scripts/deploy_php_sftp.py rollback `
  --release-id <성공 배포에서 기록한 release_id> `
  --remote-root /www_root/activist `
  --public-url-root https://alignpe.gabia.io/activist `
  --api-v2-base-url https://alignpe.gabia.io/activist/api.php/api/v2 `
  --rollback-health-url 'https://alignpe.gabia.io/activist/api.php?action=health' `
  --protected-token-env BSIDE_OPS_TOKEN
```

rollback을 시작하기 직전의 현재 파일도 별도 `pre-rollback-*` emergency backup에
보존한다. 보호 API가 여전히 `closed`인지 확인하고 현재 대상이 emergency snapshot과
정확히 같은지도 다시 확인한 뒤에만 복원을 시작한다. rollback 도중 실패하면 이
emergency backup을 별도 recovery workspace에서 자동 복원한다. DB,
SourceRight, 감사·정정 데이터는 삭제하거나 되돌리지 않는다.

## Gabia 운영 호환 경로

Gabia 운영 서버에서는 일반 SFTP exclusive-create가 빈 파일을 만든 뒤 쓰기 단계에서
실패하는 것이 확인됐다. 운영 배포와 rollback에는 다음 explicit opt-in을 추가한다.

```powershell
$env:BSIDE_CORE_RELEASE_SHA = $releaseSha

python scripts/deploy_php_sftp.py deploy `
  --local-root deploy/activist `
  --expected-sha $releaseSha `
  --confirm-production-write $releaseSha `
  --gabia-core-compatibility-host alignpartnerscap.com `
  --remote-root /www_root/activist `
  --public-url-root https://alignpe.gabia.io/activist `
  --api-v2-base-url https://alignpe.gabia.io/activist/api.php/api/v2 `
  --rollback-health-url 'https://alignpe.gabia.io/activist/api.php?action=health' `
  --protected-token-env BSIDE_OPS_TOKEN
```

이 경로는 SSH host, pinned fingerprint, legacy RSA 예외 host, document root,
public/API/rollback URL이 코드에 고정된 운영값과 모두 일치할 때만 활성화된다.
또한 배포 checkout 전체가 clean이고 Git HEAD, manifest, `--expected-sha`,
`--confirm-production-write`, `BSIDE_CORE_RELEASE_SHA`가 모두 같은 40자리 SHA여야
한다. SHA는 코드에 하드코딩하지 않는다.

활성화 전에 실제 서버에서 exclusive-create 실패 형태, mode `0700` private claim,
write/readback, target-absent standard rename, 기존 target no-replace를 매번 다시
검증한다. 기존 `_private/.htaccess`는 변경하지 않고 byte와 mode를 배포 전후에
확인한다. Gabia의 private 차단 응답은 HTTP 403/404 또는 exact HTTP 302
`Location: http://errdoc.gabia.io/403.html`만 허용한다.

OPcache probe는 extension·함수·INI·status 타입을 검증한다. OPcache가 실제로
비활성 상태이면 `disabled_verified`, 활성 상태에서 reset이 정확히 성공한 경우에만
`reset_verified`를 기록한다. 이 호환 경로는 config 또는 token을 생성·회전하지
않으며 기존 core `deployment-lock` 하나만 사용한다.

실제 Gabia rollback은 복원할 backup ID와 현재 배포 SHA를 각각 명령행과 환경
변수로 이중 확인한다.

```powershell
$rollbackReleaseId = '<verified-backup-release-id>'
$currentReleaseSha = '<current-40-character-release-sha>'
$env:BSIDE_CORE_ROLLBACK_RELEASE_ID = $rollbackReleaseId
$env:BSIDE_CORE_ROLLBACK_CURRENT_SHA = $currentReleaseSha

python scripts/deploy_php_sftp.py rollback `
  --release-id $rollbackReleaseId `
  --confirm-rollback-release-id $rollbackReleaseId `
  --expected-current-sha $currentReleaseSha `
  --confirm-rollback-current-sha $currentReleaseSha `
  --gabia-core-compatibility-host alignpartnerscap.com `
  --remote-root /www_root/activist `
  --public-url-root https://alignpe.gabia.io/activist `
  --api-v2-base-url https://alignpe.gabia.io/activist/api.php/api/v2 `
  --rollback-health-url 'https://alignpe.gabia.io/activist/api.php?action=health' `
  --protected-token-env BSIDE_OPS_TOKEN
```

확인값 불일치는 SSH 연결과 원격 capability probe 전에 중단한다. 확인값이 맞아도
원격 manifest와 core 파일에서 검증한 현재 SHA가 다르면 probe와 rollback을 시작하지
않는다. deploy와 rollback은 `finally`에서 기존 `_private/.htaccess`의 exact bytes와
mode를 다시 검사하며 drift가 있으면 core 작업 자체가 성공했더라도 성공을 반환하지
않는다. dry-run은 원격 capability probe를 만들지 않으므로 Gabia opt-in과 함께
실행할 수 없다.

## 실패 후 운영 확인

- stale `deployment-lock`을 자동으로 깨지 않는다. 실행 중인 배포가 없음을 별도
  확인하고 `owner.json`의 release ID·시각을 감사한 후 사람 승인으로 처리한다.
- backup manifest가 없는 부분 backup은 rollback 대상으로 사용하지 않는다.
- backup blob의 크기·SHA-256이 다르면 복원을 시작하지 않는다.
- `.htaccess`, SQL, PHP, OpenAPI, manifest의 공개 접근 차단을 별도 HTTP smoke로
  확인한다.
- 실제 배포 뒤에도 기존 `global-alpha-api-deployment-smoke`를 실행해 유효한
  보호 token의 Authorization 전달과 운영 경계를 추가 확인한다.

## 로컬 검증

실제 서버에 연결하지 않는 단위 테스트:

```powershell
python -m pytest tests/test_php_sftp_deploy.py -q
ruff check curator/php_sftp_deploy.py scripts/deploy_php_sftp.py tests/test_php_sftp_deploy.py
mypy curator/php_sftp_deploy.py
```

테스트는 메모리 SFTP를 사용해 remote readback, backup, manifest-last,
첫 배포 absent-file 삭제, 자동 rollback, 명시적 rollback, dry-run 무변경,
OPcache token 비노출·항상 삭제, symlink·tamper 차단을 검증한다.
