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
$releaseId = '<사전에 고정한 고유 DeployCore release_id>'
$privateReportRoot = '<owner-only DACL로 보호된 로컬 evidence 절대경로>'
$reportPath = "$privateReportRoot\deploy-report.json"
$previousSha = 'c06b374d09e18b29a14cce46719fe4f1842f9047'
$dartDisabledEvidence = 'github-variable:DART_OFFICIAL_INGEST_ENABLED=false@<run-or-audit-id>'
$env:BSIDE_CORE_RELEASE_SHA = $releaseSha
$env:BSIDE_PRIVATE_REPORT_ROOT = $privateReportRoot
$env:BSIDE_SCHEMA_BRIDGE_DEPLOY_PREVIOUS_SHA = $previousSha
$env:BSIDE_SCHEMA_BRIDGE_DART_DISABLED_EVIDENCE = $dartDisabledEvidence
python scripts/deploy_php_sftp.py deploy `
  --local-root deploy/activist `
  --expected-sha $releaseSha `
  --confirm-production-write $releaseSha `
  --release-id $releaseId `
  --report-output $reportPath `
  --private-report-root $privateReportRoot `
  --schema-upgrade-from 11 `
  --expected-previous-sha $previousSha `
  --confirm-previous-sha $previousSha `
  --dart-disabled-evidence $dartDisabledEvidence `
  --gabia-core-compatibility-host alignpartnerscap.com `
  --remote-root /www_root/activist `
  --public-url-root https://alignpe.gabia.io/activist `
  --api-v2-base-url https://alignpe.gabia.io/activist/api.php/api/v2 `
  --rollback-health-url 'https://alignpe.gabia.io/activist/api.php?action=health' `
  --protected-token-env BSIDE_OPS_TOKEN
```

이 단계는 기존 release가 정확히 c06이고 v1·v2가 모두 `closed`임을 먼저
검증한다. 교체 뒤에는
새 health와 OpenAPI가 schema 12·exact SHA이고 공개·보호 데이터 경로가 모두
HTTP 503 `schema_version_mismatch`(`expected=12`, `actual=11`)인지 확인한다.
성공 JSON의 `deployment_smoke_mode`가
`pending_schema_upgrade_11_to_12`가 아니면 migration을 시작하지 않는다.
Gabia mutating deploy는 `--release-id`와 새 owner-only 절대
`--report-output`, `--private-report-root`를 반드시 받는다. private root는
`BSIDE_PRIVATE_REPORT_ROOT`와도 정확히 같아야 한다. report는 작업 전에
`prepared` checkpoint로 exclusive-create된다. 검증된 predecessor backup을
만든 뒤에는 `backup_ready`, 첫 파일 교체 직전에는 `commit_started`, 설치
뒤에는 `restored`, smoke 뒤에는 `verified`를 각각 원자 기록하고 디렉터리
metadata까지 동기화한다. 성공 후에만 `completed`가 된다. completed report의
`backup_ready`, `release_id`,
`backup_identity.manifest_path`, `backup_identity.manifest_sha256`,
`backup_identity.candidate_code_revision`을 하나의 복구 세트로 보존한다.
`prepared`만 남아 있으면 원격 변경 전 실패다. `backup_ready` 이후에 중단된
경우에는 report를 삭제하거나 새 release ID를 만들지 않고 아래
`schema-bridge-abort`로 동일 backup을 재사용한다.

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
PHP-first 단계에서 실행 프로세스가 살아 있고 예외 처리가 완료되면 같은 lock
안에서 자동으로 c06 파일을 복원한다. 프로세스 종료나 SFTP 단절로 순차 교체가
중단되면 자동 복원이 완료됐다고 가정하지 않고, durable `backup_ready`를 입력으로
아래 `schema-bridge-abort`를 실행한다. migration 012 적용 여부와 관계없이
release rollback은 MySQL을 변경하지 않으며 c06 PHP만 복원한다. migration 012와
schema 12 데이터는 그대로 보존한다. DB restore는 별도 catastrophe 수동 절차이지
이 release rollback의 일부가 아니다.

schema 12가 이미 운영 중인 이후의 일반 closed 배포는 bridge 인자 없이 다음과
같이 실행한다.

```powershell
$releaseId = '<사전에 고정한 고유 DeployCore release_id>'
$privateReportRoot = '<owner-only DACL로 보호된 로컬 evidence 절대경로>'
$reportPath = "$privateReportRoot\deploy-report.json"
$env:BSIDE_CORE_RELEASE_SHA = $releaseSha
$env:BSIDE_PRIVATE_REPORT_ROOT = $privateReportRoot
python scripts/deploy_php_sftp.py deploy `
  --local-root deploy/activist `
  --expected-sha $releaseSha `
  --confirm-production-write $releaseSha `
  --release-id $releaseId `
  --report-output $reportPath `
  --private-report-root $privateReportRoot `
  --remote-root /www_root/activist `
  --public-url-root https://alignpe.gabia.io/activist `
  --api-v2-base-url https://alignpe.gabia.io/activist/api.php/api/v2 `
  --rollback-health-url 'https://alignpe.gabia.io/activist/api.php?action=health' `
  --protected-token-env BSIDE_OPS_TOKEN
```

비운영 경로에서는 `--release-id`를 생략하면 SHA·UTC 시각·난수를 결합한 ID가
생성된다. Gabia 운영 변경에서는 위와 같이 ID를 사전에 고정하고 durable report를
필수로 남긴다.

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

일반 `rollback`은 embedded 이전 manifest가 schema 11 predecessor인 DeployCore
bridge backup을 의도적으로 거부한다. schema 12 DB 위에 c06 PHP만 복원하는
혼합 상태를 성공으로 오인하지 않기 위한 경계이며, 일반 rollback의 정상
`closed` 계약을 완화하지 않는다.

## one-time schema bridge PHP 복구

이 전용 명령은 현재 배포가 clean local candidate checkout·candidate manifest와
byte-for-byte 같을 때 사용한다. candidate API는 v1·v2 `closed`와 함께
`expected_schema=12, actual_schema=11`인 migration 대기 상태 또는 schema 12가
정상 적용된 상태 중 하나를 증명해야 한다. 두 경우 모두 MySQL은 읽기 전용으로
관측할 뿐 변경하지 않는다. 복원 뒤 c06 v2 health가 노출하는 schema 11은 API
contract version이며 운영 DB를 schema 11로 되돌렸다는 뜻이 아니다.

```powershell
$releaseId = '<completed deploy report의 release_id>'
$candidateSha = '<completed deploy report의 candidate code revision>'
$previousSha = 'c06b374d09e18b29a14cce46719fe4f1842f9047'
$backupManifestSha = '<completed deploy report의 backup manifest SHA-256>'
$privateReportRoot = '<owner-only DACL로 보호된 로컬 evidence 절대경로>'
$recoveryReport = "$privateReportRoot\bridge-rollback.json"
$dartDisabledEvidence = 'github-variable:DART_OFFICIAL_INGEST_ENABLED=false@<run-or-audit-id>'

$env:BSIDE_SCHEMA_BRIDGE_ROLLBACK_RELEASE_ID = $releaseId
$env:BSIDE_SCHEMA_BRIDGE_ROLLBACK_CURRENT_SHA = $candidateSha
$env:BSIDE_SCHEMA_BRIDGE_ROLLBACK_PREVIOUS_SHA = $previousSha
$env:BSIDE_SCHEMA_BRIDGE_ROLLBACK_BACKUP_SHA256 = $backupManifestSha
$env:BSIDE_SCHEMA_BRIDGE_DART_DISABLED_EVIDENCE = $dartDisabledEvidence
$env:BSIDE_PRIVATE_REPORT_ROOT = $privateReportRoot

python scripts/deploy_php_sftp.py schema-bridge-rollback `
  --local-root deploy/activist `
  --expected-sha $candidateSha `
  --release-id $releaseId `
  --expected-current-sha $candidateSha `
  --expected-previous-sha $previousSha `
  --expected-backup-manifest-sha256 $backupManifestSha `
  --confirm-rollback-release-id $releaseId `
  --confirm-rollback-current-sha $candidateSha `
  --confirm-rollback-previous-sha $previousSha `
  --confirm-backup-manifest-sha256 $backupManifestSha `
  --dart-disabled-evidence $dartDisabledEvidence `
  --report-output $recoveryReport `
  --private-report-root $privateReportRoot `
  --gabia-core-compatibility-host alignpartnerscap.com `
  --remote-root /www_root/activist `
  --public-url-root https://alignpe.gabia.io/activist `
  --api-v2-base-url https://alignpe.gabia.io/activist/api.php/api/v2 `
  --rollback-health-url 'https://alignpe.gabia.io/activist/api.php?action=health' `
  --protected-token-env BSIDE_OPS_TOKEN
```

명령은 pinned Gabia SSH identity, exact remote candidate manifest와 모든 bytes,
DeployCore release ID와 top-level backup manifest hash, embedded c06 manifest와
file hash를 lock 전과 lock 안에서 다시 검증한다. 별도 candidate emergency
snapshot과 그 hash를 durable journal의 `emergency_ready`에 기록한 뒤에만
복원을 시작한다. manifest를 마지막에 복원하고, OPcache 확인 후 exact c06,
v1·v2 `closed`, legacy health를 통과해야만 성공한다. 결과에는
`candidate_database_schema_version_before=11|12`,
`restored_api_schema_contract_version=11`, `database_mutated=false`가 별도로
기록된다. top-level backup manifest나 blob이 중간에 바뀌면 production target
복원 전에 중단한다.

### 중단된 bridge commit 재개

`backup_ready` 뒤 프로세스가 종료돼 candidate와 c06 파일이 섞였거나 c06 복원
중 manifest가 아직 없는 상태라면 일반 deploy·rollback을 다시 실행하지 않는다.
완료 또는 중단된 원본 bridge deploy journal을 그대로 입력해 다음 명령을 실행한다.

```powershell
$bridgeDeployReport = '<원본 schema-bridge-deploy journal 절대경로>'
$abortReport = "$privateReportRoot\bridge-abort.json"
$staleLockOwner = '<중단된 owner.json의 exact release_id>'
$writerAbsenceEvidence = 'github-actions:no-running-php-writers@YYYYMMDDTHHMMSSZ:owner_sha256=<64 lowercase hex>:acquired_at_sha256=<64 lowercase hex>:nonce=<32 lowercase hex>'
$env:BSIDE_SCHEMA_BRIDGE_STALE_LOCK_OWNER_RELEASE_ID = $staleLockOwner
$env:BSIDE_SCHEMA_BRIDGE_STALE_LOCK_WRITER_ABSENCE_EVIDENCE = $writerAbsenceEvidence

python scripts/deploy_php_sftp.py schema-bridge-abort `
  --local-root deploy/activist `
  --expected-sha $candidateSha `
  --release-id $releaseId `
  --expected-current-sha $candidateSha `
  --expected-previous-sha $previousSha `
  --expected-backup-manifest-sha256 $backupManifestSha `
  --confirm-rollback-release-id $releaseId `
  --confirm-rollback-current-sha $candidateSha `
  --confirm-rollback-previous-sha $previousSha `
  --confirm-backup-manifest-sha256 $backupManifestSha `
  --dart-disabled-evidence $dartDisabledEvidence `
  --bridge-deploy-report $bridgeDeployReport `
  --stale-lock-owner-release-id $staleLockOwner `
  --confirm-stale-lock-owner-release-id $staleLockOwner `
  --stale-lock-writer-absence-evidence $writerAbsenceEvidence `
  --report-output $abortReport `
  --private-report-root $privateReportRoot `
  --gabia-core-compatibility-host alignpartnerscap.com `
  --remote-root /www_root/activist `
  --public-url-root https://alignpe.gabia.io/activist `
  --api-v2-base-url https://alignpe.gabia.io/activist/api.php/api/v2 `
  --rollback-health-url 'https://alignpe.gabia.io/activist/api.php?action=health' `
  --protected-token-env BSIDE_OPS_TOKEN
```

재개 경로는 각 대상의 bytes·mode·존재 상태가 exact candidate 또는 원본 backup의
exact c06 중 하나일 때만 진행한다. restore가 manifest를 마지막에 쓰기 위해 잠시
manifest가 없는 상태도 별도로 허용한다. 제3 bytes, mode drift, symlink, 예상 밖
파일은 첫 production 변경 전에 거부한다. 이미 c06이 완전히 복원돼 있다면
검증된 no-op으로 journal을 완료한다. 혼합 상태에서는 PHP가 일관되지 않아 DB
version을 관측할 수 없으므로
`database_schema_observation=unavailable_due_partial_php`를 기록하며, 이 역시
DB 변경을 뜻하지 않는다.

강제 종료로 `_private/deployment-lock/owner.json`이 남은 owner-present
상태에는 위 stale-lock 인자를 사용한다. `owner.json`이 없는 ownerless
crash window는 아래 영문 recovery contract의 읽기 전용 inspector와 별도
remote-identity 계약을 따른다. 먼저 GitHub Actions와 다른 운영 writer가 없음을 독립적으로
확인하고 그 run·audit ID를 evidence에 기록한다. CLI 값과 환경변수 값이 모두
일치해야 하며, owner JSON의 exact bytes·mode, acquire 시각, release ID가
`php-v2-`, `schema11-bridge-rollback-`, `schema11-bridge-abort-` 중 하나인
경우에만 abort가 takeover한다. 삭제 직전 같은 owner bytes를 다시 확인하고
`stale_lock_takeover_ready`를 durable report에 먼저 기록한다. 기존 lock 제거와
새 abort lock 획득 뒤 `stale_lock_takeover_complete`와 cleanup 검증을 기록한다.
owner가 다르거나 symlink·mode drift·추가 entry가 있으면 기존 lock을 복원·보존하고
production 파일을 변경하지 않는다. stale lock이 없으면 이 세 인자와 두 환경변수는
모두 생략한다.

이 경로에는 raw `BSIDE_OPS_TOKEN`이 필요하다. 토큰의 보호된 local DPAPI recovery
escrow는 PHP/DB migration과 rollback 가능 기간에 삭제하지 않는다. 서버 token
hash를 final 값으로 바꾸는 Phase2 뒤에도 Production Alpha 7일 안정화 종료 또는
명시적인 `recovery-retire` 승인 전까지 escrow를 유지한다. 복구 명령은 PHP config,
서버 token hash 또는 DPAPI checkpoint를 생성·회전·삭제하지 않는다.

Windows의 POSIX mode `0600` 표시는 DACL을 보장하지 않는다. 따라서 private
wrapper가 report root를 상속 차단된 owner-only DACL로 먼저 만들고 검증해야 하며,
도구는 그 exact 절대경로를 CLI와 `BSIDE_PRIVATE_REPORT_ROOT`로 이중 결합한다.
POSIX에서는 private root의 group/other 권한이 하나라도 열려 있으면 중단한다.
`prepared` checkpoint는 실행 실패 근거일 뿐 성공 evidence가 아니며,
`status=completed`와 nested `report.ok=true`가 모두 있어야 한다.

## Gabia 운영 호환 경로

Gabia 운영 서버에서는 일반 SFTP exclusive-create가 빈 파일을 만든 뒤 쓰기 단계에서
실패하는 것이 확인됐다. 운영 배포와 rollback에는 다음 explicit opt-in을 추가한다.

```powershell
$releaseId = '<사전에 고정한 고유 DeployCore release_id>'
$privateReportRoot = '<owner-only DACL로 보호된 로컬 evidence 절대경로>'
$reportPath = "$privateReportRoot\deploy-report.json"
$env:BSIDE_CORE_RELEASE_SHA = $releaseSha
$env:BSIDE_PRIVATE_REPORT_ROOT = $privateReportRoot

python scripts/deploy_php_sftp.py deploy `
  --local-root deploy/activist `
  --expected-sha $releaseSha `
  --confirm-production-write $releaseSha `
  --release-id $releaseId `
  --report-output $reportPath `
  --private-report-root $privateReportRoot `
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
$privateReportRoot = '<owner-only DACL로 보호된 로컬 evidence 절대경로>'
$rollbackReport = "$privateReportRoot\rollback-report.json"
$env:BSIDE_CORE_ROLLBACK_RELEASE_ID = $rollbackReleaseId
$env:BSIDE_CORE_ROLLBACK_CURRENT_SHA = $currentReleaseSha
$env:BSIDE_PRIVATE_REPORT_ROOT = $privateReportRoot

python scripts/deploy_php_sftp.py rollback `
  --release-id $rollbackReleaseId `
  --confirm-rollback-release-id $rollbackReleaseId `
  --expected-current-sha $currentReleaseSha `
  --confirm-rollback-current-sha $currentReleaseSha `
  --report-output $rollbackReport `
  --private-report-root $privateReportRoot `
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

## Schema bridge crash-recovery contract

The one-time schema 11 to 12 PHP bridge uses the following narrow recovery
contract:

- A bridge deploy release ID must start with `php-v2-`. Admission rejects any
  other prefix before it reads or mutates the remote server, and the durable
  `schema-bridge-deploy` journal applies the same rule.
- The exact c06 predecessor manifest, all eight manifest-attested public
  artifacts, and every restored bridge artifact must have mode `0644`. Reads
  compare file type, size, and mode again after the content read. A symlink,
  permission drift, size drift, or byte drift fails closed.
- A normal `backup_ready` abort requires the exact backup-manifest SHA-256.
  Rollback never changes MySQL; schema 11 and schema 12 are both valid observed
  database states.
- A crash after lock acquisition but before `backup_ready` may use only a
  source deploy journal that is still `prepared` and contains the fixed
  candidate SHA, `php-v2-` release ID, journal nonce, exact c06 predecessor,
  and DART-off evidence. For this `prepared_no_backup` case, omit both backup
  hash CLI options and do not set
  `BSIDE_SCHEMA_BRIDGE_ROLLBACK_BACKUP_SHA256`.
- `prepared_no_backup` recovery verifies exact c06 bytes and `0644` modes,
  absence of migration 012, unchanged candidate migration 011, and closed v1
  and v2 responses before and after lock takeover. It does not change any
  public release artifact or the database. It does create and remove the
  one-time public OPcache probe used by the strict cache check. Its report
  records `public_release_files_mutated=false`,
  `ephemeral_opcache_probe_created_and_removed=true`,
  `manifest_commit_not_applicable=true`, and preserves any derived private
  stage or partial-backup path for manual inspection. A missing durable source
  deploy journal is not recoverable automatically.

Stale-lock evidence has this exact form:

```text
github-actions:no-running-php-writers@YYYYMMDDTHHMMSSZ:owner_sha256=<64 lowercase hex>:acquired_at_sha256=<64 lowercase hex>:nonce=<32 lowercase hex>
```

For an initial takeover, the evidence timestamp must be no more than 10
minutes old. The owner-present acquisition time, or the inspector-attested
ownerless remote mtime, must be at least 15 minutes old. For owner-present
locks, the hashes bind the evidence to the exact `owner.json` bytes and its
acquisition time. For ownerless locks, they bind the evidence to the
canonical remote `lstat` identity and its exact remote mtime.

An exact recorded evidence string may be older than 10 minutes only while
resuming the same owner-only durable journal from
`stale_lock_takeover_ready|complete`. Resume revalidates the nonce-bound
journal base identity, exactly one fixed backup/prebackup identity, and the
complete takeover identity. It never authorizes a new ownerless or unrelated
lock. The journal nonce is included in `journal_identity_sha256`; changing it,
including substituting another valid 32-lowercase-hex value, invalidates
prepare, update, completion, and resume admission.

For a lock directory left between `mkdir` and `owner.json`, or between owner
deletion and `rmdir`, first run the read-only inspector through the same pinned
SFTP connection:

```powershell
python scripts/deploy_php_sftp.py inspect-ownerless-lock `
  --ssh-host <host> `
  --ssh-port 22 `
  --ssh-user <user> `
  --ssh-host-key-sha256 <pinned-SHA256-fingerprint> `
  --remote-root /www_root/activist
```

The inspector performs no remote mutation. It returns the canonical
`remote_identity`, `owner_sha256`, `remote_mtime`, and
`stale_lock_first_observed_at`. Do not construct these values manually.
Use the literal owner ID `ownerless`, copy `owner_sha256` into the independent
writer-absence evidence, hash the returned
`stale_lock_first_observed_at` as `acquired_at_sha256`, and pass that exact
timestamp with `--stale-lock-first-observed-at`. The compatibility option name
is retained, but for ownerless recovery its value must equal the inspector's
attested remote mtime. Set the matching environment values:

```text
BSIDE_SCHEMA_BRIDGE_STALE_LOCK_OWNER_RELEASE_ID=ownerless
BSIDE_SCHEMA_BRIDGE_STALE_LOCK_FIRST_OBSERVED_AT=<exact UTC timestamp>
BSIDE_SCHEMA_BRIDGE_STALE_LOCK_WRITER_ABSENCE_EVIDENCE=<bound evidence>
```

Only an exact empty mode-`0700` directory is eligible. Its canonical identity
contains the lock path, mode, size, remote mtime, and the uid, gid, inode, and
device attributes supplied by SFTP. The directory identity and emptiness are
read twice, and the exact identity, emptiness, and minimum 15-minute remote
mtime age are checked again immediately before deletion. A recreated writer
lock therefore fails closed even if it appears in the final race window. An
unexpected entry, different owner, invalid mode, changed identity, or
forged/stale/too-young evidence preserves the lock and all public files.
`stale_lock_takeover_ready` is committed before removing the old lock. If the
process stops before the old ownerless lock is deleted, the remaining empty
directory cannot be distinguished from a newly created writer lock. Recovery
must start with a new journal and newly aged, freshly bound evidence. If the
old lock was deleted and the process stops immediately before, during, or
after acquiring the replacement abort lock, rerunning with the same durable
journal resumes the recorded takeover identity rather than minting a new one.
After `stale_lock_takeover_complete`, a present lock must be the exact recorded
replacement owner; a newly created ownerless or different-owner lock is
preserved and refused. During a resumed ownerless `takeover_ready`, a present
empty directory is also refused because it cannot be distinguished from a new
writer. Recovery then requires a new journal and newly aged, freshly bound
evidence.

Completed recovery evidence enforces these state pairs:

- rollback: DB `11|12`, matching `candidate_schema_11|12`, emergency snapshot
  required;
- abort `candidate`: the same DB/observation pair and emergency snapshot
  required;
- abort `mixed` or `predecessor_restore_transition`: no DB value,
  `unavailable_due_partial_php`, and no emergency snapshot;
- abort `predecessor` or `prebackup_c06`: no DB value,
  `unavailable_due_c06_contract`, and no emergency snapshot.

Cross-state reports, missing emergency identities, mismatched backup paths,
different remote roots, and substituted takeover identities cannot be marked
`completed`.

테스트는 메모리 SFTP를 사용해 remote readback, backup, manifest-last,
첫 배포 absent-file 삭제, 자동 rollback, 명시적 rollback, dry-run 무변경,
OPcache token 비노출·항상 삭제, symlink·tamper 차단을 검증한다.
