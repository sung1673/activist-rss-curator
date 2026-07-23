# 거버넌스 공개 전환과 롤백

두 workflow는 자동 실행되지 않는다. `governance-release` environment의 승인 규칙을 통과한 기본 branch 수동 실행만 운영 상태를 변경한다. cutover와 rollback은 같은 non-cancelling concurrency lock을 사용하므로 동시에 진행할 수 없다. 무인 수집·품질 작업은 별도 `governance-runtime` environment를 사용한다.

## 공개 전환 전 조건

다음을 먼저 확인한다.

- release candidate가 기본 branch에 있으며 14일 shadow 중 사용한 SHA와 같다.
- 최근 72시간 이내 같은 SHA에서 생성된 `governance-release-evidence` artifact가 있다.
- evidence에는 shadow, operations, performance, benchmark, usability, release approval 여섯 파일이 모두 있다.
- usability는 기관·고액자산가·해외기관 각각 정확히 5명, 총 15명이며 12명 이상이 180초 안에 네 항목을 확인했다.
- 법률·편집·제품 승인이 모두 `approved`이고 각 보관 문서 SHA-256이 등록됐다.
- 같은 SHA의 `daily.yml` 성공 run에 `pages-<run_id>-<attempt>` governance artifact가 있다.
- 서버 release state는 승인된 `preview`다.
- 승인된 로컬 운영자가 dispatch 직전에 `gh variable set PAGES_OWNER --body governance --repo sung1673/activist-rss-curator`를 실행하고 `gh variable get PAGES_OWNER --repo sung1673/activist-rss-curator` 결과가 `governance`인지 확인했다.
- dispatch 시점에는 `GOVERNANCE_PIPELINE_MODE=shadow`다. cutover가 성공하고 익명 public smoke가 통과한 뒤 승인된 로컬 운영자가 `live`로 변경한다.
- 전환 adapter boolean `ENABLE_PAGES`, `ENABLE_GOVERNANCE_PAGES`, `ENABLE_GOVERNANCE_SHADOW`는 모두 `false`다. 기존 legacy artifact는 boolean을 끈 뒤에도 그대로 서비스된다.
- `ENABLE_TELEGRAM_DELIVERY=false`, `ENABLE_GOVERNANCE_DELIVERY=false`다.
- 보호된 release 설정에 `BSIDE_ADMIN_TOKEN`, `GOVERNANCE_PREVIEW_TOKEN`이 있고 `GOVERNANCE_API_BASE_URL`이 실제 Gabia API base를 가리킨다.

`Governance protected cutover`에는 evidence run ID/name, governance Pages run ID/name, 8자 이상의 사유를 입력한다. workflow는 source run의 성공 여부, 기본 branch, exact SHA, 예상 workflow, artifact의 GitHub SHA-256과 만료 여부를 검증한다. 다운로드는 digest mismatch를 오류로 처리한다.

검증된 artifact만 Pages에 올린 뒤 `/`, `/governance/`, `/feed.xml`, API health, preview events, JSON export를 확인한다. 그 다음 optimistic state version으로 `preview → live`를 수행하고 인증 헤더 없이 events·Atom·JSON·CSV export를 다시 검사한다. workflow는 dispatch 시점의 `PAGES_OWNER=governance`와 `GOVERNANCE_PIPELINE_MODE=shadow` snapshot을 검증하지만 repository variable을 직접 변경하지 않는다.

GitHub의 repository variable 수정 API는 별도 `Variables: write` 권한을 요구한다. 장기 PAT나 계획 밖의 광범위한 token을 workflow에 추가하지 않기 위해 소유권 변경은 이미 인증된 로컬 `gh` 세션을 가진 승인 운영자에게 둔다. workflow가 `PAGES_OWNER=governance`를 관측하지 못하면 artifact 배포 전에 fail-closed한다. 배포 또는 smoke가 live 전 실패하면 preview도 closed로 내리며, 운영자는 즉시 로컬 명령으로 `PAGES_OWNER=legacy`를 복구한다.

## 수동 repository variable 인계

workflow는 아래 명령을 실행하지 않고 각 성공·자동복구·rollback summary에 그대로 출력한다. 승인 운영자는 현재 인증된 로컬 `gh` 세션에서만 복사해 실행한다. `gh auth status`와 뒤따르는 `test` 검증이 모두 성공하기 전에는 완료로 기록하지 않는다.

cutover 성공 후에는 다음을 실행한다. 이 명령 전에는 `PAGES_OWNER=governance`, `GOVERNANCE_PIPELINE_MODE=shadow`이므로 legacy 예약 배포는 owner 불일치로, governance 예약 배포는 mode 불일치로 각각 fail-closed한다.

```bash
gh auth status
gh variable set GOVERNANCE_PIPELINE_MODE --body live --repo sung1673/activist-rss-curator
test "$(gh variable get GOVERNANCE_PIPELINE_MODE --repo sung1673/activist-rss-curator)" = "live"
test "$(gh variable get PAGES_OWNER --repo sung1673/activist-rss-curator)" = "governance"
```

cutover 자동복구 또는 명시적 rollback 성공 후에는 다음을 실행한다. 명령 전에는 legacy artifact가 복원되고 API는 `closed`지만 선언값이 `PAGES_OWNER=governance`, `GOVERNANCE_PIPELINE_MODE=shadow`이므로 두 예약 배포가 모두 fail-closed한다. mode를 먼저 `shadow`로 확정하고 owner를 마지막에 `legacy`로 넘긴다.

```bash
gh auth status
gh variable set GOVERNANCE_PIPELINE_MODE --body shadow --repo sung1673/activist-rss-curator
gh variable set PAGES_OWNER --body legacy --repo sung1673/activist-rss-curator
test "$(gh variable get GOVERNANCE_PIPELINE_MODE --repo sung1673/activist-rss-curator)" = "shadow"
test "$(gh variable get PAGES_OWNER --repo sung1673/activist-rss-curator)" = "legacy"
```

워크플로 summary 자체가 해당 run과 승인 environment에 귀속되는 영구 안내이므로 별도 GitHub issue를 자동 생성하지 않는다. 수동 명령 실패는 variable 미인계 상태로 취급하고 summary의 명령을 다시 검증하며, workflow token 권한이나 PAT를 추가해 우회하지 않는다.

## 사전 고정 legacy 롤백 artifact

정상 legacy 배포를 확인한 즉시 아래 repository variable 네 개를 기록한다.

- `LEGACY_ROLLBACK_RUN_ID`: 성공한 `build-feed.yml` run ID
- `LEGACY_ROLLBACK_ARTIFACT_NAME`: 예: `legacy-pages-archive-seed`
- `LEGACY_ROLLBACK_CODE_REVISION`: 해당 run의 full 40자리 SHA
- `LEGACY_ROLLBACK_ARTIFACT_DIGEST`: GitHub artifact의 `sha256:<64 hex>`

artifact가 만료되기 전에 새 정상 legacy artifact로 이 네 값을 함께 교체한다. run ID나 이름만 고정하고 digest를 생략할 수 없다.

### 90일 피드 호환성 계약

`daily.yml`은 현재 작업 디렉터리나 최신 artifact를 임의로 사용하지 않는다. 위 네 변수로 지정된 artifact의 source run이 성공한 기본 branch의 정확한 `.github/workflows/build-feed.yml`인지, run SHA가 `LEGACY_ROLLBACK_CODE_REVISION`과 같은지, 이름이 일치하는 만료되지 않은 artifact가 정확히 하나인지, GitHub가 기록한 digest가 pin과 같은지를 먼저 확인한다. 그 뒤 artifact ZIP 원본을 내려받아 ZIP 자체의 SHA-256을 다시 계산한다.

검증기는 경로 탈출, 절대 경로, 역슬래시 경로, 대소문자 충돌을 포함한 중복 경로, 중복 날짜, 심볼릭 링크·특수 파일, 암호화 entry와 크기 예산 초과를 거부한다. root의 `feed.xml`과 artifact에서 가장 최근 날짜까지 이어지는 정확히 90개의 `feed/YYYY-MM-DD.html`만 호환 자산으로 복사한다. 기존 날짜 파일을 만들거나 빈 날짜를 합성하지 않는다. 연속 90일이 실제로 없으면 daily artifact 생성과 cutover가 모두 fail-closed하며, legacy 수집을 유지해 완전한 새 `legacy-pages-archive-seed`를 만든 뒤 네 pin을 함께 갱신해야 한다.

생성된 governance artifact에는 `legacy-feed-compatibility.json`이 포함된다. 이 manifest는 legacy run·artifact ID·이름·코드 SHA·artifact digest, 90일 시작일·종료일·개수, `feed.xml`과 각 날짜 파일의 byte 수·SHA-256을 기록한다. 전환 workflow는 이 manifest의 출처가 현재 네 pin과 일치하는지, 날짜 파일이 정확히 90개이며 연속인지, 파일 해시가 일치하는지 다시 확인한다. 또한 `/index.html`과 `/governance/index.html`이 동일한지 확인해 legacy 호환 자산이 governance root를 덮어쓰지 않았음을 검증한다.

현재 확인된 seed의 연속 범위는 `2026-05-01`부터 `2026-07-20`까지 81일이므로 이 계약을 통과하지 못한다. `2026-05-01`부터 최소 `2026-07-29`까지 실제 생성된 90일 연속 페이지를 포함하는 새 seed가 필요하다. 90일 미만 차단은 코드 오류가 아니라 의도된 출시 차단이며, 완전한 seed가 준비되기 전에는 검사를 우회하거나 과거 페이지를 합성하지 않는다.

## 롤백 순서

`Governance protected rollback`에서 확인 문자열 `ROLLBACK`과 8자 이상의 사유를 입력한다.

1. 사전 고정 legacy artifact의 run, workflow, branch, code revision, digest를 확인한다.
2. rolling recovery bundle을 다시 검증한 뒤 FIFO Pages 배포 잠금을 획득한다. 잠금을 기다리는 동안 공개 API는 기존 상태를 유지한다.
3. 잠금 안에서 현재 state version을 읽어 governance release state를 `closed`로 변경하고, 즉시 검증된 legacy artifact를 GitHub Pages에 배포한다.
4. legacy 루트·RSS·legacy API가 정상이고 governance data API가 503 closed인지 확인한다.
5. 승인된 로컬 운영자가 `GOVERNANCE_PIPELINE_MODE=shadow`로 내리되 `PAGES_OWNER=governance`는 유지한 채 rollback을 dispatch한다. workflow가 stale Pages producer를 취소하고 bundle을 검증한 다음, Pages 잠금 안에서 API를 `closed`로 내리고 legacy artifact를 배포·검증한 뒤에만 로컬에서 `PAGES_OWNER=legacy`로 바꾼다.
6. 전환 중 새 producer는 frozen owner/mode에 따라 non-deploy concurrency group으로 격리되고, 실제 배포 경계에서도 owner와 server state를 다시 확인한다. 따라서 pending rollback을 대체하거나 복구 artifact를 덮어쓸 수 없다.

롤백은 MySQL row, 신규 거버넌스 데이터, `SourceRight` 만료·철회 이력, 편집 감사 로그를 삭제하거나 되감지 않는다. `PAGES_OWNER`와 `GOVERNANCE_PIPELINE_MODE`도 workflow가 변경하지 않는다. legacy 배포 성공 직후 승인된 로컬 운영자가 위 명령으로 mode를 `shadow`로 확인하고 `PAGES_OWNER=legacy`로 변경하며, 두 검증 명령이 성공하기 전까지 두 예약 Pages producer는 모두 차단된다.

## 권한 경계

- `github-pages` environment는 검증된 정적 artifact 배포에만 사용한다.
- `governance-release` environment는 evidence 승인과 release-state 변경을 보호하고 required reviewer를 둔다.
- `governance-runtime` environment는 무인 수집·품질 증빙만 담당하며 release 전환 권한을 갖지 않는다.
- artifact 조회는 `actions:read`, Pages 배포는 `pages:write`와 `id-token:write`만 사용한다. workflow에는 repository variable write 권한이나 별도 PAT를 부여하지 않는다.
- 어떤 단계도 DB delete, truncate, schema rollback, SourceRight 복구를 실행하지 않는다.
