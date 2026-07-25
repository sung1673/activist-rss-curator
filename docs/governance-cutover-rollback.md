# Production Alpha 공개 전환과 롤백

두 workflow는 자동 실행되지 않는다. `governance-release` environment의 승인 규칙을 통과한 기본 branch 수동 실행만 운영 상태를 변경한다. cutover와 rollback은 같은 non-cancelling concurrency lock을 사용하므로 동시에 진행할 수 없다. 무인 수집·품질 작업은 별도 `governance-runtime` environment를 사용한다.

## 공개 전환 전 조건

다음을 먼저 확인한다.

- release candidate가 기본 branch에 있으며 30일 수집·24시간 watchdog·사람 검수에 사용한 40자리 SHA와 같다.
- 최근 48시간 이내 같은 SHA에서 생성된 `global-alpha-release-evidence` artifact가 있고, 전환 시점의 run 생성·`evidence_as_of`·관측 종료 시각은 모두 60분 이내다.
- evidence에는 DB에서 다시 계산한 4개 공식 connector의 30일 수집 영수증, 최소 287개 watchdog 관측, 실제 사건 60건·동일 사건 후보 120쌍·Top 5 사람 검수, 화면·성능·롤백 훈련과 감독자·SourceRight 승인이 있다.
- `KR/US/JP=market-wide`, `GB=official-register`, `CA/AU=link-only` 범위가 화면·API·증빙에서 일치한다. 캐나다 SEDAR+와 호주 ASX 전문을 수집하거나 저장하지 않는다.
- 같은 SHA의 `daily.yml` 성공 run에 `pages-<run_id>-<attempt>` governance artifact가 있고, 그 exact run·attempt·artifact ID·이름·GitHub digest·전체 사이트 content digest가 Alpha evidence의 `pages-artifact-identity.json`에 고정돼 있다.
- v1과 v2 서버 release state가 모두 승인된 `preview`이며 state version이 증빙 이후 바뀌지 않았다.
- 승인된 로컬 운영자가 dispatch 직전에 `gh variable set PAGES_OWNER --body governance --repo sung1673/activist-rss-curator`를 실행하고 `gh variable get PAGES_OWNER --repo sung1673/activist-rss-curator` 결과가 `governance`인지 확인했다.
- dispatch 시점에는 `GOVERNANCE_PIPELINE_MODE=shadow`다. cutover가 성공하고 익명 public smoke가 통과한 뒤 승인된 로컬 운영자가 `live`로 변경한다.
- 전환 adapter boolean `ENABLE_PAGES`, `ENABLE_GOVERNANCE_PAGES`, `ENABLE_GOVERNANCE_SHADOW`는 모두 `false`다. 기존 legacy artifact는 boolean을 끈 뒤에도 그대로 서비스된다.
- `ENABLE_TELEGRAM_DELIVERY=false`, `ENABLE_GOVERNANCE_DELIVERY=false`다.
- 보호된 release 설정에 `BSIDE_ADMIN_TOKEN`, `BSIDE_RELEASE_AUTHORIZER_TOKEN`, `GOVERNANCE_PREVIEW_TOKEN`이 있고 `GOVERNANCE_API_BASE_URL`이 실제 Gabia API base를 가리킨다.
- `BSIDE_RELEASE_AUTHORIZER_TOKEN`은 reviewer가 보호하는 `governance-release` environment에만 있고, PHP에는 평문이 아닌 정확한 `release_authorizer` 역할의 SHA-256으로 등록되어 있다. 일반 admin token은 승인 발급에 사용할 수 없다.

`Governance protected cutover`에는 evidence run ID/name과 8자 이상의 사유만 입력한다. 별도 governance Pages run ID/name 입력은 받지 않는다. workflow는 evidence에 고정된 daily run·attempt·artifact ID·이름·GitHub SHA-256만 다시 조회해 다운로드하며, 같은 SHA의 다른 artifact도 거절한다. 다운로드 뒤 root와 `/governance`의 `index.html`, `config.js`, `app.js`, `styles.css`가 서로 byte-identical인지, 해당 terminal content identity가 24시간 watchdog 전 관측과 같은지, 전체 사이트 content digest가 evidence와 같은지 재계산한다. 하나라도 다르거나 digest mismatch가 발생하면 배포 전 fail-closed한다.

검증된 artifact만 Pages에 올리기 전에 preview `/sources/status`에서 정확한 필수 6개 connector가 모두 `public_ready=true`인지 확인한다. 배포 뒤 `/`, `/governance/`, `/feed.xml`, API health, preview events, JSON export를 확인한다. 그 다음 보호 환경이 새 32바이트 nonce를 만들고 `BSIDE_RELEASE_AUTHORIZER_TOKEN`으로 candidate SHA, evidence artifact digest·run ID·artifact ID, v1·v2 state version과 10분 만료를 일회용 승인에 묶는다. workflow는 `BSIDE_ADMIN_TOKEN`으로 `POST /api/v2/admin/cutover`를 한 번 호출한다. 서버는 같은 MySQL transaction에서 승인, 두 release state, 필수 6개 connector와 그 SourceRight를 모두 잠근다. KR·US·JP·GB·CA·AU 중 공개 문서가 0건인 국가도 connector identity, active/error-free 상태, 실행 주기 기반 15~45분 최신 성공·확인 시각, SEC cursor UTC 시각, link-only 최근 관측·ACK, 증빙·유효·미철회 상태, 수집 및 공개 재배포 자격을 똑같이 재검사한다. 이어 기존 v1·v2 공개 문서 SourceRight guard까지 통과한 경우에만 v1과 v2를 함께 `live`로 바꾼다. nonce 원문은 로그에서 마스킹되고 DB에는 SHA-256만 저장된다.

v1·v2의 일반 `POST /admin/release-state`는 직접 `preview → live`를 시도하면 409 `protected_atomic_cutover_required`를 반환한다. 만료·철회·재사용·SHA·digest·version 불일치와 필수 connector/권한·최신성 오류는 상태를 한 건도 바꾸지 않는다. 필수 소스 오류는 409 `required_alpha_sources_invalid`이며 해당 승인도 미소비 상태로 남는다. 성공 뒤에는 인증 헤더 없이 `/sources/status`의 정확한 필수 6개 준비 상태와 events·Atom·JSON·CSV export를 다시 검사한다. workflow는 dispatch 시점의 `PAGES_OWNER=governance`와 `GOVERNANCE_PIPELINE_MODE=shadow` snapshot을 검증하지만 repository variable을 직접 변경하지 않는다.

이 24시간 경로는 범위를 투명하게 표시하는 Production Alpha 전환 전용이다. 정식 GA 선언에는 기존 14일 shadow, 300사건·500쌍 benchmark, 사용성·법률 승인 게이트를 별도로 적용한다.

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
- `governance-release` environment는 evidence 승인과 원자적 release 전환을 보호하고 required reviewer를 둔다. `BSIDE_RELEASE_AUTHORIZER_TOKEN`은 이 environment에만 둔다.
- `governance-runtime` environment는 무인 수집·품질 증빙만 담당하며 release 전환 권한을 갖지 않는다.
- artifact 조회는 `actions:read`, Pages 배포는 `pages:write`와 `id-token:write`만 사용한다. workflow에는 repository variable write 권한이나 별도 PAT를 부여하지 않는다.
- 어떤 단계도 DB delete, truncate, schema rollback, SourceRight 복구를 실행하지 않는다.
