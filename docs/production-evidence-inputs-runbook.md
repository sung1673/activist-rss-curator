# 운영 릴리스 증빙 입력 생성 절차

`curator.release_evidence_inputs`는 운영 DB의 실제 관측값과 보호된 사람 검수 artifact를 결합해 기존 `curator.release_evidence`가 소비하는 여섯 개 입력 파일을 만든다. 누락 날짜, 혼합 Git SHA, 미검수 shadow 차이, 합성값, KIND run 누락 중 하나라도 발견되면 artifact를 만들지 않는다. 일별 누적 콘텐츠 corpus의 분모가 0인 날은 rate를 `null`로 보존할 수 있지만 최근 7일 합산 분모는 반드시 0보다 커야 한다.

## 자동 실행 순서

| KST | workflow | 결과 |
|---|---|---|
| 필요 시 1회 | `Governance protected human evidence artifact` | 같은 기본 브랜치 SHA의 `governance-human-evidence` |
| 매일 00:35 | `Governance production evidence inputs` | `governance-release-evidence-inputs` |
| 매일 00:45 | `Governance production evidence export` | 최종 `governance-release-evidence` |

00:35 workflow는 다음 조건을 모두 확인한 사람 artifact만 사용한다.

- `.github/workflows/human-evidence-artifact.yml`의 성공 run에서 생성됨
- 현재 기본 브랜치와 같은 full 40자 Git SHA
- 기본 브랜치에서 실행됨
- 만료되지 않은 GitHub artifact이며 SHA-256 digest가 존재함
- 루트에 `benchmark.json`, `usability.json`, `release-approval.json`만 존재함

운영 API는 `GET /api/v1/ops/release-evidence?from=YYYY-MM-DD&to=YYYY-MM-DD`를 `BSIDE_OPS_TOKEN` Bearer 인증으로 호출한다. URL과 토큰은 기존 `BSIDE_API_BASE_URL`, `BSIDE_OPS_TOKEN`만 사용하므로 PAT나 새로운 장기 Secret은 필요 없다.

## 운영 API의 필수 원시 계약

API envelope는 `evidence_source=production_db_export`, `is_synthetic=false`, `distribution_mode=web_only`, `release_state=preview|live`여야 한다. 14일 shadow 기간에는 서버를 `preview`로 두고 증빙을 검증한 후에만 `live`로 전환한다. `closed` 상태에서는 릴리스 증빙을 만들지 않는다. 요청한 14일 범위와 정확히 일치하고 `code_revisions`에는 현재 SHA 하나만 있어야 한다.

- `collection_runs`: 일자·source key·SHA별 attempt/success/raw/ACK와 최초·마지막 관측 시각. 최근 7일 모두 DART와 KIND가 존재하고 모든 실행이 성공해야 한다.
- `web_distribution_days`: durable GitHub Actions 관측에서 만든 일자·SHA별 Pages/API 배포 attempt/success/failure, denominator/rate, target, duration p95, 실패 탐지 p95. 최근 7일 모두 Pages 배포 시도가 1건 이상이어야 한다.
- `availability.daily_route_build_counts`: 최근 7일의 일자·route·정확한 SHA별 attempt/success/failure/denominator/rate와 `watchdog-v1-kst-5m-minute01` cadence 증빙. `/`, `/governance/`, `/feed.xml`, `/api/v1/health` 각각 KST 00:01~23:56의 288개 slot이 모두 있어야 한다. raw `observed_at`은 가장 최근 `[slot, slot+5분)`에 귀속하며 23:56 slot은 다음 KST 날짜 00:00:59까지 포함한다. route별 `expected_slot_count=covered_slot_count=288`, `missing_slot_count=off_cadence_count=0`, 72자리 bitmap의 popcount 일치, 첫·마지막 slot 관측, 실제 interval p95·일 경계 포함 최대 공백 600초 이하를 모두 검증한다. duplicate 관측은 별도로 세며 누락 slot을 대신할 수 없다.
- `web_vitals.groups`: 일자·route·metric·device·SHA별 실제 sample count와 p75. 브라우저 HTML journey인 `/today`, `/events`, `/issuers`, `/calendar`의 모바일 LCP·INP·CLS가 route마다 하루 5개 이상이어야 한다. XML/JSON endpoint에는 Web Vitals를 요구하지 않는다. 일별 성능 게이트에는 네 HTML route 중 가장 나쁜 p75를 사용한다.
- `shadow_days`: 14일 각각의 legacy/candidate 성공 상태, 두 엔진의 KST day-end 누적 canonical `comparison_key` 목록, `review_status_counts`, lossless legacy crosswalk. 두 key set은 매일 비어 있지 않고 전일의 집합을 포함해야 하며 eligible 분모도 감소할 수 없다. crosswalk는 매일 eligible 분모가 1 이상이고 eligible과 crosswalked가 정확히 같으며 unmatched·ambiguous가 모두 0이어야 한다. 신규 공시 0건인 날은 전일의 검증된 실제 corpus와 동일한 집합·분모를 유지한다.
- `shadow_discrepancies`: 일자·SHA·review status별 원시 건수. `pending`이 한 건이라도 있으면 실패한다.
- `operations_days`: 최근 7일의 SHA, `dart_success_poll_interval_p95_minutes`, `kind_observation_lag_p95_minutes`, 공식 근거·고위험 사람 검수·원문 언어·SourceRight의 원시 분자와 분모. 콘텐츠 지표는 `content_scope=governance_corpus_2021_plus_kst_day_end_v2`인 2021년 이후 누적 corpus를 해당 KST 일자 종료 시점으로 고정한다. v2 분모는 공개·승인된 사건·캠페인·주장·의안·약속·타임라인이 참조한 모든 source class의 고유 문서이며, 이후 원문 변형·권리 만료·철회로 공개 화면에서 빠져도 분모에서 제거하지 않는다. 정확한 `content_snapshot_at`, `content_metric_assignment=immutable_quality_observation`, observation ID, payload SHA-256을 가져야 한다. mutable DB 현재값이나 null fallback은 받지 않는다. DART/KIND source별 `raw_count`는 `source_outcomes.raw_count`를 최우선으로 사용해 실제 제출 문서 ACK와 대조한다. 전체 공식 지연 gate에는 두 값 중 큰 값을 사용한다.
- 일별 snapshot 이후의 권리 변경은 `preview → live` 트랜잭션의 current-rights guard가 같은 v2 분모로 재검사한다. 현재 활성·AI·재배포·기간·비철회·증빙 조건을 모두 만족하지 못한 참조 문서가 하나라도 있으면 전환은 409로 실패하며 상태 version은 증가하지 않는다.

Pages/API 배포 성공률은 `web_distribution_days`의 durable GitHub Actions attempt/success만 사용한다. watchdog availability 성공 건수를 배포 성공 건수로 바꾸지 않는다. 실제 배포 실패가 있었으면 해당 실패의 detection p95를 사용하고, 실패가 없던 날에는 route별 일 경계 포함 실제 최대 공백 중 최댓값을 탐지 상한으로 사용한다. 일별 availability 분모는 정확히 1,152 slot, 7일 합계는 8,064 slot이어야 한다.

KIND 지연은 서버가 `source_key=kind`인 `EventObservation.first_observed_at`과 연결된 KIND 공식 문서의 실제 `published_at` 차이로만 계산한다. 날짜의 자정이나 collection run lag로 대체하지 않는다. 실제 KIND 관측이 0건인 무공시일은 관측·표본 수를 모두 0으로 기록하고 지연을 `null`인 N/A로 보존한다. 관측이 1건 이상이면 모든 관측에 실제 시각 표본이 있어야 하며 지연도 숫자여야 한다. 최근 7일 전체가 N/A일 수는 없고, 창 전체에는 실제 KIND 관측과 완전한 시각 표본이 1건 이상 있어야 한다.

same-event precision은 일별 운영 수치를 만들지 않는다. 보호된 사람 라벨 `benchmark.json`의 500쌍 이상 표본만 정답 근거로 사용한다. DB의 과거 same-story 컬럼은 호환을 위한 reserved zero이며 API payload·hash·`operations_days`에는 포함하지 않는다.

DART와 KIND 성공률은 합산하지 않는다. 생성된 `operations.jsonl`은 아래 값을 별도로 기록한다.

- `dart_ingest_expected_count`, `dart_ingest_succeeded_count`, `dart_ingest_success_rate`
- `kind_ingest_expected_count`, `kind_ingest_succeeded_count`, `kind_ingest_success_rate`

따라서 DART-only canary는 계속 실행할 수 있지만 KIND가 없는 상태에서는 정식 릴리스 증빙을 만들 수 없다.

## 사람 검수 artifact 생성

GitHub Actions에는 로컬 파일을 기존 run의 artifact로 직접 올리는 일반 `gh` 명령이 없다. 파일을 저장소에 커밋하거나 workflow input에 평문으로 넣지 않기 위해, 보호된 GitHub Environment의 일회성 Secret을 운반 채널로 사용한다.

먼저 저장소 Settings → Environments의 `governance-release`에 다음 보호를 설정한다.

1. 필수 reviewer를 지정한다.
2. 가능한 요금제에서는 self-review 방지를 켠다.
3. deployment branch를 기본 브랜치로 제한한다.

세 JSON은 각각 35 KiB 이하여야 한다. Base64 변환 후에도 GitHub Environment Secret의 48 KiB 제한보다 작게 유지하기 위한 상한이다. 평가자 ID와 승인자 reference는 가명 식별자를 사용하고 이름, 이메일, IP, query string을 넣지 않는다. 로컬 PowerShell에서 다음을 실행한다.

```powershell
function Set-OneUseEvidenceSecret([string]$Name, [string]$Path) {
  $bytes = [IO.File]::ReadAllBytes((Resolve-Path $Path))
  $encoded = [Convert]::ToBase64String($bytes)
  $encoded | gh secret set $Name --env governance-release
  Remove-Variable bytes, encoded
}

Set-OneUseEvidenceSecret GOVERNANCE_BENCHMARK_EVIDENCE_B64 .\benchmark.json
Set-OneUseEvidenceSecret GOVERNANCE_USABILITY_EVIDENCE_B64 .\usability.json
Set-OneUseEvidenceSecret GOVERNANCE_RELEASE_APPROVAL_B64 .\release-approval.json

gh workflow run human-evidence-artifact.yml --ref main `
  -f confirmation=UPLOAD_VALIDATED_HUMAN_EVIDENCE
gh run list --workflow human-evidence-artifact.yml --limit 1
gh run watch <RUN_ID> --exit-status
```

성공 여부와 관계없이 바로 일회성 Secret을 삭제한다.

```powershell
gh secret delete GOVERNANCE_BENCHMARK_EVIDENCE_B64 --env governance-release
gh secret delete GOVERNANCE_USABILITY_EVIDENCE_B64 --env governance-release
gh secret delete GOVERNANCE_RELEASE_APPROVAL_B64 --env governance-release
```

Secret 값은 workflow 로그에 출력되지 않는다. workflow는 복호화 후 파일명·크기·JSON·production provenance·현재 SHA를 검증하며, 정확히 세 파일만 90일 artifact로 보존한다. 사람 파일은 저장소, Pages, PR artifact에 커밋하지 않는다.

## 수동 증빙 입력 생성과 확인

보통 00:35 예약 실행을 기다리면 된다. 같은 날 수동 실행이 필요하면 보호된 사람 artifact run ID를 지정한다.

```powershell
gh workflow run release-evidence-inputs.yml --ref main `
  -f human_run_id=<HUMAN_EVIDENCE_RUN_ID> `
  -f human_artifact_name=governance-human-evidence `
  -f through_date=2026-08-31
gh run list --workflow release-evidence-inputs.yml --limit 1
gh run watch <RUN_ID> --exit-status
```

성공 artifact `governance-release-evidence-inputs`의 루트에는 아래 파일만 있어야 한다.

- `shadow.jsonl`: 종료일을 포함한 정확히 14일
- `operations.jsonl`: 정확히 최근 7일
- `performance.jsonl`: 정확히 최근 7일
- `benchmark.json`: 보호된 사람 artifact의 원본 bytes
- `usability.json`: 보호된 사람 artifact의 원본 bytes
- `release-approval.json`: 보호된 사람 artifact의 원본 bytes

workflow summary에 사람 artifact의 run ID·artifact ID·digest와 결과 artifact의 ID·digest가 함께 기록된다. 00:45 exporter는 다시 같은 SHA, 기본 브랜치, 성공 run, GitHub digest를 확인한다.

## 의도된 실패 조건

다음 상태에서 예약 workflow가 실패하는 것은 정상적인 공개 차단 동작이다.

- 아직 보호된 사람 검수 artifact가 없음
- KIND 실데이터가 아직 없음
- 14일 고정 SHA shadow가 끝나지 않음
- 최근 7일 중 하루라도 운영·성능 관측이 없음
- availability 시도가 0이거나 Web Vitals 실제 표본이 route당 5개 미만임
- DART/KIND 중 하나가 실패하거나 누락됨
- 미검수 discrepancy가 남음
- 운영 API와 사람 artifact의 SHA가 다름
- API가 실제 원시 분자·분모 대신 합성 성공값을 반환하거나, 일별 누적 corpus의 0분모에 0/100% 같은 rate를 채움
- 최근 7일 누적 콘텐츠 분모가 0이거나 실제 KIND 관측·완전한 시각 표본이 한 건도 없음

이 실패를 우회해 파일을 수동 작성하거나 이전 날짜의 성공값을 복제하지 않는다.
