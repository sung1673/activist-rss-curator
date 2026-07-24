# Shadow 비교와 공개 전환 게이트

`curator.release_gate`는 실제 시간을 대신하거나 지표를 생성하지 않는다. 운영 DB·관측 시스템에서 내보낸 14일 shadow 결과와 7일 연속 운영·성능 지표, 사람 라벨 benchmark·사용성 평가·승인 보고서를 읽어 공개 전환 가능 여부를 판정한다.

> 현재 상태(2026-07-22): fail-closed consumer와 production evidence assembler가 구현됐다. assembler는 보호된 same-SHA 입력 artifact만 검증하며 누락값이나 지표를 합성하지 않는다. 따라서 실제 shadow·운영·RUM·사람 라벨·사용성·승인 입력이 모두 준비되기 전에는 유효한 release artifact가 생성되지 않는다.

## 증빙 묶음

한 번의 판정에는 다음 여섯 파일이 필요하다.

| 파일 | 최소 범위 | 용도 |
|---|---:|---|
| `shadow.jsonl` | 최근 14일 연속 | 기존 엔진과 신규 엔진 사건 비교, 실행 성공과 차이 검수 확인 |
| `operations.jsonl` | 최근 7일 연속 | 공시 수집·지연, 발송·탐지, 공식 근거, 검수, 원문·권한 지표 |
| `performance.jsonl` | 같은 최근 7일 | 가용성, 모바일 LCP·INP·CLS |
| `benchmark.json` | 현재 리비전 1건 | 사람 라벨 500 pair·300 사건의 precision/recall |
| `usability.json` | 실제 평가자 15명 | 세 집단 각 5명의 180초 과업 성공 여부 |
| `release-approval.json` | 현재 리비전 1건 | 법률·편집·제품 승인과 보관 문서 digest |

모든 파일은 같은 7~64자리 hexadecimal `code_revision`을 가리켜야 한다. 일별 증빙은 `environment=production`, `is_synthetic=false`, timezone이 있는 `collected_at`, 실제 export 종류를 나타내는 `evidence_source`를 포함한다. `fixture`, `synthetic`, `test`, `sample` 출처는 거부한다.

판정 시 `--expected-revision`으로 현재 checkout의 revision도 전달한다. 증빙과 checkout이 같은 SHA의 짧은/전체 표기처럼 prefix 관계일 때만 같은 revision으로 인정하며, 무관한 revision의 과거 증빙 재사용은 실패한다.

정식 Actions 판정은 증빙 artifact를 만든 실행이 성공했고 현재 checkout과 정확히 같은 SHA이며 72시간 이내인지 먼저 확인한다. 그 실행의 `created_at`을 `--evidence-as-of`로 전달하고 shadow·운영·성능 창의 마지막 날짜가 2일보다 오래되면 전환을 거부한다. 따라서 오래된 성공 지표를 새 전환의 근거로 재사용할 수 없다.

JSON Schema:

- [`shadow-comparison-day.schema.json`](schemas/shadow-comparison-day.schema.json)
- [`operations-gate-day.schema.json`](schemas/operations-gate-day.schema.json)
- [`performance-gate-day.schema.json`](schemas/performance-gate-day.schema.json)
- [`usability-evidence.schema.json`](schemas/usability-evidence.schema.json)
- [`release-approval.schema.json`](schemas/release-approval.schema.json)

### Shadow 비교 키

두 엔진의 내부 ID가 달라도 같은 사건을 비교할 수 있도록 export 단계에서 `comparison_key`를 만든다. 키는 제품의 same-event 계약인 회사·행위·대상·기한을 정규화한 값이어야 한다. 테마나 제목 유사도만으로 같은 키를 부여하지 않는다.

```json
{
  "schema_version": 1,
  "date": "2026-07-14",
  "environment": "production",
  "evidence_source": "mysql_export",
  "is_synthetic": false,
  "collected_at": "2026-07-14T23:59:00+09:00",
  "code_revision": "0123456789abcdef0123456789abcdef01234567",
  "legacy_run": {
    "status": "succeeded",
    "events": [{"comparison_key": "00126380:general_meeting:2027-03-19"}]
  },
  "candidate_run": {
    "status": "succeeded",
    "events": [{"comparison_key": "00126380:general_meeting:2027-03-19"}]
  },
  "discrepancies_reviewed": true
}
```

보고서는 일별 기존·신규·일치 건수, 신규 증감, Jaccard, 각 엔진에만 존재하는 키를 남긴다. 기존 엔진을 정답으로 간주하지 않으므로 agreement 자체에 임의 하한을 두지 않는다. 대신 14일 연속 실행 성공, 신규 사건 실제 관측, 모든 차이의 편집 검수를 필수로 하고 정확도는 사람 라벨 benchmark로 판정한다.

### 운영 지표

`operations.jsonl`의 각 행은 다음 지표를 포함한다.

- `official_ingest_success_rate` 0.99 이상이고 DART·KIND 각각의 raw 시도/성공 기준 성공률도 0.99 이상
- DART 성공 poll 간격 p95와 KIND 접수시각→최초 관측 지연 p95가 각각 45분 이하(단일 `official_lag_p95_minutes`는 둘의 최댓값)
- `distribution_mode=web_only`이며 `web_distribution_attempted_count`가 1 이상
- Pages/API 배포 `web_distribution_success_rate` 0.995 이상
- 웹 배포 실패 `web_distribution_failure_detection_p95_minutes` 10 이하
- `telegram_delivery_attempted_count=0` (Telegram outbound는 정책상 비활성)
- `official_evidence_link_rate` 0.95 이상
- `same_story_precision` 0.97 이상이며 `same_story_evaluated_pair_count` 1 이상
- `top_sensitive_human_review_rate` 1.0
- `original_language_preservation_rate` 1.0
- `valid_source_right_rate` 1.0

`performance.jsonl`의 각 행은 가용성 시도·성공 raw count로 0.999 이상을 재계산할 수 있어야 한다. 모바일 LCP·INP·CLS는 각각 4개 route×5회인 최소 20개의 실제 표본을 요구하며 LCP p75 2.5초 이하, INP p75 200ms 이하, CLS p75 0.1 이하를 충족해야 한다. 평균이나 7일 합산 값이 좋은 것으로 실패한 하루를 상쇄할 수 없으며 일별 모든 게이트를 통과해야 한다.

### 사용성·승인 증빙

`usability.json`은 `evidence_source=human_usability_export`, full 40자리 SHA, 평가 원본에서 다시 계산할 수 있는 dataset SHA-256을 사용한다. 기관, 고액자산가, 해외기관은 각각 정확히 5명이며 총 15명 중 12명 이상이 180초 안에 사건·당사자·공식 근거·현재 상태를 모두 확인해야 한다.

`release-approval.json`은 같은 SHA와 usability·benchmark dataset digest를 참조한다. 법률, 편집, 제품 역할이 각각 한 번씩 실제 승인해야 하며 보관 문서 URI와 SHA-256이 필요하다. `release_approved=false`, 역할 누락, 거절, 합성 source는 전환을 차단한다.

## Benchmark 증빙 생성

정식 보고서는 fixture 허용이나 `--report-only` 없이 production 환경과 현재 코드 리비전을 명시해 만든다.

```powershell
$revision = (git rev-parse HEAD).Trim()
.\.venv\Scripts\python.exe -m curator.quality_benchmark `
  --same-story data/benchmarks/same_story_pairs.jsonl `
  --relevance data/benchmarks/relevance_events.jsonl `
  --environment production `
  --code-revision $revision `
  --output evidence\benchmark.json
```

보고서에는 라벨 출처, 입력 데이터셋 SHA-256, 환경, 코드 리비전이 포함된다. 라벨 출처가 `human|adjudicated` 이외이거나 500 pair, 공식 문서가 연결된 실제 사건 300개, 사람이 확인한 비관련 hard-negative 120개, same-story precision 0.97, relevance precision 0.90·recall 0.95 중 하나라도 미달하면 최종 전환 게이트를 통과할 수 없다. 작은 합성 fixture는 낮춘 임계값으로 benchmark 함수를 호출하더라도 `is_synthetic=true`, `release_eligible=false`로 기록되어 최종 판정에서 거부된다.

## 로컬 판정

```powershell
$revision = (git rev-parse HEAD).Trim()
.\.venv\Scripts\python.exe -m curator.release_gate `
  --shadow evidence\shadow.jsonl `
  --operations evidence\operations.jsonl `
  --performance evidence\performance.jsonl `
  --benchmark evidence\benchmark.json `
  --usability evidence\usability.json `
  --approval evidence\release-approval.json `
  --expected-revision $revision `
  --evidence-as-of 2026-07-16T00:00:00Z `
  --output evidence\release-gate.json `
  --shadow-output evidence\shadow-comparison.json
```

종료 코드는 통과 `0`, 유효하지만 기준 미달 `1`, 누락·합성·잘못된 형식 등 사용할 수 없는 증빙 `2`다. 기준 미달 보고서도 `--output`에 저장되어 실패 원인을 확인할 수 있다.

## GitHub Actions 판정

운영 export 작업은 여섯 파일과 digest manifest를 `governance-release-evidence` artifact로 업로드한다. `Governance release transition gate`를 수동 실행하면서 해당 artifact를 만든 workflow run ID와 artifact 이름을 입력한다. 워크플로는 `github.sha`를 `--expected-revision`으로 전달해 선택한 branch/tag의 checkout과 증빙이 같은 revision인지 확인한다. 원본 artifact를 수정하지 않고 보고서 두 개를 90일 보존하며, 판정이 `0`이 아니면 최종 job을 실패시킨다.

판정 workflow 자체는 배포하지 않는다. 통과한 evidence와 immutable Pages artifact를 사용한 실제 전환·복구 절차는 [거버넌스 공개 전환과 롤백](governance-cutover-rollback.md)을 따른다.
