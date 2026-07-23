# 운영 release evidence 내보내기

`curator.release_evidence`는 지표를 만들거나 빈 값을 성공으로 바꾸지 않는다. 운영 DB/API를 읽는 선행 작업이 만든 접근 통제 GitHub artifact를 검증하고, 동일 코드 SHA의 공개 전환 증빙 묶음으로 정규화하는 fail-closed assembler다.

## 입력 계약

선행 artifact `governance-release-evidence-inputs`의 루트에는 아래 여섯 파일이 모두 있어야 한다.

- `shadow.jsonl`: 종료일을 포함한 14일 연속 legacy/candidate KST day-end 누적 snapshot, 사람 검수 상태와 lossless nonempty legacy crosswalk
- `operations.jsonl`: 종료일을 포함한 7일 연속 운영 원시 분자·분모와 계산 지표
- `performance.jsonl`: 같은 7일의 watchdog/RUM 지표와 availability·LCP·INP·CLS 표본 수. availability에는 `watchdog-v1-kst-5m-minute01`, 일 1,152/7일 8,064 slot의 완전 coverage, 실제 interval p95·일 경계 포함 최대 공백, 첫·마지막 관측과 raw missing·duplicate·off-cadence count를 보존한다.
- `benchmark.json`: 실제 사람 라벨만 사용한 `curator.quality_benchmark` production 보고서
- `usability.json`: 기관·고액자산가·해외기관 각 5명 이상의 실제 평가 결과
- `release-approval.json`: 법률·편집·제품 담당자의 결정과 보관 문서 digest

모든 레코드는 `environment=production`, `is_synthetic=false`, full 40자리 `code_revision`, timezone이 있는 수집 시각, 실제 source를 나타내는 `evidence_source`를 가진다. `fixture`, `synthetic`, `sample`, `test`가 포함된 source는 거절한다. performance와 최근 7일 집계 rate의 분모가 0이거나 계산된 rate와 보고된 rate가 다르면 artifact를 만들지 않는다. 일별 누적 콘텐츠 corpus의 분모가 0인 날만 rate `null`을 허용하며 7일 합산 분모는 반드시 양수여야 한다.

각 shadow 일자의 두 누적 key set은 비어 있지 않고 전일 집합을 포함해야 한다. legacy crosswalk eligible 분모도 감소할 수 없으며, 매일 eligible이 1 이상이고 eligible과 crosswalked가 정확히 같으며 unmatched·ambiguous가 모두 0이어야 한다. 신규 공시 0건인 날은 검증된 전일 실제 corpus를 그대로 사용하며 0분모를 성공으로 만들지 않는다. KIND 무공시일은 실제 관측·시각 표본 수가 모두 0이고 지연이 `null`인 경우에만 N/A로 허용한다. 최근 7일 창에는 실제 KIND 관측과 완전한 시각 표본이 1건 이상 있어야 한다.

동일 사건 precision과 500쌍 이상 표본 기준은 사람이 확정한 `benchmark.json`에서만 판정한다. 이를 매일의 운영 수치로 복제하거나 합성하지 않는다. `operations.jsonl`은 실제 수집·배포·공식 근거·고위험 검수·원문 보존·SourceRight의 일별 원시 분자와 분모만 담는다.

`release-approval.json`은 usability와 benchmark 데이터셋 SHA-256을 직접 참조한다. 각 승인에는 개인정보 대신 내부 보관대장 reference, 문서 URI, 문서 SHA-256을 넣는다. 승인 또는 거절이라는 실제 결정을 기록할 수 있지만 `pending`을 승인처럼 취급할 수는 없다.

## 예약 workflow

[`release-evidence.yml`](../.github/workflows/release-evidence.yml)은 매일 KST 00:45(UTC 15:45)에 실행된다. 예약 실행은 `GOVERNANCE_PIPELINE_MODE=shadow|live`일 때만 활성화된다.

workflow는 다음 조건을 모두 만족하는 최신 입력 artifact만 선택한다.

1. 현재 checkout과 정확히 같은 full SHA
2. 저장소 기본 branch에서 실행
3. producer workflow 결론이 `success`
4. 만료되지 않은 artifact이며 GitHub SHA-256 digest가 존재
5. 현재 evidence workflow와 다른 run

download 단계는 digest mismatch를 오류로 처리한다. 출력 `governance-release-evidence` artifact는 90일 보존하고, 여섯 evidence 파일과 입력·출력 파일 digest 및 source artifact ID를 담은 `bundle-manifest.json`을 포함한다. 저장소에는 커밋하지 않는다.

선행 입력 artifact가 아직 없거나 14일/7일 window, 사람 평가, 승인 중 하나가 준비되지 않았다면 예약 작업은 실패한다. 이것은 누락 데이터를 합성하는 대신 공개 전환을 차단하기 위한 동작이다.

## 수동 재현

```powershell
$sha = (git rev-parse HEAD).Trim()
python -m curator.release_evidence `
  --source-dir evidence-input `
  --output-dir evidence-output `
  --expected-revision $sha `
  --through-date 2026-08-31 `
  --source-run-id 123456789 `
  --source-artifact-id 987654321 `
  --source-artifact-digest sha256:<64-hex-digest>
```

출력 디렉터리가 이미 있으면 덮어쓰지 않는다. 다른 source artifact나 수정된 입력으로 다시 내보낼 때는 새 디렉터리와 새 workflow run을 사용한다.
