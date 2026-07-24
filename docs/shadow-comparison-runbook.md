# 실제 엔진 shadow 비교 운영

[`shadow-compare.yml`](../.github/workflows/shadow-compare.yml)은 KST 00:20에 완료된 전일의 legacy 결과와 governance candidate 결과를 비교한다. `GOVERNANCE_PIPELINE_MODE=shadow|live`에서만 실행되며 취소형 concurrency를 사용하지 않는다. Telegram outbound는 이 workflow와 두 producer 모두에서 비활성 상태다.

## 실제 입력과 불변성

- `build-feed.yml`은 정상 수집과 DB 동기화가 끝난 뒤 그 실행의 `data/state.json`에 실제로 존재하는 legacy 공개 cluster를 `shadow-engine-legacy-<KST day>-<run>-<attempt>` artifact로 저장한다.
- `ingest-official.yml`은 DART와 필수 KIND 수집·MySQL ACK가 모두 성공한 뒤 그 실행이 실제 생성한 complete-identity 사건을 `shadow-engine-candidate-<KST day>-<run>-<attempt>` artifact로 저장한다.
- candidate의 complete event에는 유효한 `eventcmp:v1:<sha256>`와 하나 이상의 안정 근거가 모두 있어야 한다. 안정 근거는 `document_id`, DART/KIND 접수번호, 정규화된 canonical URL만 허용한다.
- legacy snapshot은 같은 날 공개된 cluster의 안정 record ID와 위 근거를 보존한다. runner는 이 근거의 exact match로만 legacy record를 canonical event에 연결한다. 제목·요약·테마·회사명 유사도로 identity를 추측하거나 새 key를 만들지 않는다.
- 동일 근거가 복수 canonical event를 가리키거나, 공식 문서 ID·접수번호가 있는데 연결 대상이 없으면 producer가 실패한다. 일반 뉴스 URL만 있고 candidate에 exact URL 근거가 없는 cluster는 공식 사건 비교 대상에서 제외한다.
- snapshot schema v3는 `record_scope=kst_observation_day_delta_v1`, full 40자 Git SHA, KST observation day, 실제 collection source run ID와 GitHub producer run ID, 정렬·중복 제거된 근거, key-list SHA-256, legacy-record SHA-256, 전체 snapshot SHA-256을 가진다. manifest의 workflow run ID·artifact 이름·producer run ID가 정확히 일치하지 않으면 거부한다. artifact 보존 기간은 21일이다.

일일 runner는 두 workflow의 성공한 default-branch artifact만 조회한다. 같은 KST 날짜에 다른 SHA의 성공 artifact가 하나라도 있으면 14일 창 재시작 대상으로 보고 즉시 실패한다. GitHub가 제공한 artifact SHA-256을 다운로드 ZIP에 다시 계산하고, 경로 이동·심볼릭 링크·추가 파일이 없는 `engine-output.json` 한 개만 안전하게 연다.

전일의 `governance-shadow-comparison-<day>-<SHA>`가 있으면 그 artifact도 같은 방식으로 검증해 `same_sha_cumulative_kst_day_end_v1` corpus를 이어 간다. 전일 날짜가 정확히 하루 전인지, SHA·workflow·default branch·artifact digest·내부 `report_sha256`·corpus payload hash가 모두 맞는지 확인하고, 전일 운영 DB shadow row의 두 key set과 crosswalk까지 다시 대조한다. 따라서 신규 공시가 0건인 주말·무공시일에도 전일까지 실제 관측된 비어 있지 않은 corpus가 분모가 된다. 전일 DB row가 있는데 receipt가 없으면 조용히 bootstrap하지 않고 실패한다. SHA 변경 또는 전일 성공일 누락 뒤의 새 bootstrap은 당일 실제 eligible record가 1건 이상일 때만 가능하다.

## DB 기록 순서

1. `BSIDE_EDITOR_TOKEN` Bearer 인증으로 이전 날짜의 `pending` discrepancy를 조회한다. 한 건이라도 있으면 아무것도 쓰지 않는다.
2. 검증된 전일 corpus와 양쪽 당일 delta snapshot을 합치고 stable-evidence index를 만든다. 같은 legacy record는 정확히 한 canonical key에 연결돼야 한다. 일별 누적 `eligible_legacy_record_count`는 1 이상, `crosswalked=eligible`, `unmatched=ambiguous=0`, `coverage_rate=1.0`이어야 한다.
3. `/api/v1/admin/shadow-runs`의 같은 날짜·SHA row를 먼저 읽고 신규 POST 또는 `expected_updated_at`을 포함한 동일-content 재실행을 수행한다. key set뿐 아니라 `legacy_crosswalk` count와 hash도 일치해야 하며 기존 content가 다르면 교체하지 않고 실패한다.
4. legacy-only는 `candidate_missing`, candidate-only는 `candidate_added` discrepancy로 생성한다. deterministic discrepancy ID와 `pending` 상태만 전송한다.
5. 사람이 이미 `reviewed|resolved|dismissed`한 row는 변경하지 않는다. 비교 집합에서 사라진 same-day discrepancy도 자동 해결하지 않고 작업을 실패시켜 사람 판단을 요구한다.
6. 모든 POST 뒤 GET으로 날짜·SHA·key set·digest·count, crosswalk denominator/hash와 discrepancy ID를 다시 대조한다. ACK 불일치는 workflow 실패다.
7. 성공 시 `governance-shadow-comparison-<day>-<SHA>` receipt artifact를 남긴다. 이 파일에는 누적 원시 key set/digest/count, 안정 근거 기반 corpus payload와 crosswalk row/hash, 당일 입력 artifact 식별자, 전일 receipt hash chain, API ACK가 포함된다.

## 필수 설정

- Repository variable: `GOVERNANCE_PIPELINE_MODE=shadow` 또는 `live`
- Repository variable: `GOVERNANCE_API_BASE_URL` (`.../api.php/api/v1`)
- Repository secret: `BSIDE_EDITOR_TOKEN`
- `ENABLE_TELEGRAM_DELIVERY=false`, `ENABLE_GOVERNANCE_DELIVERY=false`

정식 14일 shadow에서는 release-candidate SHA와 identity 규칙을 바꾸지 않는다. 코드 SHA 변경, 어느 한 엔진의 일일 성공 artifact 누락, 전일 receipt chain 단절, mixed SHA, invalid key, 누적 legacy eligible 0건, crosswalk 누락·모호성, API ACK 불일치, 전일 미검수 discrepancy가 발생하면 해당 날짜는 성공일로 계산하지 않으며 14일 창을 다시 시작한다.
