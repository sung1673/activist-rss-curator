# 전일 품질 스냅샷

`quality-snapshot.yml`은 KST 00:25에 종료된 전일의 운영 품질 수치를 불변 레코드로 고정한다. 00:35 release-evidence 입력 생성기는 이 레코드가 없으면 실패한다.

이 작업은 `GET /api/v1/ops/release-evidence`에서 다음 조건을 모두 확인한다.

- 범위가 전일 KST 하루와 정확히 일치한다.
- `evidence_source=production_db_export`, `is_synthetic=false`, `distribution_mode=web_only`다.
- release state가 `preview` 또는 `live`다.
- 전체 자료가 workflow의 40자리 Git SHA 하나에만 귀속된다.
- DART 성공 poll 간격과 KIND의 실제 접수시각→최초 관측 지연이 모두 존재한다.
- 공식 근거, 고위험 사건 사람 검수, 원문 보존, SourceRight의 원시 분자·분모가 정수이며 분자가 분모를 넘지 않는다.

검증 후 `POST /api/v1/ops/quality-observations`로 한 건을 저장하고, 다시 export하여 `content_metric_assignment=immutable_quality_observation`, 동일 observation ID와 저장 payload SHA-256을 확인한다. 부분 ACK, 다중 SHA, KIND 지연 누락, 0으로 대체한 지연, 모호한 일자 귀속은 모두 workflow 실패다.

스키마 7에 남은 `same_story_*` 컬럼은 마이그레이션 호환용 예약 필드이며 이 작업에서는 0으로 고정한다. 릴리스의 same-event precision은 이 값이 아니라 검수자 2명이 확정한 보호된 `benchmark.json`만 사용한다.

필요 설정은 기존 `BSIDE_API_BASE_URL`(또는 `GOVERNANCE_API_BASE_URL`)과 `BSIDE_OPS_TOKEN`이다. receipt artifact에는 토큰이나 API 응답 원문을 넣지 않고 검증된 observation, ACK 수, 서버 payload hash만 90일 보관한다. Telegram·governance outbound 변수가 true이면 실행하지 않는다.
