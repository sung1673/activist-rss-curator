# 공식 공시 백필과 품질 릴리스 게이트

이 문서는 2021년 이후 DART·KIND 거버넌스 공시를 MySQL에 재시작 가능하게 적재하고, relevance와 same-story 품질을 사람 라벨 데이터로 검증하는 운영 계약을 정의한다.

## 공식 공시 백필

백필은 `curator.official_backfill`이 기존 `curator.official_ingest`와 `/api/v1` HMAC 업서트를 그대로 사용한다. 날짜 청크는 `[시작일, 종료일)` 반개구간이며 DART·KIND 커넥터에는 마지막 날을 포함하는 범위로 변환한다. 예를 들어 `--from-date 2021-01-01 --to-date 2021-01-15`는 1월 1일부터 1월 14일까지다.

필수 런타임 설정은 다음과 같다.

- DART 수집: `DART_API_KEY`
- KIND 수집: `KIND_DISCLOSURE_ENDPOINT`와 필요 시 `KIND_API_KEY`. 어댑터 키는 URL query가 아니라 `Authorization: Bearer` 헤더로 전달한다.
- 실제 적재: `ACTIVIST_API_URL`, `ACTIVIST_API_SECRET`
- 운영 체크포인트·quota·SourceRight API: `BSIDE_API_BASE_URL`, `BSIDE_OPS_TOKEN`
- 적재 대상 고정: 배포된 PHP가 반환하는 값과 같은 `BSIDE_BACKEND_BINDING_ID`
- 로컬에서 DART를 실제 호출할 때도 `CURATOR_REQUIRE_DURABLE_DART_QUOTA=1`을 지정한다. 중앙 MySQL quota 원장이 준비되지 않으면 호출하지 않고 실패한다.
- `.env`, `.env.local`, `.env.api`는 이 순서로 읽으며 이미 설정된 환경변수를 덮어쓰지 않는다.

먼저 한 청크를 읽고 정규화만 검증한다. dry-run은 원격 API와 체크포인트를 전혀 변경하지 않는다.

```powershell
.\.venv\Scripts\python.exe -m curator.official_backfill `
  --from-date 2021-01-01 --to-date 2021-01-15 `
  --source dart --chunk-days 1 --max-chunks 1 --dry-run
```

실제 백필은 범위를 고정해 실행한다. `--max-pages`는 소스·청크별 API 페이지 상한이고, `--max-chunks`는 한 번의 실행에서 처리할 미완료 청크 수의 상한이다.

커넥터가 보고한 전체 페이지가 `--max-pages`를 넘으면 해당 청크를 성공으로 잘라 저장하지 않고 실패 처리한다. 페이지 상한을 늘린 뒤 같은 청크를 다시 실행해야 한다.

### 공식 커넥터 계약

[OpenDART 공시검색 개발가이드](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019001)의 상세유형 중 `D001`, `D003`, `D004`, `E001`, `E002`, `E004`, `E005`, `E006`을 각각 1회 조회한 뒤 전체 공시검색을 조회한다. 상세 조회 응답에는 요청한 코드를 메타데이터로 붙이고 접수번호로 전체 조회와 중복 제거한다. `D002`와 `D005`는 임원·주요주주 개인 보유·거래계획이므로 5% 보유 사건으로 승격하지 않는다. 합병·분할·증자·사채 등 `B001`, `C004`, `E003`의 넓은 유형은 상세유형만으로 사건을 단정하지 않고 원문 보고서명 규칙이 일치할 때만 채택한다.

`last_reprt_at=N`으로 정정 전후 접수를 모두 가져온다. DART 응답의 `rm=정`은 현재 접수가 정정본이라는 뜻이 아니라 뒤에 정정 접수가 존재한다는 뜻으로 저장한다. 현재 정정 여부는 보고서명 앞의 공식 정정 표식으로만 판정한다. `rm=철` 또는 명시적 철회·취소 보고서는 원문을 삭제하지 않고 `withdrawn`으로 보존한다. 정정과 별도 철회 접수는 회사·기본 보고서명·제출인이 같고 선행 체인이 하나뿐일 때만 다음 버전으로 연결한다. 후보가 둘 이상이면 추정 연결하지 않는다.

DART와 KIND 모두 요청한 페이지와 응답 페이지가 다르거나, 전체 페이지·전체 건수가 조회 도중 바뀌거나, 성공 응답이 중간 빈 페이지를 반환하거나, 최종 행 수가 보고된 전체 건수와 다르면 해당 소스·날짜 창 전체를 실패 처리한다. 커넥터나 파서가 후반부에 실패했을 때 앞 페이지에서 읽은 일부 공시도 원격 DB에 보내지 않는다. 다음 재시도에서 전체 날짜 창을 다시 읽는다.

KIND는 공개된 범용 JSON API 계약이 없으므로 `KIND_DISCLOSURE_ENDPOINT` 앞의 어댑터가 아래 계약을 충족해야 한다.

KIND의 일반 HTML 화면이나 오늘의 공시 RSS는 이 endpoint가 아니다. 운영 어댑터가 준비되기 전에는 수동 workflow의 `include_kind=false`로 DART-only smoke/shadow만 실행할 수 있으며, 예약 실행은 KIND를 필수로 요구해 fail-closed된다. KIND 선택 시에는 dry-run을 포함해 운영 DB에 편집 승인된 `official:kind` SourceRight가 있어야 하며, 상세 계약은 [KIND SourceRight 수집 사전검증](kind-source-right-preflight.md)을 따른다.

- 응답은 전체 범위를 담은 최상위 JSON 배열이거나 JSON 객체다.
- 객체의 행 배열은 최상위 또는 `data` 안의 `items`, `list`, `results` 중 하나다.
- 객체는 `page|page_no|current_page`와 `total_pages|total_page|last_page`를 제공한다. 비페이지 응답은 `unpaginated=true`를 명시한다.
- `ok=false`, `success=false`, 비어 있지 않은 `error`, 실패 `status`는 커넥터 실패다. 데이터 없음은 `status=013|no_data|empty`로 명시한다.
- 가능하면 `total_count|total_items`를 제공하며, 제공된 전체 건수와 실제 행 수가 다르면 실패한다.
- 모든 행은 안정 접수번호와 8자리 DART `corp_code`를 포함한다. URL은 절대 HTTP(S)만 허용하며 그 외 값은 KIND 공식 뷰어 URL로 대체한다.
- 날짜만 있으면 DART와 동일하게 UTC 자정으로 저장한다. 오프셋 없는 시각은 KIND의 지역시각(Asia/Seoul)으로 해석하고 UTC로 정규화하며, 오프셋이 있으면 그대로 존중한다.

기사·공시 제목은 분류와 체인 키 계산에만 읽고 저장 값은 공백·언어·문자를 포함한 원문 그대로 유지한다. 자동 번역은 하지 않는다.

```powershell
.\.venv\Scripts\python.exe -m curator.official_backfill `
  --from-date 2021-01-01 --to-date 2026-01-01 `
  --source dart --chunk-days 1 --max-pages 100 --max-chunks 10
```

같은 명령을 다시 실행하면 완료된 청크를 건너뛰고 다음 청크부터 시작한다. 실패한 청크는 완료로 기록하지 않으며 기본값은 즉시 중단이다. 독립적인 오류를 모두 기록하려는 운영 실행에만 `--continue-on-error`를 사용한다. 최초 회사 마스터를 함께 적재할 때는 `--sync-company-master`를 한 번 지정한다.

### 체크포인트 계약

실행 여부를 결정하는 단일 기준은 운영 MySQL의 원격 체크포인트다. 기본 경로
`data/backfill_official_checkpoint.json`은 각 원격 쓰기 ACK 뒤에 갱신되는
증빙·복구 사본일 뿐이며, 이 로컬 파일만으로 완료 창을 건너뛰지 않는다. 이
파일은 Git 추적 대상이 아니고 JSON Schema는
[`schemas/official-backfill-checkpoint.schema.json`](schemas/official-backfill-checkpoint.schema.json)에 있다.

- `schema_version`: 현재 `1`
- `job`: 요청 범위, 청크 크기, 소스, 페이지 한도, 회사 마스터 옵션과 이 값들의 SHA-256 `fingerprint`
- `completed_windows`: 원격 MySQL 동기화까지 성공한 청크만 기록
- `failed_windows`: 오류, 시도 횟수, 수집 요약을 기록하고 다음 성공 시 제거
- `code_revision`: apply 청크를 실행한 7~40자 Git revision. 작업 fingerprint에는
  포함하지 않지만 30일 사람 검수 증빙은 모든 청크의 revision이 같아야 한다.
- `idempotency_key`: 작업 fingerprint와 날짜 청크로 만든 안정 키
- `company_master_synced`: 회사 마스터 적재가 완료되었는지 표시

체크포인트는 먼저 운영 MySQL에 optimistic version으로 기록하고 정확한 ACK를
받은 뒤, 로컬 증빙 사본을 임시 파일로 써 원자 교체한다. 범위·소스·페이지
한도 중 하나라도 달라지면 fingerprint 불일치로 중단한다. 다른 작업을 시작할
때만 `--restart`로 원격 체크포인트를 명시적으로 교체한다. 공시 접수번호 기반
`Document` ID, 안정적인 사건 ID, 청크별 결정적 실행 ID를 사용하므로 동일
청크의 재시도는 신규 row 생성이 아니라 업서트다.

실패 창의 `attempt`는 재시도마다 증가하며 성공하면 실패 맵에서 제거된다. 완료·실패 맵에 같은 창이 있거나, 키와 내부 날짜·상태·시도 횟수가 맞지 않는 손상 체크포인트는 자동으로 건너뛰지 않고 중단한다. 청크의 안정 `idempotency_key`가 동일 실행 ID를 보장하므로 `retrieved_at`과 수집 실행시각은 실제 재시도 시각을 기록한다.

정기 수집은 KST 당일과 앞선 2일을 겹쳐 읽고 접수번호 업서트로 중복을 제거한다. 자정 이후 오전 9시 전에도 UTC 날짜가 아니라 KST 당일을 조회한다. 이는 영속 커서가 아니므로 겹침 범위를 넘는 장애 복구에는 이 체크포인트 백필을 사용해야 한다. 운영 DB에서 소스별 마지막 완전 성공 창을 읽는 영속 증분 커서는 스테이징 API 계약이 확정된 뒤 연결한다.

각 collection run의 `metrics_json`에는 소스별 요청·페이지·원시 행·채택·비대상 제외·중복·오류 유형·부분 폐기·경과시간이 저장된다. OpenDART 공시검색은 접수 날짜만 제공하므로 자정과의 차이를 수집 지연으로 둔갑시키지 않고 `lag_seconds_p95`는 비워 둔다. p95 45분 목표는 스테이징에서 DART/KIND가 제공하는 신뢰 가능한 접수시각 또는 별도 최초 관측시각 계약을 확정한 뒤 측정한다.

### 스테이징 계약 테스트가 필요한 항목

- 실제 OpenDART 키로 상세유형 8개와 전체조회가 같은 접수번호를 안정적으로 반환하는지, `rm=정/철` 실제 표본의 의미가 위 계약과 맞는지 확인
- 실제 KIND 어댑터의 오류·데이터 없음·페이지·전체 건수 필드와 거래정지·상장심사 URL 변경 표본 확인
- 조회 중 신규 공시가 들어와 전체 건수가 바뀔 때 실패 후 재시도가 완전한 날짜 창을 만드는지 확인
- 429, 5xx, 연결 시간초과 후 다음 예약 실행과 체크포인트 백필이 누락 없이 복구하는지 확인
- DB의 최초 관측시각으로 공식 소스 수집 지연 p95를 계산하고 `collection_runs.lag_seconds_p95`에 기록하는 운영 계약 확정

종료 코드는 성공 `0`, 수집·동기화 청크 실패 `1`, 설정·체크포인트 오류 `2`다.

## 품질 benchmark JSONL

릴리스 평가기는 다음 두 JSONL을 한 행당 한 객체로 읽는다.

- [`schemas/same-story-pair.schema.json`](schemas/same-story-pair.schema.json): 기사 pair와 `same_story`, `related_but_different`, `different` 라벨
- [`schemas/relevance-event.schema.json`](schemas/relevance-event.schema.json): 기사·안정 event ID와 `relevant`, `not_relevant` 라벨

기사 제목·요약은 원문을 그대로 저장한다. 각 행에는 중복되지 않는 표본 ID, timezone이 있는 기사 시각, `annotator_id`, `labeled_at`, `label_source`가 필요하다. 릴리스 실행에서 인정하는 `label_source`는 `human`과 사람 간 이견 조정을 마친 `adjudicated`뿐이다. 자동 생성·약한 라벨·fixture를 `human`으로 바꾸어 표본 수를 채우면 안 된다.

`tests/fixtures/quality_benchmark`의 작은 JSONL은 스키마와 CLI 실행만 확인하는 합성 fixture다. 실제 기사나 실제 사람 라벨이 아니며 기본 릴리스 실행은 이를 거부한다.

### 평가와 릴리스 기준

평가기는 현재 운영 relevance 분류기와 same-story 클러스터 규칙을 직접 호출한다. 재현성을 위해 same-story 평가 중 외부 AI 호출은 끄지만 나머지 정규화, 회사·사건 토큰, 시간창, 제목·요약 유사도 규칙은 운영 코드와 같다.

기본 게이트는 계획의 정량 목표를 그대로 적용한다.

- same-story: 서로 다른 `pair_id` 500개 이상, 양성·음성 모두 포함, precision 0.97 이상
- relevance: 공식 문서가 연결된 서로 다른 실제 `event_id` 300개 이상, 사람이 확인한 비관련 hard-negative 120개 이상, 양성·음성 모두 포함, precision 0.90·recall 0.95 이상
- 두 작업 모두 precision, recall, F1, accuracy와 confusion matrix를 출력한다.

라벨 데이터가 준비되면 다음과 같이 실행한다.

```powershell
$revision = (git rev-parse HEAD).Trim()
.\.venv\Scripts\python.exe -m curator.quality_benchmark `
  --same-story data/benchmarks/same_story_pairs.jsonl `
  --relevance data/benchmarks/relevance_events.jsonl `
  --environment production `
  --code-revision $revision
```

표본 수가 부족하거나 품질 기준에 미달하면 보고서의 `release_gate_passed`가 `false`가 되고 종료 코드 `1`을 반환한다. JSONL 구조·중복 ID·라벨 출처가 잘못되면 종료 코드 `2`다. 필요할 경우 recall·F1 등 추가 하한을 인자로 강화할 수 있지만 기본 precision/recall 목표를 낮춘 실행은 정식 릴리스 증빙으로 사용하지 않는다.

합성 fixture의 출력 형식만 확인할 때는 반드시 비릴리스 모드를 함께 사용한다. 이 명령은 종료 코드 `0`으로 보고서를 보여주지만 `release_gate_passed`는 표본 부족 때문에 계속 `false`다.

```powershell
.\.venv\Scripts\python.exe -m curator.quality_benchmark `
  --same-story tests/fixtures/quality_benchmark/same_story_pairs.sample.jsonl `
  --relevance tests/fixtures/quality_benchmark/relevance_events.sample.jsonl `
  --allow-fixture-labels --report-only
```

사람 라벨 데이터의 저작권·개인정보·이용권한은 별도로 검토한다. 저장소에는 이번 구현으로 실제 인적 라벨을 만들어 넣지 않았으며, 운영 데이터셋을 비공개로 둘 경우 CI에는 접근 통제된 artifact를 주입해 같은 CLI를 실행한다.

Benchmark 보고서와 14일 shadow·7일 운영·성능 증빙을 합쳐 최종 공개 전환을 판정하는 방법은 [Shadow 비교와 공개 전환 게이트](release-transition-gate.md)를 따른다.
