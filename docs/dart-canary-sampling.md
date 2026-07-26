# DART 정정·철회 canary 표본 검증

`official-backfill` workflow를 `mode=dry-run`, `source=dart|both`로 실행하면
입력한 백필 날짜 창보다 먼저 DART canary를 수행한다. 기존 workflow 입력 계약
(`mode`, `source`, `from_date`, `to_date`, `max_windows`,
`sync_company_master`)은 변경하지 않는다.

## 검증 범위

canary는 실행 시점의 KST 당일처럼 아직 완료되지 않은 날짜를 사용하지 않는다.
직전 완료일을 기준으로 다음 범위를 읽는다.

- 직전 완료일 1일: 실제 DART 응답을 끝까지 pagination하고 production과 같은
  parser 및 payload builder로 정규화한다.
- 직전 완료일을 포함한 최근 365개 완료일: 7일 이하의 작은 창으로 전 범위를
  조회한 뒤 가장 최근의 정정 공시 5건과 철회·취소 공시 5건을 결정적으로
  선정한다.
- 표본 제목, 원문 URL, 원문 언어, 정정·철회 상태를 production payload와
  대조한다. 제목은 번역하거나 정규화해 덮어쓰지 않으며, 저장 제목이 원문과
  한 글자라도 다르면 실패한다.

정정 표본과 철회 표본이 각각 한 건 이상 존재해야 성공한다. 어느 한 종류가
없거나, 응답 page/count가 바뀌거나, 빈 중간 page, parse 오류, truncation이
발생하면 표본을 임의로 보완하지 않고 workflow를 실패시킨다.

## 요청 예산과 상태 020

canary, dry-run, apply 백필, 정기 수집과 회사 마스터 수집은 모두 MySQL의
KST 일자별 전역 OpenDART 요청 원장을 공유한다. 모든 물리 HTTP 시도 직전에
`/api/v1/ops/dart-quota`가 정확히 1회를 원자적으로 승인·차감하며, 실패한 HTTP
시도와 재시도도 각각 1회로 센다. quota API timeout·5xx·불완전 ACK가 발생하면
DART 요청을 보내지 않고 workflow를 실패시킨다. 서로 다른 날짜 범위나 backfill
fingerprint로 다시 실행해도 모든 키를 합산한 KST 일 40,000회 제한을 우회할
수 없다. Canary 한 실행의 별도 안전 예산은 계속 10,000회다.

OpenDART 상태 `020`은 일반 HTTP 재시도 오류로 취급하지 않는다. 원장에 해당
credential만 다음 KST 자정까지 차단한 ACK를 남기고 pool의 다음 유효 키로
동일한 논리 요청을 계속한다. `901`은 해당 credential을 durable disable하고
다음 유효 키로 계속한다. ACK 실패 또는 사용 가능한 키가 없을 때만 workflow를
실패시키며, 모든 키가 `020`으로 차단된 apply 백필은 기존 durable checkpoint에
차단일을 남겨 다음 quota 기간에 같은 window에서 재개한다.

## 비변경 보장과 증빙

`curator.dart_canary_sample`은 DART 읽기와 메모리 내 정규화만 수행한다. MySQL,
원격 PHP API, 로컬·원격 백필 checkpoint를 쓰지 않는다. Telegram outbound도
사용하지 않는다.

workflow artifact `official-backfill-report-<run_id>`에는 다음 파일이 함께
90일간 저장된다.

- `dart-canary-sample-report.json`: 날짜 경계, 실제 요청 수, 수집·정규화 건수,
  자동 선정 표본과 제목 SHA-256, 누락 표본 종류
- `official-backfill-report.json`: 입력 날짜 창의 기존 dry-run/apply 결과
- `official-backfill.stderr.log`: 기존 backfill 오류 로그

로컬에서 같은 canary만 실행하려면 다음 명령을 사용한다.

```powershell
$env:OPENDART_API_KEYS = "<lowercase-40hex-key>,<lowercase-40hex-key>"
python -m curator.dart_canary_sample `
  --lookback-days 365 `
  --scan-chunk-days 7 `
  --sample-limit-per-kind 5 `
  --request-budget 10000 `
  --report dart-canary-sample-report.json
```

`OPENDART_API_KEYS`는 줄바꿈 또는 쉼표 구분을 지원하며 pool이 우선이다.
`DART_API_KEY`는 pool이 없는 단일 키 호환 fallback으로만 사용한다. 두 값을
동시에 설정하면 fail-closed한다. GitHub Actions에서는 collector보다 먼저 각
키를 개별 mask하며 키 원문과 요청 URL을 출력하지 않는다.

성공 종료 코드는 `0`, 필수 표본 미확보는 `1`, 설정·connector·quota·예산 오류는
`2`다.
