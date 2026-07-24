# DART 30일 사람 검수 표본

`Official disclosure backfill`의 `apply` 실행이 정확히 30개의 완료된 KST 일자를
성공적으로 적재하면, workflow는 운영 DB에서 DART 문서 corpus를 다시 읽어 사람
검수용 100건 표본을 생성한다.

부분 실행이나 30일이 아닌 정상 백필은 표본 생성 대상이 아니므로
`not_applicable`로 기록하고 백필 자체는 성공 처리한다. 반대로 정확히 30일을
완료한 `apply` 실행은 표본 생성 대상이며, 아래 증빙 조건이나 표본 계약을
충족하지 못하면 백필 workflow를 실패시킨다.

## 생성 조건

- 날짜 범위는 `[from_date, to_date)` 반개구간이며 정확히 30일이어야 한다.
- 모든 일별 checkpoint가 성공 상태이고 failed window가 없어야 한다.
- 각 window의 raw/ACK 수가 같고, ACK 합계가 API corpus 전체 건수와 같아야 한다.
- 각 window에는 실행 시점의 7~40자 소문자 16진수 `code_revision`이 기록되며,
  30개 window와 표본 exporter의 `code_revision`이 모두 같아야 한다. 누락되거나
  서로 다른 revision이 섞이면 증빙 생성을 거부한다.
- corpus의 각 DART 문서는 정확히 하나의 canonical 사건과 연결되어야 한다.
- API pagination 전체의 건수와 SHA-256 digest가 일치해야 한다.
- corpus API의 `backend_binding_id`가 적재 시 사용한 운영 MySQL 바인딩과
  일치해야 한다.
- 조건 하나라도 맞지 않으면 표본을 만들지 않고 workflow를 실패시킨다.

## 표본 방식

고정 seed `20260724`를 사용해 `event_type × revision_status` 층을 순환하고, 각
층 안에서는 회사를 round-robin으로 순환한다. 같은 입력·seed·코드에서는 항상
같은 100건이 선택된다. 정정 상태는 다음 여섯 가지다.

- `current`
- `original_superseded`
- `correction_linked`
- `correction_unlinked`
- `withdrawal_linked`
- `withdrawal_unlinked`

생성물은 UTF-8 JSONL, Excel 호환 UTF-8 BOM CSV, manifest JSON이다. manifest는
corpus·표본·checkpoint·backfill report·운영 DB 바인딩·각 파일의 digest와
분포를 기록한다.
CSV의 `review_outcome`, `review_note`는 두 명의 독립 검수자가 작성하기 위한 빈
필드다. JSONL은 원문 문자열을 그대로 보존한다. CSV는 스프레드시트가
`=`, `+`, `-`, `@` 또는 선행 공백·탭 뒤의 해당 문자를 수식으로 실행하지
않도록 위험한 셀 앞에 작은따옴표를 붙인다.

이 artifact 자체는 출시 승인 자료가 아니다. manifest의 `release_eligible`은
항상 `false`이며, 독립 검수와 불일치 합의가 완료된 결과만 후속 benchmark
evidence에 포함한다.

## 운영 보안

corpus API는 ops/admin 인증에만 열리고 공개 release state가 `closed`여도 운영
검증을 위해 접근할 수 있다. 응답에는 내부 `payload_json`, DB 식별자, 인증정보를
포함하지 않는다. GitHub workflow는 공개 API URL과 운영 URL을 canonical 비교한
뒤에만 호출한다.
