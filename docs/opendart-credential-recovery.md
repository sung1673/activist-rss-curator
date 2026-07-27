# OpenDART 인증키 901 복구 절차

OpenDART 공시검색이 `status=901`을 반환하면 해당 credential을 durable
disable하고 같은 키로 재시도하지 않는다. pool에 다른 유효 키가 있으면 동일한
논리 요청은 다음 키로 계속한다.
OpenDART 개발가이드에서 901은 사용자 계정의 개인정보 보유기간이 만료되어
인증키를 사용할 수 없는 상태로 정의한다.

1. OpenDART에 로그인해 사용자 정보와 개인정보 보유 동의를 갱신한다.
2. `인증키 신청/관리`에서 기존 키의 사용 가능 상태를 확인한다.
3. 기존 키가 복구되지 않으면 새 인증키를 발급한다.
4. 보호된 `governance-runtime` 환경의 `OPENDART_API_KEYS`에서 비활성 키를
   제거하고 새로 발급된 키를 등록한다. 값은 줄바꿈 또는 쉼표로 구분한 중복
   없는 소문자 40자리 hex 목록이다. `DART_API_KEY`는 pool이 없는 단일 키
   호환 환경에서만 새 값으로 교체한다. 키를 이슈, Actions 입력, 로그, 문서,
   커밋에 기록하지 않는다. Actions는 각 pool 키를 collector 전에 개별 mask한다.
   `BSIDE_BACKEND_BINDING_ID` 변수는 배포된 PHP가 반환하는 운영 MySQL
   바인딩 값과 같아야 하며, 인증키 교체 과정에서 수정하지 않는다.
5. `Official disclosure backfill`을 최근 완료일 1일 범위의 `dry-run`으로
   실행한다. 이 단계까지는 `GOVERNANCE_PIPELINE_MODE=off`와 v1·v2
   `release_state=closed`를 유지한다.
6. dry-run이 성공하면 v1·v2가 계속 `closed`인지 확인하고
   `GOVERNANCE_PIPELINE_MODE=dart_canary`로 변경한다. `apply`는 `off`에서
   실행할 수 없으며 SourceRight preflight가 데이터 변경 전에 실패한다.
7. 성공한 같은 범위를 `apply`로 실행한 뒤 동일 입력을 한 번 더 실행해
   회사·문서·사건·checkpoint가 증가하지 않는지 확인한다.

키 복구와 1일 dry-run 확인 전에는 `GOVERNANCE_PIPELINE_MODE=off`를 유지하고
DART workflow를 반복 실행하지 않는다. 각 시도는 성공 여부와 무관하게 운영
quota ledger의 물리 요청 1건으로 기록될 수 있다. dry-run 성공 뒤에만
`dart_canary`로 변경해 위 1일 apply·동일 입력 재실행을 검증한다. apply 검증이
실패하면 `DART_OFFICIAL_INGEST_ENABLED=false`로 닫고 원인을 해결하기 전까지
추가 apply를 실행하지 않는다.
원장은 모든 키를 합산해 KST 하루 40,000건을 허용하고 단일 실행은 10,000건으로
제한한다. `020`은 해당 키만 다음 KST 자정까지 차단하지만 `901` disable은
자정에 자동 해제되지 않으므로 복구된 계정의 새 키로 pool을 교체해야 한다.
quota ACK의 `backend_binding_id`가 GitHub 변수와 다르면 수집기는 OpenDART
네트워크 요청 전에 fail-closed한다.

공식 오류 코드:
<https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019001>
