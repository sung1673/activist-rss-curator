# OpenDART 인증키 901 복구 절차

OpenDART 공시검색이 `status=901`을 반환하면 수집 코드를 재시도하지 않는다.
OpenDART 개발가이드에서 901은 사용자 계정의 개인정보 보유기간이 만료되어
인증키를 사용할 수 없는 상태로 정의한다.

1. OpenDART에 로그인해 사용자 정보와 개인정보 보유 동의를 갱신한다.
2. `인증키 신청/관리`에서 기존 키의 사용 가능 상태를 확인한다.
3. 기존 키가 복구되지 않으면 새 인증키를 발급한다.
4. GitHub 저장소의 `DART_API_KEY` Secret만 새 값으로 교체한다. 키를 이슈,
   Actions 입력, 로그, 문서, 커밋에 기록하지 않는다.
   `BSIDE_BACKEND_BINDING_ID` 변수는 배포된 PHP가 반환하는 운영 MySQL
   바인딩 값과 같아야 하며, 인증키 교체 과정에서 수정하지 않는다.
5. `Official disclosure backfill`을 최근 완료일 1일 범위의 `dry-run`으로
   실행한다.
6. 성공한 같은 범위를 `apply`로 실행한 뒤 동일 입력을 한 번 더 실행해
   회사·문서·사건·checkpoint가 증가하지 않는지 확인한다.

복구 확인 전에는 `GOVERNANCE_PIPELINE_MODE=off`를 유지하고 DART workflow를
반복 실행하지 않는다. 각 시도는 성공 여부와 무관하게 운영 quota ledger의
물리 요청 1건으로 기록될 수 있다. 키 복구 뒤 위 1일 dry-run과 apply 검증을
마칠 때만 `dart_canary`로 변경한다.
quota ACK의 `backend_binding_id`가 GitHub 변수와 다르면 수집기는 OpenDART
네트워크 요청 전에 fail-closed한다.

공식 오류 코드:
<https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019001>
