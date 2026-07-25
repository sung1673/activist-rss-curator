# KIND 연동 결정과 외부 선행조건

기준일: 2026-07-16

## 결정

`KIND_DISCLOSURE_ENDPOINT`에는 KIND HTML 화면이나 RSS 주소를 넣지 않는다. 현재 공개 KRX OPEN API 서비스 목록에는 공시 API가 없고, KIND의 공식 RSS는 당일 보조 피드여서 이 저장소가 요구하는 기간 조회·pagination·완전성·DART `corp_code` 계약을 충족하지 못한다.

검증된 어댑터가 준비될 때까지 다음 원칙을 적용한다.

- Production Alpha 예약 공식 수집은 `KIND_CONNECTOR_MODE=off`를 기본으로 DART-only 실행한다.
- KIND를 GA 범위에 편입할 때 `KIND_CONNECTOR_MODE=active`로 전환하며, 그때부터 예약 실행은 endpoint·권한·계약을 필수로 요구하고 하나라도 다르면 fail-closed한다.
- 수동 workflow는 `include_kind=true`로 예약 토글과 별개인 KIND 검증을 실행하거나 `include_kind=false`로 DART-only smoke를 실행할 수 있다.
- KIND 화면 scraping이나 비공식 사설 라이브러리를 운영 경로에 넣지 않는다.
- 공개된 사실과 KRX 화면·데이터베이스의 자동수집·재배포 조건은 별도로 확인한다.

## 공식 경로 조사

- [KRX OPEN API 서비스 목록](https://openapi.krx.co.kr/contents/OPP/INFO/service/OPPINFO004.cmd)은 지수·주식·증권상품·채권·파생·일반상품·ESG를 제공하며 공시는 열거하지 않는다.
- [KRX OPEN API 이용방법](https://openapi.krx.co.kr/contents/OPP/INFO/OPPINFO003.jsp)은 회원가입, 인증키 발급, 서비스 활용 신청·승인과 `AUTH_KEY` 요청 헤더를 안내한다.
- [KRX OPEN API 2026-06-01 공지](https://openapi.krx.co.kr/contents/OPP/COMM/notice/OPPCOMM001_S2.cmd?bbsSeq=5)는 제공 목록에 없는 데이터는 화면 검색·다운로드 또는 데이터 구매를 이용하고 비공식 사설 라이브러리를 사용하지 말라고 안내한다.
- [KIND 오늘의 공시](https://kind.krx.co.kr/disclosure/todaydisclosure.do?method=searchTodayDisclosureMain)는 공식 RSS를 제공하지만 당일 보조 피드다.
- [KRX Data Marketplace 데이터 구입 안내](https://data.krx.co.kr/contents/MDC/INFO/informationController/MDCINFO008.cmd)는 공개 API 밖의 역사·증분 데이터 협의 경로다.
- [KRX 법적고지](https://info.krx.co.kr/contents/KRX/06/06070200/KRX06070200.jsp)는 서비스 자료의 이용 조건을 별도로 고지한다.

## 내부 JSON 어댑터 계약

어댑터는 다음 query를 받아야 한다.

```text
GET /disclosures
  ?start_date=YYYY-MM-DD
  &end_date=YYYY-MM-DD
  &page=1
  &page_size=100
Authorization: Bearer <optional adapter token>
```

응답은 `page`, `total_pages`, 가능하면 `total_count`, `items`를 포함한다. 각 행은 안정 접수번호, 8자리 DART `corp_code`, 회사명, 원문 제목, 접수 일시, 원문 URL을 제공해야 한다. 수집기와 preflight는 접수 일시까지 파싱하며 API key를 URL query에 넣지 않는다.

KIND RSS를 보조 증분 경로로 사용할 경우 내부 어댑터가 5~15분 polling, 접수번호 중복 제거, DART 회사 마스터 결합, 모호한 회사 mapping 차단, 자체 날짜 범위·pagination 저장소를 제공해야 한다. 2021년 이후 백필은 KRX가 허용한 Excel 또는 구매 데이터로 별도 적재한다.

## 외부 확인 항목

KRX에 다음을 확인한 뒤 어댑터와 `SourceRight`를 활성화한다.

- 2021년 이후 공시·시장조치 역사 데이터
- 증분 또는 일 단위 전달 방식과 완전성 기준
- 접수번호, 종목코드, 회사 식별자, 접수시각, 제목, 정정·취소 상태
- 내부 저장·AI 처리 범위
- 사실·메타데이터·원문 링크의 공개 서비스 제공 범위
