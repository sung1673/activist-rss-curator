# 운영 모바일 Web Vitals 수집

`Governance mobile Web Vitals` workflow는 shadow·live 기간에 매일 KST 23:00 실행된다. 다음 날 KST 00:35 production evidence input 수집보다 95분 먼저 시작하므로, 완료된 KST 일자의 실제 브라우저 표본이 evidence exporter에 포함된다.

## 측정 계약

- HTML SPA journey는 `/today`, `/events`, `/issuers`, `/calendar` 네 route template으로 고정한다. watchdog의 `/`, `/governance/`, `/feed.xml`, `/api/v1/health` availability route와 섞지 않는다.
- 각 route마다 새 Pixel 5 모바일 Chromium context를 5회 실행한다. 총 20회 journey에서 LCP·INP·CLS 60개 관측값을 얻는다.
- LCP와 CLS는 route의 초기 렌더가 끝난 뒤 고정한다. 이어서 실제 primary-navigation 링크를 Playwright가 클릭하고 `PerformanceEventTiming.interactionId`가 있는 실제 Event Timing duration을 INP로 기록한다.
- LCP, Layout Shift 또는 Event Timing observer가 없거나, LCP·INP 값이 생성되지 않거나, route가 API 오류 화면을 렌더하면 전체 실행을 실패시킨다. 누락 값을 0이나 임의 값으로 보완하지 않는다. CLS 0은 실제 layout shift가 없을 수 있으므로 유효하다.
- 한 workflow 실행의 모든 관측은 같은 KST 날짜여야 한다. 자정을 넘어가면 서로 다른 날의 표본을 섞지 않고 실패한다.
- API에는 `source=first_party`, `device_class=mobile`, route template, metric, value, 측정시각, full 40자리 build SHA만 보낸다. 60개 관측은 API 상한에 맞춰 50개와 10개 두 batch로 전송하며 각 `accepted_count`를 완전 대조한다.

## SHA·인증·개인정보 안전장치

수집기는 먼저 배포된 `/governance/config.js`를 읽는다. 그 안의 full build SHA가 checkout한 default-branch Git SHA와 완전히 같고, 공개 API base도 workflow 설정과 같을 때만 브라우저를 실행한다. short SHA, 이전 배포, 다른 API base는 모두 fail-closed다.

preview token은 URL query, Pages asset, artifact, console 출력에 넣지 않는다. 각 브라우저 context의 `sessionStorage`에 init script로만 주입하며 trace·video·screenshot은 만들지 않는다. 앱 내장 RUM 요청은 probe context에서 차단하고, 검증된 60개 batch만 수집기가 `Authorization: Bearer`로 전송한다. 결과 artifact에는 token이나 URL이 없고 날짜·SHA·route·표본 및 ACK 건수만 30일 보존한다.

workflow는 `curator.operation_mode`를 먼저 실행한다. `ENABLE_TELEGRAM_DELIVERY=false`와 `ENABLE_GOVERNANCE_DELIVERY=false`가 아니면 측정도 전송도 하지 않는다.

## 수동 검증

default branch에서만 `workflow_dispatch`할 수 있다. 필요한 설정은 다음과 같다.

- Secret `GOVERNANCE_PREVIEW_TOKEN`
- Secret `BSIDE_API_BASE_URL` 또는 Variable `GOVERNANCE_API_BASE_URL`
- Variable `BSIDE_PUBLIC_WEB_URL` (없으면 `https://news.bside.ai`)
- Variable `GOVERNANCE_PIPELINE_MODE=shadow|live`
- Variables `ENABLE_TELEGRAM_DELIVERY=false`, `ENABLE_GOVERNANCE_DELIVERY=false`

실행 성공 기준은 `web-vitals-run-summary.json`의 `observation_count=60`, `accepted_count=60`, `api_batch_sizes=[50,10]`, 네 route와 세 metric, 동일 build SHA가 모두 존재하는 것이다.
