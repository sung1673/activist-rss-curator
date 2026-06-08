# Story Review Benchmark 운영 메모

`public/feed/story-review.html`은 관리자 token gate로 열리는 운영 검수 페이지입니다.

## 표시 항목

- 묶음 후보: 데일리에서 서로 다른 이슈로 분리됐지만 제목, 본문, 회사, 이벤트 토큰상 같은 story로 묶였어야 할 가능성이 있는 후보입니다.
- Benchmark 누락 점검: `telegram_sources.candidate_source_handles`에 등록한 기준 공개 채널이 공유한 기사 URL 중 데일리 story에 반영되지 않은 URL을 보여줍니다.

Benchmark 누락 항목에는 다음 정보가 표시됩니다.

- 원문 기사 URL
- Telegram 원문 링크
- 공유 채널
- state 처리 상태: `rejected`, `duplicate`, `accepted`, `not_in_state`
- rejection 또는 duplicate reason이 있으면 해당 사유

## Telegram 알림

매일 story-review Telegram 메시지는 분리 후보 수와 benchmark 누락률을 함께 요약합니다.

분리 후보가 없어도 benchmark 누락 URL이 있으면 운영 메시지를 발송합니다. 이 메시지의 관리자 링크를 통해 `story-review.html`에 접근해 누락 URL과 묶음 후보를 함께 확인합니다.

## 운영 기준

- `@activistkorea`처럼 기준 채널로 삼을 공개 채널은 `telegram_sources.candidate_source_handles`에 넣습니다.
- benchmark coverage는 해당 기준 채널의 URL 공유가 데일리 story에 실제 반영됐는지를 보는 운영 KPI입니다.
- 누락 URL이 많으면 수집 누락, relevance 탈락, date filter, duplicate 처리, story clustering 문제를 분리해 확인해야 합니다.
