# Story Review Benchmark 운영 메모

Story Review 결과는 내부 편집 검수 데이터입니다. 정적 `public/feed/story-review.html`과 관련 메타데이터는 클라이언트 측 token gate만으로 후보 내용을 안전하게 보호할 수 없으므로 공개 Pages artifact에 배포하지 않습니다. 인증된 서버 측 편집 UI가 마련될 때까지 로컬 또는 접근 통제된 내부 환경에서만 확인합니다.

## 표시 항목

- 묶음 후보: 데일리에서 서로 다른 이슈로 분리됐지만 제목, 본문, 회사, 이벤트 토큰상 같은 story로 묶였어야 할 가능성이 있는 후보입니다.
- Benchmark 누락 점검: `telegram_sources.candidate_source_handles`에 등록한 기준 공개 채널이 공유한 기사 URL 중 데일리 story에 반영되지 않은 URL을 보여줍니다.

Benchmark 누락 항목에는 다음 정보가 표시됩니다.

- 원문 기사 URL
- Telegram 원문 링크
- 공유 채널
- state 처리 상태: `rejected`, `duplicate`, `accepted`, `not_in_state`
- rejection 또는 duplicate reason이 있으면 해당 사유

## 검수 접근

Story Review 알림과 관리자 링크를 Telegram 비공개 채팅으로 발송하지 않으며 token도 메시지나 URL로 전달하지 않습니다. 공개 Telegram 채널 발송은 별도 콘텐츠 경로로 유지됩니다. 검수자는 접근 통제된 내부 환경에서 명시적으로 등록된 token을 직접 입력해 분리 후보와 benchmark 누락 항목을 확인합니다.

## 운영 기준

- `@activistkorea`처럼 기준 채널로 삼을 공개 채널은 `telegram_sources.candidate_source_handles`에 넣습니다.
- benchmark coverage는 해당 기준 채널의 URL 공유가 데일리 story에 실제 반영됐는지를 보는 운영 KPI입니다.
- 누락 URL이 많으면 수집 누락, relevance 탈락, date filter, duplicate 처리, story clustering 문제를 분리해 확인해야 합니다.
