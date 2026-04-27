# 한국어 뉴스 큐레이션용 정제 RSS 생성기

Google Alerts RSS를 주기적으로 가져와 오래된 기사, 중복 기사, 관련도 낮은 기사를 제거하고 유사 기사들을 하나의 묶음으로 만든 뒤 `public/feed.xml`을 생성합니다. 선택적으로 Telegram Bot API를 사용해 정제된 묶음을 채널에 직접 발행할 수 있습니다.

## 구조

기존 구조:

```text
Google Alerts RSS -> rss2tg_bot -> Telegram channel
```

변경 구조:

```text
Google Alerts RSS -> GitHub Actions 정제 -> GitHub Pages feed.xml -> rss2tg_bot -> Telegram channel
```

직접 발행 구조:

```text
Google Alerts RSS -> GitHub Actions 정제 -> Telegram Bot API -> Telegram channel
```

AI 데일리 리뷰 구조:

```text
Google Alerts/Google News RSS -> GitHub Actions 정제 -> GitHub Models daily digest -> Telegram Bot API -> Telegram channel
```

RSS item 1개가 cluster 1개이며, item description 안에 유사 기사 여러 링크가 들어갑니다. 직접 발행을 켜면 Telegram 메시지는 HTML 링크 서식을 사용해 긴 기사 URL 대신 `매체명 - 제목` 형태의 클릭 가능한 줄로 표시합니다. 기업명이 추정되는 묶음은 `신한금융`, `고려아연`처럼 메시지 안에서 하위 그룹으로 나눠 표시합니다.

## 설치

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 로컬 실행

```bash
python -m curator.main
```

실행 후 아래 파일이 갱신됩니다.

- `public/feed.xml`
- `public/index.html`
- `data/state.json`

테스트는 다음처럼 실행합니다.

```bash
pytest
```

## 설정

`config.yaml`에서 날짜 필터, clustering buffer, 중복 기준, 발행 개수를 조정합니다.

Google Alerts RSS URL은 public repo에 직접 저장하지 말고 GitHub Actions Secret으로 관리하는 것을 권장합니다.
`feed.xml`의 channel link에는 원본 Alert URL을 쓰지 않고, 필요하면 `public_feed_url`에 GitHub Pages의 공개 feed URL만 넣습니다.
`config.yaml`의 `feeds`에는 공개되어도 괜찮은 Google News 보조 RSS를 둘 수 있습니다. `CURATOR_FEEDS` Secret이 있어도 보조 RSS는 함께 수집되며, 비공개 Google Alerts RSS가 먼저 처리됩니다.

Secret 이름:

```text
CURATOR_FEEDS
TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID
```

여러 Google Alerts RSS URL은 쉼표 또는 줄바꿈으로 구분합니다.

```text
https://www.google.com/alerts/feeds/...
https://www.google.com/alerts/feeds/...
```

Telegram 직접 발행을 사용할 때 bot token은 절대 `config.yaml`이나 workflow 파일에 직접 쓰지 말고 `TELEGRAM_BOT_TOKEN` Secret에 저장합니다. 채널 username을 공개해도 괜찮다면 `config.yaml`의 `telegram.chat_id`를 사용할 수 있고, 숨기고 싶다면 `TELEGRAM_CHAT_ID` Secret에 `@channel_username` 또는 numeric chat id를 저장합니다.

`config.yaml`에는 공개 가능한 Google News 보조 RSS를 두 축으로 추가할 수 있습니다.

- 국내: 주주제안, 행동주의 주주, 소액주주연대, 지배구조, 밸류업, 자사주 소각, 스튜어드십, 자본시장법/상법, 임원보수 공시, 코너스톤 투자자, ETF 의결권, 해외부동산펀드 위험설명서 등
- 해외: `South Korea Value-up Program`, `Korea discount`, `shareholder activism`, `proxy fight`, `activist investor campaign`, `open letter`, `universal proxy` 등

보조 RSS 검색어는 개별 기업명이나 특정 펀드명보다 이벤트와 제도 키워드 중심으로 구성합니다. 기업명 후보 목록은 검색용이 아니라 이미 수집된 기사들을 묶기 위한 내부 규칙으로만 사용합니다.

## GitHub Actions

`.github/workflows/build-feed.yml`은 다음을 수행합니다.

- KST 07:00-23:45에는 15분마다 실행
- KST 00:00-06:00에는 1시간마다 실행
- Python 3.12 설치
- `requirements.txt` 설치
- `python -m curator.main` 실행
- `public/feed.xml`, `public/index.html`, `data/state.json` 변경 시 commit & push

GitHub Models를 사용하는 daily digest는 workflow의 `models: read` 권한과 자동 제공되는 `GITHUB_TOKEN`을 사용합니다. 별도 OpenAI API key는 필요하지 않으며, 호출이 실패하면 fallback 리뷰로 계속 실행됩니다.

수동 실행도 `workflow_dispatch`로 가능합니다. 수동 실행 화면에서 `Send a Telegram smoke-test message`를 켜면 실제 뉴스 발행과 별개로 테스트 메시지 1건을 채널에 보내 bot token과 채널 관리자 권한을 확인할 수 있습니다.
`Send a daily digest preview message`를 켜면 최근 24시간 기준 daily digest 미리보기를 채널에 전송합니다. `Digest preview prefix`에 `NONE`을 넣으면 미리보기 접두어 없이 재발송할 수 있습니다.

## GitHub Pages

Repository Settings의 Pages 메뉴에서 배포 source를 설정합니다. GitHub Pages가 repository root 기준 폴더 선택을 지원하는 경우 `public` 폴더를 source로 지정합니다. 환경에 따라 root 또는 `docs`만 선택 가능한 저장소라면 Pages 설정에 맞게 공개 폴더를 조정하거나 `public` 결과물을 별도 배포 workflow로 publishing하면 됩니다.

배포 후 `feed.xml` URL은 보통 다음 형식입니다.

```text
https://<owner>.github.io/<repo>/feed.xml
```

## rss2tg_bot 등록

`rss2tg_bot`에서 GitHub Pages에 공개된 `feed.xml` URL을 구독 URL로 등록합니다.

```text
https://<owner>.github.io/<repo>/feed.xml
```

`rss2tg_bot`은 RSS item 단위로 메시지를 발행하므로, 이 프로젝트는 묶음 1개를 RSS item 1개로 만듭니다.

권장 설정:

- output format: `full article (experimental)`
- `disable web page previews`: 활성화
- `show the source name`: 선택 사항

RSS 본문에는 기사 1건을 한 줄로 표시합니다. rss2tg_bot이 본문 HTML 링크를 보존하지 않는 경우가 있어, 기사 목록에는 `mk.co.kr` 같은 도메인 대신 `매일경제` 같은 출처명을 표시합니다. RSS item link에는 GitHub Pages 중간 링크가 아니라 원문 기사 URL을 사용합니다. `msn.com`처럼 원문 확인이 어려운 중계 링크는 수집 단계에서 제외합니다.

## Telegram 직접 발행

직접 발행을 사용하면 `rss2tg_bot` 없이 이 프로젝트가 Telegram Bot API로 채널에 메시지를 보냅니다. 메시지는 긴 URL을 직접 노출하지 않고 HTML 링크로 표시합니다. 키워드 기반 섹션 라벨은 오분류 가능성이 있어 메시지에 표시하지 않으며, 내부 분류값이나 기준시각, 대표기사보기 링크도 표시하지 않습니다. 단일 기사 업데이트는 제목을 상단에 중복 표시하지 않고 링크 아래에 짧은 본문 미리보기를 붙이며, Telegram 웹페이지 preview가 표시되도록 전송합니다.

필수 조건:

- BotFather에서 만든 bot token을 `TELEGRAM_BOT_TOKEN` Secret에 저장
- bot을 채널 관리자에 추가
- `config.yaml`의 `telegram.chat_id` 또는 `TELEGRAM_CHAT_ID` Secret 설정

처음 Secret을 연결한 실행에서는 기존 published cluster를 발송하지 않도록 기준선으로만 저장합니다. 이후 새로 published 되는 cluster부터 전송합니다. 전송한 guid는 `data/state.json`의 `telegram_sent_cluster_guids`에 저장되어 중복 발송을 막습니다.

bot 연결만 즉시 확인하려면 Actions의 `Build curated RSS feed` 수동 실행에서 `Send a Telegram smoke-test message` 옵션을 켭니다.

## AI 데일리 리뷰

일반 기사 묶음 메시지는 AI를 호출하지 않고 제목과 기사 링크만 발행합니다. GitHub Models는 매일 아침 리뷰에만 사용하며, 기본 모델은 `openai/gpt-4.1`입니다.

매일 KST 06:30-06:59 사이 첫 실행에서 최근 24시간의 published/pending cluster를 모아 `데일리 거버넌스 리뷰`를 전송합니다. 07:00 전에 도착하도록 06:30 실행에서 먼저 생성하며, 리뷰는 짧은 bullet 요약과 국내/해외 기사 링크 목록으로 구성됩니다. 비슷한 제목과 핵심 토큰을 가진 기사는 대표 제목 아래 여러 언론사 링크로 묶어 보여줍니다. 이미 보낸 날짜는 `data/state.json`의 `daily_digest_sent_dates`에 저장해 중복 전송을 막습니다.

## 운영 정책

- 새 cluster는 pending 상태로 시작합니다.
- 기본 45분 buffer 후 묶음으로 발행합니다.
- high relevance cluster는 20분 후 발행 가능합니다.
- pending 상태가 3시간을 넘으면 강제 발행합니다.
- article published date 기준 7일 초과 기사는 제외합니다.
- 발행 대상은 high, medium relevance입니다.
- low relevance 기사는 `state.json`의 `rejected_articles`에 저장합니다.
- `msn.com` 같은 중계 링크는 기본적으로 제외합니다.
- 이미 published된 cluster에 유사 기사가 나중에 들어오면 기존 item을 수정하지 않고 `[추가 N건] ...` follow-up cluster로 새 guid를 발행합니다.
- `feed.xml`에는 최근 published cluster 50개만 유지합니다.
- Telegram 직접 발행은 전송 성공한 cluster guid를 state에 저장하고, 이미 보낸 cluster는 다시 보내지 않습니다.
- 매일 KST 06:30에는 최근 24시간 묶음을 데일리 digest로 별도 발행합니다.

## 한계

- `rss2tg_bot`이 메시지 렌더링을 최종 결정하므로 정확한 표시 형식은 bot 설정에 따라 달라질 수 있습니다.
- 너무 긴 description은 텔레그램에서 분할될 수 있습니다. 기본 제한은 3500자입니다.
- 이미 발행된 텔레그램 메시지를 RSS만으로 안정적으로 수정하는 것은 기대하지 않습니다.
- GitHub Actions 기반 Telegram 발행은 대화형 봇이 아니라 예약 실행형 발행 봇입니다. 채널에 명령어를 보내 즉시 설정을 바꾸는 용도에는 맞지 않습니다.
- GitHub Models 호출 한도나 권한 문제로 daily digest 생성이 실패할 수 있으며, 이 경우 fallback 리뷰로 발행합니다.
- 기업명 추정은 단순 규칙 기반입니다. `extract_company_candidates()`를 확장해 개선할 수 있습니다.
