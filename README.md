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

RSS item 1개가 cluster 1개이며, item description 안에 유사 기사 여러 링크가 들어갑니다. 직접 발행을 켜면 Telegram 메시지는 HTML 링크 서식을 사용해 긴 기사 URL 대신 클릭 가능한 기사 제목으로 표시합니다. 한 실행에서 여러 cluster가 발행될 때는 개별 메시지로 흩뿌리지 않고 digest 스타일의 `주주·자본시장 브리핑` 한 묶음으로 발행합니다.

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
- `public/404.html`
- `public/feed/latest.html`
- `public/feed/<YYYY-MM-DD>.html`
- `data/state.json`
- `data/archive/articles/<YYYY-MM-DD>.jsonl`
- `data/archive/index.json`

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

- 국내: 주주제안, 행동주의 주주, 소액주주연대, 지배구조, 밸류업, 자사주 소각, 자사주 취득 후 소각, 상법 개정, 일반주주 의결권, 스튜어드십, 금융회사 지배구조, 사외이사, 성과보상, 자본시장법/상법, 상장폐지, 상장적격성 실질심사, 거래정지 개선기간, 의무공개매수, CB/EB, 전환사채 리픽싱, STO 제도화, 증권사 IB, 임원보수 공시, 코너스톤 투자자, ETF 의결권, 해외부동산펀드 위험설명서 등
- 해외: `South Korea Value-up Program`, `Korea discount`, `shareholder activism`, `proxy fight`, `activist investor campaign`, `open letter`, `universal proxy` 등

보조 RSS 검색어는 개별 기업명이나 특정 펀드명보다 이벤트와 제도 키워드 중심으로 구성합니다. 기업명 후보 목록은 검색용이 아니라 이미 수집된 기사들을 묶기 위한 내부 규칙으로만 사용합니다.

유사 텔레그램 채널은 기사 원문을 재게시하기 위한 소스가 아니라, 반복적으로 등장하는 자본시장 이슈를 포착하는 레이더로 사용합니다. 1차 확장 키워드는 자사주 제도, 상법/공시, 일반주주 의결권, 경영권 분쟁, 스튜어드십, 밸류업 지수/ETF, CB/EB, STO, 증권사 IB처럼 기사형 뉴스로 이어질 가능성이 높은 항목부터 좁혀 반영합니다.

Digest의 기사 개수 관련 설정값은 `0`이면 무제한으로 처리합니다. 텔레그램 API의 메시지 길이 제한은 피할 수 없으므로, 전체 digest가 길어지면 여러 메시지로 나누어 전송합니다. 이때 단순 글자 수가 아니라 `주주행동·거버넌스`, `자본시장·공시·상장`, `영문` 같은 큰 카테고리와 기사 묶음 단위로 분할합니다. 중복으로 걸러진 기사는 여러 매체 링크를 펼치지 않고 대표 기사 1개만 골라 일반 기사처럼 표시합니다.

## 우선순위와 아카이브

`state.json`은 운영 캐시이고, 장기 기사 관리는 `data/archive`가 담당합니다. 매 실행마다 수집·중복·거절된 기사에 `priority_score`, `priority_level`, `priority_reasons`, `story_key`를 붙인 뒤 `data/archive/articles/YYYY-MM-DD.jsonl`에 upsert합니다. 이 구조는 GitHub만으로 운영 가능하면서도 나중에 SQLite, Postgres, Turso 같은 DB로 옮기기 쉬운 레코드 단위 형식입니다.

우선순위 레벨은 다음처럼 사용합니다.

- `top`: 당일 상단 배치 또는 수동 확인이 필요한 핵심 기사
- `watch`: 후속 기사 추적 대상
- `normal`: 일반 발행 대상
- `archive`: 보관은 하지만 우선 노출하지 않는 기사
- `suppress`: 보관만 하고 우선순위 표면에서는 제외

수동 조정은 `data/priority_overrides.yaml`에서 관리합니다. 공개 repo에 올라가도 되는 편집 규칙만 넣고, 민감한 메모나 비공개 판단 근거는 넣지 않습니다. 예를 들어 특정 URL hash, story key, 제목 키워드에 `score_delta`, `level`, `suppress`, `reasons`를 지정할 수 있습니다.

## GitHub Actions

`.github/workflows/build-feed.yml`은 다음을 수행합니다.

- KST 08:05~00:35에는 30분마다 실행 (`:05`, `:35`)
- KST 01:00~06:00 야간 구간은 03:35, 06:05 두 번으로 나누어 묶음 발행
- KST 07:00대 실행은 건너뜀
- Python 3.12 설치
- `requirements.txt` 설치
- 필요할 때만 `python -m curator.main` 실행
- `public/feed.xml`, `public/index.html`, `public/404.html`, `public/feed`, `data/state.json`, `data/archive` 변경 시 commit & push

Push 실행은 변경 파일을 보고 자동으로 모드를 나눕니다.

- `full`: 기사 수집, 중복 제거, 묶음화, RSS/Telegram 업데이트를 모두 실행합니다. `config.yaml`, `requirements.txt`, 수집/분류/중복/발송 관련 `curator/*.py`, 관련 테스트가 바뀐 경우에만 사용합니다.
- `page_only`: 기존 `data/state.json`을 기반으로 데일리 HTML과 Pages 산출물만 다시 만듭니다. `curator/daily_report.py`, `tests/test_daily_report.py`, `public/404.html` 같은 레이아웃/템플릿 변경은 기사 수집을 건너뜁니다.

수동 실행의 `run_mode`에서 `full` 또는 `page_only`를 직접 고를 수 있습니다. 기본 `auto`는 수동 실행에서는 기존 운영과 동일하게 full로 처리됩니다. 커밋 메시지에 `[page-only]`를 넣으면 강제로 page-only, `[force-collect]` 또는 `[send-regular-update]`를 넣으면 강제로 full로 실행합니다.

GitHub Models를 사용하는 daily digest는 workflow의 `models: read` 권한과 자동 제공되는 `GITHUB_TOKEN`을 사용합니다. 별도 OpenAI API key는 필요하지 않으며, 호출이 실패하면 fallback 리뷰로 계속 실행됩니다.

수동 실행도 `workflow_dispatch`로 가능합니다. 수동 실행 화면에서 `Send a Telegram smoke-test message`를 켜면 실제 뉴스 발행과 별개로 테스트 메시지 1건을 채널에 보내 bot token과 채널 관리자 권한을 확인할 수 있습니다.
`Send a daily digest preview message`를 켜면 최근 24시간 기준 daily digest 미리보기를 채널에 전송합니다. `Digest preview prefix`에 `NONE`을 넣으면 미리보기 접두어 없이 재발송할 수 있습니다.

## GitHub Pages

Repository Settings의 Pages 메뉴에서 배포 source를 설정합니다. GitHub Pages가 repository root 기준 폴더 선택을 지원하는 경우 `public` 폴더를 source로 지정합니다. 환경에 따라 root 또는 `docs`만 선택 가능한 저장소라면 Pages 설정에 맞게 공개 폴더를 조정하거나 `public` 결과물을 별도 배포 workflow로 publishing하면 됩니다.

배포 후 정제 RSS와 데일리 페이지 URL은 다음 형식입니다.

```text
https://news.bside.ai/feed.xml
https://news.bside.ai/feed/latest.html
```

### Custom domain

`news.bside.ai` 같은 하위 도메인을 쓰려면 GitHub Pages의 Custom domain에 `news.bside.ai`를 등록하고, DNS 제공자에서 다음 CNAME을 설정합니다.

```text
news CNAME <owner>.github.io
```

GitHub Actions 기반 Pages 배포에서는 GitHub Pages Settings의 Custom domain과 DNS가 기준입니다. 이 저장소는 배포 산출물에 `public/CNAME`도 함께 포함해 `news.bside.ai` 설정이 유지되도록 합니다. DNS 전파와 HTTPS 인증서 발급에는 시간이 걸릴 수 있으며, 가능하면 GitHub 계정에서 `bside.ai` 도메인을 먼저 verify 해 두는 것을 권장합니다.

공개 repo에는 bot token, API key, 비공개 채널 ID 같은 값을 커밋하지 않습니다. 이 프로젝트는 `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `CURATOR_FEEDS`를 GitHub Actions Secrets에서만 읽도록 운영합니다. `public/`과 `data/state.json`에는 공개 가능한 기사 URL, 제목, 처리 상태만 남는 구조를 유지합니다.

## rss2tg_bot 등록

`rss2tg_bot`에서 GitHub Pages에 공개된 `feed.xml` URL을 구독 URL로 등록합니다.

```text
https://news.bside.ai/feed.xml
```

`rss2tg_bot`은 RSS item 단위로 메시지를 발행하므로, 이 프로젝트는 묶음 1개를 RSS item 1개로 만듭니다.

권장 설정:

- output format: `full article (experimental)`
- `disable web page previews`: 활성화
- `show the source name`: 선택 사항

RSS 본문에는 기사 1건을 한 줄로 표시합니다. rss2tg_bot이 본문 HTML 링크를 보존하지 않는 경우가 있어, 기사 목록에는 `mk.co.kr` 같은 도메인 대신 `매일경제` 같은 출처명을 표시합니다. RSS item link에는 GitHub Pages 중간 링크가 아니라 원문 기사 URL을 사용합니다. `msn.com`처럼 원문 확인이 어려운 중계 링크는 수집 단계에서 제외합니다.

## Telegram 직접 발행

직접 발행을 사용하면 `rss2tg_bot` 없이 이 프로젝트가 Telegram Bot API로 채널에 메시지를 보냅니다. 메시지는 긴 URL을 직접 노출하지 않고 HTML 링크로 표시합니다. 키워드 기반 섹션 라벨은 오분류 가능성이 있어 메시지에 표시하지 않으며, 내부 분류값이나 기준시각, 대표기사보기 링크도 표시하지 않습니다. 단일 기사 업데이트는 제목 링크만 짧게 표시하고 Telegram 웹페이지 preview가 표시되도록 전송합니다.

한 실행에서 발행할 cluster가 2개 이상이면 별도 제목줄 없이 요약과 기사 링크만 묶어서 전송합니다. 이 묶음 메시지는 GitHub Models의 `openai/gpt-4.1`을 사용해 2~3개 bullet 요약을 만들고, 국문/영문 기사 링크를 digest처럼 정리합니다. 요약은 `임박`, `부각`, `지속`처럼 짧은 명사형으로 끝나도록 후처리합니다. AI 호출이 실패하면 규칙 기반 fallback 요약으로 계속 발행합니다.

묶음 판단은 규칙 기반을 기본으로 하되, 같은 회사와 넓은 키워드 때문에 애매하게 붙을 수 있는 기사 pair는 GitHub Models를 보수적 심판으로 사용합니다. AI는 `same_story`, `related_but_different`, `different` 중 하나만 판단하며, `same_story`이고 confidence가 기준값 이상일 때만 묶음을 허용합니다. 기본 설정은 실행당 최대 8회만 확인하므로 quota를 과도하게 쓰지 않습니다.

중복으로 걸러진 기사는 시간당 업데이트에 따로 표시하지 않습니다. 중복 기사 기록은 state에 남겨 두고, 데일리 리뷰에서는 별도 `중복 기사` 섹션을 만들지 않고 유사한 일반 기사 묶음 안에 함께 표시합니다.

필수 조건:

- BotFather에서 만든 bot token을 `TELEGRAM_BOT_TOKEN` Secret에 저장
- bot을 채널 관리자에 추가
- `config.yaml`의 `telegram.chat_id` 또는 `TELEGRAM_CHAT_ID` Secret 설정

처음 Secret을 연결한 실행에서는 기존 published cluster를 발송하지 않도록 기준선으로만 저장합니다. 이후 새로 published 되는 cluster부터 전송합니다. 전송한 guid는 `data/state.json`의 `telegram_sent_cluster_guids`에 저장되어 중복 발송을 막습니다.

bot 연결만 즉시 확인하려면 Actions의 `Build curated RSS feed` 수동 실행에서 `Send a Telegram smoke-test message` 옵션을 켭니다.

## AI 데일리 리뷰

일반 단일 기사 메시지는 AI를 호출하지 않고 제목 링크만 발행합니다. 여러 기사 묶음과 매일 아침 리뷰는 GitHub Models를 사용할 수 있으며, 기본 모델은 `openai/gpt-4.1`입니다.

일반 업데이트는 KST 08:05~00:35에는 매시 `:05`, `:35`에 실행하고, KST 01:00~06:00 구간은 03:35와 06:05 두 번으로 나누어 전송합니다. 03:35 발송은 01:00 이후, 06:05 발송은 03:30 이후 수집분을 묶습니다. GitHub Actions schedule은 혼잡 시간대에 지연될 수 있으므로 정각 대신 약간 비껴 실행합니다. 비슷한 제목과 핵심 토큰을 가진 기사는 대표 제목 아래 여러 언론사 링크로 묶어 보여줍니다.

데일리 HTML은 텔레그램 메시지보다 먼저 생성되고 GitHub Pages 배포 후 메시지가 발송됩니다. 텔레그램 데일리 메시지는 수집 기사 수, 이슈 수, 매체 수, 메인 기사 일부와 데일리 링크만 간결하게 표시합니다.

## 운영 정책

- 새 cluster는 pending 상태로 시작합니다.
- 기본 45분 buffer 후 묶음으로 발행합니다.
- high relevance cluster는 20분 후 발행 가능합니다.
- pending 상태가 3시간을 넘으면 강제 발행합니다.
- article published date 기준 7일 초과 기사는 제외합니다.
- 기본 설정에서는 발송일 기준 전일보다 오래된 기사도 제외합니다.
- 발행 대상은 high, medium relevance입니다.
- low relevance 기사는 `state.json`의 `rejected_articles`에 저장합니다.
- 모든 수집 기사는 우선순위 점수와 함께 `data/archive/articles/YYYY-MM-DD.jsonl`에 보관합니다.
- 우선순위 override는 `data/priority_overrides.yaml`에서 수동 관리합니다.
- `msn.com` 같은 중계 링크는 기본적으로 제외합니다.
- 이미 published된 cluster에 유사 기사가 나중에 들어오면 기존 item을 수정하지 않고 `[추가 N건] ...` follow-up cluster로 새 guid를 발행합니다.
- `feed.xml`에는 최근 published cluster 50개만 유지합니다.
- Telegram 직접 발행은 전송 성공한 cluster guid를 state에 저장하고, 이미 보낸 cluster는 다시 보내지 않습니다.
- 중복 언급은 최근 30일 안의 기존 기사만 참고합니다.
- state 보관 상한은 기본 60일입니다. `articles`, `rejected_articles`, `published_clusters`, Telegram/Digest 전송 기록은 이 범위를 기준으로 정리됩니다.
- archive 보관 상한은 기본 365일입니다. GitHub 저장소 용량이 커지면 `archive.retention_days`를 줄이거나 외부 DB로 이전합니다.

## 한계

- `rss2tg_bot`이 메시지 렌더링을 최종 결정하므로 정확한 표시 형식은 bot 설정에 따라 달라질 수 있습니다.
- 너무 긴 description은 텔레그램에서 분할될 수 있습니다. 기본 제한은 3500자입니다.
- 이미 발행된 텔레그램 메시지를 RSS만으로 안정적으로 수정하는 것은 기대하지 않습니다.
- GitHub Actions 기반 Telegram 발행은 대화형 봇이 아니라 예약 실행형 발행 봇입니다. 채널에 명령어를 보내 즉시 설정을 바꾸는 용도에는 맞지 않습니다.
- GitHub Models 호출 한도나 권한 문제로 daily digest 또는 AI 묶음 심판이 실패할 수 있으며, 이 경우 규칙 기반 로직으로 계속 실행합니다.
- 기업명 추정은 단순 규칙 기반입니다. `extract_company_candidates()`를 확장해 개선할 수 있습니다.
