# 한국어 뉴스 큐레이션용 정제 RSS 생성기

Google Alerts RSS를 15분마다 가져와 오래된 기사, 중복 기사, 관련도 낮은 기사를 제거하고 유사 기사들을 하나의 묶음으로 만든 뒤 `rss2tg_bot`이 구독할 수 있는 `public/feed.xml`을 생성합니다.

## 구조

기존 구조:

```text
Google Alerts RSS -> rss2tg_bot -> Telegram channel
```

변경 구조:

```text
Google Alerts RSS -> GitHub Actions 정제 -> GitHub Pages feed.xml -> rss2tg_bot -> Telegram channel
```

Telegram bot은 새로 만들지 않습니다. 이 프로젝트는 `rss2tg_bot`이 읽을 정제 RSS만 생성합니다. RSS item 1개가 cluster 1개이며, item description 안에 유사 기사 여러 링크가 들어갑니다.

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
- `public/items/*.html`
- `public/u/*.html`
- `data/state.json`

테스트는 다음처럼 실행합니다.

```bash
pytest
```

## 설정

`config.yaml`에서 날짜 필터, clustering buffer, 중복 기준, 발행 개수를 조정합니다.

Google Alerts RSS URL은 public repo에 직접 저장하지 말고 GitHub Actions Secret으로 관리하는 것을 권장합니다.
`feed.xml`의 channel link에는 원본 Alert URL을 쓰지 않고, 필요하면 `public_feed_url`에 GitHub Pages의 공개 feed URL만 넣습니다.

Secret 이름:

```text
CURATOR_FEEDS
```

여러 Google Alerts RSS URL은 쉼표 또는 줄바꿈으로 구분합니다.

```text
https://www.google.com/alerts/feeds/...
https://www.google.com/alerts/feeds/...
```

## GitHub Actions

`.github/workflows/build-feed.yml`은 다음을 수행합니다.

- 15분마다 실행
- Python 3.12 설치
- `requirements.txt` 설치
- `python -m curator.main` 실행
- `public/feed.xml`, `public/index.html`, `public/items`, `public/u`, `data/state.json` 변경 시 commit & push

수동 실행도 `workflow_dispatch`로 가능합니다.

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

RSS 본문에는 기사 링크를 HTML anchor 형태로 넣습니다. 긴 원문 URL은 `public/u/*.html` 짧은 리다이렉트 링크로 감싸고, RSS item의 대표 link는 `public/items/*.html` 묶음 상세 페이지를 사용합니다. 이렇게 하면 bot이 item URL을 별도로 붙여도 MSN처럼 긴 URL이 메시지에 직접 노출되는 일을 줄일 수 있습니다.

## 운영 정책

- 새 cluster는 pending 상태로 시작합니다.
- 기본 45분 buffer 후 묶음으로 발행합니다.
- high relevance cluster는 20분 후 발행 가능합니다.
- pending 상태가 3시간을 넘으면 강제 발행합니다.
- article published date 기준 7일 초과 기사는 제외합니다.
- 발행 대상은 high, medium relevance입니다.
- low relevance 기사는 `state.json`의 `rejected_articles`에 저장합니다.
- 이미 published된 cluster에 유사 기사가 나중에 들어오면 기존 item을 수정하지 않고 `[추가 N건] ...` follow-up cluster로 새 guid를 발행합니다.
- `feed.xml`에는 최근 published cluster 50개만 유지합니다.

## 한계

- `rss2tg_bot`이 메시지 렌더링을 최종 결정하므로 정확한 표시 형식은 bot 설정에 따라 달라질 수 있습니다.
- 너무 긴 description은 텔레그램에서 분할될 수 있습니다. 기본 제한은 3500자입니다.
- 이미 발행된 텔레그램 메시지를 RSS만으로 안정적으로 수정하는 것은 기대하지 않습니다.
- 기업명 추정은 단순 규칙 기반입니다. `extract_company_candidates()`를 확장해 개선할 수 있습니다.
