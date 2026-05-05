# Telegram 공개 채널 이슈 보강 운영 메모

## 정책

- 수집 대상은 공개 Telegram 채널만입니다.
- 전용 읽기 계정의 MTProto 세션을 사용하며 `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELEGRAM_SESSION` 또는 `TELEGRAM_SESSION_STRING`은 GitHub Secrets 또는 로컬 `.env`에만 둡니다.
- 자동 입장은 기본 비활성화입니다. 후보 채널은 `pending` 상태로 저장하고 운영자가 승인한 뒤에만 수집 대상으로 올립니다.
- 화면 표현은 투자 추천이 아니라 시장 반응, 언급 현황, 관련 출처 보조 정보로 유지합니다.

## 수집 흐름

1. `config.yaml`의 `telegram_sources.channels` 또는 `python -m curator.telegram_sources add <handle>`로 수동 채널을 등록합니다.
   이미 읽기 계정이 여러 공개 채널에 가입되어 있다면 `python -m curator.telegram_sources import-joined --dry-run`으로 먼저 스캔하고, 확인 후 `--enable`을 붙여 수집 대상으로 등록합니다.
2. `curator.main` 실행 중 `telegram_sources.enabled`가 켜져 있으면 enabled 채널을 순회합니다.
3. 최초 수집은 채널당 `backfill_limit` 기본 100개, 이후에는 `last_message_id` 이후 새 메시지만 가져옵니다.
4. 채널 하나에서 FloodWait 또는 권한 오류가 발생해도 해당 채널만 실패 기록하고 다른 채널 수집은 계속합니다.

코드 레벨에서도 수집 대상은 `username`이 있는 Telegram broadcast channel로 제한합니다. 공개 그룹, supergroup/megagroup, 개인 대화, 저장한 메시지, bot/user dialog는 `source_type`과 Telethon entity 검증에서 제외됩니다.

## 기사 매칭 기준

- URL 직접 매칭: 메시지 본문 URL을 추출하고 tracking query, fragment, trailing slash 등을 정리한 뒤 기존 기사 URL과 비교합니다.
- canonical URL 매칭: 정규화 URL hash가 같은 경우 매칭합니다.
- 키워드 추정 매칭: URL이 없을 때 기사 제목, 요약, 메시지의 핵심 토큰 overlap이 충분한 경우 낮은 score로 연결합니다.
- UI에서는 URL 직접 매칭과 키워드 추정 매칭을 구분해서 표시해야 합니다.

## 삭제와 수정 추적

- 수정 메시지는 같은 `(channel, telegram_message_id)` row를 업데이트하며 `edited_at`과 본문 변경을 반영합니다.
- 공개 채널 polling만으로 삭제 이벤트를 완벽하게 추적하기는 어렵습니다.
- 삭제 보정은 최근 메시지 window를 다시 확인할 수 있는 관리/백필 경로에서 `reconcile_recent_deletions`로 처리합니다.

## 후보 채널과 자동 입장

- 추천 후보는 `pending`, `accepted`, `rejected`, `joined`, `failed` 상태를 가집니다.
- 경제, 증권, 주식, 공시, 실적, 환율, 채권, 뉴스 등은 품질 점수 가점입니다.
- 수익보장, 리딩방, 무료추천, 선물, 카지노, 레퍼럴, VIP방 등은 감점입니다.
- 자동 입장이 켜져 있어도 하루 최대 join 수와 랜덤 지연을 둡니다. FloodWait, private, invite required, too many channels 계열 오류는 후보 실패로만 기록합니다.

## 로컬 설정과 GitHub Secrets

로컬 테스트용 비밀값은 repository root의 `.env.telegram`에 둘 수 있습니다. 이 파일은 `.gitignore`에 포함되어 커밋되지 않습니다.

```text
TELEGRAM_API_ID=123456
TELEGRAM_API_HASH=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TELEGRAM_SESSION=data/telegram-reader
```

처음 로컬에서 실행하면 Telethon이 전화번호와 로그인 코드를 요청하고 `data/telegram-reader.session` 파일을 만듭니다. 이 파일 역시 커밋하지 않습니다.

GitHub Actions에서 수집하려면 file session 대신 `TELEGRAM_SESSION_STRING` Secret을 권장합니다. 로컬에서 다음 명령으로 생성합니다.

```powershell
.\.venv\Scripts\python.exe -m curator.telegram_sources make-session --out .env.telegram
```

생성된 `TELEGRAM_SESSION_STRING` 값을 GitHub repository의 Actions Secret에 추가합니다. `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`도 같은 위치에 Secret으로 넣습니다.

이미 가입된 공개 채널을 가져올 때는 먼저 dry-run으로 규모와 품질 점수를 봅니다.

```powershell
.\.venv\Scripts\python.exe -m curator.telegram_sources import-joined --limit 500 --min-quality 60 --dry-run
```

확인 후 수집 대상으로 바로 켜려면 `--enable`을 붙입니다.

```powershell
.\.venv\Scripts\python.exe -m curator.telegram_sources import-joined --limit 500 --min-quality 60 --enable
```

## 유사 채널 후보 발견

Telethon의 `channels.GetChannelRecommendationsRequest`를 사용할 수 있는 계정/채널에서는 seed 채널 기준 유사 채널 후보를 가져올 수 있습니다. 후보는 바로 가입하지 않고 `pending` 상태로만 저장합니다.

```powershell
.\.venv\Scripts\python.exe -m curator.telegram_sources discover --limit 20 --dry-run
```

확인 후 dry-run을 제거하면 `data/state.json`의 `telegram_channel_candidates`에 후보가 저장됩니다. 자동 입장은 `auto_join_enabled: false`가 기본값입니다.

## 2주 샘플 및 6개월 백필

정기 GitHub Actions는 새 메시지만 가볍게 수집하고, 과거 6개월 백필은 네트워크 여유가 있는 Windows 로컬에서 실행하는 방식을 권장합니다. 먼저 2주 샘플로 규모를 확인합니다.

```powershell
.\.venv\Scripts\python.exe -m curator.telegram_sources backfill-messages --days 14 --channel-limit 20 --limit-per-channel 1000 --dry-run
```

DB에 실제 반영하려면 `--dry-run`을 제거합니다. 원격 DB API 동기화를 잠시 끄려면 `--no-remote`를 붙입니다.

```powershell
.\.venv\Scripts\python.exe -m curator.telegram_sources backfill-messages --days 180 --channel-limit 0 --limit-per-channel 3000
```

출력에는 채널 수, 수집 메시지 수, 삽입/업데이트 수, 실패 채널 수, 최근 샘플 기준 일/월/연 저장량 추정치가 포함됩니다.

대량 백필은 채널별 메시지 수 차이가 커서 오래 걸릴 수 있습니다. 중간 실패 때 다시 처음부터 하지 않도록 기본 CLI는 채널 하나가 끝날 때마다 `data/state.json`을 checkpoint 저장합니다. 특정 채널에서 Telethon의 `old message`/security 경고가 반복되면 해당 채널을 건너뛰고 이어서 실행합니다.

```powershell
.\.venv\Scripts\python.exe -m curator.telegram_sources backfill-messages `
  --days 180 `
  --limit-per-channel 1000 `
  --skip-handles GoUpstock `
  --timeout-per-channel 90
```

이미 앞 채널을 처리했다면 `--start-after`로 이어받을 수 있습니다.

```powershell
.\.venv\Scripts\python.exe -m curator.telegram_sources backfill-messages `
  --days 180 `
  --limit-per-channel 1000 `
  --start-after GoUpstock
```

처음부터 6개월 전체를 한 번에 돌리기보다 `--max-messages 5000`처럼 잘라 실행하면 DB 반영과 장애 확인이 쉽습니다.

## 운영 대시보드

`curator.main`은 공개-safe 운영 점검 페이지 `public/feed/telegram-admin.html`을 생성합니다.

- 수집 가능 공개 채널 수
- enabled 채널 수와 실패 채널 수
- 최근 24시간/14일 메시지 수
- 기사 매칭 수
- 채널별 최근 수집 상태
- 메시지 유형 분포
- 최근 14일 키워드
- 월간/연간 DB 저장량 추정

GitHub Pages가 공개 페이지이므로 비밀값, session, raw_json 전문, 관리자 쓰기 기능은 노출하지 않습니다. 채널 승인/비활성화/백필 실행은 CLI 또는 내부 API에서만 수행합니다.

## 분석 활용 방향

Telegram 메시지는 기사와 같은 방식의 원문 뉴스가 아니라 시장 반응 보조 신호입니다. 현재 구조에서는 다음 분석이 유용합니다.

- 같은 기사 URL이 여러 채널에서 공유되는지 확인해 기사 반응도를 계산합니다.
- 같은 키워드/종목/제도 표현이 짧은 시간 여러 채널에서 반복되는지 확인해 이슈 확산을 감지합니다.
- URL 직접 매칭과 키워드 추정 매칭을 구분해 신뢰도를 다르게 표시합니다.
- 채널별 품질 점수, 반복 공유 성향, 홍보성/루머성 risk flag를 누적해 source quality를 관리합니다.
- 뉴스 중요도와 Telegram 언급량이 동시에 높아지는 경우를 핵심 이슈 후보로 올립니다.
