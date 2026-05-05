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
