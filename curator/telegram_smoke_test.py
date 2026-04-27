from __future__ import annotations

from pathlib import Path

from .config import load_config
from .dates import format_kst, now_in_timezone
from .telegram_publisher import send_telegram_message, telegram_bot_token, telegram_chat_id, telegram_is_configured


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def build_smoke_test_message(config: dict[str, object]) -> str:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    now = now_in_timezone(timezone_name)
    return "\n".join(
        [
            "<b>행동주의 뉴스 봇 테스트</b>",
            "",
            "GitHub Actions에서 Telegram 직접 발행 권한을 확인했습니다.",
            f"기준시각: {format_kst(now, timezone_name)}",
        ]
    )


def main() -> None:
    config = load_config(PROJECT_ROOT / "config.yaml")
    if not telegram_is_configured(config):
        raise SystemExit("Telegram is not configured. Set TELEGRAM_BOT_TOKEN and chat id.")

    response = send_telegram_message(
        telegram_bot_token(),
        telegram_chat_id(config),
        build_smoke_test_message(config),
        config,
    )
    if not response.get("ok"):
        raise SystemExit(f"Telegram smoke test failed: {response.get('error') or 'unknown_error'}")
    print(f"Telegram smoke test sent: message_id={response.get('message_id')}")


if __name__ == "__main__":
    main()
