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
            "<b>Telegram outbound disabled</b>",
            "",
            "BSIDE는 web-only 배포 정책을 사용하며 이 메시지는 전송할 수 없습니다.",
            f"기준시각: {format_kst(now, timezone_name)}",
        ]
    )


def main() -> None:
    config = load_config(PROJECT_ROOT / "config.yaml")
    if not telegram_is_configured(config):
        raise SystemExit(
            "Telegram outbound is permanently disabled; only read-only channel collection is supported."
        )

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
