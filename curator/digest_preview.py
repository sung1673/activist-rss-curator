from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

from .config import load_config
from .dates import now_in_timezone
from .state import load_state
from .summaries import build_daily_digest_messages, digest_clusters_in_window, digest_config
from .telegram_publisher import (
    send_telegram_message,
    telegram_bot_token,
    telegram_chat_id,
    telegram_is_configured,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def preview_hours(config: dict[str, object]) -> int:
    raw_value = os.environ.get("DIGEST_PREVIEW_HOURS")
    if raw_value:
        try:
            return max(1, int(raw_value))
        except ValueError:
            pass
    return int(digest_config(config).get("window_hours", 24))


def preview_prefix() -> str:
    raw_value = os.environ.get("DIGEST_PREVIEW_PREFIX")
    if raw_value is None:
        return "[미리보기] "
    if raw_value.strip().upper() == "NONE":
        return ""
    return raw_value


def send_digest_preview(root: Path | None = None) -> dict[str, int]:
    project_root = root or PROJECT_ROOT
    config = load_config(project_root / "config.yaml")
    if not telegram_is_configured(config):
        return {"digest_preview_sent": 0, "digest_preview_failed": 0}

    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    now = now_in_timezone(timezone_name)
    start_at = now - timedelta(hours=preview_hours(config))
    state = load_state(project_root / "data" / "state.json")
    clusters = digest_clusters_in_window(state, config, start_at, now)
    if not clusters:
        return {"digest_preview_sent": 0, "digest_preview_failed": 0}

    messages = build_daily_digest_messages(clusters, config, now, start_at)
    prefix = preview_prefix()
    if messages and prefix:
        messages[0] = prefix + messages[0]

    bot_token = telegram_bot_token()
    chat_id = telegram_chat_id(config)
    sent = 0
    failed = 0
    for message in messages:
        response = send_telegram_message(bot_token, chat_id, message, config)
        if response.get("ok"):
            sent += 1
        else:
            failed += 1
    return {"digest_preview_sent": sent, "digest_preview_failed": failed}


def main() -> None:
    summary = send_digest_preview()
    print(
        "Digest preview finished: "
        + ", ".join(f"{key}={value}" for key, value in summary.items())
    )


if __name__ == "__main__":
    main()
