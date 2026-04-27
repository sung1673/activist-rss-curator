from __future__ import annotations

import os
from pathlib import Path

from .config import load_config
from .dates import now_in_timezone, parse_datetime
from .state import load_state
from .telegram_publisher import (
    build_telegram_message,
    cluster_guid_value,
    cluster_should_show_web_preview,
    publishable_articles,
    send_telegram_message,
    telegram_bot_token,
    telegram_chat_id,
    telegram_is_configured,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def resend_count() -> int:
    raw_value = os.environ.get("TELEGRAM_RESEND_COUNT", "1")
    try:
        return max(1, int(raw_value))
    except ValueError:
        return 1


def recent_sent_clusters(
    state: dict[str, object],
    config: dict[str, object],
    *,
    count: int,
) -> list[dict[str, object]]:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    sent_guids = {str(value) for value in state.get("telegram_sent_cluster_guids", [])}
    candidates = []
    for index, cluster in enumerate(state.get("published_clusters", [])):
        if cluster_guid_value(cluster) not in sent_guids:
            continue
        if not publishable_articles(cluster, config):
            continue
        published_at = parse_datetime(str(cluster.get("published_at") or ""), timezone_name)
        candidates.append((published_at, index, cluster))
    candidates.sort(key=lambda item: (item[0] is not None, item[0], item[1]), reverse=True)
    return [cluster for _published_at, _index, cluster in candidates[:count]]


def resend_recent_articles(root: Path | None = None) -> dict[str, int]:
    project_root = root or PROJECT_ROOT
    config = load_config(project_root / "config.yaml")
    if not telegram_is_configured(config):
        return {"telegram_resend_sent": 0, "telegram_resend_failed": 0}

    state = load_state(project_root / "data" / "state.json")
    clusters = recent_sent_clusters(state, config, count=resend_count())
    if not clusters:
        return {"telegram_resend_sent": 0, "telegram_resend_failed": 0}

    bot_token = telegram_bot_token()
    chat_id = telegram_chat_id(config)
    sent = 0
    failed = 0
    for cluster in clusters:
        response = send_telegram_message(
            bot_token,
            chat_id,
            build_telegram_message(cluster, config),
            config,
            disable_web_page_preview=not cluster_should_show_web_preview(cluster, config),
        )
        if response.get("ok"):
            sent += 1
        else:
            failed += 1
    return {"telegram_resend_sent": sent, "telegram_resend_failed": failed}


def main() -> None:
    summary = resend_recent_articles()
    print(
        "Telegram resend finished: "
        + ", ".join(f"{key}={value}" for key, value in summary.items())
    )


if __name__ == "__main__":
    main()
