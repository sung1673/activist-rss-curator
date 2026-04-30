from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from .config import load_config
from .dates import now_in_timezone, parse_datetime
from .state import load_state
from .summaries import build_hourly_update_messages, hourly_update_start_at
from .telegram_publisher import (
    cluster_guid_value,
    publishable_articles,
    send_telegram_message,
    telegram_bot_token,
    telegram_chat_id,
    telegram_is_configured,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def latest_digest_record(state: dict[str, object], config: dict[str, object]) -> dict[str, object] | None:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    records = [record for record in state.get("telegram_digest_records", []) if isinstance(record, dict)]
    if not records:
        return None
    return max(
        records,
        key=lambda record: parse_datetime(str(record.get("sent_at") or ""), timezone_name)
        or parse_datetime(str(record.get("window_end") or ""), timezone_name)
        or now_in_timezone(timezone_name) - timedelta(days=3650),
    )


def clusters_for_record(
    state: dict[str, object],
    config: dict[str, object],
    record: dict[str, object],
) -> list[dict[str, object]]:
    by_guid = {
        cluster_guid_value(cluster): cluster
        for cluster in state.get("published_clusters", [])
        if isinstance(cluster, dict) and cluster_guid_value(cluster)
    }
    clusters: list[dict[str, object]] = []
    for guid in record.get("cluster_guids", []):
        cluster = by_guid.get(str(guid))
        if cluster and publishable_articles(cluster, config):
            clusters.append(cluster)
    return clusters


def resend_last_digest(root: Path | None = None) -> dict[str, int]:
    project_root = root or PROJECT_ROOT
    config = load_config(project_root / "config.yaml")
    if not telegram_is_configured(config):
        return {"telegram_digest_resend_sent": 0, "telegram_digest_resend_failed": 0}

    state = load_state(project_root / "data" / "state.json")
    record = latest_digest_record(state, config)
    if not record:
        return {"telegram_digest_resend_sent": 0, "telegram_digest_resend_failed": 0}

    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    now = parse_datetime(str(record.get("window_end") or record.get("sent_at") or ""), timezone_name) or now_in_timezone(
        timezone_name
    )
    start_at = parse_datetime(str(record.get("window_start") or ""), timezone_name) or hourly_update_start_at(config, now)
    clusters = clusters_for_record(state, config, record)
    if not clusters:
        return {"telegram_digest_resend_sent": 0, "telegram_digest_resend_failed": 0}

    sent = 0
    failed = 0
    bot_token = telegram_bot_token()
    chat_id = telegram_chat_id(config)
    for message in build_hourly_update_messages(clusters, config, now, start_at):
        response = send_telegram_message(
            bot_token,
            chat_id,
            message,
            config,
            disable_web_page_preview=True,
        )
        if response.get("ok"):
            sent += 1
        else:
            failed += 1
    return {"telegram_digest_resend_sent": sent, "telegram_digest_resend_failed": failed}


def main() -> None:
    summary = resend_last_digest()
    print(
        "Telegram digest resend finished: "
        + ", ".join(f"{key}={value}" for key, value in summary.items())
    )


if __name__ == "__main__":
    main()
