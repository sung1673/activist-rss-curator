from __future__ import annotations

from math import ceil

from curator.telegram_sources import (
    message_key,
    normalize_telegram_message,
    pending_remote_messages,
    sync_telegram_to_remote_api,
)


def test_four_thousand_messages_batch_linearly_and_do_not_resend_after_ack(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """Exercise the production-sized page contract without a wall-clock assertion.

    Batch count, bounded page size, and exactly-once membership are stable on
    slow and fast CI runners, unlike a brittle elapsed-time threshold.
    """

    from curator import telegram_sources

    message_count = 4_005
    batch_size = 500
    channel = {
        "handle": "marketnews",
        "telegram_channel_id": "100",
        "source_right_id": "telegram:marketnews",
    }
    messages = [
        normalize_telegram_message(
            channel,
            {"id": message_id, "text": f"message {message_id}"},
            now,
        )
        for message_id in range(1, message_count + 1)
    ]
    state: dict[str, object] = {
        "telegram_source_channels": [channel],
        "telegram_source_messages": messages,
    }
    config["telegram_sources"] = {"remote_batch_size": batch_size}
    payloads: list[dict[str, object]] = []
    monkeypatch.setattr(telegram_sources, "remote_api_configured", lambda: True)

    def fake_post(_action: str, payload: dict[str, object]) -> dict[str, object]:
        payloads.append(payload)
        return {
            "ok": True,
            "channels": len(payload["channels"]),  # type: ignore[arg-type]
            "messages": len(payload["messages"]),  # type: ignore[arg-type]
            "article_matches": 0,
        }

    monkeypatch.setattr(telegram_sources, "post_remote_action", fake_post)

    first = sync_telegram_to_remote_api(state, config)

    page_sizes = [len(payload["messages"]) for payload in payloads]  # type: ignore[arg-type]
    delivered_keys = [
        message_key(message)
        for payload in payloads
        for message in payload["messages"]  # type: ignore[union-attr]
    ]
    assert len(payloads) == ceil(message_count / batch_size)
    assert page_sizes == [batch_size] * 8 + [5]
    assert sum(page_sizes) == message_count
    assert len(delivered_keys) == len(set(delivered_keys)) == message_count
    assert first["telegram_remote_messages"] == message_count
    assert first["telegram_remote_pending"] == 0
    assert state["telegram_remote_sync_cursors"] == {"id:100": message_count}

    calls_after_first_sync = len(payloads)
    second = sync_telegram_to_remote_api(state, config)

    assert second == {
        "telegram_remote_synced": 0,
        "telegram_remote_failed": 0,
        "telegram_remote_pending": 0,
    }
    assert len(payloads) == calls_after_first_sync
    assert pending_remote_messages(state) == []
