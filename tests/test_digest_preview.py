from __future__ import annotations

import inspect

from curator import digest_preview


def test_digest_preview_is_permanent_policy_noop(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "real-looking-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@real_looking_channel")

    result = digest_preview.send_digest_preview(tmp_path / "missing-project")

    assert result == {
        "digest_preview_sent": 0,
        "digest_preview_failed": 0,
    }


def test_digest_preview_has_no_transport_or_state_read_path() -> None:
    source = inspect.getsource(digest_preview)

    for forbidden in (
        "send_telegram_message",
        "telegram_is_configured",
        "telegram_bot_token",
        "telegram_chat_id",
        "load_config",
        "load_state",
        "build_daily_digest_messages",
        "api.telegram.org",
    ):
        assert forbidden not in source
