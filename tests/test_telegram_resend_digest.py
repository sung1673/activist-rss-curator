from __future__ import annotations

import inspect

from curator import telegram_resend_digest


def test_digest_resend_is_permanent_policy_noop(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "real-looking-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@real_looking_channel")

    result = telegram_resend_digest.resend_last_digest(tmp_path / "missing-project")

    assert result == {
        "telegram_digest_resend_sent": 0,
        "telegram_digest_resend_failed": 0,
    }


def test_digest_resend_has_no_transport_claim_or_state_read_path() -> None:
    source = inspect.getsource(telegram_resend_digest)

    for forbidden in (
        "send_telegram_message",
        "telegram_is_configured",
        "telegram_bot_token",
        "telegram_chat_id",
        "load_config",
        "load_state",
        "build_hourly_update_messages",
        "claim_delivery_outbox",
        "api.telegram.org",
    ):
        assert forbidden not in source
