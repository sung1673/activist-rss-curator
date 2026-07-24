from __future__ import annotations

from copy import deepcopy
import inspect
import json

from curator import publish_outbox
from curator.publish_outbox import process_remote_delivery_outbox


def test_remote_worker_is_disabled_before_claim_ack_or_fail(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "real-looking-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@real_looking_channel")
    monkeypatch.setenv("ACTIVIST_API_BASE_URL", "https://example.invalid/api")
    config["telegram"]["enabled"] = True
    before = deepcopy(config)

    summary = process_remote_delivery_outbox(
        config,
        now,
        limit=10_000,
        delivery_id="historical-pending-row",
    )

    assert summary == {
        "mode": "disabled",
        "distribution_mode": "web_only",
        "telegram_outbox_claimed": 0,
        "telegram_sent": 0,
        "telegram_failed": 0,
        "telegram_dead_letter": 0,
        "telegram_already_delivered": 0,
        "rights_blocked_count": 0,
        "outcome_unknown_count": 0,
        "requested_status": "disabled",
    }
    assert config == before


def test_worker_module_has_no_outbound_or_state_mutation_implementation() -> None:
    source = inspect.getsource(publish_outbox)

    for forbidden in (
        "claim_delivery_outbox",
        "ack_delivery_outbox",
        "fail_delivery_outbox",
        "enqueue_delivery_outbox",
        "send_telegram_message",
        "post_remote_action",
        "load_state",
        "save_state",
        "api.telegram.org",
    ):
        assert forbidden not in source


def test_publish_command_is_successful_without_reading_project_or_queue(
    tmp_path,
    capsys,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    missing_root = tmp_path / "does-not-exist"
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "real-looking-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@real_looking_channel")

    assert publish_outbox.main(["--root", str(missing_root), "--limit", "100"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "mode": "disabled",
        "distribution_mode": "web_only",
        "telegram_outbox_skipped": 1,
        "reason": "telegram_outbound_disabled",
        "telegram_sent": 0,
        "telegram_failed": 0,
    }
