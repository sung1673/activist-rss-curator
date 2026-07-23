from __future__ import annotations

import json

from curator import daily_report, governance_publisher, publish_outbox, telegram_publisher
from curator.main import publish_telegram_for_run, telegram_delivery_mode


def forbidden_transport(*_args, **_kwargs):  # type: ignore[no-untyped-def]
    raise AssertionError("web-only policy must not call Telegram or mutate an outbound queue")


def test_credentials_and_legacy_modes_cannot_enable_delivery(config, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "must-not-enable")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@must_not_enable")
    monkeypatch.setenv("CURATOR_DELIVERY_MODE", "legacy-direct")
    monkeypatch.delenv("CURATOR_DISABLE_TELEGRAM_SEND", raising=False)
    config["telegram"]["enabled"] = True

    assert telegram_delivery_mode() == "disabled"
    assert daily_report.daily_report_delivery_mode() == "disabled"
    assert telegram_publisher.telegram_is_configured(config) is False


def test_direct_sender_and_low_level_send_method_fail_before_http(config) -> None:  # type: ignore[no-untyped-def]
    class ForbiddenClient:
        def post(self, *_args, **_kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("Telegram HTTP must not be called")

    direct = telegram_publisher.send_telegram_message(
        "must-not-enable",
        "@must_not_enable",
        "hello",
        config,
        client=ForbiddenClient(),  # type: ignore[arg-type]
    )
    low_level = telegram_publisher._post_telegram_method(
        "must-not-enable",
        "sendMessage",
        {"chat_id": "@must_not_enable", "text": "hello"},
        timeout=1,
        client=ForbiddenClient(),  # type: ignore[arg-type]
    )
    get_chat = telegram_publisher.validate_telegram_chat(
        "must-not-enable",
        "@must_not_enable",
        config,
        client=ForbiddenClient(),  # type: ignore[arg-type]
    )

    assert direct["error"] == "telegram_outbound_disabled"
    assert direct["delivery_stage"] == "policy"
    assert low_level["error"] == "telegram_outbound_disabled"
    assert get_chat["error"] == "telegram_outbound_disabled"
    assert get_chat["delivery_stage"] == "policy"


def test_ingest_delivery_entrypoint_never_invokes_transport(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    assert publish_telegram_for_run({}, config, now, [], {"remote_api_failed": 0}) == {
        "telegram_sent": 0,
        "telegram_failed": 0,
        "telegram_outbox_enqueue_failed": 0,
        "telegram_outbox_enqueue_skipped": 1,
    }


def test_local_and_remote_outbox_entrypoints_preserve_history(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    state = {
        "published_clusters": [],
        "telegram_delivery_outbox": [{"outbox_id": "historical", "status": "pending"}],
    }
    before = json.loads(json.dumps(state))
    monkeypatch.setattr(telegram_publisher, "send_telegram_message", forbidden_transport)

    remote_enqueue = telegram_publisher.enqueue_unsent_telegram_clusters_to_remote(
        state,
        config,
        now,
    )
    local_process = telegram_publisher.process_telegram_delivery_outbox(state, config, now)
    remote_process = publish_outbox.process_remote_delivery_outbox(
        config,
        now,
        limit=100,
        delivery_id="historical",
    )

    assert remote_enqueue["telegram_outbox_enqueue_skipped"] == 1
    assert local_process["telegram_outbox_skipped"] == 1
    assert remote_process["distribution_mode"] == "web_only"
    assert remote_process["telegram_outbox_claimed"] == 0
    assert state == before


def test_daily_and_governance_enqueue_helpers_are_disabled(monkeypatch) -> None:
    assert daily_report.enqueue_daily_report({}, {})["outbound_delivery_disabled"] == 1
    assert daily_report.deliver_daily_report_direct({}, {}) == {
        "daily_report_queued": 0,
        "daily_report_sent": 0,
        "daily_report_failed": 0,
    }
    governance = governance_publisher.enqueue_published_governance_events()
    assert governance["outbound_delivery_disabled"] == 1
    assert governance["governance_deliveries_enqueued"] == 0


def test_publish_command_is_successful_web_only_noop(tmp_path, capsys, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    (tmp_path / "config.yaml").write_text("telegram:\n  enabled: true\n", encoding="utf-8")

    assert publish_outbox.main(["--root", str(tmp_path), "--limit", "100"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["distribution_mode"] == "web_only"
    assert payload["reason"] == "telegram_outbound_disabled"
