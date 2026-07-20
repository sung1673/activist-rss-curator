from __future__ import annotations

from curator.publish_outbox import process_remote_delivery_outbox


def test_remote_outbox_ack_requires_external_message_id(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import publish_outbox

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@channel")
    actions: list[tuple[str, dict[str, object]]] = []

    def fake_post(action, payload):  # type: ignore[no-untyped-def]
        actions.append((action, payload))
        if action == "claim_delivery_outbox":
            return {
                "ok": True,
                "items": [
                    {
                        "outbox_id": "delivery-1",
                        "lease_token": "lease-1",
                        "destination": "@channel",
                        "payload_text": "hello",
                    }
                ],
            }
        if action == "ack_delivery_outbox":
            return {"ok": True}
        raise AssertionError(action)

    monkeypatch.setattr(publish_outbox, "post_remote_action", fake_post)
    monkeypatch.setattr(
        publish_outbox,
        "send_telegram_message",
        lambda *_args, **_kwargs: {"ok": True, "message_id": 777, "chat_id": -100},
    )

    summary = process_remote_delivery_outbox(config, now, limit=1)

    assert summary["telegram_sent"] == 1
    assert summary["telegram_failed"] == 0
    ack_payload = next(payload for action, payload in actions if action == "ack_delivery_outbox")
    assert ack_payload["external_message_id"] == "777"
    assert ack_payload["lease_token"] == "lease-1"


def test_remote_outbox_reports_and_persists_telegram_failure(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import publish_outbox

    actions: list[tuple[str, dict[str, object]]] = []

    def fake_post(action, payload):  # type: ignore[no-untyped-def]
        actions.append((action, payload))
        if action == "claim_delivery_outbox":
            return {
                "ok": True,
                "items": [{"outbox_id": "delivery-1", "lease_token": "lease-1", "payload_text": "hello"}],
            }
        if action == "fail_delivery_outbox":
            return {"ok": True, "status": "retry"}
        raise AssertionError(action)

    monkeypatch.setattr(publish_outbox, "post_remote_action", fake_post)
    monkeypatch.setattr(
        publish_outbox,
        "send_telegram_message",
        lambda *_args, **_kwargs: {
            "ok": False,
            "error": "telegram_http_error",
            "status_code": 429,
            "retryable": True,
            "retry_after_seconds": 60,
        },
    )

    summary = process_remote_delivery_outbox(config, now, limit=1)

    assert summary["telegram_failed"] == 1
    fail_payload = next(payload for action, payload in actions if action == "fail_delivery_outbox")
    assert fail_payload["retryable"] is True
    assert fail_payload["retry_after_seconds"] == 60


def test_remote_outbox_retries_idempotent_ack_without_resending(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import publish_outbox

    send_calls = 0
    ack_calls = 0

    def fake_post(action, _payload):  # type: ignore[no-untyped-def]
        nonlocal ack_calls
        if action == "claim_delivery_outbox":
            return {
                "ok": True,
                "items": [{"delivery_id": "delivery-1", "lease_token": "lease-1", "payload": {"text": "hello"}}],
            }
        if action == "ack_delivery_outbox":
            ack_calls += 1
            if ack_calls < 3:
                raise RuntimeError("response lost")
            return {"ok": True, "status": "delivered"}
        raise AssertionError(action)

    def fake_send(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        nonlocal send_calls
        send_calls += 1
        return {"ok": True, "message_id": 777, "chat_id": -100}

    monkeypatch.setattr(publish_outbox, "post_remote_action", fake_post)
    monkeypatch.setattr(publish_outbox, "send_telegram_message", fake_send)

    summary = process_remote_delivery_outbox(config, now, limit=1)

    assert summary["telegram_sent"] == 1
    assert summary["telegram_failed"] == 0
    assert send_calls == 1
    assert ack_calls == 3


def test_remote_outbox_claims_and_acks_each_item_before_claiming_the_next(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import publish_outbox

    monkeypatch.delenv("DELIVERY_LEASE_SECONDS", raising=False)
    sequence: list[str] = []
    next_delivery = 1

    def fake_post(action, payload):  # type: ignore[no-untyped-def]
        nonlocal next_delivery
        if action == "claim_delivery_outbox":
            sequence.append("claim")
            assert payload["limit"] == 1
            assert payload["lease_seconds"] == 900
            delivery = next_delivery
            next_delivery += 1
            return {
                "ok": True,
                "items": [
                    {
                        "delivery_id": f"delivery-{delivery}",
                        "lease_token": f"lease-{delivery}",
                        "payload_text": f"message-{delivery}",
                    }
                ],
            }
        if action == "ack_delivery_outbox":
            sequence.append(f"ack:{payload['delivery_id']}")
            return {"ok": True, "status": "delivered"}
        raise AssertionError(action)

    def fake_send(_token, _destination, text, _config, **_kwargs):  # type: ignore[no-untyped-def]
        sequence.append(f"send:{text}")
        message_number = text.rsplit("-", 1)[-1]
        return {"ok": True, "message_id": message_number, "chat_id": -100}

    monkeypatch.setattr(publish_outbox, "post_remote_action", fake_post)
    monkeypatch.setattr(publish_outbox, "send_telegram_message", fake_send)

    summary = process_remote_delivery_outbox(config, now, limit=2)

    assert summary["telegram_outbox_claimed"] == 2
    assert summary["telegram_sent"] == 2
    assert summary["telegram_failed"] == 0
    assert sequence == [
        "claim",
        "send:message-1",
        "ack:delivery-1",
        "claim",
        "send:message-2",
        "ack:delivery-2",
    ]


def test_remote_outbox_quarantines_ambiguous_send_instead_of_retrying(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import publish_outbox

    failed_payloads: list[dict[str, object]] = []

    def fake_post(action, payload):  # type: ignore[no-untyped-def]
        if action == "claim_delivery_outbox":
            return {
                "ok": True,
                "items": [{"delivery_id": "delivery-1", "lease_token": "lease-1", "payload_text": "hello"}],
                "dead_letter_count": 0,
            }
        if action == "fail_delivery_outbox":
            failed_payloads.append(payload)
            return {"ok": True, "status": "dead_letter"}
        raise AssertionError(action)

    monkeypatch.setattr(publish_outbox, "post_remote_action", fake_post)
    monkeypatch.setattr(
        publish_outbox,
        "send_telegram_message",
        lambda *_args, **_kwargs: {
            "ok": False,
            "error": "telegram_request_failed",
            "retryable": True,
        },
    )

    summary = process_remote_delivery_outbox(config, now, limit=1)

    assert summary["telegram_sent"] == 0
    assert summary["telegram_failed"] == 1
    assert summary["telegram_dead_letter"] == 1
    assert failed_payloads[0]["error"] == "telegram_delivery_outcome_unknown"
    assert failed_payloads[0]["retryable"] is False


def test_remote_outbox_quarantines_send_message_5xx_instead_of_retrying(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import publish_outbox

    failed_payloads: list[dict[str, object]] = []

    def fake_post(action, payload):  # type: ignore[no-untyped-def]
        if action == "claim_delivery_outbox":
            return {
                "ok": True,
                "items": [{"delivery_id": "delivery-1", "lease_token": "lease-1", "payload_text": "hello"}],
                "dead_letter_count": 0,
            }
        if action == "fail_delivery_outbox":
            failed_payloads.append(payload)
            return {"ok": True, "status": "dead_letter"}
        raise AssertionError(action)

    monkeypatch.setattr(publish_outbox, "post_remote_action", fake_post)
    monkeypatch.setattr(
        publish_outbox,
        "send_telegram_message",
        lambda *_args, **_kwargs: {
            "ok": False,
            "error": "telegram_http_error",
            "delivery_stage": "send_message",
            "status_code": 502,
            "retryable": True,
        },
    )

    summary = process_remote_delivery_outbox(config, now, limit=1)

    assert summary["telegram_failed"] == 1
    assert failed_payloads[0]["error"] == "telegram_delivery_outcome_unknown"
    assert failed_payloads[0]["retryable"] is False


def test_remote_outbox_ack_failure_preserves_external_id_and_never_resends(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import publish_outbox

    send_calls = 0
    ack_calls = 0
    failed_payloads: list[dict[str, object]] = []

    def fake_post(action, payload):  # type: ignore[no-untyped-def]
        nonlocal ack_calls
        if action == "claim_delivery_outbox":
            return {
                "ok": True,
                "items": [{"delivery_id": "delivery-1", "lease_token": "lease-1", "payload_text": "hello"}],
                "dead_letter_count": 0,
            }
        if action == "ack_delivery_outbox":
            ack_calls += 1
            raise RuntimeError("ack unavailable")
        if action == "fail_delivery_outbox":
            failed_payloads.append(payload)
            return {"ok": True, "status": "dead_letter"}
        raise AssertionError(action)

    def fake_send(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        nonlocal send_calls
        send_calls += 1
        return {"ok": True, "message_id": 777, "chat_id": -100}

    monkeypatch.setattr(publish_outbox, "post_remote_action", fake_post)
    monkeypatch.setattr(publish_outbox, "send_telegram_message", fake_send)

    summary = process_remote_delivery_outbox(config, now, limit=1)

    assert summary["telegram_sent"] == 0
    assert summary["telegram_failed"] == 1
    assert send_calls == 1
    assert ack_calls == 3
    assert failed_payloads[0]["external_message_id"] == 777
    assert failed_payloads[0]["retryable"] is False


def test_remote_outbox_surfaces_expired_lease_outcome_unknown(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import publish_outbox

    def fake_post(action, _payload):  # type: ignore[no-untyped-def]
        if action != "claim_delivery_outbox":
            raise AssertionError(action)
        return {
            "ok": True,
            "items": [],
            "dead_letter_count": 1,
            "outcome_unknown_count": 1,
        }

    monkeypatch.setattr(publish_outbox, "post_remote_action", fake_post)

    summary = process_remote_delivery_outbox(config, now, limit=1)

    assert summary["telegram_sent"] == 0
    assert summary["telegram_dead_letter"] == 1
    assert summary["outcome_unknown_count"] == 1


def test_remote_outbox_accepts_already_delivered_requested_id(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import publish_outbox

    claims: list[dict[str, object]] = []

    def fake_post(action, payload):  # type: ignore[no-untyped-def]
        assert action == "claim_delivery_outbox"
        claims.append(payload)
        return {
            "ok": True,
            "items": [],
            "requested_status": "delivered",
            "external_message_id": "777",
            "dead_letter_count": 0,
        }

    monkeypatch.setattr(publish_outbox, "post_remote_action", fake_post)

    summary = process_remote_delivery_outbox(
        config,
        now,
        limit=1,
        delivery_id="daily:2026-07-16",
    )

    assert claims[0]["delivery_id"] == "daily:2026-07-16"
    assert summary["telegram_sent"] == 0
    assert summary["telegram_failed"] == 0
    assert summary["telegram_already_delivered"] == 1
    assert summary["requested_status"] == "delivered"


def test_publish_command_fails_when_telegram_transport_is_not_configured(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import publish_outbox

    (tmp_path / "config.yaml").write_text("telegram:\n  enabled: false\n", encoding="utf-8")
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    assert publish_outbox.main(["--root", str(tmp_path)]) == 1
