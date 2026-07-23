from __future__ import annotations

from copy import deepcopy
from datetime import timedelta

import pytest

from curator.cluster import cluster_articles
from curator.telegram_publisher import (
    _post_telegram_method,
    build_telegram_message,
    enqueue_telegram_delivery,
    enqueue_unsent_telegram_clusters,
    enqueue_unsent_telegram_clusters_to_remote,
    ensure_telegram_delivery_outbox,
    initialize_telegram_state,
    mark_telegram_sent,
    process_telegram_delivery_outbox,
    publish_unsent_telegram_clusters,
    send_telegram_message,
    telegram_is_configured,
    unsent_telegram_clusters,
    validate_telegram_chat,
)

from conftest import make_article


def published_cluster(config, now):  # type: ignore[no-untyped-def]
    state = {"pending_clusters": [], "published_clusters": []}
    articles = [
        make_article(
            "Alpha shareholder proposal",
            "https://example.com/a?x=1&y=2",
            summary="Governance proposal at Alpha.",
            relevance_level="high",
        ),
        make_article(
            "Alpha board response",
            "https://example.com/b",
            summary="Board response at Alpha.",
            relevance_level="high",
        ),
    ]
    cluster_articles(articles, state, config, now)
    cluster_articles([], state, config, now + timedelta(minutes=21))
    return state["published_clusters"][0]


class ForbiddenClient:
    def post(self, *_args, **_kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("Telegram HTTP must never be called")


def test_message_rendering_remains_available_for_read_only_preview(config, now) -> None:  # type: ignore[no-untyped-def]
    message = build_telegram_message(published_cluster(config, now), config)

    assert "<a href=" in message
    assert "https://example.com/a?x=1&amp;y=2" in message
    assert "Alpha shareholder proposal</a>" in message
    assert ">https://example.com/a?x=1&y=2<" not in message
    assert len(message) <= config["telegram"]["max_message_chars"]


def test_credentials_and_config_cannot_enable_sender(config, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "real-looking-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@real_looking_channel")
    config["telegram"]["enabled"] = True
    config["telegram"]["preflight_get_chat"] = False

    assert telegram_is_configured(config) is False


@pytest.mark.parametrize("method", ["getChat", "sendMessage", "deleteMessage", "forwardMessage"])
def test_every_bot_api_method_is_rejected_before_http(config, method) -> None:  # type: ignore[no-untyped-def]
    result = _post_telegram_method(
        "token",
        method,
        {"chat_id": "@channel"},
        timeout=20,
        client=ForbiddenClient(),
    )

    assert result == {
        "ok": False,
        "error": "telegram_outbound_disabled",
        "description": "",
        "retryable": False,
    }


def test_chat_preflight_is_rejected_even_when_configured_to_skip_it(config) -> None:  # type: ignore[no-untyped-def]
    config["telegram"]["preflight_get_chat"] = False

    result = validate_telegram_chat(
        "token",
        "@channel",
        config,
        client=ForbiddenClient(),
    )

    assert result["error"] == "telegram_outbound_disabled"
    assert result["delivery_stage"] == "policy"


def test_send_is_rejected_before_http_or_rendering(config) -> None:  # type: ignore[no-untyped-def]
    result = send_telegram_message(
        "token",
        "@channel",
        "<b>market-sensitive text</b>",
        config,
        client=ForbiddenClient(),
        disable_web_page_preview=False,
    )

    assert result["ok"] is False
    assert result["error"] == "telegram_outbound_disabled"
    assert result["delivery_stage"] == "policy"


def test_delivery_ack_never_mutates_historical_state(config, now) -> None:  # type: ignore[no-untyped-def]
    cluster = published_cluster(config, now)
    state = {
        "telegram_sent_cluster_guids": ["historical"],
        "telegram_send_records": [{"guid": "historical", "message_id": 1}],
    }
    before = deepcopy(state)

    marked = mark_telegram_sent(
        state,
        cluster,
        now,
        {"ok": True, "message_id": 999, "chat_id": -100},
    )

    assert marked is False
    assert state == before


def test_outbox_reader_does_not_create_or_replace_queue() -> None:
    missing: dict[str, object] = {}
    malformed: dict[str, object] = {"telegram_delivery_outbox": {"status": "pending"}}
    historical = {"telegram_delivery_outbox": [{"outbox_id": "old", "status": "pending"}]}

    assert ensure_telegram_delivery_outbox(missing) == []
    assert missing == {}
    assert ensure_telegram_delivery_outbox(malformed) == []
    assert malformed == {"telegram_delivery_outbox": {"status": "pending"}}
    snapshot = ensure_telegram_delivery_outbox(historical)
    assert snapshot == historical["telegram_delivery_outbox"]
    assert snapshot is not historical["telegram_delivery_outbox"]
    snapshot[0]["status"] = "delivered"
    assert historical["telegram_delivery_outbox"][0]["status"] == "pending"  # type: ignore[index]


def test_all_local_queue_entrypoints_are_read_only(config, now) -> None:  # type: ignore[no-untyped-def]
    cluster = published_cluster(config, now)
    state = {
        "published_clusters": [cluster],
        "telegram_sent_cluster_guids": [],
        "telegram_delivery_outbox": [{"outbox_id": "old", "status": "pending"}],
    }
    before = deepcopy(state)

    assert enqueue_telegram_delivery(state, cluster, config, now) is None
    assert enqueue_unsent_telegram_clusters(state, config, now) == 0
    assert process_telegram_delivery_outbox(
        state,
        config,
        now,
        client=ForbiddenClient(),
        max_items=100,
    ) == {
        "telegram_outbox_processed": 0,
        "telegram_sent": 0,
        "telegram_failed": 0,
        "telegram_retried": 0,
        "telegram_dead_letter": 0,
        "telegram_outbox_skipped": 1,
    }
    assert publish_unsent_telegram_clusters(state, config, now) == {
        "telegram_sent": 0,
        "telegram_failed": 0,
    }
    assert state == before


def test_remote_enqueue_is_a_read_only_policy_noop(config, now) -> None:  # type: ignore[no-untyped-def]
    cluster = published_cluster(config, now)
    state = {
        "published_clusters": [cluster],
        "telegram_sent_cluster_guids": [],
        "telegram_delivery_outbox": [{"outbox_id": "old", "status": "retry"}],
    }
    before = deepcopy(state)

    result = enqueue_unsent_telegram_clusters_to_remote(state, config, now)

    assert result == {
        "telegram_outbox_enqueued": 0,
        "telegram_outbox_rejected": 0,
        "telegram_outbox_enqueue_failed": 0,
        "telegram_outbox_enqueue_skipped": 1,
        "telegram_outbox_rights_blocked": 0,
    }
    assert state == before


def test_disabled_initialization_and_unsent_selection_do_not_mutate_state(config, now) -> None:  # type: ignore[no-untyped-def]
    cluster = published_cluster(config, now)
    state = {
        "published_clusters": [cluster],
        "telegram_sent_cluster_guids": [],
    }
    before = deepcopy(state)

    initialize_telegram_state(state, config, now)

    assert unsent_telegram_clusters(state, config) == []
    assert state == before
