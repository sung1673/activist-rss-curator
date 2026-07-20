from __future__ import annotations

from copy import deepcopy
from datetime import timedelta

from curator.cluster import cluster_articles
from curator.telegram_publisher import (
    build_telegram_message,
    enqueue_telegram_delivery,
    enqueue_unsent_telegram_clusters_to_remote,
    initialize_telegram_state,
    process_telegram_delivery_outbox,
    publish_unsent_telegram_clusters,
    send_telegram_message,
    unsent_telegram_clusters,
)

from conftest import make_article


def published_cluster(config, now):  # type: ignore[no-untyped-def]
    state = {"pending_clusters": [], "published_clusters": []}
    articles = [
        make_article(
            "고려아연 소액주주, 사외이사 검찰 고발",
            "https://example.com/a",
            summary="고려아연 소액주주연대",
            relevance_level="high",
        ),
        make_article(
            "고려아연 소액주주, 금융위 진정",
            "https://example.com/b",
            summary="고려아연 소액주주연대",
            relevance_level="high",
        ),
    ]
    cluster_articles(articles, state, config, now)
    cluster_articles([], state, config, now + timedelta(minutes=21))
    return state["published_clusters"][0]


def single_article_cluster(config, now):  # type: ignore[no-untyped-def]
    state = {"pending_clusters": [], "published_clusters": []}
    article = make_article(
        "금융당국, 상장회사 임원보수 공시 강화",
        "https://example.com/single",
        summary="성과보수와 주식보상 공시가 투자자 보호 쟁점으로 부각됐다",
        relevance_level="high",
    )
    cluster_articles([article], state, config, now)
    cluster_articles([], state, config, now + timedelta(minutes=21))
    return state["published_clusters"][0]


def configure_telegram_right(config, *, revoked_at=None):  # type: ignore[no-untyped-def]
    config["source_rights"] = {
        "enforce": True,
        "records": [
            {
                "source_right_id": "telegram:licensed",
                "source_category": "authorized_telegram",
                "source_identity": "licensed",
                "scope": "collection,ai,redistribution",
                "evidence_ref": "evidence://test/licensed",
                "valid_from": "2021-01-01",
                "revoked_at": revoked_at,
                "allow_ai": True,
                "allow_redistribution": True,
                "status": "active",
            }
        ],
    }


def attach_telegram_lineage(cluster):  # type: ignore[no-untyped-def]
    article = cluster["articles"][0]
    article["source_kind"] = "telegram_reference"
    article["source_right_id"] = "telegram:licensed"
    article["telegram_source_handle"] = "licensed"
    return cluster


def test_telegram_message_uses_html_links_without_visible_raw_urls(config, now) -> None:  # type: ignore[no-untyped-def]
    cluster = published_cluster(config, now)
    message = build_telegram_message(cluster, config)

    assert "<a href=" in message
    assert "대표 기사 보기" not in message
    assert "분류:" not in message
    assert "기준시각:" not in message
    assert "[ 지배구조·주주권 ]" not in message
    assert "<b>고려아연</b>" not in message
    assert "테스트뉴스 - " not in message
    assert "고려아연 소액주주, 사외이사 검찰 고발</a>" in message
    assert "1. " in message
    assert ">https://example.com/a<" not in message
    assert "\nhttps://example.com/a" not in message
    assert len(message) <= config["telegram"]["max_message_chars"]


def test_single_article_message_only_shows_trimmed_article_link(config, now) -> None:  # type: ignore[no-untyped-def]
    cluster = single_article_cluster(config, now)
    message = build_telegram_message(cluster, config)

    assert not message.startswith("<b>")
    assert "<a href=" in message
    assert "금융당국, 상장회사 임원보수 공시 강화</a>" in message
    assert "테스트뉴스 - " not in message
    assert "1. " not in message
    assert "본문:" not in message


def test_telegram_initialization_does_not_backfill_old_clusters(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@test_channel")
    old_cluster = published_cluster(config, now)
    new_cluster = deepcopy(old_cluster)
    new_cluster["guid"] = "cluster:new:20260425:1"

    state = {
        "published_clusters": [old_cluster],
        "telegram_sent_cluster_guids": [],
    }
    initialize_telegram_state(state, config, now)
    assert old_cluster["guid"] in state["telegram_sent_cluster_guids"]

    state["published_clusters"].append(new_cluster)
    assert unsent_telegram_clusters(state, config) == [new_cluster]


def test_publish_unsent_telegram_clusters_marks_success(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_publisher

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@test_channel")
    cluster = published_cluster(config, now)
    state = {
        "published_clusters": [cluster],
        "telegram_sent_cluster_guids": [],
        "telegram_send_records": [],
        "telegram_initialized_at": "2026-04-25T08:00:00+09:00",
    }

    def fake_send(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        return {"ok": True, "message_id": 123, "chat_id": -100}

    monkeypatch.setattr(telegram_publisher, "send_telegram_message", fake_send)
    summary = publish_unsent_telegram_clusters(state, config, now)

    assert summary == {"telegram_sent": 1, "telegram_failed": 0}
    assert cluster["guid"] in state["telegram_sent_cluster_guids"]
    assert state["telegram_send_records"][0]["message_id"] == 123


def test_single_article_publish_enables_web_page_preview(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_publisher

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@test_channel")
    cluster = single_article_cluster(config, now)
    state = {
        "published_clusters": [cluster],
        "telegram_sent_cluster_guids": [],
        "telegram_send_records": [],
        "telegram_initialized_at": "2026-04-25T08:00:00+09:00",
    }
    kwargs_seen = []

    def fake_send(*_args, **kwargs):  # type: ignore[no-untyped-def]
        kwargs_seen.append(kwargs)
        return {"ok": True, "message_id": 123, "chat_id": -100}

    monkeypatch.setattr(telegram_publisher, "send_telegram_message", fake_send)
    summary = publish_unsent_telegram_clusters(state, config, now)

    assert summary == {"telegram_sent": 1, "telegram_failed": 0}
    assert kwargs_seen[0]["disable_web_page_preview"] is False


def test_send_message_preflights_chat_and_requires_external_message_id(config) -> None:  # type: ignore[no-untyped-def]
    class Response:
        status_code = 200

        def __init__(self, payload):  # type: ignore[no-untyped-def]
            self.payload = payload

        def json(self):  # type: ignore[no-untyped-def]
            return self.payload

    class Client:
        def __init__(self) -> None:
            self.calls = []

        def post(self, url, json):  # type: ignore[no-untyped-def]
            self.calls.append((url, json))
            if url.endswith("/getChat"):
                return Response({"ok": True, "result": {"id": -100, "username": "test_channel"}})
            return Response({"ok": True, "result": {"chat": {"id": -100}}})

    client = Client()
    result = send_telegram_message("token", "@test_channel", "hello", config, client=client)  # type: ignore[arg-type]

    assert result["ok"] is False
    assert result["error"] == "telegram_missing_external_message_id"
    assert [url.rsplit("/", 1)[-1] for url, _payload in client.calls] == ["getChat", "sendMessage"]


def test_chat_not_found_fails_before_send(config) -> None:  # type: ignore[no-untyped-def]
    class Response:
        status_code = 400

        def json(self):  # type: ignore[no-untyped-def]
            return {"ok": False, "description": "Bad Request: chat not found"}

    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def post(self, _url, json):  # type: ignore[no-untyped-def]
            self.calls += 1
            return Response()

    client = Client()
    result = send_telegram_message("token", "@missing", "hello", config, client=client)  # type: ignore[arg-type]

    assert result["ok"] is False
    assert result["error"] == "telegram_chat_validation_failed"
    assert result["retryable"] is False
    assert client.calls == 1


def test_outbox_retries_429_and_does_not_mark_cluster_delivered(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_publisher

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@test_channel")
    cluster = single_article_cluster(config, now)
    state = {"published_clusters": [cluster], "telegram_sent_cluster_guids": []}
    enqueue_telegram_delivery(state, cluster, config, now)

    monkeypatch.setattr(
        telegram_publisher,
        "send_telegram_message",
        lambda *_args, **_kwargs: {
            "ok": False,
            "error": "telegram_http_error",
            "status_code": 429,
            "retryable": True,
            "retry_after_seconds": 42,
        },
    )
    summary = process_telegram_delivery_outbox(state, config, now)

    entry = state["telegram_delivery_outbox"][0]
    assert summary["telegram_failed"] == 1
    assert summary["telegram_retried"] == 1
    assert entry["status"] == "retry"
    assert entry["attempt_count"] == 1
    assert cluster["guid"] not in state["telegram_sent_cluster_guids"]


def test_local_outbox_quarantines_send_message_5xx(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_publisher

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@test_channel")
    cluster = single_article_cluster(config, now)
    state = {"published_clusters": [cluster], "telegram_sent_cluster_guids": []}
    enqueue_telegram_delivery(state, cluster, config, now)
    monkeypatch.setattr(
        telegram_publisher,
        "send_telegram_message",
        lambda *_args, **_kwargs: {
            "ok": False,
            "error": "telegram_api_error",
            "delivery_stage": "send_message",
            "status_code": 500,
            "retryable": True,
        },
    )

    summary = process_telegram_delivery_outbox(state, config, now)

    entry = state["telegram_delivery_outbox"][0]
    assert summary["telegram_retried"] == 0
    assert summary["telegram_dead_letter"] == 1
    assert entry["status"] == "dead_letter"
    assert entry["last_error"] == "telegram_delivery_outcome_unknown"


def test_outbox_dead_letters_nonretryable_chat_failure(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_publisher

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@missing")
    cluster = single_article_cluster(config, now)
    state = {"published_clusters": [cluster], "telegram_sent_cluster_guids": []}
    enqueue_telegram_delivery(state, cluster, config, now)
    monkeypatch.setattr(
        telegram_publisher,
        "send_telegram_message",
        lambda *_args, **_kwargs: {
            "ok": False,
            "error": "telegram_chat_validation_failed",
            "retryable": False,
        },
    )

    summary = process_telegram_delivery_outbox(state, config, now)

    assert summary["telegram_dead_letter"] == 1
    assert state["telegram_delivery_outbox"][0]["status"] == "dead_letter"


def test_unsent_clusters_are_durably_enqueued_in_remote_outbox(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_publisher

    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@test_channel")
    cluster = single_article_cluster(config, now)
    state = {"published_clusters": [cluster], "telegram_sent_cluster_guids": []}
    calls = []
    monkeypatch.setattr(telegram_publisher, "remote_api_configured", lambda: True)

    def fake_post(action, payload):  # type: ignore[no-untyped-def]
        calls.append((action, payload))
        return {"ok": True, "accepted": len(payload["deliveries"]), "rejected": 0}

    monkeypatch.setattr(telegram_publisher, "post_remote_action", fake_post)

    summary = enqueue_unsent_telegram_clusters_to_remote(state, config, now)

    assert summary["telegram_outbox_enqueued"] == 1
    assert summary["telegram_outbox_enqueue_failed"] == 0
    assert calls[0][0] == "enqueue_delivery_outbox"
    delivery = calls[0][1]["deliveries"][0]
    assert delivery["idempotency_key"] == cluster["guid"]
    assert delivery["payload"]["text"]
    assert state["telegram_delivery_outbox"][0]["status"] == "remote_queued"


def test_remote_outbox_payload_carries_cluster_and_article_right_lineage(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_publisher

    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@test_channel")
    configure_telegram_right(config)
    cluster = attach_telegram_lineage(single_article_cluster(config, now))
    state = {"published_clusters": [cluster], "telegram_sent_cluster_guids": []}
    calls = []
    monkeypatch.setattr(telegram_publisher, "remote_api_configured", lambda: True)

    def fake_post(action, payload):  # type: ignore[no-untyped-def]
        calls.append((action, payload))
        return {"ok": True, "accepted": len(payload["deliveries"]), "rejected": 0}

    monkeypatch.setattr(telegram_publisher, "post_remote_action", fake_post)

    summary = enqueue_unsent_telegram_clusters_to_remote(state, config, now)

    assert summary["telegram_outbox_enqueued"] == 1
    payload = calls[0][1]["deliveries"][0]["payload"]
    assert payload["source_kind"] == "telegram_reference"
    assert payload["source_right_id"] == "telegram:licensed"
    assert payload["rights_lineage_complete"] is True
    assert payload["source_right_ids"] == ["telegram:licensed"]
    assert payload["article_sources"] == [
        {
            "canonical_url_hash": cluster["articles"][0]["canonical_url_hash"],
            "source_kind": "telegram_reference",
            "source_right_id": "telegram:licensed",
        }
    ]


def test_revoked_right_blocks_existing_pending_outbox_before_remote_enqueue(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_publisher

    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@test_channel")
    configure_telegram_right(config)
    cluster = attach_telegram_lineage(single_article_cluster(config, now))
    state = {"published_clusters": [cluster], "telegram_sent_cluster_guids": []}
    assert enqueue_telegram_delivery(state, cluster, config, now) is not None
    config["source_rights"]["records"][0]["revoked_at"] = now.date().isoformat()  # type: ignore[index]
    calls = []
    monkeypatch.setattr(telegram_publisher, "remote_api_configured", lambda: True)
    monkeypatch.setattr(telegram_publisher, "post_remote_action", lambda *args, **kwargs: calls.append((args, kwargs)))

    summary = enqueue_unsent_telegram_clusters_to_remote(state, config, now)

    assert summary["telegram_outbox_enqueued"] == 0
    assert summary["telegram_outbox_rights_blocked"] == 1
    assert calls == []
    assert state["telegram_delivery_outbox"][0]["status"] == "blocked_source_right"


def test_mixed_cluster_delivery_uses_only_independent_article_after_revocation(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_publisher

    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@test_channel")
    configure_telegram_right(config, revoked_at=now.date().isoformat())
    cluster = published_cluster(config, now)
    attach_telegram_lineage(cluster)
    cluster["articles"][1]["source_kind"] = "direct"
    telegram_title = cluster["articles"][0]["clean_title"]
    independent_title = cluster["articles"][1]["clean_title"]
    state = {"published_clusters": [cluster], "telegram_sent_cluster_guids": []}
    calls = []
    monkeypatch.setattr(telegram_publisher, "remote_api_configured", lambda: True)

    def fake_post(action, payload):  # type: ignore[no-untyped-def]
        calls.append((action, payload))
        return {"ok": True, "accepted": len(payload["deliveries"]), "rejected": 0}

    monkeypatch.setattr(telegram_publisher, "post_remote_action", fake_post)

    summary = enqueue_unsent_telegram_clusters_to_remote(state, config, now)

    assert summary["telegram_outbox_enqueued"] == 1
    payload = calls[0][1]["deliveries"][0]["payload"]
    assert telegram_title not in payload["text"]
    assert independent_title in payload["text"]
    assert payload["source_kind"] == "direct"
    assert payload["source_right_id"] is None
    assert payload["source_right_ids"] == []
    assert len(payload["article_sources"]) == 1
