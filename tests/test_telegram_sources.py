from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from conftest import make_article

from curator.state import article_record
from curator.telegram_sources import (
    TelegramFloodWait,
    auto_join_candidates,
    backfill_telegram_messages,
    canonicalize_telegram_url,
    canonicalize_telegram_channels,
    channel_quality_metrics,
    collect_telegram_sources,
    extract_urls,
    expand_similar_channels,
    import_joined_public_channels,
    load_env_files,
    mark_deleted_message,
    match_message_to_articles,
    message_key,
    normalize_telegram_message,
    ordered_message_tokens,
    parse_handle_list,
    pending_remote_messages,
    rematch_telegram_articles,
    reconcile_recent_deletions,
    refresh_channel_runtime_quality,
    score_channel_candidate,
    telegram_candidate_articles,
    telegram_issue_signals,
    telegram_run_record,
    telegram_state_stats,
    sync_telegram_metadata_to_remote_api,
    sync_telegram_to_remote_api,
    sync_telegram_batch_to_remote_api,
    upsert_telegram_message,
)


class FakeTelegramClient:
    def __init__(
        self,
        messages_by_handle: dict[str, list[dict[str, object]]] | None = None,
        *,
        fail_handles: set[str] | None = None,
        joined_channels: list[dict[str, object]] | None = None,
        recommendations_by_handle: dict[str, list[dict[str, object]]] | None = None,
    ) -> None:
        self.messages_by_handle = messages_by_handle or {}
        self.fail_handles = fail_handles or set()
        self.joined_channels = joined_channels or []
        self.recommendations_by_handle = recommendations_by_handle or {}
        self.join_calls: list[dict[str, object]] = []
        self.iter_calls: list[dict[str, object]] = []

    async def get_channel_info(self, channel: dict[str, object]) -> dict[str, object]:
        handle = str(channel.get("handle") or "")
        if handle in self.fail_handles:
            raise TelegramFloodWait(42)
        return {
            "handle": handle,
            "telegram_channel_id": f"id-{handle}",
            "title": f"{handle} 채널",
            "joined": True,
        }

    async def iter_messages(
        self,
        channel: dict[str, object],
        *,
        min_id: int,
        limit: int,
        since: datetime | None = None,
        max_id: int = 0,
    ) -> list[dict[str, object]]:
        handle = str(channel.get("handle") or "")
        self.iter_calls.append(
            {
                "handle": handle,
                "min_id": min_id,
                "max_id": max_id,
                "limit": limit,
            }
        )
        messages = [
            message
            for message in self.messages_by_handle.get(handle, [])
            if int(message.get("id") or message.get("telegram_message_id") or 0)
            > min_id
            and (
                not max_id
                or int(message.get("id") or message.get("telegram_message_id") or 0)
                < max_id
            )
        ]
        if since is not None:
            messages = [
                message
                for message in messages
                if not isinstance(message.get("date"), datetime)
                or message["date"] >= since
            ]
        if since is not None or max_id or not min_id:
            page = sorted(
                messages,
                key=lambda message: int(
                    message.get("id") or message.get("telegram_message_id") or 0
                ),
                reverse=True,
            )[:limit]
            return list(reversed(page))
        return sorted(
            messages,
            key=lambda message: int(
                message.get("id") or message.get("telegram_message_id") or 0
            ),
        )[:limit]

    async def recommend_channels(
        self, seed_channel: dict[str, object], *, limit: int
    ) -> list[dict[str, object]]:
        handle = str(seed_channel.get("handle") or "")
        recommendations = self.recommendations_by_handle.get(handle) or [
            {
                "handle": "good_stock_news",
                "title": "경제 증권 주식 뉴스",
                "description": "공시 실적 환율",
            },
            {
                "handle": "bad_vip",
                "title": "급등주 보장 VIP방",
                "description": "무료추천 리딩방",
            },
        ]
        return recommendations[:limit]

    async def join_channel(self, candidate: dict[str, object]) -> dict[str, object]:
        self.join_calls.append(candidate)
        return {"ok": True}

    async def list_joined_public_channels(
        self, *, limit: int
    ) -> list[dict[str, object]]:
        return self.joined_channels[:limit]

    async def close(self) -> None:
        return None


def telegram_config(config: dict[str, object]) -> dict[str, object]:
    config["telegram_sources"] = {
        "enabled": True,
        "channels": [{"handle": "marketnews"}],
        "backfill_limit": 100,
        "incremental_limit": 200,
        "weak_match_min_overlap": 2,
        "weak_match_limit_per_message": 5,
        "discover_enabled": False,
        "auto_join_enabled": False,
    }
    config["source_rights"] = {
        "enforce": True,
        "records": [
            {
                "source_right_id": "telegram:marketnews",
                "source_category": "authorized_telegram",
                "source_identity": "marketnews",
                "scope": "collection,ai,redistribution",
                "evidence_ref": "evidence://test/marketnews",
                "valid_from": "2021-01-01",
                "allow_ai": True,
                "allow_redistribution": True,
                "status": "active",
            }
        ],
    }
    return config


def authorize_telegram_handles(config: dict[str, object], *handles: str) -> None:
    config["source_rights"] = {
        "enforce": True,
        "records": [
            {
                "source_right_id": f"telegram:{handle}",
                "source_category": "authorized_telegram",
                "source_identity": handle,
                "scope": "collection,ai,redistribution",
                "evidence_ref": f"evidence://test/{handle}",
                "valid_from": "2021-01-01",
                "allow_ai": True,
                "allow_redistribution": True,
                "status": "active",
            }
            for handle in handles
        ],
    }


def test_extract_urls_strips_trailing_punctuation() -> None:
    assert extract_urls("확인 https://example.com/a?utm_source=x). 다음") == [
        "https://example.com/a?utm_source=x"
    ]


def test_canonicalize_telegram_url_removes_tracking_params() -> None:
    assert (
        canonicalize_telegram_url(
            "HTTPS://Example.COM/news/?utm_source=tg&fbclid=1#frag"
        )
        == "https://example.com/news"
    )


def test_load_env_files_includes_api_env(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.delenv("ACTIVIST_API_URL", raising=False)
    (tmp_path / ".env.api").write_text(
        "ACTIVIST_API_URL=https://example.com/api.php\n", encoding="utf-8"
    )

    loaded = load_env_files(tmp_path)

    assert tmp_path / ".env.api" in loaded
    assert "example.com/api.php" in os.environ["ACTIVIST_API_URL"]


def test_parse_handle_list_does_not_split_letter_s() -> None:
    assert parse_handle_list("GoUpstock, LS_WooBond realtime_stock_news") == {
        "GoUpstock",
        "LS_WooBond",
        "realtime_stock_news",
    }


def test_telegram_candidate_articles_promotes_reference_channel_urls(
    config, now
) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {
        "candidate_source_enabled": True,
        "candidate_source_handles": ["activistkorea"],
        "candidate_window_hours": 168,
        "candidate_limit_per_run": 10,
    }
    authorize_telegram_handles(config, "activistkorea")
    message = normalize_telegram_message(
        {
            "handle": "activistkorea",
            "title": "Activist Korea",
            "source_right_id": "telegram:activistkorea",
        },
        {
            "id": 10,
            "date": now,
            "text": "Shareholder governance article\nhttps://example.com/article?utm_source=tg",
        },
        now,
    )
    state = {"telegram_source_messages": [message], "articles": []}

    candidates = telegram_candidate_articles(state, config, now)

    assert len(candidates) == 1
    assert candidates[0]["canonical_url"] == "https://example.com/article"
    assert candidates[0]["telegram_candidate"] is True
    assert candidates[0]["feed_name"] == "Telegram:activistkorea"
    assert candidates[0]["source_right_id"] == "telegram:activistkorea"


def test_telegram_message_upsert_prevents_duplicates_and_tracks_edits(now) -> None:  # type: ignore[no-untyped-def]
    state: dict[str, object] = {}
    channel = {"handle": "marketnews", "telegram_channel_id": "100"}
    first = normalize_telegram_message(channel, {"id": 7, "text": "첫 메시지"}, now)
    edited = normalize_telegram_message(
        channel, {"id": 7, "text": "수정 메시지", "edit_date": now}, now
    )

    assert upsert_telegram_message(state, first) == "inserted"
    assert upsert_telegram_message(state, first) == "unchanged"
    assert upsert_telegram_message(state, edited) == "updated"
    assert len(state["telegram_source_messages"]) == 1  # type: ignore[index]
    assert state["telegram_source_messages"][0]["text"] == "수정 메시지"  # type: ignore[index]


def test_channel_identity_migrates_handle_only_rows_messages_and_matches(now) -> None:  # type: ignore[no-untyped-def]
    state: dict[str, object] = {
        "telegram_source_channels": [
            {
                "handle": "marketnews",
                "telegram_channel_id": "100",
                "last_message_id": 7,
            },
            {"handle": "MarketNews", "last_message_id": 4},
        ],
        "telegram_source_messages": [
            normalize_telegram_message(
                {"handle": "MarketNews"}, {"id": 7, "text": "message"}, now
            )
        ],
        "telegram_article_matches": [
            {
                "article_id": "a1",
                "telegram_message_key": "handle:MarketNews:7",
                "channel_handle": "MarketNews",
            }
        ],
    }

    removed = canonicalize_telegram_channels(state)

    assert removed == 1
    assert len(state["telegram_source_channels"]) == 1
    channel = state["telegram_source_channels"][0]
    assert channel["telegram_channel_id"] == "100"
    assert channel["last_message_id"] == 7
    assert message_key(state["telegram_source_messages"][0]) == "id:100:7"
    assert state["telegram_article_matches"][0]["telegram_message_key"] == "id:100:7"


def test_conflicting_authoritative_channel_ids_are_not_merged(now) -> None:  # type: ignore[no-untyped-def]
    state: dict[str, object] = {
        "telegram_source_channels": [
            {"handle": "reused_handle", "telegram_channel_id": "100"},
            {"handle": "reused_handle", "telegram_channel_id": "200"},
        ],
        "telegram_source_messages": [
            normalize_telegram_message(
                {"handle": "reused_handle", "telegram_channel_id": channel_id},
                {"id": 1, "text": channel_id},
                now,
            )
            for channel_id in ("100", "200")
        ],
    }

    assert canonicalize_telegram_channels(state) == 0
    assert len(state["telegram_source_channels"]) == 2
    assert {message_key(message) for message in state["telegram_source_messages"]} == {
        "id:100:1",
        "id:200:1",
    }


def test_reassigned_handle_disables_stale_channel_identity(config, now) -> None:  # type: ignore[no-untyped-def]
    telegram_config(config)
    state: dict[str, object] = {
        "telegram_source_channels": [
            {
                "handle": "marketnews",
                "telegram_channel_id": "old-id",
                "last_message_id": 1,
                "enabled": True,
            }
        ]
    }
    client = FakeTelegramClient({"marketnews": [{"id": 2, "text": "message"}]})

    summary = collect_telegram_sources(state, config, now, client=client)

    assert summary["telegram_messages_inserted"] == 0
    assert summary["telegram_channel_failed"] == 1
    assert len(state["telegram_source_channels"]) == 1
    stale = state["telegram_source_channels"][0]
    assert stale["enabled"] is False
    assert stale["last_error"] == "channel_identity_review_required"
    assert stale["identity_review_required"] is True
    assert stale["observed_telegram_channel_id"] == "id-marketnews"
    assert state["telegram_source_messages"] == []
    assert client.iter_calls == []

    # The configured handle is registered on every run, but must not revive a
    # channel until an operator clears its authoritative-ID review state.
    second = collect_telegram_sources(state, config, now, client=client)
    assert second["telegram_messages_inserted"] == 0
    assert second["telegram_channel_failed"] == 0
    assert len(state["telegram_source_channels"]) == 1
    assert state["telegram_source_channels"][0]["enabled"] is False


def test_reassigned_handle_requires_explicit_observed_id_approval(config, now) -> None:  # type: ignore[no-untyped-def]
    telegram_config(config)
    config["telegram_sources"]["channels"] = [  # type: ignore[index]
        {
            "handle": "marketnews",
            "telegram_channel_id": "id-marketnews",
            "identity_review_approved": True,
        }
    ]
    state: dict[str, object] = {
        "telegram_source_channels": [
            {
                "handle": "marketnews",
                "telegram_channel_id": "old-id",
                "enabled": False,
                "identity_review_required": True,
                "observed_telegram_channel_id": "id-marketnews",
                "last_error": "channel_identity_review_required",
            }
        ]
    }
    client = FakeTelegramClient({"marketnews": [{"id": 1, "text": "approved"}]})

    summary = collect_telegram_sources(state, config, now, client=client)

    assert summary["telegram_messages_inserted"] == 1
    assert len(state["telegram_source_channels"]) == 2
    stale = next(
        channel
        for channel in state["telegram_source_channels"]  # type: ignore[union-attr]
        if channel["telegram_channel_id"] == "old-id"
    )
    approved = next(
        channel
        for channel in state["telegram_source_channels"]  # type: ignore[union-attr]
        if channel["telegram_channel_id"] == "id-marketnews"
    )
    assert stale["enabled"] is False
    assert approved["enabled"] is True


def test_incremental_collection_exhausts_more_than_one_thousand_messages(
    config, now
) -> None:  # type: ignore[no-untyped-def]
    telegram_config(config)
    config["telegram_sources"]["incremental_limit"] = 200
    state: dict[str, object] = {
        "telegram_source_channels": [
            {
                "handle": "marketnews",
                "telegram_channel_id": "id-marketnews",
                "last_message_id": 1,
                "enabled": True,
            }
        ]
    }
    client = FakeTelegramClient(
        {
            "marketnews": [
                {"id": message_id, "text": f"message {message_id}"}
                for message_id in range(2, 1003)
            ]
        }
    )

    summary = collect_telegram_sources(state, config, now, client=client)

    assert summary["telegram_messages_inserted"] == 1001
    assert summary["telegram_incremental_pages"] == 6
    assert state["telegram_source_channels"][0]["last_message_id"] == 1002
    assert len(state["telegram_source_messages"]) == 1001


def test_incremental_page_budget_checkpoints_without_marking_channel_failed(
    config, now
) -> None:  # type: ignore[no-untyped-def]
    telegram_config(config)
    config["telegram_sources"]["incremental_limit"] = 200
    config["telegram_sources"]["incremental_max_pages"] = 1
    state: dict[str, object] = {
        "telegram_source_channels": [
            {
                "handle": "marketnews",
                "telegram_channel_id": "id-marketnews",
                "last_message_id": 1,
                "enabled": True,
            }
        ]
    }
    client = FakeTelegramClient(
        {
            "marketnews": [
                {"id": message_id, "text": f"message {message_id}"}
                for message_id in range(2, 403)
            ]
        }
    )

    first = collect_telegram_sources(state, config, now, client=client)
    second = collect_telegram_sources(state, config, now, client=client)
    third = collect_telegram_sources(state, config, now, client=client)

    assert first["telegram_messages_inserted"] == 200
    assert second["telegram_messages_inserted"] == 200
    assert third["telegram_messages_inserted"] == 1
    assert first["telegram_incremental_backlog_channels"] == 1
    assert second["telegram_incremental_backlog_channels"] == 1
    assert third["telegram_incremental_backlog_channels"] == 0
    assert first["telegram_channel_failed"] == 0
    assert state["telegram_source_channels"][0]["last_message_id"] == 402  # type: ignore[index]
    assert len(state["telegram_source_messages"]) == 401


def test_collection_syncs_every_new_message_before_local_prune(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_sources

    telegram_config(config)
    config["telegram_sources"].update(
        {
            "incremental_limit": 7000,
            "incremental_max_pages": 1,
            "local_state_message_limit": 5000,
            "remote_batch_size": 1000,
        }
    )
    state: dict[str, object] = {
        "telegram_source_channels": [
            {
                "handle": "marketnews",
                "telegram_channel_id": "id-marketnews",
                "last_message_id": 1,
                "enabled": True,
            }
        ]
    }
    client = FakeTelegramClient(
        {
            "marketnews": [
                {"id": message_id, "text": f"message {message_id}"}
                for message_id in range(2, 6002)
            ]
        }
    )
    uploaded_ids: list[int] = []
    monkeypatch.setattr(telegram_sources, "remote_api_configured", lambda: True)

    def fake_post(_action, payload):  # type: ignore[no-untyped-def]
        uploaded_ids.extend(
            int(message["telegram_message_id"]) for message in payload["messages"]
        )
        return {"ok": True, "messages": len(payload["messages"]), "article_matches": 0}

    monkeypatch.setattr(telegram_sources, "post_remote_action", fake_post)

    summary = collect_telegram_sources(state, config, now, client=client)

    assert len(uploaded_ids) == 6000
    assert sorted(uploaded_ids) == list(range(2, 6002))
    assert summary["telegram_remote_messages"] == 6000
    assert summary["telegram_remote_pending"] == 0
    assert summary["telegram_messages_pruned"] == 1000
    assert summary["telegram_prune_deferred"] == 0
    assert len(state["telegram_source_messages"]) == 5000


def test_collection_defers_prune_when_remote_batch_fails(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_sources

    telegram_config(config)
    config["telegram_sources"].update(
        {
            "incremental_limit": 7000,
            "incremental_max_pages": 1,
            "local_state_message_limit": 5000,
            "remote_batch_size": 2000,
        }
    )
    state: dict[str, object] = {
        "telegram_source_channels": [
            {
                "handle": "marketnews",
                "telegram_channel_id": "id-marketnews",
                "last_message_id": 1,
                "enabled": True,
            }
        ]
    }
    client = FakeTelegramClient(
        {
            "marketnews": [
                {"id": message_id, "text": f"message {message_id}"}
                for message_id in range(2, 6002)
            ]
        }
    )
    calls = 0
    monkeypatch.setattr(telegram_sources, "remote_api_configured", lambda: True)

    def fake_post(_action, payload):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        if calls == 2:
            return {"ok": False, "error": "db_unavailable"}
        return {"ok": True, "messages": len(payload["messages"]), "article_matches": 0}

    monkeypatch.setattr(telegram_sources, "post_remote_action", fake_post)

    summary = collect_telegram_sources(state, config, now, client=client)

    assert summary["telegram_remote_failed"] == 1
    assert summary["telegram_remote_pending"] == 4000
    assert summary["telegram_prune_deferred"] == 1
    assert summary["telegram_messages_pruned"] == 0
    assert len(state["telegram_source_messages"]) == 6000
    assert state["telegram_remote_sync_cursors"]["id:id-marketnews"] == 2001  # type: ignore[index]


def test_remote_sync_batches_all_pending_messages_and_advances_cursor(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_sources

    telegram_config(config)
    config["telegram_sources"]["remote_batch_size"] = 300
    channel = {"handle": "marketnews", "telegram_channel_id": "100"}
    messages = [
        normalize_telegram_message(
            channel, {"id": message_id, "text": f"message {message_id}"}, now
        )
        for message_id in range(1, 1002)
    ]
    state: dict[str, object] = {"telegram_source_messages": messages}
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(telegram_sources, "remote_api_configured", lambda: True)

    def fake_post(_action, payload):  # type: ignore[no-untyped-def]
        calls.append(payload)
        return {"ok": True, "messages": len(payload["messages"]), "article_matches": 0}

    monkeypatch.setattr(telegram_sources, "post_remote_action", fake_post)

    summary = sync_telegram_to_remote_api(state, config)

    assert [len(payload["messages"]) for payload in calls] == [300, 300, 300, 101]
    assert summary["telegram_remote_messages"] == 1001
    assert summary["telegram_remote_pending"] == 0
    assert pending_remote_messages(state) == []
    assert state["telegram_remote_sync_cursors"]["id:100"] == 1001


def test_remote_partial_ack_does_not_advance_cursor(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_sources

    telegram_config(config)
    channel = {"handle": "marketnews", "telegram_channel_id": "100"}
    state: dict[str, object] = {
        "telegram_source_messages": [
            normalize_telegram_message(
                channel, {"id": message_id, "text": "message"}, now
            )
            for message_id in range(1, 4)
        ]
    }
    monkeypatch.setattr(telegram_sources, "remote_api_configured", lambda: True)
    monkeypatch.setattr(
        telegram_sources,
        "post_remote_action",
        lambda *_args, **_kwargs: {"ok": True, "messages": 2, "article_matches": 0},
    )

    summary = sync_telegram_to_remote_api(state, config)

    assert summary["telegram_remote_failed"] == 1
    assert summary["telegram_remote_last_error"] == "remote_partial_message_ack"
    assert summary["telegram_remote_pending"] == 3
    assert state["telegram_remote_sync_cursors"] == {}


def test_remote_chunk_failure_advances_only_through_durable_channel_cursor(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_sources

    telegram_config(config)
    config["telegram_sources"]["remote_batch_size"] = 300
    channel = {
        "handle": "marketnews",
        "telegram_channel_id": "100",
        "last_message_id": 400,
        "enabled": True,
    }
    state: dict[str, object] = {
        "telegram_source_channels": [channel],
        "telegram_source_messages": [
            normalize_telegram_message(
                channel, {"id": message_id, "text": "message"}, now
            )
            for message_id in range(1, 401)
        ],
    }
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(telegram_sources, "remote_api_configured", lambda: True)

    def fake_post(_action, payload):  # type: ignore[no-untyped-def]
        calls.append(payload)
        if len(calls) == 2:
            return {"ok": False, "error": "db_unavailable"}
        return {"ok": True, "messages": len(payload["messages"]), "article_matches": 0}

    monkeypatch.setattr(telegram_sources, "post_remote_action", fake_post)

    summary = sync_telegram_to_remote_api(state, config)

    assert summary["telegram_remote_failed"] == 1
    assert state["telegram_remote_sync_cursors"]["id:100"] == 300
    assert summary["telegram_remote_pending"] == 100
    assert calls[0]["channels"][0]["last_message_id"] == 300
    assert calls[1]["channels"][0]["last_message_id"] == 400


def test_metadata_sync_never_advances_beyond_acknowledged_message_cursor(
    config, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_sources

    telegram_config(config)
    channel = {
        "handle": "marketnews",
        "telegram_channel_id": "100",
        "last_message_id": 400,
        "enabled": True,
        "last_error": "remote_partial_message_ack",
    }
    state: dict[str, object] = {
        "telegram_source_channels": [channel],
        "telegram_remote_sync_cursors": {"id:100": 300},
    }
    payloads: list[dict[str, object]] = []
    monkeypatch.setattr(telegram_sources, "remote_api_configured", lambda: True)

    def fake_post(_action, payload):  # type: ignore[no-untyped-def]
        payloads.append(payload)
        return {"ok": True, "channels": 1, "issue_signals": 0}

    monkeypatch.setattr(telegram_sources, "post_remote_action", fake_post)

    summary = sync_telegram_metadata_to_remote_api(state)

    assert summary["telegram_remote_metadata_synced"] == 1
    assert payloads[0]["channels"][0]["last_message_id"] == 300  # type: ignore[index]
    assert channel["last_message_id"] == 400


def test_metadata_sync_marks_signal_rebuild_as_authoritative(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_sources

    state: dict[str, object] = {
        "telegram_source_channels": [],
        "telegram_issue_signals": [{"article_id": "signal:1"}],
    }
    payloads: list[dict[str, object]] = []
    monkeypatch.setattr(telegram_sources, "remote_api_configured", lambda: True)

    def fake_post(_action, payload):  # type: ignore[no-untyped-def]
        payloads.append(payload)
        return {
            "ok": True,
            "issue_signals": 1,
            "issue_signals_deleted": 2,
        }

    monkeypatch.setattr(telegram_sources, "post_remote_action", fake_post)

    summary = sync_telegram_metadata_to_remote_api(
        state,
        replace_issue_signals=True,
        replace_issue_signals_since=now - timedelta(hours=72),
    )

    assert payloads[0]["replace_issue_signals"] is True
    assert payloads[0]["issue_signals_replace_since"]
    assert summary["telegram_remote_signals_deleted"] == 2


def test_newly_registered_channel_is_rights_checked_before_client_access(
    config, now
) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {
        "enabled": True,
        "channels": [{"handle": "unlicensed"}],
        "discover_enabled": False,
        "auto_join_enabled": False,
    }
    config["source_rights"] = {"enforce": True, "records": []}
    client = FakeTelegramClient(
        {"unlicensed": [{"id": 1, "text": "must not be collected"}]}
    )
    state: dict[str, object] = {}

    summary = collect_telegram_sources(state, config, now, client=client)

    assert summary["telegram_source_rights_blocked"] == 1
    assert summary.get("telegram_messages_inserted", 0) == 0
    assert state["telegram_source_channels"][0]["source_right_blocked"] is True
    assert state["telegram_source_messages"] == []


def test_article_url_direct_matching(config, now) -> None:  # type: ignore[no-untyped-def]
    article = make_article("고려아연 주주제안", "https://example.com/a?utm_source=news")
    state = {"articles": [article_record(article, "accepted", now)]}
    client = FakeTelegramClient(
        {"marketnews": [{"id": 1, "text": "공유 https://example.com/a?utm_medium=tg"}]}
    )

    summary = collect_telegram_sources(state, telegram_config(config), now, client)

    assert summary["telegram_messages_inserted"] == 1
    assert summary["telegram_matches_inserted"] == 1
    match = state["telegram_article_matches"][0]  # type: ignore[index]
    assert match["match_type"] == "exact_url"
    assert match["score"] == 1.0


def test_duplicate_article_url_alias_matches_parent_article(config, now) -> None:  # type: ignore[no-untyped-def]
    article = article_record(
        make_article("고려아연 경영권 분쟁", "https://example.com/canonical"),
        "accepted",
        now,
    )
    article["duplicate_matches"] = [
        {"canonical_url": "https://news.example.com/a?utm_source=alert"}
    ]
    state = {"articles": [article]}
    message = normalize_telegram_message(
        {"handle": "marketnews"},
        {"id": 3, "text": "공유 https://news.example.com/a?utm_medium=tg"},
        now,
    )

    matches = match_message_to_articles(state, message, telegram_config(config))

    assert matches[0]["article_id"] == article["canonical_url_hash"]
    assert matches[0]["match_type"] == "exact_url"


def test_keyword_weak_matching_without_url(config, now) -> None:  # type: ignore[no-untyped-def]
    article = make_article(
        "한화솔루션 유상증자 정정 요구",
        "https://example.com/h",
        summary="금감원이 유상증자 신고서 정정을 요구했다.",
    )
    state = {"articles": [article_record(article, "accepted", now)]}
    client = FakeTelegramClient(
        {
            "marketnews": [
                {
                    "id": 2,
                    "text": "한화솔루션 유상증자 정정 요구 이슈가 시장에서 언급됨",
                }
            ]
        }
    )

    collect_telegram_sources(state, telegram_config(config), now, client)

    match = state["telegram_article_matches"][0]  # type: ignore[index]
    assert match["match_type"] == "keyword"
    assert "키워드 추정" in match["reason"]


def test_keyword_weak_matching_requires_entity_and_event(config, now) -> None:  # type: ignore[no-untyped-def]
    article = make_article(
        "삼성전자 노조 리스크에 목표가 하향",
        "https://example.com/s",
        summary="노조 리스크가 보도됐다.",
    )
    state = {"articles": [article_record(article, "accepted", now)]}
    message = normalize_telegram_message(
        {"handle": "marketnews"},
        {"id": 4, "text": "삼성전자 실적 발표와 시장 반응"},
        now,
    )

    matches = match_message_to_articles(state, message, telegram_config(config))

    assert matches == []


def test_rematch_rebuilds_article_matches_with_current_policy(config, now) -> None:  # type: ignore[no-untyped-def]
    article = make_article(
        "한화솔루션 유상증자 정정 요구",
        "https://example.com/h",
        summary="금감원이 유상증자 신고서 정정을 요구했다.",
    )
    message = normalize_telegram_message(
        {"handle": "marketnews"},
        {"id": 5, "text": "한화솔루션 유상증자 정정 요구 이슈"},
        now,
    )
    state = {
        "articles": [article_record(article, "accepted", now)],
        "telegram_source_messages": [message],
        "telegram_article_matches": [
            {
                "article_id": "old",
                "telegram_message_key": message_key(message),
                "match_type": "keyword",
            }
        ],
    }

    summary = rematch_telegram_articles(state, telegram_config(config))

    assert summary["telegram_rematch_old_matches"] == 1
    assert summary["telegram_rematch_new_matches"] == 1
    assert (
        state["telegram_article_matches"][0]["article_id"]
        == article["canonical_url_hash"]
    )  # type: ignore[index]


def test_telegram_issue_signals_include_topic_bursts(config, now) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {
        "signal_window_hours": 72,
        "signal_min_messages": 2,
        "signal_min_channels": 2,
        "signal_limit": 10,
    }  # type: ignore[index]
    authorize_telegram_handles(config, "first", "second")
    state = {
        "telegram_source_messages": [
            normalize_telegram_message(
                {"handle": "first"},
                {"id": 1, "text": "삼성전자 자사주 소각 주주환원 이슈", "date": now},
                now,
            ),
            normalize_telegram_message(
                {"handle": "second"},
                {"id": 2, "text": "삼성전자 자사주 소각 확대 보도", "date": now},
                now,
            ),
        ]
    }

    signals = telegram_issue_signals(state, config, now=now)

    assert any(signal.get("signal_type") == "topic_burst" for signal in signals)
    assert any(
        "삼성전자" in str(signal.get("signal_title") or "") for signal in signals
    )


def test_telegram_signal_tokens_drop_url_boilerplate() -> None:
    tokens = ordered_message_tokens(
        {
            "text": (
                "http://spot.rassiro.com/rd/20260506/1023242 "
                "프리미엄 컨버전스 미디어 시그널 투자의 바른 길을 함께 합니다"
            )
        }
    )

    assert "spot" not in tokens
    assert "rassiro" not in tokens
    assert "rd" not in tokens
    assert "프리미엄" not in tokens
    assert "투자" not in tokens


def test_telegram_topic_burst_does_not_treat_interface_board_as_governance(
    config, now
) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {
        "signal_window_hours": 72,
        "signal_min_messages": 2,
        "signal_min_channels": 2,
        "signal_limit": 10,
    }  # type: ignore[index]
    authorize_telegram_handles(config, "first", "second")
    state = {
        "telegram_source_messages": [
            normalize_telegram_message(
                {"handle": "first"},
                {
                    "id": 1,
                    "text": "엑시콘 CLT Interface Board 공급계약체결 삼성전자",
                    "date": now,
                },
                now,
            ),
            normalize_telegram_message(
                {"handle": "second"},
                {
                    "id": 2,
                    "text": "엑시콘 Interface Board 계약상대 삼성전자 계약내용 공시",
                    "date": now,
                },
                now,
            ),
        ]
    }

    signals = telegram_issue_signals(state, config, now=now)

    assert not any(
        "이사회" in str(signal.get("signal_title") or "") for signal in signals
    )


def test_telegram_topic_burst_ignores_disclosure_template_tokens(config, now) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {
        "signal_window_hours": 72,
        "signal_min_messages": 2,
        "signal_min_channels": 2,
        "signal_limit": 10,
    }  # type: ignore[index]
    authorize_telegram_handles(config, "first", "second", "third")
    state = {
        "telegram_source_messages": [
            normalize_telegram_message(
                {"handle": "first"},
                {
                    "id": 1,
                    "text": "기업명: 현대백화점 시가총액: 2조 4,711억 보고서명: 연결재무제표기준영업 잠정실적 공정공시 매출액 영업익 순이익 공시링크 회사정보 최근 실적 추이",
                    "date": now,
                },
                now,
            ),
            normalize_telegram_message(
                {"handle": "second"},
                {
                    "id": 2,
                    "text": "기업명: 에스엠 시가총액: 2조 보고서명: 영업 잠정실적 공정공시 매출액 영업익 순이익 공시링크 회사정보 최근 실적 추이",
                    "date": now,
                },
                now,
            ),
            normalize_telegram_message(
                {"handle": "third"},
                {
                    "id": 3,
                    "text": "기업명: 지누스 시가총액: 2,833억 보고서명: 연결재무제표기준영업 잠정실적 공정공시 매출액 영업익 순이익 공시링크 회사정보 최근 실적 추이",
                    "date": now,
                },
                now,
            ),
        ]
    }

    signals = telegram_issue_signals(state, config, now=now)
    titles = [str(signal.get("signal_title") or "") for signal in signals]

    assert not any(
        "보고서명" in title
        or "2조" in title
        or "공시링크" in title
        or "회사정보" in title
        for title in titles
    )
    assert ordered_message_tokens(
        {
            "text": "보고서명 공정공시 공시링크 회사정보 최근 실적 추이 2조 4,711억 A069960 report stockinfo7.com"
        }
    ) == ["실적"]


def test_telegram_topic_burst_keeps_governance_board_context(config, now) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {
        "signal_window_hours": 72,
        "signal_min_messages": 2,
        "signal_min_channels": 2,
        "signal_limit": 10,
    }  # type: ignore[index]
    authorize_telegram_handles(config, "first", "second")
    state = {
        "telegram_source_messages": [
            normalize_telegram_message(
                {"handle": "first"},
                {
                    "id": 1,
                    "text": "WEX Board Members Proxy Contest settlement",
                    "date": now,
                },
                now,
            ),
            normalize_telegram_message(
                {"handle": "second"},
                {
                    "id": 2,
                    "text": "WEX board seats shareholder proxy campaign",
                    "date": now,
                },
                now,
            ),
        ]
    }

    signals = telegram_issue_signals(state, config, now=now)

    assert any(
        "wex" in str(signal.get("signal_title") or "")
        and "이사회" in str(signal.get("signal_title") or "")
        for signal in signals
    )


def test_telegram_issue_signals_include_url_bursts(config, now) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {
        "signal_window_hours": 72,
        "signal_min_messages": 2,
        "signal_min_channels": 2,
        "signal_limit": 10,
    }  # type: ignore[index]
    authorize_telegram_handles(config, "first", "second")
    state = {
        "telegram_source_messages": [
            normalize_telegram_message(
                {"handle": "first"},
                {
                    "id": 1,
                    "text": "공유 https://example.com/a?utm_source=tg",
                    "date": now,
                },
                now,
            ),
            normalize_telegram_message(
                {"handle": "second"},
                {
                    "id": 2,
                    "text": "확인 https://example.com/a?utm_medium=chat",
                    "date": now,
                },
                now,
            ),
        ]
    }

    signals = telegram_issue_signals(state, config, now=now)

    assert any(signal.get("signal_type") == "url_burst" for signal in signals)


def test_telegram_signals_keep_right_lineage_and_drop_revoked_inputs(
    config, now
) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {
        "signal_window_hours": 72,
        "signal_min_messages": 2,
        "signal_min_channels": 2,
        "signal_limit": 10,
    }
    authorize_telegram_handles(config, "first", "second")
    active_messages = [
        normalize_telegram_message(
            {"handle": handle, "source_right_id": f"telegram:{handle}"},
            {
                "id": index,
                "text": f"공유 https://example.com/a?utm_source={handle}",
                "date": now,
            },
            now,
        )
        for index, handle in enumerate(("first", "second"), start=1)
    ]

    signals = telegram_issue_signals(
        {"telegram_source_messages": active_messages}, config, now=now
    )
    url_signal = next(
        signal for signal in signals if signal.get("signal_type") == "url_burst"
    )
    assert url_signal["source_kind"] == "telegram_signal"
    assert url_signal["source_right_ids"] == ["telegram:first", "telegram:second"]
    assert {
        message["source_right_id"] for message in url_signal["top_related_messages"]
    } == {"telegram:first", "telegram:second"}

    config["source_rights"]["records"][1]["revoked_at"] = now.date().isoformat()  # type: ignore[index]
    assert (
        telegram_issue_signals(
            {"telegram_source_messages": active_messages}, config, now=now
        )
        == []
    )


def test_channel_quality_metrics_uses_matches_and_risk_flags(now) -> None:  # type: ignore[no-untyped-def]
    channel = {"handle": "marketnews", "title": "경제 증권 뉴스", "quality_score": 70}
    first = normalize_telegram_message(
        channel, {"id": 1, "text": "공유 https://example.com/a", "date": now}, now
    )
    second = normalize_telegram_message(
        channel, {"id": 2, "text": "급등주 추천 루머", "date": now}, now
    )
    state = {
        "telegram_source_messages": [first, second],
        "telegram_article_matches": [
            {
                "article_id": "article",
                "telegram_message_key": message_key(first),
                "channel_handle": "marketnews",
                "match_type": "exact_url",
                "score": 1.0,
            }
        ],
    }

    metrics = channel_quality_metrics(state, channel)

    assert metrics["messages"] == 2
    assert metrics["direct_matches"] == 1
    assert metrics["risk_messages"] == 1
    assert 50 <= metrics["signal_quality_score"] < 70


def test_channel_candidate_scoring() -> None:
    good = score_channel_candidate(
        {"title": "경제 증권 주식 뉴스", "description": "공시 실적 환율 채권"}
    )
    bad = score_channel_candidate(
        {"title": "수익보장 급등주 보장 VIP방", "description": "무료추천 리딩방"}
    )

    assert good > 70
    assert bad < 30


def test_expand_similar_channels_joins_high_quality_until_target(config, now) -> None:  # type: ignore[no-untyped-def]
    state = {
        "telegram_source_channels": [
            {
                "handle": "seed",
                "title": "증권사 리서치 공시 뉴스",
                "enabled": True,
                "quality_score": 88,
            }
        ]
    }
    client = FakeTelegramClient(
        recommendations_by_handle={
            "seed": [
                {
                    "handle": "good_first",
                    "title": "경제 증권 리서치 뉴스",
                    "description": "공시 실적 기업분석",
                },
                {
                    "handle": "bad_vip",
                    "title": "급등주 보장 VIP방",
                    "description": "무료추천 리딩방",
                },
                {
                    "handle": "good_second",
                    "title": "글로벌 증권 뉴스",
                    "description": "해외주식 환율 채권 리포트",
                },
            ]
        }
    )

    summary = asyncio.run(
        expand_similar_channels(
            state,
            config,
            now,
            client,
            target_multiplier=3,
            recommendation_limit=10,
            delay_min_seconds=0,
            delay_max_seconds=0,
        )
    )

    handles = {channel["handle"] for channel in state["telegram_source_channels"]}  # type: ignore[index]
    assert summary["telegram_expand_current_enabled"] == 1
    assert summary["telegram_expand_target_count"] == 3
    assert summary["telegram_expand_joined"] == 2
    assert handles == {"seed", "good_first", "good_second"}
    assert [candidate["handle"] for candidate in client.join_calls] == [
        "good_first",
        "good_second",
    ]


def test_expand_similar_channels_dry_run_does_not_join(config, now) -> None:  # type: ignore[no-untyped-def]
    state = {
        "telegram_source_channels": [
            {
                "handle": "seed",
                "title": "증권사 리서치 공시 뉴스",
                "enabled": True,
                "quality_score": 88,
            }
        ]
    }
    client = FakeTelegramClient()

    summary = asyncio.run(
        expand_similar_channels(
            state,
            config,
            now,
            client,
            target_multiplier=3,
            delay_min_seconds=0,
            delay_max_seconds=0,
            dry_run=True,
        )
    )

    assert summary["telegram_expand_join_targets"] == 1
    assert summary["telegram_expand_joined"] == 0
    assert client.join_calls == []
    assert len(state["telegram_source_channels"]) == 1  # type: ignore[index]


def test_auto_join_disabled_prevents_join_call(config, now) -> None:  # type: ignore[no-untyped-def]
    state = {
        "telegram_channel_candidates": [
            {"handle": "good_stock_news", "status": "accepted", "quality_score": 90}
        ],
    }
    config["telegram_sources"] = {
        "auto_join_enabled": False,
        "auto_join_daily_limit": 10,
    }  # type: ignore[index]
    client = FakeTelegramClient()

    joined = asyncio.run(auto_join_candidates(state, config, now, client))

    assert joined == 0
    assert client.join_calls == []


def test_import_joined_public_channels_respects_quality_and_enable(config) -> None:  # type: ignore[no-untyped-def]
    state: dict[str, object] = {}
    client = FakeTelegramClient(
        joined_channels=[
            {
                "handle": "good_stock_news",
                "title": "경제 증권 주식 뉴스",
                "description": "공시 실적 환율 채권",
            },
            {
                "handle": "bad_vip",
                "title": "수익보장 급등주 보장 VIP방",
                "description": "무료추천 리딩방",
            },
        ]
    )

    summary = import_joined_public_channels(
        state, config, client=client, enable=True, min_quality=70
    )

    assert summary["telegram_joined_seen"] == 2
    assert summary["telegram_joined_imported"] == 1
    assert summary["telegram_joined_skipped_low_quality"] == 1
    assert state["telegram_source_channels"][0]["handle"] == "good_stock_news"  # type: ignore[index]
    assert state["telegram_source_channels"][0]["enabled"] is True  # type: ignore[index]
    assert state["telegram_source_channels"][0]["source"] == "discovered"  # type: ignore[index]


def test_import_joined_public_channels_keeps_only_public_channel_records(
    config,
) -> None:  # type: ignore[no-untyped-def]
    state: dict[str, object] = {}
    client = FakeTelegramClient(
        joined_channels=[
            {
                "handle": "good_stock_news",
                "title": "경제 증권 주식 뉴스",
                "source_type": "public_channel",
                "is_public_channel": True,
            },
            {
                "handle": "public_stock_group",
                "title": "공개 주식 토론방",
                "source_type": "public_group",
                "is_public_channel": False,
            },
        ]
    )

    summary = import_joined_public_channels(state, config, client=client, enable=True)

    assert summary["telegram_joined_seen"] == 2
    assert summary["telegram_joined_imported"] == 1
    assert state["telegram_source_channels"][0]["handle"] == "good_stock_news"  # type: ignore[index]


def test_floodwait_marks_channel_failure_and_continues(config, now) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {  # type: ignore[index]
        "enabled": True,
        "channels": [{"handle": "slow"}, {"handle": "marketnews"}],
        "backfill_limit": 100,
        "incremental_limit": 200,
    }
    config["source_rights"] = {
        "enforce": True,
        "records": [
            {
                "source_right_id": f"telegram:{handle}",
                "source_category": "authorized_telegram",
                "source_identity": handle,
                "scope": "collection",
                "evidence_ref": f"evidence://test/{handle}",
                "valid_from": "2021-01-01",
                "status": "active",
            }
            for handle in ("slow", "marketnews")
        ],
    }
    state = {"articles": []}
    client = FakeTelegramClient(
        {"marketnews": [{"id": 1, "text": "정상 메시지"}]}, fail_handles={"slow"}
    )

    summary = collect_telegram_sources(state, config, now, client)

    assert summary["telegram_channel_failed"] == 1
    assert summary["telegram_messages_inserted"] == 1
    failed_channel = next(
        channel
        for channel in state["telegram_source_channels"]
        if channel["handle"] == "slow"
    )  # type: ignore[index]
    assert failed_channel["last_error"] == "flood_wait_42s"


def test_backfill_messages_filters_by_window_and_estimates_growth(config, now) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {  # type: ignore[index]
        "enabled": True,
        "channels": [{"handle": "marketnews"}],
        "weak_match_min_overlap": 2,
        "weak_match_limit_per_message": 5,
    }
    authorize_telegram_handles(config, "marketnews")
    state = {"articles": []}
    client = FakeTelegramClient(
        {
            "marketnews": [
                {"id": 1, "text": "오래된 메시지", "date": now - timedelta(days=10)},
                {"id": 2, "text": "최근 메시지", "date": now},
            ]
        }
    )

    summary = backfill_telegram_messages(
        state,
        config,
        now,
        days=3,
        limit_per_channel=100,
        client=client,
        sync_remote=False,
    )

    assert summary["telegram_backfill_messages_seen"] == 1
    assert summary["telegram_messages_inserted"] == 1
    assert summary["telegram_estimated_daily_messages"] > 0


def test_backfill_messages_accepts_channel_workers(config, now) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {  # type: ignore[index]
        "enabled": True,
        "channels": [{"handle": "first"}, {"handle": "second"}],
        "backfill_channel_workers": 2,
    }
    authorize_telegram_handles(config, "first", "second")
    state = {"articles": []}
    client = FakeTelegramClient(
        {
            "first": [{"id": 1, "text": "첫 채널", "date": now}],
            "second": [{"id": 2, "text": "둘째 채널", "date": now}],
        }
    )

    summary = backfill_telegram_messages(
        state,
        config,
        now,
        days=3,
        limit_per_channel=100,
        client=client,
        sync_remote=False,
    )

    assert summary["telegram_backfill_channel_workers"] == 2
    assert summary["telegram_backfill_messages_seen"] == 2
    assert all(
        "fetch_elapsed_seconds" in row
        for row in summary["telegram_backfill_per_channel"]
    )  # type: ignore[union-attr]


def test_backfill_paginates_the_complete_window_beyond_one_page(config, now) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {  # type: ignore[index]
        "enabled": True,
        "channels": [{"handle": "marketnews"}],
        "backfill_channel_workers": 1,
    }
    authorize_telegram_handles(config, "marketnews")
    state: dict[str, object] = {"articles": []}
    client = FakeTelegramClient(
        {
            "marketnews": [
                {"id": message_id, "text": f"history-{message_id}", "date": now}
                for message_id in range(1, 8)
            ]
        }
    )

    summary = backfill_telegram_messages(
        state,
        config,
        now,
        days=3,
        limit_per_channel=3,
        max_messages=100,
        client=client,
        sync_remote=False,
    )

    assert summary["telegram_backfill_messages_seen"] == 7
    assert summary["telegram_backfill_channels_completed"] == 1
    assert summary["telegram_backfill_truncated_channels"] == 0
    assert [call["max_id"] for call in client.iter_calls] == [0, 5, 2]
    assert [
        message["telegram_message_id"] for message in state["telegram_source_messages"]
    ] == list(  # type: ignore[index]
        range(1, 8)
    )


def test_backfill_global_cap_stops_at_channel_boundary_with_resume_metadata(
    config, now
) -> None:  # type: ignore[no-untyped-def]
    handles = ("first", "second")
    config["telegram_sources"] = {  # type: ignore[index]
        "enabled": True,
        "channels": [{"handle": handle} for handle in handles],
        "backfill_channel_workers": 1,
    }
    authorize_telegram_handles(config, *handles)
    state: dict[str, object] = {"articles": []}
    client = FakeTelegramClient(
        {
            "first": [
                {"id": message_id, "text": f"first-{message_id}", "date": now}
                for message_id in range(1, 4)
            ],
            "second": [
                {"id": message_id, "text": f"second-{message_id}", "date": now}
                for message_id in range(1, 5)
            ],
        }
    )
    checkpoints: list[dict[str, object]] = []

    summary = backfill_telegram_messages(
        state,
        config,
        now,
        days=3,
        limit_per_channel=2,
        max_messages=5,
        client=client,
        sync_remote=False,
        checkpoint_callback=checkpoints.append,
    )

    assert summary["telegram_backfill_messages_seen"] == 5
    assert summary["telegram_backfill_channels_completed"] == 1
    assert summary["telegram_backfill_truncated_channels"] == 1
    assert summary["telegram_backfill_global_limit_reached"] == 1
    assert summary["telegram_backfill_resume_handle"] == "second"
    assert summary["telegram_backfill_resume_after_handle"] == "first"
    assert summary["telegram_backfill_resume_before_message_id"] == 3
    assert [record["status"] for record in checkpoints] == [
        "page",
        "ok",
        "truncated",
    ]
    assert {
        (message["handle"], message["telegram_message_id"])
        for message in state["telegram_source_messages"]  # type: ignore[index]
    } == {
        ("first", 1),
        ("first", 2),
        ("first", 3),
        ("second", 3),
        ("second", 4),
    }


def test_backfill_resumes_a_truncated_channel_before_saved_message_id(
    config, now
) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {  # type: ignore[index]
        "enabled": True,
        "channels": [{"handle": "marketnews"}],
        "backfill_channel_workers": 1,
    }
    authorize_telegram_handles(config, "marketnews")
    state: dict[str, object] = {"articles": []}
    client = FakeTelegramClient(
        {
            "marketnews": [
                {"id": message_id, "text": f"history-{message_id}", "date": now}
                for message_id in range(1, 8)
            ]
        }
    )

    first = backfill_telegram_messages(
        state,
        config,
        now,
        days=3,
        limit_per_channel=3,
        max_messages=5,
        client=client,
        sync_remote=False,
    )
    assert first["telegram_backfill_truncated_channels"] == 1
    assert first["telegram_backfill_resume_before_message_id"] == 3

    resumed = backfill_telegram_messages(
        state,
        config,
        now,
        days=3,
        limit_per_channel=3,
        max_messages=100,
        only_handles={"marketnews"},
        before_message_id=3,
        client=client,
        sync_remote=False,
    )

    assert resumed["telegram_backfill_truncated_channels"] == 0
    assert resumed["telegram_backfill_channels_completed"] == 1
    assert {
        message["telegram_message_id"]
        for message in state["telegram_source_messages"]  # type: ignore[index]
    } == set(range(1, 8))


def test_backfill_processing_timeout_checkpoints_the_current_page_cursor(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_sources

    config["telegram_sources"] = {  # type: ignore[index]
        "enabled": True,
        "channels": [{"handle": "marketnews"}],
        "backfill_processing_timeout_seconds": 0.01,
    }
    authorize_telegram_handles(config, "marketnews")
    state: dict[str, object] = {"articles": []}
    client = FakeTelegramClient(
        {
            "marketnews": [
                {"id": message_id, "text": f"history-{message_id}", "date": now}
                for message_id in range(1, 4)
            ]
        }
    )
    original_normalize = telegram_sources.normalize_telegram_message

    def slow_normalize(*args, **kwargs):  # type: ignore[no-untyped-def]
        result = original_normalize(*args, **kwargs)
        time.sleep(0.02)
        return result

    monkeypatch.setattr(telegram_sources, "normalize_telegram_message", slow_normalize)
    checkpoints: list[dict[str, object]] = []

    summary = backfill_telegram_messages(
        state,
        config,
        now,
        days=3,
        limit_per_channel=3,
        max_messages=100,
        client=client,
        sync_remote=False,
        checkpoint_callback=checkpoints.append,
    )

    assert summary["telegram_channel_failed"] == 1
    assert checkpoints[-1]["status"] == "failed"
    assert checkpoints[-1]["resume_before_message_id"] == 0
    assert len(state["telegram_source_messages"]) == 1


def test_backfill_rights_checks_config_only_channel_before_client_access(
    config, now
) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {
        "enabled": True,
        "channels": [{"handle": "unlicensed"}],
    }
    config["source_rights"] = {"enforce": True, "records": []}
    state: dict[str, object] = {}
    client = FakeTelegramClient(
        {"unlicensed": [{"id": 1, "text": "must not be collected", "date": now}]}
    )

    summary = backfill_telegram_messages(
        state,
        config,
        now,
        days=3,
        limit_per_channel=100,
        client=client,
        sync_remote=False,
    )

    assert summary["telegram_source_rights_blocked"] == 1
    assert summary["telegram_backfill_channels"] == 0
    assert state["telegram_source_messages"] == []


def test_backfill_canonicalizes_handle_only_identity_and_references(
    config, now
) -> None:  # type: ignore[no-untyped-def]
    telegram_config(config)
    old_channel = {"handle": "marketnews", "enabled": True}
    old_message = normalize_telegram_message(
        old_channel, {"id": 1, "text": "same", "date": now}, now
    )
    old_key = message_key(old_message)
    state: dict[str, object] = {
        "articles": [],
        "telegram_source_channels": [old_channel],
        "telegram_source_messages": [old_message],
        "telegram_article_matches": [
            {
                "article_id": "article-1",
                "telegram_message_key": old_key,
                "channel_handle": "marketnews",
                "match_type": "exact_url",
                "score": 1.0,
            }
        ],
    }
    client = FakeTelegramClient(
        {"marketnews": [{"id": 1, "text": "same", "date": now}]}
    )

    summary = backfill_telegram_messages(
        state,
        config,
        now,
        days=3,
        limit_per_channel=100,
        client=client,
        sync_remote=False,
    )

    assert summary["telegram_messages_inserted"] == 0
    assert len(state["telegram_source_channels"]) == 1
    assert (
        state["telegram_source_channels"][0]["telegram_channel_id"] == "id-marketnews"
    )  # type: ignore[index]
    assert len(state["telegram_source_messages"]) == 1
    assert message_key(state["telegram_source_messages"][0]) == "id:id-marketnews:1"  # type: ignore[index]
    assert (
        state["telegram_article_matches"][0]["telegram_message_key"]
        == "id:id-marketnews:1"
    )  # type: ignore[index]


def test_backfill_disables_reassigned_stale_channel_identity(config, now) -> None:  # type: ignore[no-untyped-def]
    telegram_config(config)
    state: dict[str, object] = {
        "articles": [],
        "telegram_source_channels": [
            {
                "handle": "marketnews",
                "telegram_channel_id": "old-id",
                "last_message_id": 10,
                "enabled": True,
            }
        ],
    }
    client = FakeTelegramClient(
        {"marketnews": [{"id": 1, "text": "new identity", "date": now}]}
    )

    summary = backfill_telegram_messages(
        state,
        config,
        now,
        days=3,
        limit_per_channel=100,
        client=client,
        sync_remote=False,
    )

    assert len(state["telegram_source_channels"]) == 1
    stale = state["telegram_source_channels"][0]  # type: ignore[index]
    assert stale["enabled"] is False
    assert stale["last_error"] == "channel_identity_review_required"
    assert stale["identity_review_required"] is True
    assert stale["observed_telegram_channel_id"] == "id-marketnews"
    assert summary["telegram_channel_failed"] == 1
    assert summary["telegram_messages_inserted"] == 0
    assert state["telegram_source_messages"] == []
    assert client.iter_calls == []


def test_forced_repair_replays_rows_below_a_previously_overadvanced_cursor(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_sources

    telegram_config(config)
    state: dict[str, object] = {
        "articles": [],
        "telegram_source_channels": [
            {
                "handle": "marketnews",
                "telegram_channel_id": "id-marketnews",
                "last_message_id": 999,
                "enabled": True,
            }
        ],
        "telegram_remote_sync_cursors": {"id:id-marketnews": 999},
    }
    client = FakeTelegramClient(
        {
            "marketnews": [
                {"id": message_id, "text": f"missing-{message_id}", "date": now}
                for message_id in range(1, 4)
            ]
        }
    )
    uploaded_ids: list[int] = []
    monkeypatch.setattr(telegram_sources, "remote_api_configured", lambda: True)

    def fake_post(_action, payload):  # type: ignore[no-untyped-def]
        uploaded_ids.extend(
            int(message["telegram_message_id"]) for message in payload["messages"]
        )
        return {"ok": True, "messages": len(payload["messages"]), "article_matches": 0}

    monkeypatch.setattr(telegram_sources, "post_remote_action", fake_post)
    summary = backfill_telegram_messages(
        state,
        config,
        now,
        days=3,
        limit_per_channel=100,
        client=client,
        sync_remote=True,
        force_remote_resync=True,
    )

    assert uploaded_ids == [1, 2, 3]
    assert summary["telegram_force_remote_resync"] == 1
    assert summary["telegram_remote_messages"] == 3
    assert summary["telegram_remote_pending"] == 0


def test_backfill_persists_freshly_recomputed_signals_without_pending_messages(
    config,
    now,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_sources

    telegram_config(config)
    state: dict[str, object] = {"articles": []}
    client = FakeTelegramClient(
        {"marketnews": [{"id": 1, "text": "message", "date": now}]}
    )
    expected_signal = {
        "article_id": "telegram-topic:fresh",
        "signal_type": "topic_burst",
        "related_telegram_count": 1,
    }
    payloads: list[dict[str, object]] = []
    monkeypatch.setattr(telegram_sources, "remote_api_configured", lambda: True)
    monkeypatch.setattr(
        telegram_sources,
        "telegram_issue_signals",
        lambda *_args, **_kwargs: [expected_signal],
    )

    def fake_post(_action, payload):  # type: ignore[no-untyped-def]
        payloads.append(payload)
        return {
            "ok": True,
            "channels": len(payload["channels"]),
            "messages": len(payload["messages"]),
            "article_matches": len(payload["article_matches"]),
            "issue_signals": len(payload["issue_signals"]),
        }

    monkeypatch.setattr(telegram_sources, "post_remote_action", fake_post)

    summary = backfill_telegram_messages(
        state,
        config,
        now,
        days=3,
        limit_per_channel=100,
        client=client,
        sync_remote=True,
    )

    assert payloads[-1]["messages"] == []
    assert payloads[-1]["issue_signals"] == [expected_signal]
    assert summary["telegram_remote_metadata_synced"] == 1
    assert summary["telegram_remote_signals"] == 1


def test_backfill_remote_failure_resumes_on_fresh_runner_without_gaps(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_sources

    handles = ("first", "second")
    config["telegram_sources"] = {
        "enabled": True,
        "channels": [{"handle": handle} for handle in handles],
        "remote_batch_size": 2,
        "local_state_message_limit": 2,
        "backfill_channel_workers": 1,
    }
    authorize_telegram_handles(config, *handles)
    client = FakeTelegramClient(
        {
            handle: [
                {"id": message_id, "text": f"{handle}-{message_id}", "date": now}
                for message_id in range(1, 4)
            ]
            for handle in handles
        }
    )
    monkeypatch.setattr(telegram_sources, "remote_api_configured", lambda: True)
    durable_ids: set[tuple[str, int]] = set()
    durable_cursors: dict[str, int] = {}
    first_calls = 0

    def flaky_post(_action, payload):  # type: ignore[no-untyped-def]
        nonlocal first_calls
        first_calls += 1
        if first_calls == 2:
            return {"ok": False, "error": "db_unavailable"}
        for message in payload["messages"]:
            identity = f"id:{message['telegram_channel_id']}"
            message_id = int(message["telegram_message_id"])
            durable_ids.add((identity, message_id))
            durable_cursors[identity] = max(
                durable_cursors.get(identity, 0), message_id
            )
        return {"ok": True, "messages": len(payload["messages"]), "article_matches": 0}

    monkeypatch.setattr(telegram_sources, "post_remote_action", flaky_post)
    first_state: dict[str, object] = {"articles": []}
    checkpoints: list[dict[str, object]] = []

    with pytest.raises(RuntimeError, match="telegram_remote_checkpoint_failed"):
        backfill_telegram_messages(
            first_state,
            config,
            now,
            days=3,
            limit_per_channel=3,
            client=client,
            sync_remote=True,
            checkpoint_callback=checkpoints.append,
        )

    assert durable_ids == {("id:id-first", 1), ("id:id-first", 2)}
    assert len(pending_remote_messages(first_state)) == 1
    assert len(checkpoints) == 1
    assert checkpoints[0]["status"] == "page"
    assert checkpoints[0]["resume_before_message_id"] == 0
    assert checkpoints[0]["remote_checkpoint_complete"] == 0
    assert checkpoints[0]["telegram_remote_failed"] == 1
    assert checkpoints[0]["telegram_remote_pending"] == 1
    assert len(first_state["telegram_source_messages"]) == 3

    # Emulate a new Actions runner hydrated from the MySQL channel cursor only.
    fresh_state: dict[str, object] = {
        "articles": [],
        "telegram_source_channels": [
            {
                "handle": handle,
                "telegram_channel_id": f"id-{handle}",
                "last_message_id": durable_cursors.get(f"id:id-{handle}", 0),
                "enabled": True,
            }
            for handle in handles
        ],
        "telegram_remote_sync_cursors": dict(durable_cursors),
    }
    resumed_uploads: list[tuple[str, int]] = []

    def healthy_post(_action, payload):  # type: ignore[no-untyped-def]
        for message in payload["messages"]:
            identity = f"id:{message['telegram_channel_id']}"
            item = (identity, int(message["telegram_message_id"]))
            resumed_uploads.append(item)
            durable_ids.add(item)
        return {"ok": True, "messages": len(payload["messages"]), "article_matches": 0}

    monkeypatch.setattr(telegram_sources, "post_remote_action", healthy_post)
    summary = backfill_telegram_messages(
        fresh_state,
        config,
        now,
        days=3,
        limit_per_channel=3,
        client=client,
        sync_remote=True,
    )

    assert resumed_uploads == [
        ("id:id-first", 3),
        ("id:id-second", 1),
        ("id:id-second", 2),
        ("id:id-second", 3),
    ]
    assert durable_ids == {
        (f"id:id-{handle}", message_id)
        for handle in handles
        for message_id in range(1, 4)
    }
    assert summary["telegram_remote_pending"] == 0
    assert summary["telegram_prune_deferred"] == 0
    assert len(fresh_state["telegram_source_messages"]) == 2


def test_backfill_syncs_multi_channel_messages_and_matches_before_prune(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_sources

    handles = tuple(f"channel_{index}" for index in range(12))
    config["telegram_sources"] = {
        "enabled": True,
        "channels": [{"handle": handle} for handle in handles],
        "remote_batch_size": 300,
        "local_state_message_limit": 5000,
        "backfill_channel_workers": 3,
        "weak_match_min_overlap": 2,
        "weak_match_limit_per_message": 5,
    }
    authorize_telegram_handles(config, *handles)
    article = article_record(
        make_article("Governance event", "https://example.com/governance"),
        "accepted",
        now,
    )
    state: dict[str, object] = {"articles": [article]}
    client = FakeTelegramClient(
        {
            handle: [
                {
                    "id": message_id,
                    "text": "https://example.com/governance",
                    "date": now,
                }
                for message_id in range(1, 502)
            ]
            for handle in handles
        }
    )
    uploaded_messages: set[tuple[str, int]] = set()
    uploaded_match_keys: set[str] = set()
    durable_channel_cursors: dict[str, int] = {}
    monkeypatch.setattr(telegram_sources, "remote_api_configured", lambda: True)

    def fake_post(_action, payload):  # type: ignore[no-untyped-def]
        for message in payload["messages"]:
            identity = f"id:{message['telegram_channel_id']}"
            uploaded_messages.add((identity, int(message["telegram_message_id"])))
        for match in payload["article_matches"]:
            uploaded_match_keys.add(str(match["telegram_message_key"]))
        for channel in payload["channels"]:
            identity = f"id:{channel['telegram_channel_id']}"
            durable_channel_cursors[identity] = max(
                durable_channel_cursors.get(identity, 0),
                int(channel["last_message_id"]),
            )
        return {
            "ok": True,
            "messages": len(payload["messages"]),
            "article_matches": len(payload["article_matches"]),
        }

    monkeypatch.setattr(telegram_sources, "post_remote_action", fake_post)
    summary = backfill_telegram_messages(
        state,
        config,
        now,
        days=3,
        limit_per_channel=501,
        client=client,
        sync_remote=True,
        max_messages=10_000,
    )

    assert len(uploaded_messages) == 6012
    assert len(uploaded_match_keys) == 6012
    assert {value for value in durable_channel_cursors.values() if value} == {501}
    assert len([value for value in durable_channel_cursors.values() if value]) == len(
        handles
    )
    assert summary["telegram_remote_messages"] == 6012
    assert summary["telegram_remote_matches"] == 6012
    assert summary["telegram_remote_pending"] == 0
    assert summary["telegram_messages_pruned"] == 1012
    assert len(state["telegram_source_messages"]) == 5000


def test_remote_match_index_is_built_once_across_many_chunks(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_sources

    class CountingList(list):
        iterations = 0

        def __iter__(self):  # type: ignore[no-untyped-def]
            type(self).iterations += 1
            return super().__iter__()

    telegram_config(config)
    config["telegram_sources"]["remote_batch_size"] = 10
    channel = {"handle": "marketnews", "telegram_channel_id": "100", "enabled": True}
    messages = [
        normalize_telegram_message(channel, {"id": value, "text": "message"}, now)
        for value in range(1, 101)
    ]
    matches = CountingList(
        {
            "article_id": f"article-{value}",
            "telegram_message_key": message_key(message),
            "match_type": "exact_url",
        }
        for value, message in enumerate(messages, start=1)
    )
    state: dict[str, object] = {"telegram_source_channels": [channel]}
    monkeypatch.setattr(telegram_sources, "remote_api_configured", lambda: True)
    monkeypatch.setattr(
        telegram_sources,
        "post_remote_action",
        lambda _action, payload: {
            "ok": True,
            "messages": len(payload["messages"]),
            "article_matches": len(payload["article_matches"]),
        },
    )

    summary = sync_telegram_batch_to_remote_api(
        state, config, messages=messages, matches=matches
    )

    assert summary["telegram_remote_synced"] == 10
    assert summary["telegram_remote_matches"] == 100
    assert CountingList.iterations == 1


def test_runtime_quality_indexes_messages_and_matches_once_per_refresh(
    config, now
) -> None:  # type: ignore[no-untyped-def]
    class CountingList(list):
        iterations = 0

        def __iter__(self):  # type: ignore[no-untyped-def]
            type(self).iterations += 1
            return super().__iter__()

    channels = [
        {"handle": f"channel_{index}", "quality_score": 50} for index in range(50)
    ]
    messages = CountingList(
        normalize_telegram_message(channel, {"id": 1, "text": "message"}, now)
        for channel in channels
    )
    matches = CountingList(
        {
            "article_id": f"article-{index}",
            "telegram_message_key": message_key(message),
            "channel_handle": message["handle"],
            "match_type": "exact_url",
        }
        for index, message in enumerate(messages)
    )
    CountingList.iterations = 0
    state: dict[str, object] = {
        "telegram_source_channels": channels,
        "telegram_source_messages": messages,
        "telegram_article_matches": matches,
    }

    refresh_channel_runtime_quality(state)

    assert CountingList.iterations == 2
    assert all(channel["quality_metrics"]["messages"] == 1 for channel in channels)  # type: ignore[index]


def test_telegram_run_record_keeps_compact_channel_progress(now) -> None:  # type: ignore[no-untyped-def]
    record = telegram_run_record(
        now,
        "backfill",
        {
            "telegram_messages_inserted": 3,
            "telegram_backfill_per_channel": [
                {
                    "handle": "marketnews",
                    "title": "경제 뉴스",
                    "status": "ok",
                    "messages_seen": 3,
                    "inserted": 3,
                    "elapsed_seconds": 1.2,
                }
            ],
        },
    )

    assert record["mode"] == "backfill"
    assert record["telegram_backfill_per_channel"][0]["handle"] == "marketnews"  # type: ignore[index]


def test_telegram_state_stats_suggests_resume_after_last_collected(config, now) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {
        "enabled": True,
        "channels": [{"handle": "first"}, {"handle": "second"}, {"handle": "third"}],
    }  # type: ignore[index]
    state: dict[str, object] = {}
    upsert_telegram_message(
        state,
        normalize_telegram_message({"handle": "first"}, {"id": 1, "text": "a"}, now),
    )
    upsert_telegram_message(
        state,
        normalize_telegram_message({"handle": "second"}, {"id": 2, "text": "b"}, now),
    )

    stats = telegram_state_stats(state, config)

    assert stats["telegram_channels_enabled"] == 3
    assert stats["last_processed_handle"] == "second"
    assert stats["first_uncollected_handle"] == "third"
    assert stats["uncollected_handles"] == ["third"]
    assert "--only-handles third" in stats["next_backfill_command"]


def test_telegram_state_stats_treats_zero_message_collected_channel_as_processed(
    config, now
) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {
        "enabled": True,
        "channels": [{"handle": "empty"}],
    }  # type: ignore[index]
    state = {
        "telegram_source_channels": [
            {"handle": "empty", "enabled": True, "last_collected_at": now.isoformat()}
        ],
    }

    stats = telegram_state_stats(state, config)

    assert stats["first_uncollected_handle"] == ""
    assert stats["next_backfill_command"] == ""


def test_backfill_messages_only_handles_limits_collection(config, now) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {  # type: ignore[index]
        "enabled": True,
        "channels": [{"handle": "first"}, {"handle": "second"}],
    }
    authorize_telegram_handles(config, "first", "second")
    state = {"articles": []}
    client = FakeTelegramClient(
        {
            "first": [{"id": 1, "text": "첫 채널", "date": now}],
            "second": [{"id": 2, "text": "둘째 채널", "date": now}],
        }
    )

    summary = backfill_telegram_messages(
        state,
        config,
        now,
        days=3,
        limit_per_channel=100,
        client=client,
        sync_remote=False,
        only_handles={"second"},
    )

    assert summary["telegram_backfill_channels"] == 1
    assert summary["telegram_backfill_messages_seen"] == 1
    assert state["telegram_source_messages"][0]["handle"] == "second"  # type: ignore[index]


def test_deleted_message_marking(now) -> None:  # type: ignore[no-untyped-def]
    state: dict[str, object] = {}
    channel = {"handle": "marketnews", "telegram_channel_id": "100"}
    first = normalize_telegram_message(channel, {"id": 1, "text": "삭제 예정"}, now)
    second = normalize_telegram_message(channel, {"id": 2, "text": "유지"}, now)
    upsert_telegram_message(state, first)
    upsert_telegram_message(state, second)

    assert mark_deleted_message(state, channel, 1, now)
    assert state["telegram_source_messages"][0]["deleted_at"]  # type: ignore[index]

    marked = reconcile_recent_deletions(state, channel, {2}, now, recent_limit=10)
    assert marked == 0


def test_collect_skips_when_disabled(config) -> None:  # type: ignore[no-untyped-def]
    now = datetime(2026, 5, 4, 10, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    config["telegram_sources"] = {
        "enabled": False,
        "channels": [{"handle": "marketnews"}],
    }  # type: ignore[index]

    assert collect_telegram_sources({}, config, now)["telegram_source_skipped"] == 1


def test_collect_does_not_fail_feed_build_when_session_is_invalid(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_sources

    class BrokenAdapter:
        def __init__(self, _config: dict[str, object]) -> None:
            pass

        async def __aenter__(self) -> "BrokenAdapter":
            raise EOFError("session requires interactive login")

        async def __aexit__(self, *_args: object) -> None:
            return None

    monkeypatch.setattr(telegram_sources, "TelethonClientAdapter", BrokenAdapter)
    state: dict[str, object] = {}
    summary = collect_telegram_sources(state, telegram_config(config), now)

    assert summary["telegram_source_connect_failed"] == 1
    assert summary["telegram_source_error"] == "EOFError"
    assert state["telegram_source_runs"][0]["telegram_source_connect_failed"] == 1  # type: ignore[index]
