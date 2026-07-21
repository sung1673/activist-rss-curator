from __future__ import annotations

import json

import pytest

from curator.remote_state import (
    fetch_runtime_resource,
    hydrate_runtime_state,
    hydrate_telegram_signal_window,
    preflight_telegram_signal_runtime,
)
from curator.telegram_sources import pending_remote_messages


def test_complete_runtime_resource_fails_instead_of_returning_a_capped_prefix(
    now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("ACTIVIST_API_URL", "https://api.example.test")
    monkeypatch.setenv("ACTIVIST_API_SECRET", "secret")
    monkeypatch.setattr(
        "curator.remote_state.post_remote_action",
        lambda *_args, **_kwargs: {
            "ok": True,
            "state": {
                "records": [{"message_key": "id:100:1"}],
                "has_more": True,
                "next_cursor": "next",
            },
        },
    )

    with pytest.raises(RuntimeError, match="exceeded complete limit"):
        fetch_runtime_resource(
            "telegram_signal_messages",
            since=now,
            max_records=1,
            require_complete=True,
        )


def test_signal_runtime_preflight_checks_both_repair_resources(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    resources: list[str] = []

    def fake_fetch(resource: str, **kwargs):  # type: ignore[no-untyped-def]
        resources.append(resource)
        assert kwargs["max_records"] == 1
        return []

    monkeypatch.setenv("CURATOR_DATA_SOURCE", "mysql")
    monkeypatch.setattr("curator.remote_state.fetch_runtime_resource", fake_fetch)
    monkeypatch.setattr(
        "curator.remote_state.post_remote_action",
        lambda *_args, **_kwargs: {
            "ok": True,
            "signal_rebuild_protocol": "staging-v1",
            "max_payload_bytes": 2_097_152,
            "live_revision": 7,
        },
    )

    summary = preflight_telegram_signal_runtime(config, now)

    assert summary["telegram_signal_runtime_preflight"] == 1
    assert summary["telegram_signal_staging_preflight"] == 1
    assert summary["telegram_signal_live_revision"] == 7
    assert resources == ["telegram_signal_messages", "telegram_signal_matches"]


def test_signal_runtime_preflight_fails_before_reads_without_staging_protocol(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("CURATOR_DATA_SOURCE", "mysql")
    monkeypatch.setattr(
        "curator.remote_state.post_remote_action",
        lambda *_args, **_kwargs: {"ok": True},
    )
    monkeypatch.setattr(
        "curator.remote_state.fetch_runtime_resource",
        lambda *_args, **_kwargs: pytest.fail("runtime reads must not begin"),
    )

    with pytest.raises(RuntimeError, match="staging protocol unavailable"):
        preflight_telegram_signal_runtime(config, now)


def test_signal_runtime_preflight_rejects_smaller_remote_body_limit(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("CURATOR_DATA_SOURCE", "mysql")
    monkeypatch.setattr(
        "curator.remote_state.post_remote_action",
        lambda *_args, **_kwargs: {
            "ok": True,
            "signal_rebuild_protocol": "staging-v1",
            "max_payload_bytes": 65_536,
            "live_revision": 0,
        },
    )
    monkeypatch.setattr(
        "curator.remote_state.fetch_runtime_resource",
        lambda *_args, **_kwargs: pytest.fail("runtime reads must not begin"),
    )

    with pytest.raises(RuntimeError, match="body limit is too small"):
        preflight_telegram_signal_runtime(config, now)


def test_signal_runtime_preflight_requires_live_revision(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("CURATOR_DATA_SOURCE", "mysql")
    monkeypatch.setattr(
        "curator.remote_state.post_remote_action",
        lambda *_args, **_kwargs: {
            "ok": True,
            "signal_rebuild_protocol": "staging-v1",
            "max_payload_bytes": 2_097_152,
        },
    )
    monkeypatch.setattr(
        "curator.remote_state.fetch_runtime_resource",
        lambda *_args, **_kwargs: pytest.fail("runtime reads must not begin"),
    )

    with pytest.raises(RuntimeError, match="live revision unavailable"):
        preflight_telegram_signal_runtime(config, now)


def test_signal_window_hydration_is_atomic_and_rights_filtered(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    original_messages = [{"message_key": "previous"}]
    original_matches = [{"telegram_message_key": "previous"}]
    state: dict[str, object] = {
        "telegram_source_messages": original_messages,
        "telegram_article_matches": original_matches,
    }
    resources: dict[str, list[dict[str, object]]] = {
        "source_rights": [
            {
                "source_right_id": "telegram:licensed",
                "source_type": "authorized_telegram",
                "source_key": "licensed",
                "permission_scope": "collection,ai,redistribution",
                "evidence_uri": "evidence://license/1",
                "valid_from": "2021-01-01 00:00:00",
                "ai_allowed": "1",
                "redistribution_allowed": "1",
                "status": "active",
            }
        ],
        "telegram_signal_messages": [
            {
                "message_key": "id:100:1",
                "channel_handle": "licensed",
                "telegram_channel_id": "100",
                "telegram_message_id": 1,
                "posted_at": now.isoformat(),
                "text": "자사주 소각",
                "urls_json": "[]",
                "risk_flags_json": "[]",
            },
            {
                "message_key": "id:200:1",
                "channel_handle": "unlicensed",
                "telegram_channel_id": "200",
                "telegram_message_id": 1,
                "posted_at": now.isoformat(),
                "text": "허가 없는 메시지",
                "urls_json": "[]",
                "risk_flags_json": "[]",
            },
        ],
        "telegram_signal_matches": [
            {
                "article_id": "article:1",
                "message_key": "id:100:1",
                "match_type": "exact_url",
                "score": 1,
            },
            {
                "article_id": "article:2",
                "message_key": "id:200:1",
                "match_type": "exact_url",
                "score": 1,
            },
        ],
    }

    def fake_fetch(resource: str, **kwargs):  # type: ignore[no-untyped-def]
        assert kwargs["require_complete"] is True
        return resources[resource]

    monkeypatch.setenv("CURATOR_DATA_SOURCE", "mysql")
    monkeypatch.setattr("curator.remote_state.fetch_runtime_resource", fake_fetch)
    monkeypatch.setattr(
        "curator.remote_state.telegram_snapshot_capabilities",
        lambda _config: {
            "telegram_signal_remote_max_payload_bytes": 2_097_152,
            "telegram_signal_live_revision": 23,
        },
    )

    summary = hydrate_telegram_signal_window(state, config, now)

    assert summary["telegram_signal_window_rebuilt"] == 1
    assert summary["telegram_signal_window_messages"] == 1
    assert summary["telegram_signal_window_matches"] == 1
    assert summary["telegram_signal_live_revision"] == 23
    assert state["telegram_source_messages"][0]["message_key"] == "id:100:1"  # type: ignore[index]
    assert state["telegram_article_matches"][0]["article_id"] == "article:1"  # type: ignore[index]

    def failing_fetch(resource: str, **_kwargs):  # type: ignore[no-untyped-def]
        if resource == "telegram_signal_matches":
            raise RuntimeError("incomplete")
        return resources[resource]

    state["telegram_source_messages"] = original_messages
    state["telegram_article_matches"] = original_matches
    monkeypatch.setattr("curator.remote_state.fetch_runtime_resource", failing_fetch)

    with pytest.raises(RuntimeError, match="incomplete"):
        hydrate_telegram_signal_window(state, config, now)
    assert state["telegram_source_messages"] is original_messages
    assert state["telegram_article_matches"] is original_matches


def test_mysql_runtime_hydrates_clusters_telegram_rights_and_outbox(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    resources: dict[str, list[dict[str, object]]] = {
        "articles": [
            {
                "record_id": "article:1",
                "canonical_url": "https://example.com/a",
                "title": "삼성전자 자사주 소각",
                "published_at": now.isoformat(),
                "seen_at": now.isoformat(),
                "status": "accepted",
                "relevance_level": "high",
                "story_key": "story:1",
            }
        ],
        "stories": [
            {
                "story_key": "story:1",
                "guid": "cluster:1",
                "representative_title": "삼성전자 자사주 소각",
                "representative_url": "https://example.com/a",
                "status": "published",
                "published_at": now.isoformat(),
                "article_count": 1,
            }
        ],
        "telegram_channels": [
            {
                "handle": "licensed",
                "telegram_channel_id": "100",
                "enabled": 1,
                "last_message_id": 1,
                "last_error": "channel_identity_review_required",
                "payload_json": json.dumps(
                    {
                        "identity_review_required": True,
                        "identity_review_reason": "authoritative_channel_id_changed",
                        "observed_telegram_channel_id": "101",
                    }
                ),
            }
        ],
        "telegram_messages": [
            {
                "message_key": "id:100:1",
                "channel_handle": "licensed",
                "telegram_channel_id": "100",
                "telegram_message_id": 1,
                "posted_at": now.isoformat(),
                "text": "자사주 소각",
                "urls_json": '["https://example.com/a"]',
                "risk_flags_json": "[]",
            }
        ],
        "telegram_article_matches": [
            {
                "article_id": "hash:1",
                "message_key": "id:100:1",
                "match_type": "exact_url",
                "score": 1,
            }
        ],
        "telegram_issue_signals": [],
        "delivery_outbox": [
            {
                "delivery_id": "delivery:1",
                "delivery_channel": "telegram",
                "status": "delivered",
                "attempt_count": 1,
                "payload_json": json.dumps(
                    {"text": "alert", "cluster_guid": "cluster:1"}
                ),
            }
        ],
        "source_rights": [
            {
                "source_right_id": "telegram:licensed",
                "source_type": "authorized_telegram",
                "source_key": "licensed",
                "permission_scope": "collection,ai,redistribution",
                "evidence_uri": "evidence://license/1",
                "valid_from": "2021-01-01 00:00:00",
                "ai_allowed": "1",
                "redistribution_allowed": "1",
                "status": "active",
            }
        ],
        "collection_runs": [],
        "companies": [{"company_id": "00126380", "legal_name": "삼성전자"}],
        "governance_events": [],
        "documents": [],
    }

    def fake_post(
        _action: str, payload: dict[str, object], **_kwargs: object
    ) -> dict[str, object]:
        assert payload["order"] == "updated_desc"
        resource = str(payload["resource"])
        return {
            "ok": True,
            "state": {
                "records": resources.get(resource, []),
                "has_more": False,
                "next_cursor": None,
            },
        }

    monkeypatch.setenv("CURATOR_DATA_SOURCE", "mysql")
    monkeypatch.setenv("ACTIVIST_API_URL", "https://api.example.test")
    monkeypatch.setenv("ACTIVIST_API_SECRET", "secret")
    monkeypatch.setattr("curator.remote_state.post_remote_action", fake_post)

    state: dict[str, object] = {}
    summary = hydrate_runtime_state(state, config, now)

    assert summary["runtime_hydrated"] == 1
    assert (
        state["published_clusters"][0]["articles"][0]["clean_title"]
        == "삼성전자 자사주 소각"
    )  # type: ignore[index]
    assert state["telegram_source_messages"][0]["source_kind"] == "authorized_telegram"  # type: ignore[index]
    assert (
        state["telegram_source_messages"][0]["source_right_id"] == "telegram:licensed"
    )  # type: ignore[index]
    assert state["telegram_article_matches"][0]["telegram_message_key"] == "id:100:1"  # type: ignore[index]
    assert state["telegram_sent_cluster_guids"] == ["cluster:1"]
    assert state["telegram_remote_sync_cursors"] == {"id:100": 1}
    assert state["telegram_source_channels"][0]["identity_review_required"] is True  # type: ignore[index]
    assert state["telegram_source_channels"][0]["observed_telegram_channel_id"] == "101"  # type: ignore[index]
    assert pending_remote_messages(state) == []
    assert any(
        record["source_identity"] == "licensed"
        for record in config["source_rights"]["records"]
    )  # type: ignore[index]


def test_runtime_hydration_applies_revocation_before_derived_rows(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    resources: dict[str, list[dict[str, object]]] = {
        "articles": [
            {
                "record_id": "article:telegram",
                "canonical_url": "https://example.com/telegram",
                "title": "철회된 Telegram 정보",
                "published_at": now.isoformat(),
                "seen_at": now.isoformat(),
                "status": "accepted",
                "story_key": "story:telegram",
                "source_kind": "telegram_reference",
                "source_right_id": "telegram:revoked",
                "telegram_source_handle": "revoked",
            }
        ],
        "stories": [
            {
                "story_key": "story:telegram",
                "representative_title": "철회된 Telegram 정보",
                "representative_url": "https://example.com/telegram",
                "status": "published",
            }
        ],
        "telegram_channels": [
            {"handle": "revoked", "telegram_channel_id": "200", "enabled": 1}
        ],
        "telegram_messages": [
            {
                "message_key": "id:200:1",
                "channel_handle": "revoked",
                "telegram_channel_id": "200",
                "telegram_message_id": 1,
                "posted_at": now.isoformat(),
                "text": "철회된 정보",
                "urls_json": "[]",
                "risk_flags_json": "[]",
            }
        ],
        "telegram_article_matches": [],
        "telegram_issue_signals": [
            {
                "article_id": "telegram-topic:revoked",
                "payload_json": json.dumps(
                    {
                        "source_kind": "telegram_signal",
                        "source_right_ids": ["telegram:revoked"],
                    }
                ),
            }
        ],
        "delivery_outbox": [],
        "source_rights": [
            {
                "source_right_id": "telegram:revoked",
                "source_type": "authorized_telegram",
                "source_key": "revoked",
                "permission_scope": "collection,ai,redistribution",
                "evidence_uri": "evidence://license/revoked",
                "valid_from": "2021-01-01 00:00:00",
                "revoked_at": now.date().isoformat(),
                "ai_allowed": 1,
                "redistribution_allowed": 1,
                "status": "active",
            }
        ],
        "collection_runs": [],
        "companies": [],
        "governance_events": [],
        "documents": [],
    }

    def fake_post(
        _action: str, payload: dict[str, object], **_kwargs: object
    ) -> dict[str, object]:
        return {
            "ok": True,
            "state": {
                "records": resources.get(str(payload["resource"]), []),
                "has_more": False,
                "next_cursor": None,
            },
        }

    monkeypatch.setenv("CURATOR_DATA_SOURCE", "mysql")
    monkeypatch.setenv("ACTIVIST_API_URL", "https://api.example.test")
    monkeypatch.setenv("ACTIVIST_API_SECRET", "secret")
    monkeypatch.setattr("curator.remote_state.post_remote_action", fake_post)

    state: dict[str, object] = {}
    hydrate_runtime_state(state, config, now)

    assert state["articles"] == []
    assert state["published_clusters"] == []
    assert state["telegram_source_messages"] == []
    assert state["telegram_issue_signals"] == []


def test_mysql_runtime_requires_remote_credentials(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("CURATOR_DATA_SOURCE", "mysql")
    monkeypatch.delenv("ACTIVIST_API_URL", raising=False)
    monkeypatch.delenv("ACTIVIST_API_SECRET", raising=False)
    try:
        hydrate_runtime_state({}, config, now)
    except RuntimeError as exc:
        assert "ACTIVIST_API_URL/SECRET" in str(exc)
    else:
        raise AssertionError("mysql runtime must fail closed without credentials")


def test_mysql_empty_rights_table_replaces_local_active_rights(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    config["source_rights"] = {
        "enforce": True,
        "records": [
            {
                "source_right_id": "telegram:local-only",
                "source_category": "authorized_telegram",
                "source_identity": "local-only",
                "scope": "collection,ai,redistribution",
                "evidence_ref": "evidence://stale-local-record",
                "valid_from": "2021-01-01",
                "allow_ai": True,
                "allow_redistribution": True,
                "status": "active",
            }
        ],
    }

    def fake_post(
        _action: str, _payload: dict[str, object], **_kwargs: object
    ) -> dict[str, object]:
        return {
            "ok": True,
            "state": {"records": [], "has_more": False, "next_cursor": None},
        }

    monkeypatch.setenv("CURATOR_DATA_SOURCE", "mysql")
    monkeypatch.setenv("ACTIVIST_API_URL", "https://api.example.test")
    monkeypatch.setenv("ACTIVIST_API_SECRET", "secret")
    monkeypatch.setattr("curator.remote_state.post_remote_action", fake_post)

    summary = hydrate_runtime_state({}, config, now)

    assert summary["runtime_source_rights"] == 0
    assert config["source_rights"] == {"enforce": True, "records": []}
