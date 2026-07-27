from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

import curator.main as main_module
from curator.main import (
    FAILURE_KEYS,
    article_is_before_previous_day,
    main,
    prune_excluded_pending_articles,
    publish_telegram_for_run,
    telegram_delivery_mode,
)

from conftest import make_article


def test_official_scope_preserves_structured_failure_telemetry(
    tmp_path,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    structured_summary: dict[str, object] = {
        "official_failed": 1,
        "official_remote_failure_details": [
            {
                "scope": "data_batch",
                "batch_number": 2,
                "error_code": "internal_error",
            }
        ],
    }
    monkeypatch.setenv("CURATOR_INGEST_SCOPE", "official")
    monkeypatch.setattr(
        main_module,
        "load_config",
        lambda _path: {"timezone": "Asia/Seoul"},
    )
    monkeypatch.setattr(
        "curator.official_ingest.run",
        lambda *_args, **_kwargs: structured_summary,
    )

    assert main_module.run(tmp_path) is structured_summary


def test_article_before_previous_day_filter(config) -> None:  # type: ignore[no-untyped-def]
    now = datetime(2026, 4, 28, 8, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    old_article = make_article(
        "오래된 주주제안 기사",
        "https://example.com/old",
        published_at="2026-04-26T23:59:00+09:00",
    )
    previous_day_article = make_article(
        "전일 주주제안 기사",
        "https://example.com/previous-day",
        published_at="2026-04-27T00:00:00+09:00",
    )

    assert article_is_before_previous_day(old_article, config, now)
    assert not article_is_before_previous_day(previous_day_article, config, now)


def test_previous_day_filter_can_be_disabled(config) -> None:  # type: ignore[no-untyped-def]
    config["date_filter"]["exclude_before_previous_day"] = False  # type: ignore[index]
    now = datetime(2026, 4, 28, 8, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    old_article = make_article(
        "오래된 주주제안 기사",
        "https://example.com/old",
        published_at="2026-04-26T23:59:00+09:00",
    )

    assert not article_is_before_previous_day(old_article, config, now)


def test_prune_excluded_pending_articles_removes_old_articles_from_state(config) -> None:  # type: ignore[no-untyped-def]
    now = datetime(2026, 4, 28, 8, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    old_article = make_article(
        "오래된 주주제안 기사",
        "https://example.com/old",
        published_at="2026-04-26T23:59:00+09:00",
    )
    fresh_article = make_article(
        "전일 주주제안 기사",
        "https://example.com/fresh",
        published_at="2026-04-27T09:00:00+09:00",
    )
    state = {
        "pending_clusters": [{"articles": [old_article, fresh_article], "article_count": 2}],
        "published_clusters": [{"articles": [old_article], "article_count": 1}],
    }

    prune_excluded_pending_articles(state, config, now)

    assert state["pending_clusters"][0]["articles"] == [fresh_article]
    assert state["pending_clusters"][0]["article_count"] == 1
    assert state["published_clusters"] == []


def test_prune_removes_revoked_telegram_lineage_and_refreshes_mixed_cluster(config) -> None:  # type: ignore[no-untyped-def]
    # Date-only rights boundaries are UTC midnight; 10:00 KST is safely after
    # the 2026-04-28 revocation boundary.
    now = datetime(2026, 4, 28, 10, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    config["source_rights"] = {
        "enforce": True,
        "records": [
            {
                "source_right_id": "telegram:revoked",
                "source_category": "authorized_telegram",
                "source_identity": "revoked",
                "scope": "collection,ai,redistribution",
                "evidence_ref": "evidence://test/revoked",
                "valid_from": "2021-01-01",
                "revoked_at": "2026-04-28",
                "allow_ai": True,
                "allow_redistribution": True,
                "status": "active",
            }
        ],
    }
    telegram_article = make_article(
        "철회된 Telegram 주장",
        "https://example.com/telegram",
        published_at="2026-04-28T07:00:00+09:00",
    )
    telegram_article.update(
        {
            "source_kind": "telegram_reference",
            "source_right_id": "telegram:revoked",
            "telegram_source_handle": "revoked",
        }
    )
    direct_article = make_article(
        "공식 근거가 있는 독립 기사",
        "https://example.com/direct",
        published_at="2026-04-28T07:10:00+09:00",
    )
    direct_article["source_kind"] = "direct"
    state = {
        "pending_clusters": [
            {
                "articles": [telegram_article, direct_article],
                "article_count": 2,
                "representative_title": telegram_article["title"],
                "representative_url": telegram_article["canonical_url"],
                "source_kind": "telegram_reference",
                "source_right_id": "telegram:revoked",
            }
        ],
        "published_clusters": [{"articles": [telegram_article], "article_count": 1}],
    }

    prune_excluded_pending_articles(state, config, now)

    mixed = state["pending_clusters"][0]
    assert mixed["articles"] == [direct_article]
    assert mixed["representative_title"] == direct_article["clean_title"]
    assert mixed["representative_url"] == direct_article["canonical_url"]
    assert mixed["source_kind"] == "direct"
    assert "source_right_id" not in mixed
    assert state["published_clusters"] == []


def test_main_exits_nonzero_when_operational_failure_is_reported(monkeypatch) -> None:
    monkeypatch.setattr("curator.main.run", lambda: {"telegram_sent": 0, "telegram_outbox_enqueue_failed": 1})
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 1


def test_main_exits_nonzero_when_telegram_remote_sync_is_partial(monkeypatch) -> None:
    monkeypatch.setattr("curator.main.run", lambda: {"telegram_remote_failed": 1})
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 1


@pytest.mark.parametrize(
    "failure_key",
    ("telegram_channel_failed", "telegram_source_connect_failed", "telegram_source_not_configured"),
)
def test_main_exits_nonzero_when_telegram_collection_is_incomplete(failure_key: str, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("curator.main.run", lambda: {failure_key: 1})

    with pytest.raises(SystemExit) as exc:
        main()

    assert exc.value.code == 1


def test_main_writes_complete_metrics_on_success(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    metrics_path = tmp_path / "curator-run-metrics.json"
    revision = "a" * 40
    run_id = 30187532649
    run_attempt = 3
    monkeypatch.setenv("CURATOR_RUN_METRICS_PATH", str(metrics_path))
    monkeypatch.setenv("GITHUB_SHA", revision)
    monkeypatch.setenv("GITHUB_RUN_ID", str(run_id))
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", str(run_attempt))
    monkeypatch.setattr("curator.main.run", lambda: {"telegram_sent": 0})

    main()

    payload = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert payload["ok"] is True
    assert payload["status"] == "complete"
    assert payload["code_revision"] == revision
    assert payload["github_run_id"] == run_id
    assert payload["github_run_attempt"] == run_attempt
    assert all(payload[key] == 0 for key in FAILURE_KEYS)


def test_main_fails_when_run_metrics_cannot_be_written(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    metrics_path = tmp_path / "metrics-directory"
    metrics_path.mkdir()
    monkeypatch.setenv("CURATOR_RUN_METRICS_PATH", str(metrics_path))
    monkeypatch.setattr("curator.main.run", lambda: {"telegram_sent": 0})

    with pytest.raises(RuntimeError, match="curator_run_metrics_write_failed"):
        main()


def test_legacy_direct_setting_cannot_reenable_delivery(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("CURATOR_DELIVERY_MODE", "legacy-direct")
    monkeypatch.delenv("CURATOR_DISABLE_TELEGRAM_SEND", raising=False)
    calls: list[str] = []

    summary = publish_telegram_for_run({}, config, now, [], {"remote_api_failed": 0})

    assert calls == []
    assert summary["telegram_sent"] == 0
    assert summary["telegram_failed"] == 0
    assert summary["telegram_outbox_enqueue_skipped"] == 1


def test_ingest_disabled_delivery_neither_sends_nor_enqueues(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("CURATOR_DELIVERY_MODE", "disabled")
    monkeypatch.delenv("CURATOR_DISABLE_TELEGRAM_SEND", raising=False)

    summary = publish_telegram_for_run({}, config, now, [], {"remote_api_failed": 0})

    assert summary == {
        "telegram_sent": 0,
        "telegram_failed": 0,
        "telegram_outbox_enqueue_failed": 0,
        "telegram_outbox_enqueue_skipped": 1,
    }


def test_historical_disable_flag_overrides_outbox_mode(monkeypatch) -> None:
    monkeypatch.setenv("CURATOR_DELIVERY_MODE", "outbox-enqueue")
    monkeypatch.setenv("CURATOR_DISABLE_TELEGRAM_SEND", "1")

    assert telegram_delivery_mode() == "disabled"
