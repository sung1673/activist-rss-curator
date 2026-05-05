from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from conftest import make_article

from curator.state import article_record
from curator.telegram_sources import (
    TelegramFloodWait,
    auto_join_candidates,
    backfill_telegram_messages,
    canonicalize_telegram_url,
    collect_telegram_sources,
    extract_urls,
    import_joined_public_channels,
    load_env_files,
    mark_deleted_message,
    match_message_to_articles,
    message_key,
    normalize_telegram_message,
    rematch_telegram_articles,
    reconcile_recent_deletions,
    score_channel_candidate,
    telegram_run_record,
    telegram_state_stats,
    upsert_telegram_message,
)


class FakeTelegramClient:
    def __init__(
        self,
        messages_by_handle: dict[str, list[dict[str, object]]] | None = None,
        *,
        fail_handles: set[str] | None = None,
        joined_channels: list[dict[str, object]] | None = None,
    ) -> None:
        self.messages_by_handle = messages_by_handle or {}
        self.fail_handles = fail_handles or set()
        self.joined_channels = joined_channels or []
        self.join_calls: list[dict[str, object]] = []

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
    ) -> list[dict[str, object]]:
        handle = str(channel.get("handle") or "")
        messages = [
            message
            for message in self.messages_by_handle.get(handle, [])
            if int(message.get("id") or message.get("telegram_message_id") or 0) > min_id
        ]
        if since is not None:
            messages = [
                message
                for message in messages
                if not isinstance(message.get("date"), datetime) or message["date"] >= since
            ]
        return messages[:limit]

    async def recommend_channels(self, seed_channel: dict[str, object], *, limit: int) -> list[dict[str, object]]:
        return [
            {"handle": "good_stock_news", "title": "경제 증권 주식 뉴스", "description": "공시 실적 환율"},
            {"handle": "bad_vip", "title": "급등주 보장 VIP방", "description": "무료추천 리딩방"},
        ][:limit]

    async def join_channel(self, candidate: dict[str, object]) -> dict[str, object]:
        self.join_calls.append(candidate)
        return {"ok": True}

    async def list_joined_public_channels(self, *, limit: int) -> list[dict[str, object]]:
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
    return config


def test_extract_urls_strips_trailing_punctuation() -> None:
    assert extract_urls("확인 https://example.com/a?utm_source=x). 다음") == ["https://example.com/a?utm_source=x"]


def test_canonicalize_telegram_url_removes_tracking_params() -> None:
    assert canonicalize_telegram_url("HTTPS://Example.COM/news/?utm_source=tg&fbclid=1#frag") == "https://example.com/news"


def test_load_env_files_includes_api_env(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.delenv("ACTIVIST_API_URL", raising=False)
    (tmp_path / ".env.api").write_text("ACTIVIST_API_URL=https://example.com/api.php\n", encoding="utf-8")

    loaded = load_env_files(tmp_path)

    assert tmp_path / ".env.api" in loaded
    assert "example.com/api.php" in os.environ["ACTIVIST_API_URL"]


def test_telegram_message_upsert_prevents_duplicates_and_tracks_edits(now) -> None:  # type: ignore[no-untyped-def]
    state: dict[str, object] = {}
    channel = {"handle": "marketnews", "telegram_channel_id": "100"}
    first = normalize_telegram_message(channel, {"id": 7, "text": "첫 메시지"}, now)
    edited = normalize_telegram_message(channel, {"id": 7, "text": "수정 메시지", "edit_date": now}, now)

    assert upsert_telegram_message(state, first) == "inserted"
    assert upsert_telegram_message(state, first) == "unchanged"
    assert upsert_telegram_message(state, edited) == "updated"
    assert len(state["telegram_source_messages"]) == 1  # type: ignore[index]
    assert state["telegram_source_messages"][0]["text"] == "수정 메시지"  # type: ignore[index]


def test_article_url_direct_matching(config, now) -> None:  # type: ignore[no-untyped-def]
    article = make_article("고려아연 주주제안", "https://example.com/a?utm_source=news")
    state = {"articles": [article_record(article, "accepted", now)]}
    client = FakeTelegramClient({"marketnews": [{"id": 1, "text": "공유 https://example.com/a?utm_medium=tg"}]})

    summary = collect_telegram_sources(state, telegram_config(config), now, client)

    assert summary["telegram_messages_inserted"] == 1
    assert summary["telegram_matches_inserted"] == 1
    match = state["telegram_article_matches"][0]  # type: ignore[index]
    assert match["match_type"] == "exact_url"
    assert match["score"] == 1.0


def test_duplicate_article_url_alias_matches_parent_article(config, now) -> None:  # type: ignore[no-untyped-def]
    article = article_record(make_article("고려아연 경영권 분쟁", "https://example.com/canonical"), "accepted", now)
    article["duplicate_matches"] = [{"canonical_url": "https://news.example.com/a?utm_source=alert"}]
    state = {"articles": [article]}
    message = normalize_telegram_message({"handle": "marketnews"}, {"id": 3, "text": "공유 https://news.example.com/a?utm_medium=tg"}, now)

    matches = match_message_to_articles(state, message, telegram_config(config))

    assert matches[0]["article_id"] == article["canonical_url_hash"]
    assert matches[0]["match_type"] == "exact_url"


def test_keyword_weak_matching_without_url(config, now) -> None:  # type: ignore[no-untyped-def]
    article = make_article("한화솔루션 유상증자 정정 요구", "https://example.com/h", summary="금감원이 유상증자 신고서 정정을 요구했다.")
    state = {"articles": [article_record(article, "accepted", now)]}
    client = FakeTelegramClient({"marketnews": [{"id": 2, "text": "한화솔루션 유상증자 정정 요구 이슈가 시장에서 언급됨"}]})

    collect_telegram_sources(state, telegram_config(config), now, client)

    match = state["telegram_article_matches"][0]  # type: ignore[index]
    assert match["match_type"] == "keyword"
    assert "키워드 추정" in match["reason"]


def test_keyword_weak_matching_requires_entity_and_event(config, now) -> None:  # type: ignore[no-untyped-def]
    article = make_article("삼성전자 노조 리스크에 목표가 하향", "https://example.com/s", summary="노조 리스크가 보도됐다.")
    state = {"articles": [article_record(article, "accepted", now)]}
    message = normalize_telegram_message({"handle": "marketnews"}, {"id": 4, "text": "삼성전자 실적 발표와 시장 반응"}, now)

    matches = match_message_to_articles(state, message, telegram_config(config))

    assert matches == []


def test_rematch_rebuilds_article_matches_with_current_policy(config, now) -> None:  # type: ignore[no-untyped-def]
    article = make_article("한화솔루션 유상증자 정정 요구", "https://example.com/h", summary="금감원이 유상증자 신고서 정정을 요구했다.")
    message = normalize_telegram_message(
        {"handle": "marketnews"},
        {"id": 5, "text": "한화솔루션 유상증자 정정 요구 이슈"},
        now,
    )
    state = {
        "articles": [article_record(article, "accepted", now)],
        "telegram_source_messages": [message],
        "telegram_article_matches": [{"article_id": "old", "telegram_message_key": message_key(message), "match_type": "keyword"}],
    }

    summary = rematch_telegram_articles(state, telegram_config(config))

    assert summary["telegram_rematch_old_matches"] == 1
    assert summary["telegram_rematch_new_matches"] == 1
    assert state["telegram_article_matches"][0]["article_id"] == article["canonical_url_hash"]  # type: ignore[index]


def test_channel_candidate_scoring() -> None:
    good = score_channel_candidate({"title": "경제 증권 주식 뉴스", "description": "공시 실적 환율 채권"})
    bad = score_channel_candidate({"title": "수익보장 급등주 보장 VIP방", "description": "무료추천 리딩방"})

    assert good > 70
    assert bad < 30


def test_auto_join_disabled_prevents_join_call(config, now) -> None:  # type: ignore[no-untyped-def]
    state = {
        "telegram_channel_candidates": [{"handle": "good_stock_news", "status": "accepted", "quality_score": 90}],
    }
    config["telegram_sources"] = {"auto_join_enabled": False, "auto_join_daily_limit": 10}  # type: ignore[index]
    client = FakeTelegramClient()

    joined = asyncio.run(auto_join_candidates(state, config, now, client))

    assert joined == 0
    assert client.join_calls == []


def test_import_joined_public_channels_respects_quality_and_enable(config) -> None:  # type: ignore[no-untyped-def]
    state: dict[str, object] = {}
    client = FakeTelegramClient(
        joined_channels=[
            {"handle": "good_stock_news", "title": "경제 증권 주식 뉴스", "description": "공시 실적 환율 채권"},
            {"handle": "bad_vip", "title": "수익보장 급등주 보장 VIP방", "description": "무료추천 리딩방"},
        ]
    )

    summary = import_joined_public_channels(state, config, client=client, enable=True, min_quality=70)

    assert summary["telegram_joined_seen"] == 2
    assert summary["telegram_joined_imported"] == 1
    assert summary["telegram_joined_skipped_low_quality"] == 1
    assert state["telegram_source_channels"][0]["handle"] == "good_stock_news"  # type: ignore[index]
    assert state["telegram_source_channels"][0]["enabled"] is True  # type: ignore[index]
    assert state["telegram_source_channels"][0]["source"] == "discovered"  # type: ignore[index]


def test_import_joined_public_channels_keeps_only_public_channel_records(config) -> None:  # type: ignore[no-untyped-def]
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
    state = {"articles": []}
    client = FakeTelegramClient({"marketnews": [{"id": 1, "text": "정상 메시지"}]}, fail_handles={"slow"})

    summary = collect_telegram_sources(state, config, now, client)

    assert summary["telegram_channel_failed"] == 1
    assert summary["telegram_messages_inserted"] == 1
    failed_channel = next(channel for channel in state["telegram_source_channels"] if channel["handle"] == "slow")  # type: ignore[index]
    assert failed_channel["last_error"] == "flood_wait_42s"


def test_backfill_messages_filters_by_window_and_estimates_growth(config, now) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {  # type: ignore[index]
        "enabled": True,
        "channels": [{"handle": "marketnews"}],
        "weak_match_min_overlap": 2,
        "weak_match_limit_per_message": 5,
    }
    state = {"articles": []}
    client = FakeTelegramClient(
        {
            "marketnews": [
                {"id": 1, "text": "오래된 메시지", "date": now - timedelta(days=10)},
                {"id": 2, "text": "최근 메시지", "date": now},
            ]
        }
    )

    summary = backfill_telegram_messages(state, config, now, days=3, limit_per_channel=100, client=client, sync_remote=False)

    assert summary["telegram_backfill_messages_seen"] == 1
    assert summary["telegram_messages_inserted"] == 1
    assert summary["telegram_estimated_daily_messages"] > 0


def test_backfill_messages_accepts_channel_workers(config, now) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {  # type: ignore[index]
        "enabled": True,
        "channels": [{"handle": "first"}, {"handle": "second"}],
        "backfill_channel_workers": 2,
    }
    state = {"articles": []}
    client = FakeTelegramClient(
        {
            "first": [{"id": 1, "text": "첫 채널", "date": now}],
            "second": [{"id": 2, "text": "둘째 채널", "date": now}],
        }
    )

    summary = backfill_telegram_messages(state, config, now, days=3, limit_per_channel=100, client=client, sync_remote=False)

    assert summary["telegram_backfill_channel_workers"] == 2
    assert summary["telegram_backfill_messages_seen"] == 2
    assert all("fetch_elapsed_seconds" in row for row in summary["telegram_backfill_per_channel"])  # type: ignore[union-attr]


def test_telegram_run_record_keeps_compact_channel_progress(now) -> None:  # type: ignore[no-untyped-def]
    record = telegram_run_record(
        now,
        "backfill",
        {
            "telegram_messages_inserted": 3,
            "telegram_backfill_per_channel": [
                {"handle": "marketnews", "title": "경제 뉴스", "status": "ok", "messages_seen": 3, "inserted": 3, "elapsed_seconds": 1.2}
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
    upsert_telegram_message(state, normalize_telegram_message({"handle": "first"}, {"id": 1, "text": "a"}, now))
    upsert_telegram_message(state, normalize_telegram_message({"handle": "second"}, {"id": 2, "text": "b"}, now))

    stats = telegram_state_stats(state, config)

    assert stats["telegram_channels_enabled"] == 3
    assert stats["last_processed_handle"] == "second"
    assert stats["first_uncollected_handle"] == "third"
    assert stats["uncollected_handles"] == ["third"]
    assert "--only-handles third" in stats["next_backfill_command"]


def test_telegram_state_stats_treats_zero_message_collected_channel_as_processed(config, now) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {
        "enabled": True,
        "channels": [{"handle": "empty"}],
    }  # type: ignore[index]
    state = {
        "telegram_source_channels": [{"handle": "empty", "enabled": True, "last_collected_at": now.isoformat()}],
    }

    stats = telegram_state_stats(state, config)

    assert stats["first_uncollected_handle"] == ""
    assert stats["next_backfill_command"] == ""


def test_backfill_messages_only_handles_limits_collection(config, now) -> None:  # type: ignore[no-untyped-def]
    config["telegram_sources"] = {  # type: ignore[index]
        "enabled": True,
        "channels": [{"handle": "first"}, {"handle": "second"}],
    }
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
    config["telegram_sources"] = {"enabled": False, "channels": [{"handle": "marketnews"}]}  # type: ignore[index]

    assert collect_telegram_sources({}, config, now)["telegram_source_skipped"] == 1
