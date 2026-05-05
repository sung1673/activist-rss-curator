from __future__ import annotations

import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from conftest import make_article

from curator.state import article_record
from curator.telegram_sources import (
    TelegramFloodWait,
    auto_join_candidates,
    canonicalize_telegram_url,
    collect_telegram_sources,
    extract_urls,
    import_joined_public_channels,
    mark_deleted_message,
    normalize_telegram_message,
    reconcile_recent_deletions,
    score_channel_candidate,
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

    async def iter_messages(self, channel: dict[str, object], *, min_id: int, limit: int) -> list[dict[str, object]]:
        handle = str(channel.get("handle") or "")
        return [
            message
            for message in self.messages_by_handle.get(handle, [])
            if int(message.get("id") or message.get("telegram_message_id") or 0) > min_id
        ][:limit]

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


def test_keyword_weak_matching_without_url(config, now) -> None:  # type: ignore[no-untyped-def]
    article = make_article("한화솔루션 유상증자 정정 요구", "https://example.com/h", summary="금감원이 유상증자 신고서 정정을 요구했다.")
    state = {"articles": [article_record(article, "accepted", now)]}
    client = FakeTelegramClient({"marketnews": [{"id": 2, "text": "한화솔루션 유상증자 정정 요구 이슈가 시장에서 언급됨"}]})

    collect_telegram_sources(state, telegram_config(config), now, client)

    match = state["telegram_article_matches"][0]  # type: ignore[index]
    assert match["match_type"] == "keyword"
    assert "키워드 추정" in match["reason"]


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
