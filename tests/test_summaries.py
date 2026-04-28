from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from curator.cluster import cluster_articles
from curator.summaries import (
    build_daily_digest_messages,
    build_hourly_update_messages,
    digest_article_is_english,
    publish_daily_digest_if_due,
    publish_hourly_telegram_update,
    summary_bullet_lines,
)

from conftest import make_article


def published_cluster(config, now):  # type: ignore[no-untyped-def]
    state = {"pending_clusters": [], "published_clusters": []}
    cluster_articles(
        [
            make_article(
                "신한금융 밸류업 2.0 발표",
                "https://example.com/a",
                summary="신한금융 주주환원 확대",
            ),
            make_article(
                "신한금융 밸류업 주주환원 강화",
                "https://example.com/b",
                summary="배당과 자사주 소각 계획",
            ),
        ],
        state,
        config,
        now,
    )
    cluster_articles([], state, config, now + timedelta(minutes=46))
    return state["published_clusters"][0]


def digest_summary_block(message: str) -> str:
    summary = message.split("<b>요약</b>\n", 1)[1]
    for marker in ("\n\n<b>국문</b>", "\n\n<b>영문</b>"):
        if marker in summary:
            return summary.split(marker, 1)[0]
    return summary


def test_daily_digest_sends_once_in_morning_window(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setattr(summaries, "generate_daily_digest_review", lambda *_args, **_kwargs: "오늘의 리뷰\n- 주주환원 이슈 정리")

    sent_messages = []

    def fake_send(_bot_token, _chat_id, text, _config):  # type: ignore[no-untyped-def]
        sent_messages.append(text)
        return {"ok": True, "message_id": 77, "chat_id": -100}

    monkeypatch.setattr(summaries, "send_telegram_message", fake_send)
    cluster = published_cluster(config, now)
    digest_now = datetime(2026, 4, 26, 6, 30, tzinfo=ZoneInfo("Asia/Seoul"))
    cluster["published_at"] = (digest_now - timedelta(minutes=30)).isoformat()
    state = {
        "published_clusters": [cluster],
        "pending_clusters": [],
        "daily_digest_sent_dates": [],
        "daily_digest_records": [],
    }

    first = publish_daily_digest_if_due(state, config, digest_now)
    second = publish_daily_digest_if_due(state, config, digest_now + timedelta(minutes=15))

    assert first == {"daily_digest_sent": 1, "daily_digest_failed": 0}
    assert second == {"daily_digest_sent": 0, "daily_digest_failed": 0}
    assert state["daily_digest_sent_dates"] == ["2026-04-26"]
    assert state["daily_digest_records"][0]["message_ids"] == [77]
    assert "오늘의 리뷰" in sent_messages[0]


def test_daily_digest_skips_outside_send_hour(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    cluster = published_cluster(config, now)
    digest_now = datetime(2026, 4, 26, 7, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    cluster["published_at"] = (digest_now - timedelta(minutes=30)).isoformat()
    state = {
        "published_clusters": [cluster],
        "pending_clusters": [],
        "daily_digest_sent_dates": [],
        "daily_digest_records": [],
    }

    assert publish_daily_digest_if_due(state, config, digest_now) == {
        "daily_digest_sent": 0,
        "daily_digest_failed": 0,
    }


def test_daily_digest_lists_korean_and_english_article_links(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    monkeypatch.setattr(
        summaries,
        "generate_daily_digest_review",
        lambda *_args, **_kwargs: "- 국내 주주환원 흐름이 이어졌음\n- 해외 행동주의 이슈도 보였음",
    )
    config["digest"]["link_title_max_chars"] = 22  # type: ignore[index]
    domestic_article = make_article(
        "주주제안과 자사주 소각 의무화 논의가 매우 길게 이어지는 기사 제목",
        "https://example.com/domestic",
        source="국내뉴스",
        published_at="2026-04-27T09:00:00+09:00",
        summary="주주제안 자사주 소각",
    )
    domestic_article["feed_category"] = "supplemental"
    global_article = make_article(
        "Shareholder activism proxy fight expands across global boards",
        "https://example.com/global",
        source="Global News",
        published_at="2026-04-26T21:00:00+09:00",
        summary="shareholder activism proxy fight",
    )
    global_article["feed_category"] = "global"
    clusters = [
        {
            "representative_title": "국내 주주환원",
            "published_at": "2026-04-27T09:10:00+09:00",
            "theme_group": "valueup_return",
            "articles": [domestic_article],
        },
        {
            "representative_title": "해외 행동주의",
            "published_at": "2026-04-26T21:10:00+09:00",
            "theme_group": "activism_trend",
            "articles": [global_article],
        },
    ]

    message = build_daily_digest_messages(clusters, config, now, now - timedelta(hours=24))[0]

    assert "<b>국문</b>" in message
    assert "<b>영문</b>" in message
    assert 'href="https://example.com/domestic"' in message
    assert 'href="https://example.com/global"' in message
    assert "04.27 /" in message
    assert "04.26 /" in message
    assert "매우 길게 이어지는 기사 제목" not in message
    assert "..." in message


def test_global_category_korean_article_stays_in_korean_section(config, now) -> None:  # type: ignore[no-untyped-def]
    article = make_article(
        "한국 밸류업 정책과 주주환원 논의",
        "https://example.com/korean-global",
        source="국문뉴스",
        published_at="2026-04-27T09:00:00+09:00",
        summary="국내 상장사 주주환원",
    )
    article["feed_category"] = "global"
    article["feed_name"] = "google-news-en-korea"

    assert not digest_article_is_english(article)

    message = build_daily_digest_messages(
        [
            {
                "representative_title": "한국 밸류업",
                "published_at": article["published_at"],
                "articles": [article],
            }
        ],
        config,
        now,
        now - timedelta(hours=24),
    )[0]

    assert "<b>국문</b>" in message
    assert "<b>영문</b>" not in message


def test_english_title_is_english_even_with_korean_summary(config) -> None:  # type: ignore[no-untyped-def]
    article = make_article(
        "Activist investors press for board changes",
        "https://example.com/english",
        source="Global News",
        summary="국문으로 보강된 요약",
    )

    assert digest_article_is_english(article)


def test_daily_digest_groups_similar_article_titles(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    monkeypatch.setattr(summaries, "generate_daily_digest_review", lambda *_args, **_kwargs: "- 소액주주 이슈가 이어졌음")
    first = make_article(
        "고려아연 소액주주, 사외이사 검찰 고발",
        "https://www.sisajournal.com/news/articleView.html?idxno=371009",
        source="데일리안",
        published_at="2026-04-27T09:00:00+09:00",
        relevance_level="high",
    )
    first["company_candidates"] = ["고려아연"]
    second = make_article(
        "고려아연 소액주주, 검찰 고발·금융위 진정 동시 제기",
        "https://www.seoulfn.com/news/articleView.html?idxno=627481",
        source="뉴스워치",
        published_at="2026-04-27T09:10:00+09:00",
        relevance_level="high",
    )
    second["company_candidates"] = ["고려아연"]
    clusters = [
        {"representative_title": "고려아연 소액주주", "published_at": first["published_at"], "articles": [first]},
        {"representative_title": "고려아연 소액주주", "published_at": second["published_at"], "articles": [second]},
    ]

    message = build_daily_digest_messages(clusters, config, now, now - timedelta(hours=24))[0]

    assert "04.27 /" in message
    assert "(2건)" in message
    assert "① 서울파이낸스" in message
    assert "② 시사저널" in message
    assert "링크:" not in message
    assert 'href="https://www.sisajournal.com/news/articleView.html?idxno=371009"' in message
    assert 'href="https://www.seoulfn.com/news/articleView.html?idxno=627481"' in message


def test_daily_digest_fallback_summary_uses_article_topics(config, now) -> None:  # type: ignore[no-untyped-def]
    config["ai"]["daily_digest_enabled"] = False  # type: ignore[index]
    article = make_article(
        "상장사 임원보수·주식보상 공시 강화 추진",
        "https://example.com/pay",
        source="자본시장뉴스",
        published_at="2026-04-27T09:00:00+09:00",
        summary="성과보수와 주식보상 공시가 투자자 보호 쟁점으로 부각",
    )
    clusters = [
        {
            "representative_title": "임원보수 주식보상 공시",
            "published_at": article["published_at"],
            "articles": [article],
        }
    ]

    message = build_daily_digest_messages(clusters, config, now, now - timedelta(hours=24))[0]
    summary = digest_summary_block(message)

    assert "임원보수" in summary
    assert "주식보상" in summary
    assert "링크" not in summary
    assert "추려" not in summary


def test_daily_digest_filters_operational_ai_summary_lines(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    monkeypatch.setattr(
        summaries,
        "call_github_models",
        lambda *_args, **_kwargs: "- 링크 21건만 추려서 읽기 좋게 정리했음\n- 임원보수 공시 강화 흐름이 이어졌음",
    )
    article = make_article(
        "상장사 임원보수 공시 강화 논의",
        "https://example.com/pay",
        source="자본시장뉴스",
        published_at="2026-04-27T09:00:00+09:00",
        summary="주식보상과 성과보수 공시 논의",
    )
    clusters = [
        {
            "representative_title": "임원보수 공시",
            "published_at": article["published_at"],
            "articles": [article],
        }
    ]

    message = build_daily_digest_messages(clusters, config, now, now - timedelta(hours=24))[0]
    summary = digest_summary_block(message)

    assert "임원보수 공시 강화" in summary
    assert "링크 21건" not in summary
    assert "읽기 좋게" not in summary


def test_hourly_update_message_omits_duplicate_references(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    monkeypatch.setattr(summaries, "generate_hourly_digest_review", lambda *_args, **_kwargs: "- 주주제안 이슈가 이어졌음")
    cluster = published_cluster(config, now)
    duplicate = make_article("신한금융 밸류업 2.0 발표", "https://example.com/duplicate")
    duplicate["duplicate_matches"] = [
        {
            "title": "신한금융 밸류업 2.0 발표",
            "canonical_url": "https://example.com/old",
            "published_at": "2026-04-24T09:00:00+09:00",
        }
    ]

    message = build_hourly_update_messages([cluster], config, now, now - timedelta(hours=1), [duplicate])[0]

    assert "거버넌스 업데이트" in message
    assert "<b>중복 확인</b>" not in message
    assert 'href="https://example.com/old"' not in message
    assert "04.24 / 신한금융 밸류업 2.0 발표" not in message


def test_summary_bullet_lines_uses_concise_endings(config) -> None:  # type: ignore[no-untyped-def]
    bullets = summary_bullet_lines(
        "- 주총 표 대결 임박했음\n- 밸류업 논의가 이어졌음\n- ETF 의결권 영향력이 이슈로 떠올랐음",
        config,
    )

    assert bullets == [
        "- 주총 표 대결 임박",
        "- 밸류업 논의 지속",
        "- ETF 의결권 영향력 이슈 부상",
    ]


def test_hourly_update_batches_multiple_clusters_and_marks_all(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@test_channel")
    monkeypatch.setattr(summaries, "generate_hourly_digest_review", lambda *_args, **_kwargs: "- 소액주주 이슈가 이어졌음")
    first = published_cluster(config, now)
    second = published_cluster(config, now)
    second["guid"] = "cluster:second:20260425:1"
    state = {
        "published_clusters": [first, second],
        "pending_clusters": [],
        "telegram_sent_cluster_guids": [],
        "telegram_send_records": [],
        "telegram_digest_records": [],
    }
    sent_messages = []

    def fake_send(_bot_token, _chat_id, text, _config, **_kwargs):  # type: ignore[no-untyped-def]
        sent_messages.append(text)
        return {"ok": True, "message_id": 88, "chat_id": -100}

    monkeypatch.setattr(summaries, "send_telegram_message", fake_send)

    summary = publish_hourly_telegram_update(state, config, now, [])

    assert summary == {"telegram_sent": 2, "telegram_failed": 0}
    assert len(sent_messages) == 1
    assert "거버넌스 업데이트" in sent_messages[0]
    assert set(state["telegram_sent_cluster_guids"]) == {first["guid"], second["guid"]}
    assert state["telegram_digest_records"][0]["message_ids"] == [88]


def test_hourly_update_skips_configured_hours(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@test_channel")
    cluster = published_cluster(config, now)
    state = {
        "published_clusters": [cluster],
        "telegram_sent_cluster_guids": [],
    }
    skip_now = datetime(2026, 4, 26, 6, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    assert publish_hourly_telegram_update(state, config, skip_now, []) == {
        "telegram_sent": 0,
        "telegram_failed": 0,
    }
    assert state["telegram_sent_cluster_guids"] == []
