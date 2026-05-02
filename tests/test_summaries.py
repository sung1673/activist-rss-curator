from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from curator.cluster import cluster_articles
from curator.story_judge import StoryJudgement
from curator.summaries import (
    build_daily_digest_messages,
    build_hourly_update_messages,
    digest_article_entries,
    digest_article_is_english,
    duplicate_records_in_window,
    group_digest_entries,
    hourly_update_start_at,
    limited_digest_article_entries,
    publish_daily_digest_if_due,
    publish_hourly_telegram_update,
    render_digest_entry_group,
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
    for marker in (
        "\n\n<b>주주행동·경영권</b>",
        "\n\n<b>밸류업·주주환원</b>",
        "\n\n<b>자본시장 제도·공시</b>",
        "\n\n<b>국문</b>",
        "\n\n<b>해외</b>",
    ):
        if marker in summary:
            return summary.split(marker, 1)[0]
    return summary


def test_daily_digest_disabled_by_default(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    cluster = published_cluster(config, now)
    digest_now = datetime(2026, 4, 26, 6, 30, tzinfo=ZoneInfo("Asia/Seoul"))
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


def test_daily_digest_sends_once_in_morning_window(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    config["digest"]["enabled"] = True  # type: ignore[index]
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


def test_daily_digest_uses_one_representative_for_duplicate_records(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    config["digest"]["enabled"] = True  # type: ignore[index]
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setattr(summaries, "generate_daily_digest_review", lambda *_args, **_kwargs: "- 주주제안 이슈 지속")

    sent_messages = []

    def fake_send(_bot_token, _chat_id, text, _config):  # type: ignore[no-untyped-def]
        sent_messages.append(text)
        return {"ok": True, "message_id": 78, "chat_id": -100}

    monkeypatch.setattr(summaries, "send_telegram_message", fake_send)
    cluster = published_cluster(config, now)
    digest_now = datetime(2026, 4, 26, 6, 30, tzinfo=ZoneInfo("Asia/Seoul"))
    cluster["published_at"] = (digest_now - timedelta(minutes=30)).isoformat()
    state = {
        "published_clusters": [cluster],
        "pending_clusters": [],
        "articles": [
            {
                "status": "duplicate",
                "title": "중복된 소액주주 기사",
                "canonical_url": "https://example.com/duplicate",
                "feed_name": "google-news-주주제안",
                "feed_category": "core",
                "published_at": "2026-04-26T06:00:00+09:00",
                "seen_at": "2026-04-26T06:10:00+09:00",
                "duplicate_matches": [
                    {
                        "title": "중복된 소액주주 기사",
                        "canonical_url": "https://www.mk.co.kr/news/stock/1",
                        "source": "매일경제",
                        "published_at": "2026-04-26T05:50:00+09:00",
                    }
                ],
            }
        ],
        "daily_digest_sent_dates": [],
        "daily_digest_records": [],
    }

    assert publish_daily_digest_if_due(state, config, digest_now) == {
        "daily_digest_sent": 2,
        "daily_digest_failed": 0,
    }
    sent_text = "\n".join(sent_messages)
    assert "<b>중복 기사</b>" not in sent_text
    assert "(중복" not in sent_text
    assert 'href="https://www.mk.co.kr/news/stock/1"' in sent_text
    assert 'href="https://example.com/duplicate"' not in sent_text
    assert "(2건)" not in sent_text
    assert "①" not in sent_text
    assert "②" not in sent_text
    assert "수집키워드" not in sent_text


def test_daily_digest_single_duplicate_record_renders_as_single_link(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    config["digest"]["enabled"] = True  # type: ignore[index]
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setattr(summaries, "generate_daily_digest_review", lambda *_args, **_kwargs: "- 주주제안 이슈 지속")

    sent_messages = []

    def fake_send(_bot_token, _chat_id, text, _config):  # type: ignore[no-untyped-def]
        sent_messages.append(text)
        return {"ok": True, "message_id": 81, "chat_id": -100}

    monkeypatch.setattr(summaries, "send_telegram_message", fake_send)
    digest_now = datetime(2026, 4, 26, 6, 30, tzinfo=ZoneInfo("Asia/Seoul"))
    state = {
        "published_clusters": [],
        "pending_clusters": [],
        "articles": [
            {
                "status": "duplicate",
                "title": "소액주주 주주제안 관련 기사",
                "canonical_url": "https://example.com/single-duplicate",
                "published_at": "2026-04-26T06:00:00+09:00",
                "seen_at": "2026-04-26T06:10:00+09:00",
                "duplicate_matches": [],
            }
        ],
        "daily_digest_sent_dates": [],
        "daily_digest_records": [],
    }

    assert publish_daily_digest_if_due(state, config, digest_now) == {
        "daily_digest_sent": 1,
        "daily_digest_failed": 0,
    }
    assert 'href="https://example.com/single-duplicate"' in sent_messages[0]
    assert "①" not in sent_messages[0]
    assert "(1건)" not in sent_messages[0]
    assert "중복 기사" not in sent_messages[0]


def test_daily_digest_skips_after_send_window(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    cluster = published_cluster(config, now)
    digest_now = datetime(2026, 4, 26, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))
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


def test_daily_digest_tolerates_delayed_github_schedule(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    config["digest"]["enabled"] = True  # type: ignore[index]
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setattr(summaries, "generate_daily_digest_review", lambda *_args, **_kwargs: "- 주주권 이슈 지속")

    sent_messages = []

    def fake_send(_bot_token, _chat_id, text, _config):  # type: ignore[no-untyped-def]
        sent_messages.append(text)
        return {"ok": True, "message_id": 79, "chat_id": -100}

    monkeypatch.setattr(summaries, "send_telegram_message", fake_send)
    cluster = published_cluster(config, now)
    digest_now = datetime(2026, 4, 26, 7, 31, tzinfo=ZoneInfo("Asia/Seoul"))
    cluster["published_at"] = (digest_now - timedelta(minutes=30)).isoformat()
    state = {
        "published_clusters": [cluster],
        "pending_clusters": [],
        "daily_digest_sent_dates": [],
        "daily_digest_records": [],
    }

    assert publish_daily_digest_if_due(state, config, digest_now) == {
        "daily_digest_sent": 1,
        "daily_digest_failed": 0,
    }
    assert "데일리 주주·자본시장 브리핑" in sent_messages[0]


def test_daily_digest_forced_for_delayed_dedicated_schedule(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    config["digest"]["enabled"] = True  # type: ignore[index]
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("CURATOR_EVENT_SCHEDULE", "30 21 * * *")
    monkeypatch.setattr(summaries, "generate_daily_digest_review", lambda *_args, **_kwargs: "- 주주권 이슈 지속")

    sent_messages = []

    def fake_send(_bot_token, _chat_id, text, _config):  # type: ignore[no-untyped-def]
        sent_messages.append(text)
        return {"ok": True, "message_id": 80, "chat_id": -100}

    monkeypatch.setattr(summaries, "send_telegram_message", fake_send)
    cluster = published_cluster(config, now)
    digest_now = datetime(2026, 4, 26, 11, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    cluster["published_at"] = (digest_now - timedelta(minutes=30)).isoformat()
    state = {
        "published_clusters": [cluster],
        "pending_clusters": [],
        "daily_digest_sent_dates": [],
        "daily_digest_records": [],
    }

    assert publish_daily_digest_if_due(state, config, digest_now) == {
        "daily_digest_sent": 1,
        "daily_digest_failed": 0,
    }
    assert "데일리 주주·자본시장 브리핑" in sent_messages[0]


def test_daily_digest_lists_korean_and_english_article_links(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    monkeypatch.setattr(
        summaries,
        "generate_daily_digest_review",
        lambda *_args, **_kwargs: "- 국내 주주환원 흐름이 이어졌음\n- 해외 행동주의 이슈도 보였음",
    )
    config["digest"]["link_title_max_chars"] = 22  # type: ignore[index]
    domestic_article = make_article(
        "밸류업과 자사주 소각 의무화 논의가 매우 길게 이어지는 기사 제목",
        "https://example.com/domestic",
        source="국내뉴스",
        published_at="2026-04-27T09:00:00+09:00",
        summary="밸류업 자사주 소각",
    )
    domestic_article["feed_category"] = "supplemental"
    domestic_article["feed_name"] = "google-news-자사주 소각 의무화 밸류업"
    global_article = make_article(
        "Shareholder activism proxy fight expands across global boards",
        "https://example.com/global",
        source="Global News",
        published_at="2026-04-26T21:00:00+09:00",
        summary="shareholder activism proxy fight",
    )
    global_article["feed_category"] = "global"
    global_article["feed_name"] = "google-news-en-activist-proxy-fight"
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

    message = "\n".join(build_daily_digest_messages(clusters, config, now, now - timedelta(hours=24)))

    assert "<b>밸류업·주주환원</b>" in message
    assert "<b>해외</b>" in message
    assert 'href="https://example.com/domestic"' in message
    assert 'href="https://example.com/global"' in message
    assert "04.27 /" not in message
    assert "04.26 /" not in message
    assert "매우 길게 이어지는 기사 제목" not in message
    assert "..." in message
    assert "소분류:" not in message
    assert "수집키워드" not in message


def test_daily_digest_zero_count_limits_show_all_entries(config, now) -> None:  # type: ignore[no-untyped-def]
    config["digest"]["max_articles_per_cluster"] = 0  # type: ignore[index]
    config["digest"]["max_links_per_section"] = 0  # type: ignore[index]
    config["digest"]["max_links_total"] = 0  # type: ignore[index]
    config["digest"]["max_links_per_group"] = 0  # type: ignore[index]
    articles = [
        make_article(
            f"소액주주 주주제안 기사 {index}",
            f"https://example.com/article-{index}",
            source=f"언론{index}",
            published_at="2026-04-27T09:00:00+09:00",
        )
        for index in range(1, 7)
    ]
    cluster = {
        "representative_title": "소액주주 주주제안",
        "published_at": "2026-04-27T09:10:00+09:00",
        "articles": articles,
    }

    entries = limited_digest_article_entries([cluster], config)["domestic"]
    assert len(entries) == 6

    group_lines = render_digest_entry_group(entries, config)
    assert "외 " not in "\n".join(group_lines)
    assert "⑥" in "\n".join(group_lines)


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

    assert "<b>밸류업·주주환원</b>" in message
    assert "<b>해외</b>" not in message


def test_daily_digest_renders_topic_categories(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    monkeypatch.setattr(summaries, "generate_daily_digest_review", lambda *_args, **_kwargs: "- 주요 이슈 지속")
    articles = [
        make_article(
            "소액주주 주주제안으로 임시주총 표 대결",
            "https://example.com/shareholder",
            source="주주뉴스",
            summary="소액주주 주주제안 경영권 분쟁",
        ),
        make_article(
            "기업 밸류업 프로그램과 자사주 소각 확대",
            "https://example.com/valueup",
            source="밸류뉴스",
            summary="밸류업 주주환원 자사주 소각",
        ),
        make_article(
            "상장적격성 실질심사와 거래정지 개선기간 부여",
            "https://example.com/risk",
            source="리스크뉴스",
            summary="상장폐지 거래정지 투자자 보호",
        ),
    ]
    clusters = [
        {"representative_title": article["clean_title"], "published_at": article["published_at"], "articles": [article]}
        for article in articles
    ]

    message = "\n".join(build_daily_digest_messages(clusters, config, now, now - timedelta(hours=24)))

    assert "<b>주주행동·경영권</b>" in message
    assert "<b>밸류업·주주환원</b>" in message
    assert "<b>자본시장 제도·공시</b>" in message
    assert "<b>상장·공시·거래 리스크</b>" not in message


def test_daily_digest_lists_articles_without_blank_lines(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    monkeypatch.setattr(summaries, "generate_daily_digest_review", lambda *_args, **_kwargs: "- 주요 이슈 지속")
    first = make_article(
        "소액주주 주주제안으로 임시주총 표 대결",
        "https://example.com/shareholder-a",
        source="주주뉴스",
        summary="소액주주 주주제안",
    )
    second = make_article(
        "행동주의 펀드 공개서한 제출",
        "https://example.com/shareholder-b",
        source="행동뉴스",
        summary="행동주의 공개서한",
    )
    clusters = [
        {"representative_title": first["clean_title"], "published_at": first["published_at"], "articles": [first]},
        {"representative_title": second["clean_title"], "published_at": second["published_at"], "articles": [second]},
    ]

    message = build_daily_digest_messages(clusters, config, now, now - timedelta(hours=24))[0]

    assert "\n\n•" not in message
    assert "주주행동·경영권</b>\n•" in message


def test_daily_digest_groups_same_subject_valueup_event(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    monkeypatch.setattr(summaries, "generate_daily_digest_review", lambda *_args, **_kwargs: "- 밸류업 이슈 지속")
    first = make_article(
        "동원수산 저PBR 벗어나기 위한 밸류업 전략 수행해나갈 것",
        "https://www.hankyung.com/article/2026042944076",
        source="한국경제",
        published_at="2026-04-29T09:00:00+09:00",
    )
    second = make_article(
        "동원수산, 저PBR 탈출 승부수…밸류업 청사진 공개 - news.mtn.co.kr",
        "https://news.mtn.co.kr/news-detail/2026042909124681068",
        source="MTN",
        published_at="2026-04-29T09:10:00+09:00",
    )
    clusters = [
        {"representative_title": first["clean_title"], "published_at": first["published_at"], "articles": [first]},
        {"representative_title": second["clean_title"], "published_at": second["published_at"], "articles": [second]},
    ]

    entries = limited_digest_article_entries(clusters, config)["domestic"]
    groups = group_digest_entries(entries, config)
    message = build_daily_digest_messages(clusters, config, now, now - timedelta(hours=24))[0]

    assert [len(group) for group in groups] == [2]
    assert message.count("동원수산") == 1
    assert "news.mtn.co.kr" not in message
    assert "①" not in message
    assert "②" not in message


def test_digest_entries_collapse_portal_mirror_same_article(config) -> None:  # type: ignore[no-untyped-def]
    portal = make_article(
        "[정보공시 Q&A] 거버넌스 환경 변화와 ESG 공시 - v.daum.net",
        "https://v.daum.net/v/20260501131434127?f=p",
        source="v.daum.net",
        published_at="2026-05-01T13:14:34+09:00",
    )
    original = make_article(
        "거버넌스 환경 변화와 ESG 공시",
        "https://www.hankyung.com/amp/202604189712G",
        source="한국경제",
        published_at="2026-05-01T13:12:09+09:00",
    )
    portal["status"] = "duplicate"
    original["status"] = "duplicate"

    entries = digest_article_entries([], config, [portal, original])["domestic"]

    assert len(entries) == 1
    assert entries[0]["title"] == "거버넌스 환경 변화와 ESG 공시"
    assert entries[0]["url"] == "https://www.hankyung.com/amp/202604189712G"


def test_duplicate_window_uses_article_date_before_seen_date(config, now) -> None:  # type: ignore[no-untyped-def]
    old_duplicate = make_article(
        "거버넌스 환경 변화와 ESG 공시",
        "https://www.hankyung.com/amp/202604189712G",
        source="한국경제",
        published_at="2026-05-01T13:12:09+09:00",
    )
    old_duplicate["status"] = "duplicate"
    old_duplicate["seen_at"] = "2026-05-02T09:00:00+09:00"
    state = {"articles": [old_duplicate]}

    selected = duplicate_records_in_window(
        state,
        config,
        datetime(2026, 5, 2, 0, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        datetime(2026, 5, 2, 23, 59, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    assert selected == []


def test_daily_digest_groups_same_policy_event_across_titles(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    monkeypatch.setattr(summaries, "generate_daily_digest_review", lambda *_args, **_kwargs: "- 의무공개매수 논의 지속")
    first = make_article(
        "경영권 프리미엄 독식 막는다…M&A시 소액주주 지분도 의무매수 - SBSBiz",
        "https://biz.sbs.co.kr/article/1",
        source="SBSBiz",
        published_at="2026-04-29T12:00:00+09:00",
        summary="의무공개매수 자본시장법 일반주주 보호",
    )
    second = make_article(
        "박상혁 의원, 의무공개매수제 자본시장법 개정안 발의…일반주주 권익 보호",
        "https://www.etoday.co.kr/news/view/1",
        source="이투데이",
        published_at="2026-04-29T12:10:00+09:00",
        summary="M&A 일반주주 지분 의무 공개매수",
    )
    clusters = [
        {"representative_title": first["clean_title"], "published_at": first["published_at"], "articles": [first]},
        {"representative_title": second["clean_title"], "published_at": second["published_at"], "articles": [second]},
    ]

    entries = limited_digest_article_entries(clusters, config)["domestic"]
    groups = group_digest_entries(entries, config)
    message = build_daily_digest_messages(clusters, config, now, now - timedelta(hours=24))[0]

    assert [len(group) for group in groups] == [2]
    assert message.count("href=") == 1
    assert "SBSBiz" not in message


def test_daily_digest_groups_same_regulatory_designation_event(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    monkeypatch.setattr(summaries, "generate_daily_digest_review", lambda *_args, **_kwargs: "- 총수 지정 이슈 부각")
    first = make_article(
        "공정위 쿠팡 총수는 법인 아닌 김범석…5년 만에 동일인 변경",
        "https://www.hani.co.kr/1",
        source="한겨레",
        published_at="2026-04-29T12:00:00+09:00",
        summary="대기업집단 동일인 지정 사익편취 규제",
    )
    second = make_article(
        "쿠팡 김범석 총수로 변경… 공정위, 대기업집단 102개 지정",
        "https://www.skyedaily.com/1",
        source="스카이데일리",
        published_at="2026-04-29T12:10:00+09:00",
        summary="공정위 총수 동일인 지정 규제",
    )
    clusters = [
        {"representative_title": first["clean_title"], "published_at": first["published_at"], "articles": [first]},
        {"representative_title": second["clean_title"], "published_at": second["published_at"], "articles": [second]},
    ]

    entries = limited_digest_article_entries(clusters, config)["domestic"]
    groups = group_digest_entries(entries, config)
    message = build_daily_digest_messages(clusters, config, now, now - timedelta(hours=24))[0]

    assert [len(group) for group in groups] == [2]
    assert message.count("쿠팡") == 1


def test_daily_digest_starts_new_message_for_new_category(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    monkeypatch.setattr(summaries, "generate_daily_digest_review", lambda *_args, **_kwargs: "- 주요 이슈 지속")
    shareholder = make_article(
        "행동주의 펀드 공개서한 제출",
        "https://example.com/shareholder",
        source="행동뉴스",
        summary="행동주의 공개서한",
    )
    valueup = make_article(
        "동원수산 저PBR 해소 위한 밸류업 추진",
        "https://example.com/valueup",
        source="밸류뉴스",
        summary="저PBR 밸류업 주주환원",
    )
    capital = make_article(
        "상장적격성 실질심사와 거래정지 개선기간 부여",
        "https://example.com/capital",
        source="공시뉴스",
        summary="상장폐지 거래정지 투자자 보호",
    )
    clusters = [
        {"representative_title": article["clean_title"], "published_at": article["published_at"], "articles": [article]}
        for article in (shareholder, valueup, capital)
    ]

    messages = build_daily_digest_messages(clusters, config, now, now - timedelta(hours=24))

    assert len(messages) == 3
    assert "<b>주주행동·경영권</b>" in messages[0]
    assert messages[1].startswith("<b>밸류업·주주환원</b>")
    assert messages[2].startswith("<b>자본시장 제도·공시</b>")


def test_daily_digest_splits_on_section_and_group_boundaries(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    monkeypatch.setattr(summaries, "generate_daily_digest_review", lambda *_args, **_kwargs: "- 주요 이슈 지속")
    config["digest"]["max_message_chars"] = 650  # type: ignore[index]
    titles = [
        "공개서한 제출로 이사회 책임 추궁",
        "임시주총 소집 청구와 표 대결 예고",
        "감사 선임 안건 두고 주주연대 압박",
        "위임장 권유 과정에서 경영진 견제",
        "주주명부 열람 소송과 주주권 행사",
        "소액주주 캠페인으로 정관 변경 요구",
        "행동주의 펀드가 자본배치 개선 촉구",
        "이사회 교체 요구와 경영진 반박",
        "주주제안 안건 상정 여부 공방",
        "기관투자자 주주활동 확대 흐름",
        "공개매수 이후 경영권 분쟁 쟁점",
    ]
    articles = [
        make_article(
            title,
            f"https://example.com/shareholder-{index}",
            source=f"언론{index}",
            summary="소액주주 주주제안 경영권 분쟁",
        )
        for index, title in enumerate(titles, start=1)
    ]
    clusters = [
        {"representative_title": article["clean_title"], "published_at": article["published_at"], "articles": [article]}
        for article in articles
    ]

    messages = build_daily_digest_messages(clusters, config, now, now - timedelta(hours=24))

    assert len(messages) > 1
    assert all(len(message) <= 900 for message in messages)
    for message in messages[1:]:
        first_line = next(line for line in message.splitlines() if line.strip())
        assert first_line.startswith(("<b>주주행동·경영권", "<b>자본시장 제도·공시"))
        assert not first_line.startswith("  <a")
        assert not first_line.startswith("①")


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

    assert "04.27 /" not in message
    assert "(2건)" not in message
    assert "①" not in message
    assert "②" not in message
    assert "링크:" not in message
    assert 'href="https://www.seoulfn.com/news/articleView.html?idxno=627481"' in message
    assert 'href="https://www.sisajournal.com/news/articleView.html?idxno=371009"' not in message


def test_digest_does_not_group_broad_same_company_dispute_titles(config, now) -> None:  # type: ignore[no-untyped-def]
    articles = [
        make_article(
            "실체 불분명한 고려아연 소액주주연합 경영권 분쟁 속 기획 고발 의혹",
            "https://example.com/korea-zinc-minority",
            source="경기일보",
            published_at="2026-04-29T20:30:00+09:00",
            summary="고려아연 소액주주연합의 실체와 고발 배경을 둘러싼 의혹",
        ),
        make_article(
            "영풍, 고려아연 황산 거래 중단은 경영권 분쟁 수단 본안서 다툴 것",
            "https://example.com/korea-zinc-acid",
            source="뉴시안",
            published_at="2026-04-29T20:32:00+09:00",
            summary="영풍이 황산취급대행 계약 종료와 거래거절 가처분 항고 기각에 입장을 냈다",
        ),
        make_article(
            "최윤범, 고려아연 미래 성장 가속페달 영풍은 법적 분쟁 발목잡기",
            "https://example.com/korea-zinc-growth",
            source="아시아타임즈",
            published_at="2026-04-29T20:35:00+09:00",
            summary="영풍, 경영권 분쟁 후 소송전 확대해 고려아연의 신사업 투자와 함께 조명됐다",
        ),
    ]
    for article in articles:
        article["company_candidates"] = ["고려아연", "영풍"]
    clusters = [
        {
            "representative_title": article["clean_title"],
            "published_at": article["published_at"],
            "companies": article["company_candidates"],
            "articles": [article],
        }
        for article in articles
    ]

    entries = limited_digest_article_entries(clusters, config)["domestic"]
    groups = group_digest_entries(entries, config)

    assert [len(group) for group in groups] == [1, 1, 1]


def test_ai_story_judge_can_block_digest_grouping(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr(
        "curator.summaries.judge_same_story",
        lambda *_args, **_kwargs: StoryJudgement("related_but_different", 0.9, "같은 회사지만 다른 사건"),
    )
    first = make_article(
        "고려아연 소액주주, 사외이사 검찰 고발",
        "https://example.com/digest-complaint",
        source="시사저널",
        published_at="2026-04-29T20:30:00+09:00",
        relevance_level="high",
    )
    first["company_candidates"] = ["고려아연"]
    second = make_article(
        "고려아연 소액주주, 금융위 진정",
        "https://example.com/digest-fsc",
        source="서울파이낸스",
        published_at="2026-04-29T20:32:00+09:00",
        relevance_level="high",
    )
    second["company_candidates"] = ["고려아연"]
    clusters = [
        {"representative_title": first["clean_title"], "published_at": first["published_at"], "articles": [first]},
        {"representative_title": second["clean_title"], "published_at": second["published_at"], "articles": [second]},
    ]

    entries = limited_digest_article_entries(clusters, config)["domestic"]
    groups = group_digest_entries(entries, config)

    assert [len(group) for group in groups] == [1, 1]


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

    config["ai"]["daily_digest_enabled"] = True  # type: ignore[index]
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

    assert message.startswith("수집: 04.25 08:30-09:30 KST")
    assert "수집: 04.25 08:30-09:30 KST" in message
    assert "<b>요약</b>" in message
    assert "<b>중복 확인</b>" not in message
    assert 'href="https://example.com/old"' not in message
    assert "04.24 / 신한금융 밸류업 2.0 발표" not in message


def test_hourly_update_window_is_thirty_minutes(config, now) -> None:  # type: ignore[no-untyped-def]
    assert hourly_update_start_at(config, now) == now - timedelta(minutes=30)


def test_hourly_update_uses_two_overnight_half_windows(config) -> None:  # type: ignore[no-untyped-def]
    first_half = datetime(2026, 5, 1, 3, 35, tzinfo=ZoneInfo("Asia/Seoul"))
    second_half = datetime(2026, 5, 1, 6, 5, tzinfo=ZoneInfo("Asia/Seoul"))

    assert hourly_update_start_at(config, first_half) == datetime(2026, 5, 1, 1, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    assert hourly_update_start_at(config, second_half) == datetime(2026, 5, 1, 3, 30, tzinfo=ZoneInfo("Asia/Seoul"))


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
    assert sent_messages[0].startswith("수집: 04.25 09:00-09:30 KST")
    assert "수집: 04.25 09:00-09:30 KST" in sent_messages[0]
    assert "<b>요약</b>" in sent_messages[0]
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
    skip_now = datetime(2026, 4, 26, 5, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    assert publish_hourly_telegram_update(state, config, skip_now, []) == {
        "telegram_sent": 0,
        "telegram_failed": 0,
    }
    assert state["telegram_sent_cluster_guids"] == []
