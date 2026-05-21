from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from curator.story_review import (
    build_story_review,
    render_story_review_html,
    story_review_access_token,
    story_review_message,
    text_has_encoding_damage,
    token_hash,
)


def story(title: str, source: str, url: str, now: datetime, summary: str = "") -> dict[str, object]:
    return {
        "id": url.rsplit("/", 1)[-1],
        "title": title,
        "category": "밸류업·주주환원",
        "summary": summary or title,
        "links": [{"source": source, "title": title, "url": url}],
        "link_count": 1,
        "primary_url": url,
        "primary_source": source,
        "datetime": now,
        "story_key": "",
        "db_query": title,
    }


def test_build_story_review_detects_split_candidate() -> None:
    now = datetime(2026, 5, 10, 8, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    config = {
        "timezone": "Asia/Seoul",
        "public_feed_url": "https://news.bside.ai/feed.xml",
        "story_review": {"min_score": 60, "max_candidates": 5},
    }
    stories = [
        story(
            "진옥동 신한금융 회장, 북중미서 밸류업 2.0 IR…성장·주주환원 병행",
            "전자신문",
            "https://example.com/a",
            now,
            "신한금융 밸류업 2.0과 주주환원 확대 설명",
        ),
        story(
            "진옥동 성장할수록 주주환원 확대…신한금융 밸류업 2.0 북중미 설득전",
            "베타뉴스",
            "https://example.com/b",
            now - timedelta(minutes=10),
            "신한금융 북중미 IR에서 주주환원 정책을 설명",
        ),
        story(
            "WEX, 임팩티브와 합의…이사회에 이사 3인 추가",
            "Investing",
            "https://example.com/c",
            now,
            "해외 행동주의 합의 보도",
        ),
    ]

    review = build_story_review(stories, config, now - timedelta(days=1), now, "2026-05-10")

    assert review["candidate_count"] == 1
    candidate = review["candidates"][0]  # type: ignore[index]
    assert "신한금융" in candidate["left"]["title"]  # type: ignore[index]
    assert "신한금융" in candidate["right"]["title"]  # type: ignore[index]
    assert "WEX" not in candidate["left"]["title"]  # type: ignore[index]
    assert candidate["score"] >= 60


def test_story_review_detects_duplicate_listing_policy_split_candidate() -> None:
    now = datetime(2026, 5, 21, 8, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    config = {
        "timezone": "Asia/Seoul",
        "public_feed_url": "https://news.bside.ai/feed.xml",
        "story_review": {"min_score": 60, "max_candidates": 5},
    }
    stories = [
        story(
            "중복상장, 주주 동의 어떻게 받을까?...MoM 놓고 기관과 PE·증권사 격돌[자본법안 와치]",
            "에너지경제신문",
            "https://example.com/duplicate-listing-review-a",
            now,
            "중복상장 제도와 모회사 주주 동의 범위가 쟁점",
        ),
        story(
            "원칙 금지·예외 허용 중복상장 가닥...주주보호 장치 쟁점",
            "디지털투데이",
            "https://example.com/duplicate-listing-review-b",
            now - timedelta(minutes=1),
            "중복상장 규제 방향과 주주보호 장치가 논의됐다",
        ),
        story(
            "자본시장법 중복상장 제도 손질 필요...핵심은 모회사 주주 동의 범위",
            "스트레이트뉴스",
            "https://example.com/duplicate-listing-review-c",
            now - timedelta(hours=3),
            "자본시장법상 중복상장 제도와 모회사 주주 동의 범위가 제기됐다",
        ),
    ]

    review = build_story_review(stories, config, now - timedelta(days=1), now, "2026-05-21")

    assert review["candidate_count"] >= 1
    assert any("중복상장" in candidate["left"]["title"] and "중복상장" in candidate["right"]["title"] for candidate in review["candidates"])  # type: ignore[index]


def test_story_review_token_falls_back_to_derived_bot_token(monkeypatch) -> None:
    monkeypatch.delenv("STORY_REVIEW_ACCESS_TOKEN", raising=False)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123456:secret")

    token = story_review_access_token()

    assert token
    assert token != "123456:secret"
    assert token == story_review_access_token()


def test_render_story_review_html_contains_token_gate() -> None:
    html = render_story_review_html(
        {
            "date_id": "2026-05-10",
            "start_label": "2026-05-09 08:00 KST",
            "end_label": "2026-05-10 08:00 KST",
            "candidate_count": 0,
            "candidate_hash": "abc",
            "access_token_hash": token_hash("review-token"),
            "candidates": [],
        }
    )

    assert "접근 token 확인" in html
    assert token_hash("review-token") in html
    assert "오늘은 우선 검토할 분리 후보가 없습니다" in html


def test_story_review_message_uses_tokenized_admin_link(monkeypatch) -> None:
    monkeypatch.setenv("STORY_REVIEW_ACCESS_TOKEN", "review-token")
    config = {"public_feed_url": "https://news.bside.ai/feed.xml"}
    message = story_review_message(
        {
            "date_id": "2026-05-10",
            "candidate_count": 0,
            "page_url": "https://news.bside.ai/feed/story-review.html",
            "candidates": [],
        },
        config,
    )

    assert "묶음 후보 리뷰" in message
    assert "token=review-token" in message
    assert "관리자 페이지에서 후보 검토" in message


def test_story_review_skips_encoding_damaged_titles() -> None:
    now = datetime(2026, 5, 10, 8, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    damaged = "KT \udcec\uc520\udcec\uad97\udced\uc276"
    assert text_has_encoding_damage(damaged)

    review = build_story_review(
        [
            story(damaged, "NEWSWAY", "https://example.com/a", now),
            story(damaged + " 추가", "BUSINESSPLUS", "https://example.com/b", now),
        ],
        {"timezone": "Asia/Seoul", "story_review": {"min_score": 1}},
        now - timedelta(days=1),
        now,
        "2026-05-10",
    )

    assert review["candidate_count"] == 0
