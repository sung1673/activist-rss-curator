from __future__ import annotations

import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from conftest import make_article
from curator import daily_report
from curator.daily_report import build_daily_report, build_report_telegram_message, write_report_files


def report_cluster(guid: str, now: datetime) -> dict[str, object]:
    return {
        "guid": guid,
        "published_at": (now - timedelta(hours=1)).isoformat(),
        "representative_title": "한화솔루션 유상증자 정정요구",
        "articles": [
            make_article(
                "한화솔루션 유상증자 정정요구",
                "https://example.com/a",
                source="연합뉴스",
                summary="금융당국이 한화솔루션 유상증자 신고서에 정정을 요구했다.",
                published_at=(now - timedelta(hours=1)).isoformat(),
            ),
            make_article(
                "금감원, 한화솔루션 유증 또 제동",
                "https://example.com/b",
                source="뉴시스",
                summary="투자자 보호와 공시 보완 필요성이 다시 제기됐다.",
                published_at=(now - timedelta(minutes=50)).isoformat(),
            ),
        ],
        "article_count": 2,
    }


def test_daily_report_writes_techmeme_like_html(tmp_path) -> None:
    now = datetime(2026, 5, 1, 10, 20, tzinfo=ZoneInfo("Asia/Seoul"))
    (tmp_path / "data").mkdir()
    (tmp_path / "config.yaml").write_text("report:\n  image_enrich_limit: 0\n", encoding="utf-8")
    (tmp_path / "data" / "state.json").write_text(
        json.dumps({"published_clusters": [report_cluster("cluster:test", now)], "pending_clusters": [], "articles": []}),
        encoding="utf-8",
    )

    report = build_daily_report(tmp_path, now)
    paths = write_report_files(report, tmp_path)
    html = paths[0].read_text(encoding="utf-8")

    assert paths[0].name == "2026-05-01.html"
    assert (tmp_path / "public" / "feed" / "latest.html").exists()
    assert (tmp_path / "public" / "feed" / "index.html").exists()
    assert 'href="https://bside.ai"' in html
    assert "bside-logo" in html
    assert "bside-logo__image" in html
    assert 'viewBox="0 0 57 20"' in html
    assert "color: var(--accent); flex: 0 0 auto" in html
    assert "-webkit-mask:" not in html
    assert "Editor’s Brief" in html
    assert "brief__bullets" in html
    assert "story__image--logo" in html
    assert "story__source-logo" in html
    assert "floating-nav" in html
    assert "floating-nav__meta" in html
    assert "toc__brand" in html
    assert "bside-logo--nav" in html
    assert "toc__chips" in html
    assert "mobile-story-nav" in html
    assert "data-mobile-nav-story" in html
    assert "data-mobile-section-label" in html
    assert "발행일자" in html
    assert "수집기간" in html
    assert "다른 일자 보기" in html
    assert "max-width: 1000px" in html
    assert "width: 210px" in html
    assert "@media (max-width: 1160px)" in html
    assert "grid-template-columns: 82px minmax(0, 1fr)" in html
    assert "word-break: keep-all" in html
    assert "font-size: 16.5px" in html
    assert "Apple SD Gothic Neo" in html
    assert "max-width: 700px" in html
    assert "font-size: 18.5px" in html
    assert "font-size: 12.5px" in html
    assert "line-height: 1.58" in html
    assert "scroll-margin-top: 124px" in html
    assert "-webkit-line-clamp: 2" in html
    assert "data-toc-section" in html
    assert "data-nav-story-index" in html
    assert "target.scrollIntoView" in html
    assert "data-section-index" in html
    assert "data-progress-text" in html
    assert "story__image--broken" in html
    assert "기사 링크 1건 보기" not in html
    assert "More:" not in html
    assert "story__sources" in html
    assert "<p>" in html
    assert "한화솔루션 유상증자 정정요구" in html


def test_daily_report_telegram_message_links_to_report(tmp_path) -> None:
    now = datetime(2026, 5, 1, 10, 20, tzinfo=ZoneInfo("Asia/Seoul"))
    (tmp_path / "data").mkdir()
    (tmp_path / "config.yaml").write_text(
        'public_feed_url: "https://news.bside.ai/feed.xml"\nreport:\n  image_enrich_limit: 0\n',
        encoding="utf-8",
    )
    (tmp_path / "data" / "state.json").write_text(
        json.dumps({"published_clusters": [report_cluster("cluster:test", now)], "pending_clusters": [], "articles": []}),
        encoding="utf-8",
    )

    report = build_daily_report(tmp_path, now)
    message = build_report_telegram_message(report)

    assert "26년 5월 1일 주주·자본시장 데일리" in message
    assert "전체 리포트 보기" not in message
    assert "주요 기사" not in message
    assert "메인 기사" in message
    assert "수집 기사 2건" in message
    assert "이슈 " in message
    assert "매체 " in message
    assert "https://news.bside.ai/feed/2026-05-01.html" in message


def test_daily_report_write_only_writes_page_before_send(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    now = datetime(2026, 5, 1, 10, 20, tzinfo=ZoneInfo("Asia/Seoul"))
    (tmp_path / "data").mkdir()
    (tmp_path / "config.yaml").write_text("report:\n  image_enrich_limit: 0\n", encoding="utf-8")
    (tmp_path / "data" / "state.json").write_text(
        json.dumps({"published_clusters": [report_cluster("cluster:test", now)], "pending_clusters": [], "articles": []}),
        encoding="utf-8",
    )
    monkeypatch.setenv("CURATOR_DAILY_REPORT_WRITE_ONLY", "1")
    monkeypatch.setattr(daily_report, "now_in_timezone", lambda _timezone: now)
    monkeypatch.setattr(daily_report, "telegram_is_configured", lambda _config: True)

    def fail_send(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("write-only mode must not send Telegram messages")

    monkeypatch.setattr(daily_report, "send_telegram_message", fail_send)

    summary = daily_report.send_daily_report(tmp_path)

    assert summary == {"daily_report_written": 1, "daily_report_sent": 0, "daily_report_failed": 0}
    assert (tmp_path / "public" / "feed" / "latest.html").exists()
