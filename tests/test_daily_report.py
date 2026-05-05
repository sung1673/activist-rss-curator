from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from conftest import make_article
from curator import daily_report
from curator.daily_report import build_daily_report, build_report_telegram_message, mobile_article_url, write_report_files
from curator.normalize import canonical_url_hash


def report_cluster(guid: str, now: datetime) -> dict[str, object]:
    return {
        "guid": guid,
        "published_at": (now - timedelta(hours=1)).isoformat(),
        "representative_title": "한화솔루션 유상증자 정정요구",
        "articles": [
            make_article(
                "한화솔루션 유상증자 정정요구",
                "https://news.naver.com/main/read.naver?oid=001&aid=0010000001",
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
        json.dumps(
            {
                "published_clusters": [report_cluster("cluster:test", now)],
                "pending_clusters": [],
                "articles": [],
                "telegram_source_messages": [
                    {
                        "handle": "marketnews",
                        "telegram_channel_id": "100",
                        "channel_title": "시장 채널",
                        "telegram_message_id": 7,
                        "posted_at": now.isoformat(),
                        "text": "한화솔루션 유상증자 관련 시장 언급",
                        "message_url": "https://t.me/marketnews/7",
                    }
                ],
                "telegram_article_matches": [
                    {
                        "article_id": canonical_url_hash("https://news.naver.com/main/read.naver?oid=001&aid=0010000001"),
                        "telegram_message_key": "id:100:7",
                        "match_type": "canonical_url",
                        "score": 0.96,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    stale_variant_dir = tmp_path / "public" / "feed" / "variants"
    stale_variant_dir.mkdir(parents=True)
    (stale_variant_dir / "forbes.html").write_text("stale", encoding="utf-8")
    (stale_variant_dir / "social.html").write_text("stale", encoding="utf-8")

    report = build_daily_report(tmp_path, now)
    paths = write_report_files(report, tmp_path)
    html = paths[0].read_text(encoding="utf-8")

    assert paths[0].name == "2026-05-01.html"
    assert (tmp_path / "public" / "feed" / "latest.html").exists()
    assert (tmp_path / "public" / "feed" / "index.html").exists()
    assert (tmp_path / "public" / "feed" / "workbench.html").exists()
    assert not (tmp_path / "public" / "feed" / "variants" / "memo.html").exists()
    assert not (tmp_path / "public" / "feed" / "variants" / "board.html").exists()
    assert not (tmp_path / "public" / "feed" / "variants" / "pulse.html").exists()
    assert not (tmp_path / "public" / "feed" / "variants" / "deck.html").exists()
    assert not (tmp_path / "public" / "feed" / "variants" / "forbes.html").exists()
    assert not (tmp_path / "public" / "feed" / "variants" / "social.html").exists()
    assert 'href="https://bside.ai"' in html
    assert "bside-logo" in html
    assert "bside-logo__image" in html
    assert 'viewBox="0 0 57 20"' in html
    assert "color: var(--accent); flex: 0 0 auto" in html
    assert "-webkit-mask:" not in html
    assert "Editor’s Brief" not in html
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
    assert ".mobile-story-nav { display: none; }" in html
    assert "발행일자" in html
    assert "수집기간" in html
    assert "다른 일자 보기" in html
    assert "AI 워크벤치 보기" in html
    assert 'href="workbench.html"' in html
    assert "data-archive-toggle" in html
    assert "archive-panel__link is-current" in html
    assert "setArchiveOpen" in html
    assert "max-width: 1000px" in html
    assert "width: 210px" in html
    assert "@media (max-width: 1160px)" in html
    assert "grid-template-columns: 82px minmax(0, 1fr)" in html
    assert "word-break: keep-all" in html
    assert "font-size: 16.5px" in html
    assert "Apple SD Gothic Neo" in html
    assert "max-width: 700px" in html
    assert "font-size: 18.5px" in html
    assert "brief-title__eyebrow" in html
    assert "오늘의" in html
    assert "핵심 브리핑" in html
    assert "오늘의 중요 기사" in html
    assert "복수 보도, 주주권·공시 영향" in html
    assert "story__summary" in html
    assert "요점" not in html
    assert "맥락:" not in html
    assert "근거" not in html
    assert "brief__link" in html
    assert "imageCandidates" in html
    assert "tryNextImageCandidate" in html
    assert "promoteCandidateImage" in html
    assert "applyResponsiveArticleLinks" in html
    assert 'data-mobile-url="https://n.news.naver.com/article/001/0010000001"' in html
    assert "is-active-section" in html
    assert "좌우 스크롤" in html
    assert "밀어서 보기" in html
    assert "font-size: 12.5px" in html
    assert "line-height: 1.58" in html
    assert "grid-template-columns: repeat(2, minmax(0, 1fr))" in html
    assert "column-count: 2" not in html
    assert "break-inside: avoid" not in html
    assert "display: inline-grid" not in html
    assert "max-height: 4.35em" not in html
    assert "max-height: 3.55em" not in html
    assert "-webkit-line-clamp: 1" in html
    assert "Layout Lab" not in html
    assert "variants/memo.html" not in html
    assert "variants/board.html" not in html
    assert "variants/pulse.html" not in html
    assert "variants/deck.html" not in html
    assert "layout-standard" not in html
    assert "layout-pulse" not in html
    assert "layout-deck" not in html
    assert "is-mobile-context" in html
    assert "data-context-label" in html
    assert "bside-daily-read" in html
    assert "markStoryRead" in html
    assert "lastActiveSectionId" in html
    assert "scroll-margin-top: 124px" in html
    assert "-webkit-line-clamp: 2" in html
    assert "data-toc-section" in html
    assert "data-nav-story-index" in html
    assert "target.scrollIntoView" in html
    assert "data-section-index" in html
    assert "data-progress-text" in html
    assert "visualStoryEntries" in html
    assert "visualStoryIndexByHref" in html
    assert "pageTop(section)" in html
    assert "DB 맥락 보기" not in html
    assert "관련 기사 보기" in html
    assert "이슈 레이더" in html
    assert "아카이브 검색" in html
    assert "data-story-context" in html
    assert "loadStoryContext" in html
    assert "preloadPendingStoryContexts" in html
    assert "contextPending" in html
    assert "통합 표" in html
    assert "data-story-current-links" in html
    assert "story-context__table" in html
    assert "현재 묶음" in html
    assert "아카이브" in html
    assert "fetchTelegramMentions" in html
    assert "data-story-telegram-mentions" in html
    assert "Telegram 언급" in html
    assert "URL 직접" in html
    assert "db-search__summary" in html
    assert "articleMatchReasons" in html
    assert "isGenericDbPulseTitle" in html
    assert "story__image--broken" in html
    assert "✓" in html
    assert "기사 링크 1건 보기" not in html
    assert "More:" not in html
    assert "story__sources" in html
    assert "<p>" in html
    assert "한화솔루션 유상증자 정정요구" in html
    workbench_html = (tmp_path / "public" / "feed" / "workbench.html").read_text(encoding="utf-8")
    assert "AI 요약 워크벤치" in workbench_html
    assert "data-workbench-list" in workbench_html
    assert "fetchArchiveRows" in workbench_html
    assert "현재 묶음과 DB 아카이브" in workbench_html
    workbench_data = re.search(r'<script type="application/json" id="workbench-data">(.*?)</script>', workbench_html, re.S)
    assert workbench_data
    assert "&quot;" not in workbench_data.group(1)
    workbench_stories = json.loads(workbench_data.group(1))
    assert workbench_stories
    assert all(isinstance(story.get("title"), str) for story in workbench_stories)
    current_link_data = re.findall(r'<script type="application/json" data-story-current-links>(.*?)</script>', html, re.S)
    assert all("&quot;" not in script for script in current_link_data)
    assert all(isinstance(json.loads(script), list) for script in current_link_data)


def test_json_script_payload_keeps_application_json_parseable() -> None:
    payload = daily_report.json_script_payload([{"title": 'A "quoted" title', "url": "https://example.com/a</script>"}])

    assert "&quot;" not in payload
    assert "<\\/script>" in payload
    assert json.loads(payload)[0]["title"] == 'A "quoted" title'


def test_daily_report_refreshes_archive_links_on_existing_pages(tmp_path) -> None:
    first_now = datetime(2026, 5, 1, 10, 20, tzinfo=ZoneInfo("Asia/Seoul"))
    second_now = datetime(2026, 5, 2, 10, 20, tzinfo=ZoneInfo("Asia/Seoul"))
    (tmp_path / "data").mkdir()
    (tmp_path / "config.yaml").write_text("report:\n  image_enrich_limit: 0\n", encoding="utf-8")
    (tmp_path / "data" / "state.json").write_text(
        json.dumps({"published_clusters": [report_cluster("cluster:test", first_now)], "pending_clusters": [], "articles": []}),
        encoding="utf-8",
    )

    write_report_files(build_daily_report(tmp_path, first_now), tmp_path)
    first_page = tmp_path / "public" / "feed" / "2026-05-01.html"
    assert "2026-05-02.html" not in first_page.read_text(encoding="utf-8")

    write_report_files(build_daily_report(tmp_path, second_now), tmp_path)
    refreshed_first_html = first_page.read_text(encoding="utf-8")
    second_html = (tmp_path / "public" / "feed" / "2026-05-02.html").read_text(encoding="utf-8")

    assert 'href="2026-05-02.html">2026-05-02' in refreshed_first_html
    assert 'href="2026-05-01.html">2026-05-01<span>현재</span>' in refreshed_first_html
    assert 'href="2026-05-02.html">2026-05-02<span>현재</span>' in second_html


def test_mobile_article_url_uses_known_mobile_hosts() -> None:
    assert (
        mobile_article_url("https://news.naver.com/main/read.naver?mode=LSD&oid=001&aid=0010000001")
        == "https://n.news.naver.com/article/001/0010000001"
    )
    assert mobile_article_url("https://news.naver.com/article/015/0001234567") == "https://n.news.naver.com/article/015/0001234567"
    assert mobile_article_url("https://news.v.daum.net/v/20260501090000001") == "https://v.daum.net/v/20260501090000001"
    assert mobile_article_url("https://example.com/news/1") == "https://example.com/news/1"


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
