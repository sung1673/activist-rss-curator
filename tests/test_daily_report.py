from __future__ import annotations

import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from conftest import make_article
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
    assert "https://bside.ai/images/icons/bside-logo-gray.svg" in html
    assert "Editor’s Brief" in html
    assert "brief__bullets" in html
    assert "story__image--logo" in html
    assert "story__source-logo" in html
    assert "floating-nav" in html
    assert "data-toc-section" in html
    assert "data-nav-story-index" in html
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
    (tmp_path / "config.yaml").write_text("report:\n  image_enrich_limit: 0\n", encoding="utf-8")
    (tmp_path / "data" / "state.json").write_text(
        json.dumps({"published_clusters": [report_cluster("cluster:test", now)], "pending_clusters": [], "articles": []}),
        encoding="utf-8",
    )

    report = build_daily_report(tmp_path, now)
    message = build_report_telegram_message(report)

    assert "26년 5월 1일 주주·자본시장 데일리" in message
    assert "전체 리포트 보기" not in message
    assert "주요 기사" in message
    assert "주주행동·경영권" in message
    assert "feed/2026-05-01.html" in message
