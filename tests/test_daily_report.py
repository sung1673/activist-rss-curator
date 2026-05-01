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
    (tmp_path / "data" / "state.json").write_text(
        json.dumps({"published_clusters": [report_cluster("cluster:test", now)], "pending_clusters": [], "articles": []}),
        encoding="utf-8",
    )

    report = build_daily_report(tmp_path, now)
    paths = write_report_files(report, tmp_path)
    html = paths[0].read_text(encoding="utf-8")

    assert paths[0].name == "2026-05-01.html"
    assert (tmp_path / "public" / "reports" / "latest.html").exists()
    assert (tmp_path / "public" / "reports" / "index.html").exists()
    assert "BSIDE KOREA DAILY NEWS" in html
    assert "Editor’s Brief" in html
    assert "More:" in html
    assert "floating-nav" in html
    assert "<table>" in html
    assert "한화솔루션 유상증자 정정요구" in html


def test_daily_report_telegram_message_links_to_report(tmp_path) -> None:
    now = datetime(2026, 5, 1, 10, 20, tzinfo=ZoneInfo("Asia/Seoul"))
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "state.json").write_text(
        json.dumps({"published_clusters": [report_cluster("cluster:test", now)], "pending_clusters": [], "articles": []}),
        encoding="utf-8",
    )

    report = build_daily_report(tmp_path, now)
    message = build_report_telegram_message(report)

    assert "비사이드 자본시장 데일리" in message
    assert "전체 리포트 보기" in message
    assert "reports/2026-05-01.html" in message
