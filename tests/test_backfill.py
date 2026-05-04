from __future__ import annotations

from datetime import date

from conftest import make_article

from curator import backfill


def test_google_news_query_specs_strip_when_and_add_date_window(config) -> None:  # type: ignore[no-untyped-def]
    config["feeds"] = [
        {
            "name": "test-feed",
            "category": "core",
            "url": "https://news.google.com/rss/search?q=%ED%96%89%EB%8F%99%EC%A3%BC%EC%9D%98+when%3A7d&hl=ko&gl=KR&ceid=KR:ko",
        }
    ]
    specs = backfill.build_query_specs(config, include_defaults=False)
    window = backfill.DateWindow(date(2026, 1, 1), date(2026, 1, 8))
    url = backfill.google_news_search_url(specs[0], window)

    assert len(specs) == 1
    assert specs[0].query == "행동주의"
    assert "when" not in url
    assert "after%3A2026-01-01" in url
    assert "before%3A2026-01-08" in url


def test_build_date_windows_uses_exclusive_end() -> None:
    windows = backfill.build_date_windows(date(2026, 1, 1), date(2026, 1, 16), 7)

    assert [window.key for window in windows] == [
        "2026-01-01:2026-01-08",
        "2026-01-08:2026-01-15",
        "2026-01-15:2026-01-16",
    ]


def test_process_window_syncs_chunk_payload(config, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    article = make_article(
        "행동주의 주주 공개서한",
        "https://example.com/a",
        summary="행동주의 주주가 공개서한을 발송했다.",
        relevance_level="high",
        published_at="2026-01-03T09:00:00+09:00",
    )
    low_article = make_article(
        "장중 증시 특징주",
        "https://example.com/b",
        summary="단순 장중 시황이다.",
        relevance_level="low",
        published_at="2026-01-03T10:00:00+09:00",
    )
    args = backfill.build_arg_parser().parse_args(
        [
            "--dry-run",
            "--max-queries",
            "1",
            "--max-enrich-articles",
            "0",
        ]
    )
    query_specs = [backfill.QuerySpec("q", "행동주의", "core")]
    window = backfill.DateWindow(date(2026, 1, 1), date(2026, 1, 8))

    dedupe_state = backfill.default_state()
    monkeypatch.setattr(backfill, "fetch_google_alerts_articles", lambda _config: [article, low_article])

    summary = backfill.process_window(window, config, query_specs, dedupe_state, args)

    assert summary["fetched"] == 2
    assert summary["accepted"] == 1
    assert summary["rejected"] == 1
    assert len(dedupe_state["articles"]) == 2
    assert summary["remote_status"] == "dry_run"
