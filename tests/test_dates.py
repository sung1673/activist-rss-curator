from __future__ import annotations

from datetime import timedelta

from curator.dates import choose_publication_datetime, extract_published_datetime_from_html, is_too_old


def test_article_published_time_meta_is_used_first() -> None:
    html = """
    <html><head>
      <meta property="article:published_time" content="2026-04-25T08:00:00+09:00">
      <meta name="date" content="2026-04-24T08:00:00+09:00">
    </head></html>
    """
    parsed = extract_published_datetime_from_html(html)
    assert parsed is not None
    assert parsed.isoformat() == "2026-04-25T08:00:00+09:00"


def test_falls_back_to_feed_date() -> None:
    chosen, status = choose_publication_datetime(None, "2026-04-25 09:00 KST", None)
    assert status == "feed"
    assert chosen is not None
    assert chosen.hour == 9


def test_old_article_filter(now) -> None:  # type: ignore[no-untyped-def]
    assert is_too_old(now - timedelta(days=8), now, 7)
    assert not is_too_old(now - timedelta(days=6, hours=23), now, 7)
