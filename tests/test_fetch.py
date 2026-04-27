from __future__ import annotations

from curator.fetch import (
    fetch_google_alerts_articles,
    google_news_article_id,
    google_news_decoding_params,
    parse_google_news_batch_response,
)


def test_google_news_article_id_extracts_rss_article_id() -> None:
    url = "https://news.google.com/rss/articles/CBMiABC123?oc=5"
    assert google_news_article_id(url) == "CBMiABC123"


def test_google_news_decoding_params_extracts_signature_and_timestamp() -> None:
    html = '<html><body><c-wiz><div jscontroller="x" data-n-a-sg="sig" data-n-a-ts="123"></div></c-wiz></body></html>'
    assert google_news_decoding_params(html) == ("sig", "123")


def test_parse_google_news_batch_response_extracts_decoded_url() -> None:
    response_text = """)]}'

[[\"wrb.fr\",\"Fbv4je\",\"[\\\"garturlres\\\",\\\"https://example.com/news/1\\\",1]\",null,null,null,\"\"]]"""
    assert parse_google_news_batch_response(response_text) == "https://example.com/news/1"


def test_fetch_respects_max_enrich_articles(config, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import fetch

    config["feeds"] = [{"name": "test", "category": "test", "url": "https://example.com/rss.xml"}]
    config["fetch"]["max_entries_per_feed"] = 0
    config["fetch"]["max_enrich_articles"] = 1
    xml = """
    <rss><channel>
      <item><title>신한금융 밸류업</title><link>https://example.com/a</link><pubDate>Sat, 25 Apr 2026 09:00:00 +0900</pubDate></item>
      <item><title>KB금융 주주환원</title><link>https://example.com/b</link><pubDate>Sat, 25 Apr 2026 09:01:00 +0900</pubDate></item>
    </channel></rss>
    """
    calls = []

    monkeypatch.setattr(fetch, "fetch_feed_xml", lambda *_args, **_kwargs: xml)

    def fake_enrich(article, *_args, **_kwargs):  # type: ignore[no-untyped-def]
        calls.append(article["canonical_url"])
        enriched = dict(article)
        enriched["enriched"] = True
        return enriched

    monkeypatch.setattr(fetch, "enrich_article", fake_enrich)

    articles = fetch_google_alerts_articles(config)

    assert len(articles) == 2
    assert calls == ["https://example.com/a"]
    assert articles[0]["enriched"] is True
    assert "enriched" not in articles[1]
