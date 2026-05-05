from __future__ import annotations

from types import SimpleNamespace

from curator.fetch import (
    apply_decoded_google_news_url,
    decode_google_news_links_in_state,
    fetch_google_alerts_articles,
    GoogleNewsDecodeResult,
    google_news_article_id,
    google_news_decoding_params,
    image_href,
    image_url_from_entry,
    parse_google_news_batch_response,
    source_from_html,
    usable_image_url,
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


def test_apply_decoded_google_news_url_replaces_canonical_link() -> None:
    article = {
        "canonical_url": "https://news.google.com/rss/articles/CBMiABC",
        "canonical_url_hash": "old",
    }
    decoded = apply_decoded_google_news_url(article, "https://www.example.com/news/1?utm_source=google")

    assert decoded["canonical_url"] == "https://www.example.com/news/1"
    assert decoded["canonical_url_hash"] != "old"


def test_daum_page_source_is_extracted_from_site_name() -> None:
    html = '<meta property="og:site_name" content="Daum | 연합뉴스TV">'

    assert source_from_html(html, "https://v.daum.net/v/20260430221133810") == "연합뉴스TV"


def test_image_href_extracts_og_image() -> None:
    html = '<meta property="og:image" content="/thumb.jpg">'

    assert image_href(html, "https://example.com/news/1") == "https://example.com/thumb.jpg"


def test_image_href_extracts_json_ld_image() -> None:
    html = '<script type="application/ld+json">{"image":{"url":"/article.jpg"}}</script>'

    assert image_href(html, "https://example.com/news/1") == "https://example.com/article.jpg"


def test_image_href_falls_back_to_large_img() -> None:
    html = '<article><img src="/body.jpg" width="640" height="360"></article>'

    assert image_href(html, "https://example.com/news/1") == "https://example.com/body.jpg"


def test_image_url_from_entry_accepts_media_thumbnail_without_type() -> None:
    entry = SimpleNamespace(media_thumbnail=[{"url": "/thumb.jpg"}])

    assert image_url_from_entry(entry, "https://example.com/news/1") == "https://example.com/thumb.jpg"


def test_usable_image_url_rejects_generic_or_pathless_images() -> None:
    assert not usable_image_url("https://img.seoul.co.kr/")
    assert not usable_image_url("https://static.mk.co.kr/2026/css/images/ic_mai_w.png")


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


def test_parallel_feed_fetch_preserves_configured_feed_order(config, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import fetch

    config["feeds"] = [
        {"name": "first", "category": "test", "url": "https://example.com/first.xml"},
        {"name": "second", "category": "test", "url": "https://example.com/second.xml"},
    ]
    config["fetch"]["enrich_pages"] = False
    config["fetch"]["feed_fetch_workers"] = 2
    config["fetch"]["max_entries_per_feed"] = 0
    xml_by_url = {
        "https://example.com/first.xml": """
        <rss><channel>
          <item><title>첫 피드 기사</title><link>https://example.com/a</link></item>
        </channel></rss>
        """,
        "https://example.com/second.xml": """
        <rss><channel>
          <item><title>둘째 피드 기사</title><link>https://example.com/b</link></item>
        </channel></rss>
        """,
    }

    monkeypatch.setattr(fetch, "fetch_feed_xml", lambda url, **_kwargs: xml_by_url[url])

    articles = fetch_google_alerts_articles(config)

    assert [article["feed_name"] for article in articles] == ["first", "second"]
    assert [article["canonical_url"] for article in articles] == ["https://example.com/a", "https://example.com/b"]


def test_parallel_enrichment_preserves_article_order(config, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import fetch

    config["feeds"] = [{"name": "test", "category": "test", "url": "https://example.com/rss.xml"}]
    config["fetch"]["feed_fetch_workers"] = 1
    config["fetch"]["enrich_workers"] = 3
    config["fetch"]["max_entries_per_feed"] = 0
    config["fetch"]["max_enrich_articles"] = 0
    config["fetch"]["google_news_decode_limit"] = 0
    xml = """
    <rss><channel>
      <item><title>첫 기사</title><link>https://example.com/a</link></item>
      <item><title>둘째 기사</title><link>https://example.com/b</link></item>
      <item><title>셋째 기사</title><link>https://example.com/c</link></item>
    </channel></rss>
    """

    monkeypatch.setattr(fetch, "fetch_feed_xml", lambda *_args, **_kwargs: xml)

    def fake_enrich(article, *_args, **_kwargs):  # type: ignore[no-untyped-def]
        enriched = dict(article)
        enriched["enriched_url"] = article["canonical_url"]
        return enriched

    monkeypatch.setattr(fetch, "enrich_article", fake_enrich)

    articles = fetch_google_alerts_articles(config)

    assert [article["enriched_url"] for article in articles] == [
        "https://example.com/a",
        "https://example.com/b",
        "https://example.com/c",
    ]


def test_google_news_decode_runs_beyond_page_enrich_limit(config, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import fetch

    config["feeds"] = [{"name": "test", "category": "test", "url": "https://example.com/rss.xml"}]
    config["fetch"]["max_entries_per_feed"] = 0
    config["fetch"]["max_enrich_articles"] = 1
    config["fetch"]["google_news_decode_limit"] = 5
    xml = """
    <rss><channel>
      <item><title>첫 기사</title><link>https://news.google.com/rss/articles/CBMiAAA?oc=5</link><pubDate>Sat, 25 Apr 2026 09:00:00 +0900</pubDate><source url="https://a.example">A뉴스</source></item>
      <item><title>둘째 기사</title><link>https://news.google.com/rss/articles/CBMiBBB?oc=5</link><pubDate>Sat, 25 Apr 2026 09:01:00 +0900</pubDate><source url="https://b.example">B뉴스</source></item>
    </channel></rss>
    """

    monkeypatch.setattr(fetch, "fetch_feed_xml", lambda *_args, **_kwargs: xml)
    monkeypatch.setattr(
        fetch,
        "decode_google_news_url_online_result",
        lambda url, _client: GoogleNewsDecodeResult(
            decoded_url="https://origin.example/" + url.rsplit("/", 1)[-1].split("?", 1)[0]
        ),
    )

    def fake_enrich(article, *_args, **_kwargs):  # type: ignore[no-untyped-def]
        enriched = dict(article)
        enriched["enriched"] = True
        return enriched

    monkeypatch.setattr(fetch, "enrich_article", fake_enrich)

    articles = fetch_google_alerts_articles(config)

    assert articles[0]["canonical_url"] == "https://origin.example/CBMiAAA"
    assert articles[0]["enriched"] is True
    assert articles[1]["canonical_url"] == "https://origin.example/CBMiBBB"
    assert "enriched" not in articles[1]


def test_state_google_news_links_are_upgraded(config, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import fetch

    state = {
        "pending_clusters": [
            {
                "articles": [
                    {
                        "canonical_url": "https://news.google.com/rss/articles/CBMiAAA?oc=5",
                        "canonical_url_hash": "old",
                    }
                ]
            }
        ],
        "published_clusters": [],
    }
    monkeypatch.setattr(
        fetch,
        "decode_google_news_url_online_result",
        lambda *_args, **_kwargs: GoogleNewsDecodeResult(decoded_url="https://origin.example/a"),
    )

    assert decode_google_news_links_in_state(state, config) == 1
    article = state["pending_clusters"][0]["articles"][0]
    assert article["canonical_url"] == "https://origin.example/a"
    assert article["canonical_url_hash"] != "old"


def test_google_news_decode_runs_even_when_page_enrichment_disabled(config, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import fetch

    config["feeds"] = [{"name": "test", "category": "test", "url": "https://example.com/rss.xml"}]
    config["fetch"]["enrich_pages"] = False
    config["fetch"]["max_entries_per_feed"] = 0
    config["fetch"]["google_news_decode_limit"] = 1
    config["fetch"]["google_news_decode_sleep_seconds"] = 0
    xml = """
    <rss><channel>
      <item><title>첫 기사</title><link>https://news.google.com/rss/articles/CBMiAAA?oc=5</link></item>
    </channel></rss>
    """

    monkeypatch.setattr(fetch, "fetch_feed_xml", lambda *_args, **_kwargs: xml)
    monkeypatch.setattr(
        fetch,
        "decode_google_news_url_online_result",
        lambda *_args, **_kwargs: GoogleNewsDecodeResult(decoded_url="https://origin.example/a"),
    )

    articles = fetch_google_alerts_articles(config)

    assert articles[0]["canonical_url"] == "https://origin.example/a"


def test_google_news_decode_stops_on_rate_limit(config, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import fetch

    config["feeds"] = [{"name": "test", "category": "test", "url": "https://example.com/rss.xml"}]
    config["fetch"]["enrich_pages"] = False
    config["fetch"]["max_entries_per_feed"] = 0
    config["fetch"]["google_news_decode_limit"] = 5
    config["fetch"]["google_news_decode_sleep_seconds"] = 0
    calls = []
    xml = """
    <rss><channel>
      <item><title>첫 기사</title><link>https://news.google.com/rss/articles/CBMiAAA?oc=5</link></item>
      <item><title>둘째 기사</title><link>https://news.google.com/rss/articles/CBMiBBB?oc=5</link></item>
    </channel></rss>
    """

    monkeypatch.setattr(fetch, "fetch_feed_xml", lambda *_args, **_kwargs: xml)

    def fake_decode(url, _client):  # type: ignore[no-untyped-def]
        calls.append(url)
        return GoogleNewsDecodeResult(rate_limited=True, error="rate_limited")

    monkeypatch.setattr(fetch, "decode_google_news_url_online_result", fake_decode)

    articles = fetch_google_alerts_articles(config)

    assert len(calls) == 1
    assert articles[0]["canonical_url"].startswith("https://news.google.com/")
