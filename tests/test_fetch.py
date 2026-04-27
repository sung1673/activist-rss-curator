from __future__ import annotations

from curator.fetch import (
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
