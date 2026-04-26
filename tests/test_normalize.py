from __future__ import annotations

from curator.normalize import decode_google_redirect_url, normalize_title_parts, normalize_url


def test_google_redirect_url_is_unwrapped() -> None:
    url = (
        "https://www.google.com/url?sa=t&url="
        "https%3A%2F%2FExample.com%2Fnews%2F123%2F%3Futm_source%3Dgoogle%26fbclid%3Dabc%26id%3D7%23section"
    )
    assert decode_google_redirect_url(url).startswith("https://Example.com/news/123/")
    assert normalize_url(url) == "https://example.com/news/123?id=7"


def test_google_news_rss_url_is_decoded_without_network() -> None:
    url = (
        "https://news.google.com/__i/rss/rd/articles/"
        "CBMiOGh0dHBzOi8vbS5uZXdzd2F5LmNvLmtyL25ld3Mvdmlldz91ZD0yMDIyMDMwMzE1MzQzMjQ5MjYw0gEA?oc=5"
    )
    assert normalize_url(url) == "https://m.newsway.co.kr/news/view?ud=2022030315343249260"


def test_tracking_parameters_and_fragment_are_removed() -> None:
    assert (
        normalize_url("HTTPS://Example.COM/path/?utm_medium=rss&gclid=1&ref=nav&id=42#frag")
        == "https://example.com/path?id=42"
    )


def test_title_suffix_and_prefix_are_normalized() -> None:
    parts = normalize_title_parts("[단독] 행동주의 주주 제안 &amp; 공개서한 - Daum")
    assert parts["prefixes"] == ["단독"]
    assert parts["source_suffix"] == "Daum"
    assert parts["clean_title"] == "행동주의 주주 제안 & 공개서한"
    assert parts["normalized_title"] == "행동주의 주주 제안 & 공개서한"


def test_repeated_media_suffixes_are_removed() -> None:
    parts = normalize_title_parts("기업에 목소리 높였더니… 펀드 수익률 1위 - 조선비즈 - 조선비즈")
    assert parts["source_suffix"] == "조선비즈"
    assert parts["clean_title"] == "기업에 목소리 높였더니… 펀드 수익률 1위"


def test_google_alert_bold_tags_and_pipe_suffix_are_removed() -> None:
    parts = normalize_title_parts("BP, 기후 공시 없애려다 제동… <b>주주</b>들 압박 | - 임팩트온")
    assert parts["source_suffix"] == "임팩트온"
    assert parts["clean_title"] == "BP, 기후 공시 없애려다 제동… 주주들 압박"
