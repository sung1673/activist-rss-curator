from __future__ import annotations

from datetime import timedelta

from curator.cluster import cluster_articles
from curator.rss_writer import build_rss, item_description, item_link, item_title, write_index

from conftest import make_article


def published_cluster(config, now):  # type: ignore[no-untyped-def]
    state = {"pending_clusters": [], "published_clusters": []}
    articles = [
        make_article("신한금융 밸류업 2.0 발표", "https://example.com/a", summary="신한금융 주주환원 확대"),
        make_article("신한금융 밸류업 2.0 주주환원 확대", "https://example.com/b", summary="신한금융 밸류업 발표"),
    ]
    cluster_articles(articles, state, config, now)
    cluster_articles([], state, config, now + timedelta(minutes=46))
    return state["published_clusters"][0]


def test_rss_item_description_contains_multiple_links(config, now) -> None:  # type: ignore[no-untyped-def]
    cluster = published_cluster(config, now)
    description = item_description(cluster, config)
    assert "https://example.com/a" in description
    assert "https://example.com/b" in description
    assert "<a href=" in description
    assert "대표 링크:" not in description
    assert "분류:" not in description
    assert "기준시각:" not in description
    rss = build_rss([cluster], config, now + timedelta(minutes=46))
    assert "<item>" in rss
    assert "<![CDATA[" in rss
    assert 'xmlns:content="http://purl.org/rss/1.0/modules/content/"' in rss
    assert "<content:encoded><![CDATA[" in rss


def test_public_feed_does_not_expose_github_pages_item_links(config, now) -> None:  # type: ignore[no-untyped-def]
    config["public_feed_url"] = "https://example.github.io/activist-rss-curator/feed.xml"
    cluster = published_cluster(config, now)
    description = item_description(cluster, config)
    rss = build_rss([cluster], config, now + timedelta(minutes=46))

    assert "example.github.io" not in description
    assert item_link(cluster, config) == "https://example.com/a"
    assert "<link>https://example.com/a</link>" in rss


def test_msn_links_are_excluded_when_direct_links_exist(config, now) -> None:  # type: ignore[no-untyped-def]
    cluster = published_cluster(config, now)
    cluster["articles"].append(
        make_article(
            "MSN 중계 기사",
            "https://www.msn.com/ko-kr/news/other/sample/ar-AA123",
            source="msn.com",
        )
    )
    cluster["article_count"] = 3

    description = item_description(cluster, config)
    assert "msn.com" not in description
    assert "MSN 중계 기사" not in description
    assert "관련 기사 2건" in description
    assert item_title(cluster, 2).startswith("[묶음 2건]")


def test_excluded_only_cluster_is_not_published(config, now) -> None:  # type: ignore[no-untyped-def]
    state = {"pending_clusters": [], "published_clusters": []}
    article = make_article(
        "MSN 단독 중계 기사",
        "https://www.msn.com/ko-kr/news/other/sample/ar-AA123",
        source="msn.com",
        relevance_level="high",
    )
    cluster_articles([article], state, config, now)
    cluster_articles([], state, config, now + timedelta(minutes=21))
    rss = build_rss(state["published_clusters"], config, now + timedelta(minutes=21))
    assert "MSN 단독 중계 기사" not in rss


def test_source_domain_is_rendered_as_label(config, now) -> None:  # type: ignore[no-untyped-def]
    state = {"pending_clusters": [], "published_clusters": []}
    article = make_article("주주환원 확대", "https://www.mk.co.kr/news/stock/1", source="mk.co.kr")
    cluster_articles([article], state, config, now)
    cluster_articles([], state, config, now + timedelta(minutes=46))
    description = item_description(state["published_clusters"][0], config)
    assert "매일경제 | 주주환원 확대" in description
    assert ">mk.co.kr |" not in description


def test_google_news_fallback_uses_source_label_instead_of_news(config) -> None:  # type: ignore[no-untyped-def]
    article = make_article(
        "주주환원 확대",
        "https://news.google.com/rss/articles/CBMiAAA",
        source="한국경제",
    )
    from curator.rss_writer import article_source_label

    assert article_source_label(article) == "한국경제"


def test_daum_article_uses_extracted_source_label(config) -> None:  # type: ignore[no-untyped-def]
    article = make_article(
        "금감원, 한화투자증권 검사",
        "https://v.daum.net/v/20260430221133810",
        source="연합뉴스TV",
    )
    from curator.rss_writer import article_source_label

    assert article_source_label(article) == "연합뉴스TV"


def test_old_low_relevance_published_cluster_is_hidden(config, now) -> None:  # type: ignore[no-untyped-def]
    cluster = published_cluster(config, now)
    cluster["representative_title"] = "로저스 커뮤니케이션, 주총서 이사 14명·KPMG 감사인 선임 승인"
    cluster["articles"] = [
        make_article(
            "로저스 커뮤니케이션, 주총서 이사 14명·KPMG 감사인 선임 승인",
            "https://www.mk.co.kr/news/stock/1200",
            source="mk.co.kr",
        )
    ]
    cluster["article_count"] = 1
    cluster["relevance_level"] = "medium"

    rss = build_rss([cluster], config, now + timedelta(minutes=46))
    assert "<item>" not in rss
    assert "로저스 커뮤니케이션" not in rss


def test_strict_company_theme_filters_mixed_published_articles(config, now) -> None:  # type: ignore[no-untyped-def]
    cluster = published_cluster(config, now)
    cluster["representative_title"] = "고려아연 소액주주·주주연대 분쟁"
    cluster["theme_group"] = "minority_shareholder"
    cluster["companies"] = ["고려아연"]
    cluster["articles"] = [
        make_article(
            "고려아연 소액주주연대, 주주제안 추진",
            "https://www.dailian.co.kr/news/view/1",
            source="dailian.co.kr",
            summary="고려아연 소액주주연대 관련 기사",
            relevance_level="high",
        ),
        make_article(
            "풍산가 부자 주총서 표 대결",
            "https://www.bloter.net/news/articleView.html?idxno=1",
            source="bloter.net",
            summary="풍산 소액주주와 경영권 분쟁 관련 기사",
            relevance_level="high",
        ),
    ]
    cluster["article_count"] = 2

    description = item_description(cluster, config)
    assert "데일리안 | 고려아연 소액주주연대, 주주제안 추진" in description
    assert "풍산가 부자" not in description
    assert "관련 기사 1건" in description


def test_description_is_capped_at_3500_chars(config, now) -> None:  # type: ignore[no-untyped-def]
    cluster = published_cluster(config, now)
    long_articles = []
    for index in range(12):
        long_articles.append(
            make_article(
                "신한금융 밸류업 " + ("긴 제목 " * 80) + str(index),
                f"https://example.com/{index}?very_long_parameter={'x' * 200}",
            )
        )
    cluster["articles"] = long_articles
    cluster["article_count"] = len(long_articles)
    assert len(item_description(cluster, config)) <= 3500


def test_stable_guid_is_reused_in_feed(config, now) -> None:  # type: ignore[no-untyped-def]
    cluster = published_cluster(config, now)
    first = build_rss([cluster], config, now + timedelta(minutes=46))
    second = build_rss([cluster], config, now + timedelta(minutes=47))
    guid = cluster["guid"]
    assert guid in first
    assert guid in second


def test_rss_channel_link_does_not_expose_source_feed(config, now) -> None:  # type: ignore[no-untyped-def]
    config["feed_url"] = "https://alerts.example.invalid/private/token"
    rss = build_rss([], config, now)
    assert "private/token" not in rss


def test_write_index_redirects_to_latest_daily(config, now, tmp_path) -> None:  # type: ignore[no-untyped-def]
    cluster = published_cluster(config, now)
    html = write_index(
        tmp_path / "index.html",
        {
            "published_clusters": [cluster],
            "pending_clusters": [],
            "articles": [],
            "rejected_articles": [],
        },
        config,
        now,
    )
    assert "주주·자본시장 데일리" in html
    assert "./feed/latest.html" in html
    assert "./feed.xml" in html
    assert "[묶음 2건]" not in html
