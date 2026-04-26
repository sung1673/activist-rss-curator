from __future__ import annotations

from datetime import timedelta

from curator.cluster import cluster_articles
from curator.rss_writer import build_rss, item_description

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
    rss = build_rss([cluster], config, now + timedelta(minutes=46))
    assert "<item>" in rss
    assert "<![CDATA[" in rss


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
