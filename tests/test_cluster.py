from __future__ import annotations

from datetime import timedelta

from curator.cluster import cluster_articles

from conftest import make_article


def test_similar_titles_are_clustered(config, now) -> None:  # type: ignore[no-untyped-def]
    state = {"pending_clusters": [], "published_clusters": []}
    articles = [
        make_article("신한금융 밸류업 2.0 발표", "https://example.com/a", summary="신한금융 주주환원 확대"),
        make_article("신한금융 밸류업 2.0 주주환원 확대", "https://example.com/b", summary="신한금융 밸류업 발표"),
    ]
    cluster_articles(articles, state, config, now)
    assert len(state["pending_clusters"]) == 1
    assert state["pending_clusters"][0]["article_count"] == 2


def test_pending_cluster_is_not_published_before_buffer(config, now) -> None:  # type: ignore[no-untyped-def]
    state = {"pending_clusters": [], "published_clusters": []}
    cluster_articles([make_article("신한금융 밸류업 발표", "https://example.com/a")], state, config, now)
    cluster_articles([], state, config, now + timedelta(minutes=44))
    assert len(state["pending_clusters"]) == 1
    assert state["published_clusters"] == []


def test_pending_cluster_is_published_after_buffer(config, now) -> None:  # type: ignore[no-untyped-def]
    state = {"pending_clusters": [], "published_clusters": []}
    cluster_articles([make_article("신한금융 밸류업 발표", "https://example.com/a")], state, config, now)
    published = cluster_articles([], state, config, now + timedelta(minutes=46))
    assert len(published) == 1
    assert state["pending_clusters"] == []
    assert state["published_clusters"][0]["guid"].startswith("cluster:")


def test_cluster_does_not_store_source_feed_url(config, now) -> None:  # type: ignore[no-untyped-def]
    state = {"pending_clusters": [], "published_clusters": []}
    article = make_article("신한금융 밸류업 발표", "https://example.com/a")
    article["source_feed_url"] = "https://alerts.example.invalid/private/token"
    cluster_articles([article], state, config, now)
    stored_article = state["pending_clusters"][0]["articles"][0]
    assert "source_feed_url" not in stored_article
