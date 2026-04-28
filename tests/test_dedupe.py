from __future__ import annotations

from datetime import timedelta

from curator.dedupe import dedupe_articles

from conftest import make_article


def test_same_normalized_title_is_deduped(config) -> None:  # type: ignore[no-untyped-def]
    articles = [
        make_article("신한금융 밸류업 2.0 발표", "https://example.com/a"),
        make_article("신한금융 밸류업 2.0 발표 - 중앙일보", "https://example.com/b"),
    ]
    unique, duplicates = dedupe_articles(articles, {"seen_url_hashes": [], "seen_title_hashes": [], "articles": []}, config)
    assert len(unique) == 1
    assert len(duplicates) == 1
    assert duplicates[0]["duplicate_reason"] in {"same_title", "similar_title", "same_title_in_run"}


def test_duplicate_keeps_recent_reference_matches(config, now) -> None:  # type: ignore[no-untyped-def]
    previous = make_article("고려아연 소액주주, 검찰 고발", "https://example.com/old")
    state = {
        "seen_url_hashes": [],
        "seen_title_hashes": [previous["title_hash"]],
        "articles": [
            {
                "title": previous["clean_title"],
                "normalized_title": previous["normalized_title"],
                "canonical_url": previous["canonical_url"],
                "canonical_url_hash": previous["canonical_url_hash"],
                "title_hash": previous["title_hash"],
                "published_at": previous["published_at"],
                "seen_at": (now - timedelta(days=3)).isoformat(),
                "status": "accepted",
            }
        ],
    }
    article = make_article("고려아연 소액주주, 검찰 고발", "https://example.com/new")

    unique, duplicates = dedupe_articles([article], state, config, now)

    assert unique == []
    assert duplicates[0]["duplicate_matches"][0]["canonical_url"] == "https://example.com/old"
