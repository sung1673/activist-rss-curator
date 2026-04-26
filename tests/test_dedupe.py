from __future__ import annotations

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
