from __future__ import annotations

from curator.priority import annotate_state_priorities, load_priority_overrides, priority_metadata

from conftest import make_article


def test_priority_scores_high_impact_articles(config, now) -> None:  # type: ignore[no-untyped-def]
    article = make_article(
        "행동주의 주주 공개서한, 이사회 교체 요구",
        "https://example.com/a",
        source="연합뉴스",
        relevance_level="high",
    )
    article["status"] = "accepted"

    metadata = priority_metadata(article, config, now)

    assert metadata["priority_level"] in {"top", "watch"}
    assert int(metadata["priority_score"]) >= 55
    assert "high_impact_term" in metadata["priority_reasons"]


def test_priority_override_can_suppress_article(config, now) -> None:  # type: ignore[no-untyped-def]
    article = make_article("주주제안 기사", "https://example.com/a", relevance_level="high")
    overrides = {"title_keywords": {"주주제안": {"suppress": True, "reasons": ["manual_suppress"]}}}

    metadata = priority_metadata(article, config, now, overrides=overrides)

    assert metadata["priority_level"] == "suppress"
    assert "manual_suppress" in metadata["priority_reasons"]


def test_annotate_state_priorities_updates_articles_and_clusters(config, now) -> None:  # type: ignore[no-untyped-def]
    article = make_article("KCGI 공개서한과 주주제안", "https://example.com/kcgi", relevance_level="high")
    record = {
        "title": article["clean_title"],
        "canonical_url": article["canonical_url"],
        "canonical_url_hash": article["canonical_url_hash"],
        "title_hash": article["title_hash"],
        "published_at": article["published_at"],
        "seen_at": now.isoformat(),
        "status": "accepted",
        "relevance_level": "high",
        "source": "연합뉴스",
    }
    state = {
        "articles": [record],
        "rejected_articles": [],
        "pending_clusters": [],
        "published_clusters": [
            {
                "status": "published",
                "cluster_key": "cluster:kcgi",
                "article_count": 2,
                "theme_group": "shareholder_proposal",
                "articles": [article],
            }
        ],
    }

    updated = annotate_state_priorities(state, config, now, {})

    assert updated >= 2
    assert state["articles"][0]["priority_score"] > 0
    assert state["articles"][0]["story_key"] == "cluster:kcgi"
    assert state["published_clusters"][0]["priority_level"] in {"top", "watch", "normal"}


def test_load_priority_overrides_handles_missing_file(tmp_path) -> None:
    assert load_priority_overrides(tmp_path / "missing.yaml") == {}
