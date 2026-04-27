from __future__ import annotations

from datetime import timedelta

from curator.cluster import cluster_articles, enrich_article_for_clustering

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


def test_theme_articles_are_clustered_within_theme_window(config, now) -> None:  # type: ignore[no-untyped-def]
    state = {"pending_clusters": [], "published_clusters": []}
    articles = [
        make_article(
            "동성제약, 대표 교체-이사회 재편",
            "https://example.com/board-a",
            summary="동성제약 이사회 재편",
            published_at=(now - timedelta(days=4)).isoformat(),
        ),
        make_article(
            "효성중공업 임시 주총 감사위원 선임 추진",
            "https://example.com/board-b",
            summary="임시 주총에서 감사위원 겸 사외이사 선임",
            published_at=now.isoformat(),
        ),
    ]
    cluster_articles(articles, state, config, now)
    assert len(state["pending_clusters"]) == 1
    assert state["pending_clusters"][0]["article_count"] == 2
    assert state["pending_clusters"][0]["theme_grouped"] is True
    assert state["pending_clusters"][0]["representative_title"] == "이사회 재편·임시주총·감사 선임"


def test_governance_valueup_theme_does_not_join_generic_meeting_theme(config, now) -> None:  # type: ignore[no-untyped-def]
    article = make_article("주총 다 끝났는데 금융권 지배구조 개선안 난망", "https://example.com/governance")
    enriched = enrich_article_for_clustering(article)
    assert "valueup_return" in enriched["theme_groups"]
    assert "board_audit" not in enriched["theme_groups"]


def test_governance_theme_is_primary_over_generic_board_terms(config, now) -> None:  # type: ignore[no-untyped-def]
    article = make_article(
        "금융권 지배구조 개선안 발표 난망",
        "https://example.com/governance-board",
        summary="이사회 논의와 주총 일정이 함께 언급됐다",
    )
    enriched = enrich_article_for_clustering(article)
    assert enriched["theme_group"] == "valueup_return"


def test_capital_market_policy_theme_groups_related_items(config, now) -> None:  # type: ignore[no-untyped-def]
    state = {"pending_clusters": [], "published_clusters": []}
    articles = [
        make_article(
            "중복상장 심사 문턱 높인다 모회사 주주 보호 의무화",
            "https://example.com/listing",
            summary="금융위가 자본시장법 시행령을 개정한다",
            relevance_level="high",
        ),
        make_article(
            "물적분할 주주 보호 장치 강화, 주식매수청구권 논의",
            "https://example.com/spinoff",
            summary="상법과 자본시장법 개정안이 함께 거론됐다",
            relevance_level="high",
        ),
    ]
    cluster_articles(articles, state, config, now)
    assert len(state["pending_clusters"]) == 1
    assert state["pending_clusters"][0]["theme_group"] == "capital_market_policy"
    assert state["pending_clusters"][0]["representative_title"] == "정책·자본시장 제도"


def test_capital_raise_theme_requires_company_match_when_companies_exist(config, now) -> None:  # type: ignore[no-untyped-def]
    state = {"pending_clusters": [], "published_clusters": []}
    articles = [
        make_article(
            "한화솔루션 유상증자 정정신고서 제출",
            "https://example.com/hanwha",
            summary="금감원 중점 심사 이후 공시가 정정됐다",
            relevance_level="medium",
        ),
        make_article(
            "보령 유상증자 불성실공시법인 지정예고",
            "https://example.com/boryung",
            summary="다른 기업의 자본조달 공시 이슈",
            relevance_level="medium",
        ),
    ]
    cluster_articles(articles, state, config, now)
    assert len(state["pending_clusters"]) == 2


def test_minority_shareholder_theme_requires_company_match(config, now) -> None:  # type: ignore[no-untyped-def]
    state = {"pending_clusters": [], "published_clusters": []}
    articles = [
        make_article(
            "고려아연 소액주주, 사외이사 검찰 고발",
            "https://example.com/korea-zinc",
            summary="고려아연 소액주주가 이사회 의사결정 문제를 제기",
            relevance_level="high",
        ),
        make_article(
            "풍산 승계 딜레마, 주주충실 의무와 인적분할 유력",
            "https://example.com/poongsan",
            summary="풍산 소액주주와 주주충실 의무 논란",
            relevance_level="high",
        ),
    ]
    cluster_articles(articles, state, config, now)
    assert len(state["pending_clusters"]) == 2
