from __future__ import annotations

from pathlib import Path

from curator.config import article_domain_is_excluded, configured_feeds
from curator.config import load_config


def test_configured_feeds_supports_legacy_feed_url() -> None:
    feeds = configured_feeds({"feed_url": "https://example.com/feed.xml"})
    assert feeds == [{"name": "google-alert", "url": "https://example.com/feed.xml", "category": ""}]


def test_configured_feeds_prefers_feed_list() -> None:
    feeds = configured_feeds(
        {
            "feed_url": "https://example.com/legacy.xml",
            "feeds": [
                {"name": "governance", "category": "medium", "url": "https://example.com/a.xml"},
                "https://example.com/b.xml",
            ],
        }
    )
    assert feeds[0] == {"name": "governance", "category": "medium", "url": "https://example.com/a.xml"}
    assert feeds[1] == {"name": "feed-2", "category": "", "url": "https://example.com/b.xml"}


def test_configured_feeds_enforces_enabled_and_scope_policy() -> None:
    feeds = configured_feeds(
        {
            "media_feed_policy": {
                "enforce": True,
                "default_scope": "unclassified",
                "allowed_scopes": ["korean_governance"],
                "category_scopes": {"core": "korean_governance"},
                "feed_scopes": {"named-context": "korean_governance"},
            },
            "feeds": [
                {"name": "core-feed", "category": "core", "url": "https://example.com/core.xml"},
                {"name": "named-context", "category": "global", "url": "https://example.com/context.xml"},
                {
                    "name": "explicitly-disabled",
                    "category": "core",
                    "enabled": False,
                    "url": "https://example.com/disabled.xml",
                },
                {
                    "name": "explicit-broad-override",
                    "category": "core",
                    "scope": "broad_market",
                    "url": "https://example.com/broad.xml",
                },
                {"name": "unclassified", "category": "macro", "url": "https://example.com/macro.xml"},
            ],
        }
    )

    assert feeds == [
        {
            "name": "core-feed",
            "category": "core",
            "url": "https://example.com/core.xml",
            "scope": "korean_governance",
        },
        {
            "name": "named-context",
            "category": "global",
            "url": "https://example.com/context.xml",
            "scope": "korean_governance",
        },
    ]


def test_disabled_feed_is_ignored_without_scope_policy() -> None:
    assert configured_feeds(
        {
            "feeds": [
                {"name": "on", "url": "https://example.com/on.xml"},
                {"name": "off", "url": "https://example.com/off.xml", "enabled": "false"},
            ]
        }
    ) == [{"name": "on", "url": "https://example.com/on.xml", "category": ""}]


def test_load_config_supports_secret_feed_env(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CURATOR_FEEDS", "https://example.com/a.xml\nhttps://example.com/b.xml")
    config = load_config(tmp_path / "missing.yaml")
    assert configured_feeds(config) == [
        {"name": "env-feed-1", "category": "env", "url": "https://example.com/a.xml"},
        {"name": "env-feed-2", "category": "env", "url": "https://example.com/b.xml"},
    ]


def test_load_config_merges_secret_feeds_with_supplemental_feeds(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
feeds:
  - name: supplemental
    category: public
    url: https://example.com/supplemental.xml
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("CURATOR_FEEDS", "https://example.com/private.xml")

    feeds = configured_feeds(load_config(config_path))

    assert feeds == [
        {"name": "env-feed-1", "category": "env", "url": "https://example.com/private.xml"},
        {"name": "supplemental", "category": "public", "url": "https://example.com/supplemental.xml"},
    ]


def test_scoped_json_secret_feed_is_allowed_by_production_policy(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
media_feed_policy:
  enforce: true
  default_scope: unclassified
  allowed_scopes: [korean_governance]
""",
        encoding="utf-8",
    )
    monkeypatch.setenv(
        "CURATOR_FEEDS",
        '[{"name":"licensed-discovery","category":"private","scope":"korean_governance",'
        '"url":"https://example.com/private.xml"}]',
    )

    assert configured_feeds(load_config(config_path)) == [
        {
            "name": "licensed-discovery",
            "category": "private",
            "scope": "korean_governance",
            "url": "https://example.com/private.xml",
        }
    ]


def test_unscoped_legacy_secret_feed_fails_closed_under_production_policy(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
media_feed_policy:
  enforce: true
  default_scope: unclassified
  allowed_scopes: [korean_governance]
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("CURATOR_FEEDS", "https://example.com/unclassified.xml")

    assert configured_feeds(load_config(config_path)) == []


def test_repository_media_inventory_excludes_broad_topics_from_network_fetch(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.delenv("CURATOR_FEEDS", raising=False)
    monkeypatch.delenv("CURATOR_FEED_URL", raising=False)
    config = load_config(Path(__file__).resolve().parents[1] / "config.yaml")
    feeds = configured_feeds(config)
    names = {feed["name"] for feed in feeds}

    assert len(feeds) == 69
    assert {feed["scope"] for feed in feeds} <= {"korean_governance", "korean_governance_context"}
    assert "google-news-행동주의" in names
    assert "google-news-en-korea-governance-activism" in names
    assert "google-news-공개매수 일반주주 경영권" in names
    assert "google-news-주니어 ISA 법안 자본시장 정책" not in names
    assert "google-news-STO 제도화 자본시장" not in names
    assert "google-news-증권사 IB 기능 강화" not in names
    assert "google-news-en-activist-proxy-fight" not in names
    assert "google-news-en-japan-activism-governance" not in names


def test_article_domain_exclusion_matches_subdomains() -> None:
    config = {"display": {"exclude_link_domains": ["msn.com"]}}
    assert article_domain_is_excluded({"canonical_url": "https://www.msn.com/ko-kr/news/x"}, config)
    assert not article_domain_is_excluded({"canonical_url": "https://www.mk.co.kr/news/x"}, config)
