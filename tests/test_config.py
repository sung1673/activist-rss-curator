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


def test_article_domain_exclusion_matches_subdomains() -> None:
    config = {"display": {"exclude_link_domains": ["msn.com"]}}
    assert article_domain_is_excluded({"canonical_url": "https://www.msn.com/ko-kr/news/x"}, config)
    assert not article_domain_is_excluded({"canonical_url": "https://www.mk.co.kr/news/x"}, config)
