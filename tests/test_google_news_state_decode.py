from __future__ import annotations

import json
from types import SimpleNamespace

from curator import google_news_state_decode as decoder
from curator.normalize import canonical_url_hash


def test_collect_state_decode_candidates_filters_rejected_and_low(config) -> None:  # type: ignore[no-untyped-def]
    state = {
        "articles": [
            {
                "title": "행동주의 주주 기사",
                "canonical_url": "https://news.google.com/rss/articles/high",
                "status": "accepted",
                "relevance_level": "high",
                "seen_at": "2026-05-12T10:00:00+09:00",
            },
            {
                "title": "목표가 기사",
                "canonical_url": "https://news.google.com/rss/articles/low",
                "status": "accepted",
                "relevance_level": "low",
                "seen_at": "2026-05-12T11:00:00+09:00",
            },
            {
                "title": "거절 기사",
                "canonical_url": "https://news.google.com/rss/articles/rejected",
                "status": "rejected",
                "relevance_level": "high",
                "seen_at": "2026-05-12T12:00:00+09:00",
            },
        ]
    }

    candidates = decoder.collect_state_decode_candidates(state, config, limit=10)

    assert [url for url, _record in candidates] == ["https://news.google.com/rss/articles/high"]


def test_state_decode_updates_all_matching_records(tmp_path, config, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    root = tmp_path
    (root / "data").mkdir()
    (root / "config.yaml").write_text(
        """
timezone: Asia/Seoul
fetch: {}
publish:
  publish_levels:
    - high
    - medium
""".strip()
        + "\n",
        encoding="utf-8",
    )
    google_url = "https://news.google.com/rss/articles/CBMiSTATE"
    state = {
        "seen_url_hashes": [canonical_url_hash(google_url)],
        "articles": [
            {
                "title": "행동주의 주주 기사",
                "canonical_url": google_url,
                "canonical_url_hash": canonical_url_hash(google_url),
                "status": "accepted",
                "relevance_level": "high",
                "seen_at": "2026-05-12T10:00:00+09:00",
            }
        ],
        "pending_clusters": [
            {
                "representative_url": google_url,
                "articles": [
                    {
                        "title": "행동주의 주주 기사",
                        "canonical_url": google_url,
                        "canonical_url_hash": canonical_url_hash(google_url),
                        "status": "accepted",
                        "relevance_level": "high",
                    }
                ],
            }
        ],
        "published_clusters": [],
        "rejected_articles": [],
    }
    (root / "data" / "state.json").write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(
        decoder,
        "decode_google_news_url_online_result",
        lambda _url, _client: SimpleNamespace(decoded_url="https://example.com/news/1?utm_source=google", rate_limited=False),
    )

    def fake_enrich(article, _client, _config, *, decode_google_news=False):  # type: ignore[no-untyped-def]
        enriched = dict(article)
        enriched["source"] = "예시뉴스"
        enriched["image_url"] = "https://example.com/image.jpg"
        enriched["image_candidates"] = ["https://example.com/image.jpg"]
        return enriched

    monkeypatch.setattr(decoder, "enrich_article", fake_enrich)
    monkeypatch.setattr(decoder.time, "sleep", lambda _seconds: None)

    args = decoder.build_arg_parser().parse_args(
        [
            "--root",
            str(root),
            "--limit",
            "5",
            "--sleep-seconds",
            "0",
            "--max-runtime-minutes",
            "20",
        ]
    )
    stats = decoder.decode_state_google_news_urls(args)

    saved = json.loads((root / "data" / "state.json").read_text(encoding="utf-8"))
    article = saved["articles"][0]
    cluster_article = saved["pending_clusters"][0]["articles"][0]
    assert stats.decoded == 1
    assert stats.updated_records == 2
    assert article["canonical_url"] == "https://example.com/news/1"
    assert article["google_news_url"] == google_url
    assert article["source"] == "예시뉴스"
    assert cluster_article["canonical_url"] == "https://example.com/news/1"
    assert saved["pending_clusters"][0]["representative_url"] == "https://example.com/news/1"
    assert saved["seen_url_hashes"] == [canonical_url_hash("https://example.com/news/1")]


def test_state_decode_stops_on_rate_limit(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    root = tmp_path
    (root / "data").mkdir()
    (root / "config.yaml").write_text("timezone: Asia/Seoul\n", encoding="utf-8")
    state = {
        "articles": [
            {
                "title": "행동주의 주주 기사",
                "canonical_url": "https://news.google.com/rss/articles/CBMiRATE",
                "status": "accepted",
                "relevance_level": "high",
            }
        ],
        "pending_clusters": [],
        "published_clusters": [],
        "rejected_articles": [],
    }
    (root / "data" / "state.json").write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(
        decoder,
        "decode_google_news_url_online_result",
        lambda _url, _client: SimpleNamespace(decoded_url=None, rate_limited=True),
    )

    args = decoder.build_arg_parser().parse_args(
        ["--root", str(root), "--limit", "5", "--sleep-seconds", "0"]
    )
    stats = decoder.decode_state_google_news_urls(args)

    assert stats.attempted == 1
    assert stats.rate_limited
    assert stats.decoded == 0
