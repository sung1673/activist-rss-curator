from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path
from xml.etree import ElementTree
from zoneinfo import ZoneInfo

import pytest

from curator.governance_ui import build_governance_ui
from curator.pages_compat import write_legacy_pages_adapter


ROOT = Path(__file__).resolve().parents[1]


def published_cluster() -> dict[str, object]:
    published_at = "2026-07-16T05:45:00+09:00"
    return {
        "guid": "cluster:compatibility",
        "published_at": published_at,
        "representative_title": "상장회사 주주총회 결과 공시",
        "representative_url": "https://example.com/disclosure/1",
        "articles": [
            {
                "title": "상장회사 주주총회 결과 공시",
                "clean_title": "상장회사 주주총회 결과 공시",
                "canonical_url": "https://example.com/disclosure/1",
                "source": "공식 공시",
                "published_at": published_at,
                "relevance_level": "high",
            }
        ],
    }


def report_payload(*, rss_clusters: object) -> dict[str, object]:
    return {
        "config": {
            "timezone": "Asia/Seoul",
            "public_feed_url": "https://news.bside.ai/feed.xml",
            "publish": {"max_items_in_feed": 50},
        },
        "end_at": datetime(2026, 7, 16, 5, 45, tzinfo=ZoneInfo("Asia/Seoul")),
        "rss_clusters": rss_clusters,
        "clusters": [],
    }


def test_adapter_writes_valid_legacy_feed_and_root_redirect(tmp_path: Path) -> None:
    latest = tmp_path / "public" / "feed" / "latest.html"
    latest.parent.mkdir(parents=True)
    latest.write_text("<!doctype html><title>latest</title>", encoding="utf-8")

    paths = write_legacy_pages_adapter(report_payload(rss_clusters=[published_cluster()]), tmp_path)

    index_path = tmp_path / "public" / "index.html"
    feed_path = tmp_path / "public" / "feed.xml"
    assert paths == [index_path, feed_path]
    assert index_path.is_file()
    assert feed_path.is_file()
    index_html = index_path.read_text(encoding="utf-8")
    assert "./feed/latest.html" in index_html
    assert "./feed.xml" in index_html

    root = ElementTree.fromstring(feed_path.read_text(encoding="utf-8"))
    assert root.tag == "rss"
    channel = root.find("channel")
    assert channel is not None
    assert channel.findtext("link") == "https://news.bside.ai/feed.xml"
    assert channel.findtext("item/title") == "상장회사 주주총회 결과 공시"
    assert channel.findtext("item/link") == "https://example.com/disclosure/1"


def test_adapter_keeps_feed_valid_when_no_items_are_available(tmp_path: Path) -> None:
    write_legacy_pages_adapter(report_payload(rss_clusters=[]), tmp_path)

    root = ElementTree.parse(tmp_path / "public" / "feed.xml").getroot()
    assert root.tag == "rss"
    assert root.find("channel") is not None
    assert root.find("channel/item") is None
    assert (tmp_path / "public" / "index.html").is_file()


def test_governance_ui_step_preserves_root_compatibility_files(tmp_path: Path) -> None:
    governance_dir = tmp_path / "public" / "governance"
    governance_dir.mkdir(parents=True)
    for name in ("index.html", "app.js", "styles.css"):
        shutil.copyfile(ROOT / "public" / "governance" / name, governance_dir / name)
    write_legacy_pages_adapter(report_payload(rss_clusters=[]), tmp_path)

    build_governance_ui(tmp_path, "https://api.example.com/api/v1")

    assert (tmp_path / "public" / "index.html").is_file()
    assert ElementTree.parse(tmp_path / "public" / "feed.xml").getroot().tag == "rss"


@pytest.mark.parametrize(
    "field,value",
    [("config", None), ("end_at", "2026-07-16T05:45:00+09:00")],
)
def test_adapter_rejects_incomplete_report(field: str, value: object, tmp_path: Path) -> None:
    report = report_payload(rss_clusters=[])
    report[field] = value
    with pytest.raises(ValueError):
        write_legacy_pages_adapter(report, tmp_path)
