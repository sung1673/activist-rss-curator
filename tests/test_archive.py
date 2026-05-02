from __future__ import annotations

import json

from curator.archive import archive_state, read_jsonl
from curator.priority import priority_metadata

from conftest import make_article


def test_archive_state_writes_daily_jsonl_and_index(tmp_path, config, now) -> None:  # type: ignore[no-untyped-def]
    article = make_article("행동주의 주주 공개서한", "https://example.com/a", relevance_level="high")
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
    record.update(priority_metadata(record, config, now))
    state = {"articles": [record]}
    config["archive"] = {"enabled": True, "path": "data/archive", "retention_days": 365}

    summary = archive_state(tmp_path, state, config, now)

    archive_file = tmp_path / "data" / "archive" / "articles" / "2026-04-25.jsonl"
    assert summary == {"archive_records": 1, "archive_files": 1}
    assert archive_file.exists()
    records = read_jsonl(archive_file)
    assert len(records) == 1
    assert records[0]["priority_level"] in {"top", "watch", "normal"}
    index = json.loads((tmp_path / "data" / "archive" / "index.json").read_text(encoding="utf-8"))
    assert index["total_records"] == 1
    assert index["files"][0]["date"] == "2026-04-25"


def test_archive_state_upserts_records_by_record_id(tmp_path, config, now) -> None:  # type: ignore[no-untyped-def]
    record = {
        "title": "첫 제목",
        "canonical_url": "https://example.com/a",
        "canonical_url_hash": "hash-a",
        "title_hash": "title-a",
        "published_at": now.isoformat(),
        "seen_at": now.isoformat(),
        "status": "accepted",
        "priority_score": 30,
        "priority_level": "normal",
    }
    state = {"articles": [record]}
    config["archive"] = {"enabled": True, "path": "data/archive", "retention_days": 365}

    archive_state(tmp_path, state, config, now)
    record["title"] = "수정된 제목"
    record["priority_score"] = 80
    record["priority_level"] = "top"
    archive_state(tmp_path, state, config, now)

    records = read_jsonl(tmp_path / "data" / "archive" / "articles" / "2026-04-25.jsonl")
    assert len(records) == 1
    assert records[0]["title"] == "수정된 제목"
    assert records[0]["priority_level"] == "top"
