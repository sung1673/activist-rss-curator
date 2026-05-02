from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .dates import datetime_to_iso, parse_datetime
from .normalize import stable_hash
from .priority import article_hash_key


ARCHIVE_VERSION = 1


def archive_config(config: dict[str, object]) -> dict[str, Any]:
    value = config.get("archive", {})
    return value if isinstance(value, dict) else {}


def archive_enabled(config: dict[str, object]) -> bool:
    return bool(archive_config(config).get("enabled", True))


def archive_root(project_root: Path, config: dict[str, object]) -> Path:
    raw_path = str(archive_config(config).get("path") or "data/archive")
    path = Path(raw_path)
    return path if path.is_absolute() else project_root / path


def archive_retention_days(config: dict[str, object]) -> int:
    try:
        return max(1, int(archive_config(config).get("retention_days", 365)))
    except (TypeError, ValueError):
        return 365


def archive_datetime(value: object, timezone_name: str) -> datetime | None:
    if not value:
        return None
    return parse_datetime(str(value), timezone_name)


def archive_date_id(record: dict[str, object], config: dict[str, object], now: datetime) -> str:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    parsed = (
        archive_datetime(record.get("seen_at"), timezone_name)
        or archive_datetime(record.get("published_at"), timezone_name)
        or now
    )
    return parsed.astimezone(now.tzinfo).strftime("%Y-%m-%d")


def archive_record_id(record: dict[str, object], date_id: str) -> str:
    base = "|".join(
        str(record.get(key) or "")
        for key in ("canonical_url_hash", "title_hash", "status", "reason", "published_at")
    )
    if not base.strip("|"):
        base = article_hash_key(record)
    return f"{date_id}:{stable_hash(base, length=20)}"


def compact_archive_record(record: dict[str, object], config: dict[str, object], now: datetime) -> dict[str, object]:
    date_id = archive_date_id(record, config, now)
    archive_id = str(record.get("record_id") or archive_record_id(record, date_id))
    keys = (
        "title",
        "normalized_title",
        "canonical_url",
        "canonical_url_hash",
        "title_hash",
        "published_at",
        "seen_at",
        "status",
        "reason",
        "summary",
        "relevance_level",
        "source",
        "image_url",
        "feed_name",
        "feed_category",
        "relevance_keywords",
        "priority_version",
        "priority_score",
        "priority_level",
        "priority_reasons",
        "priority_updated_at",
        "story_key",
    )
    archived = {
        "archive_version": ARCHIVE_VERSION,
        "record_id": archive_id,
        "archive_date": date_id,
        "updated_at": datetime_to_iso(now),
    }
    for key in keys:
        value = record.get(key)
        if value not in (None, "", []):
            archived[key] = value
    return archived


def read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    records: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            records.append(value)
    return records


def write_jsonl(path: Path, records: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    records = sorted(records, key=lambda record: (str(record.get("seen_at") or ""), str(record.get("record_id") or "")))
    with tmp_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
            handle.write("\n")
    tmp_path.replace(path)


def upsert_daily_records(path: Path, records: list[dict[str, object]]) -> int:
    by_id: dict[str, dict[str, object]] = {
        str(record.get("record_id")): record
        for record in read_jsonl(path)
        if record.get("record_id")
    }
    for record in records:
        record_id = str(record.get("record_id") or "")
        if record_id:
            by_id[record_id] = record
    write_jsonl(path, list(by_id.values()))
    return len(records)


def prune_archive(root: Path, config: dict[str, object], now: datetime) -> None:
    retention_days = archive_retention_days(config)
    cutoff = (now - timedelta(days=retention_days)).date()
    articles_dir = root / "articles"
    if not articles_dir.exists():
        return
    for path in articles_dir.glob("*.jsonl"):
        try:
            file_date = datetime.strptime(path.stem, "%Y-%m-%d").date()
        except ValueError:
            continue
        if file_date < cutoff:
            path.unlink(missing_ok=True)


def write_archive_index(root: Path, now: datetime) -> dict[str, object]:
    articles_dir = root / "articles"
    files: list[dict[str, object]] = []
    total = 0
    if articles_dir.exists():
        for path in sorted(articles_dir.glob("*.jsonl"), reverse=True):
            count = sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
            total += count
            files.append({"date": path.stem, "path": str(path.relative_to(root)).replace("\\", "/"), "records": count})
    index = {
        "archive_version": ARCHIVE_VERSION,
        "generated_at": datetime_to_iso(now),
        "total_records": total,
        "files": files,
    }
    root.mkdir(parents=True, exist_ok=True)
    (root / "index.json").write_text(json.dumps(index, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return index


def archive_state(project_root: Path, state: dict[str, object], config: dict[str, object], now: datetime) -> dict[str, int]:
    if not archive_enabled(config):
        return {"archive_records": 0, "archive_files": 0}
    root = archive_root(project_root, config)
    records_by_date: dict[str, list[dict[str, object]]] = {}
    for record in list(state.get("articles", [])):
        if not isinstance(record, dict):
            continue
        archived = compact_archive_record(record, config, now)
        date_id = str(archived["archive_date"])
        records_by_date.setdefault(date_id, []).append(archived)

    written = 0
    for date_id, records in records_by_date.items():
        written += upsert_daily_records(root / "articles" / f"{date_id}.jsonl", records)

    prune_archive(root, config, now)
    index = write_archive_index(root, now)
    return {"archive_records": written, "archive_files": len(index.get("files", []))}
