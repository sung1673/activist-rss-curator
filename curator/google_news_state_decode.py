from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import httpx

from .config import load_config
from .dates import datetime_to_iso, now_in_timezone, parse_datetime
from .fetch import (
    USER_AGENT,
    apply_decoded_google_news_url,
    decode_google_news_url_online_result,
    enrich_article,
    google_news_article_id,
)
from .main import PROJECT_ROOT
from .normalize import normalize_url
from .relevance import relevance_details
from .remote_api import sync_state_to_remote_api
from .state import load_state, save_state


@dataclass
class StateGoogleNewsDecodeStats:
    candidates: int = 0
    attempted: int = 0
    decoded: int = 0
    enriched: int = 0
    updated_records: int = 0
    failed: int = 0
    skipped: int = 0
    rate_limited: bool = False
    timed_out: bool = False
    remote_synced: int = 0
    remote_failed: int = 0


def state_record_containers(state: dict[str, object]) -> Iterable[list[dict[str, object]]]:
    for key in ("articles", "rejected_articles"):
        records = state.get(key, [])
        if isinstance(records, list):
            yield [record for record in records if isinstance(record, dict)]
    for key in ("pending_clusters", "published_clusters"):
        clusters = state.get(key, [])
        if not isinstance(clusters, list):
            continue
        for cluster in clusters:
            if isinstance(cluster, dict) and isinstance(cluster.get("articles"), list):
                yield [record for record in cluster["articles"] if isinstance(record, dict)]  # type: ignore[index]


def state_records(state: dict[str, object]) -> Iterable[dict[str, object]]:
    for records in state_record_containers(state):
        yield from records


def state_record_url(record: dict[str, object]) -> str:
    return str(record.get("canonical_url") or record.get("link") or "")


def record_sort_datetime(record: dict[str, object], timezone_name: str) -> datetime | None:
    for key in ("seen_at", "published_at", "article_published_at", "feed_published_at", "updated_at"):
        parsed = parse_datetime(record.get(key), timezone_name)
        if parsed:
            return parsed
    return None


def record_relevance_level(record: dict[str, object]) -> str:
    explicit = str(record.get("relevance_level") or "").strip().lower()
    if explicit:
        return explicit
    details = relevance_details(
        str(record.get("clean_title") or record.get("title") or ""),
        str(record.get("summary") or ""),
    )
    return str(details.get("level") or "").strip().lower()


def should_decode_record(
    record: dict[str, object],
    config: dict[str, object],
    *,
    include_rejected: bool,
    publish_levels_only: bool,
) -> bool:
    url = state_record_url(record)
    if not google_news_article_id(url):
        return False
    if not include_rejected and str(record.get("status") or "").casefold() == "rejected":
        return False
    if publish_levels_only:
        publish_levels = {
            str(level).casefold()
            for level in config.get("publish", {}).get("publish_levels", ["high", "medium"])  # type: ignore[union-attr]
        }
        if record_relevance_level(record) not in publish_levels:
            return False
    return True


def collect_state_decode_candidates(
    state: dict[str, object],
    config: dict[str, object],
    *,
    limit: int,
    include_rejected: bool = False,
    publish_levels_only: bool = True,
) -> list[tuple[str, dict[str, object]]]:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    by_url: dict[str, tuple[str, dict[str, object]]] = {}
    for record in state_records(state):
        if not should_decode_record(
            record,
            config,
            include_rejected=include_rejected,
            publish_levels_only=publish_levels_only,
        ):
            continue
        url = state_record_url(record)
        key = normalize_url(url)
        if key and key not in by_url:
            by_url[key] = (url, record)

    candidates = sorted(
        by_url.values(),
        key=lambda item: record_sort_datetime(item[1], timezone_name) or datetime.min,
        reverse=True,
    )
    if limit > 0:
        return candidates[:limit]
    return candidates


def update_state_records_for_url(state: dict[str, object], old_url: str, repaired: dict[str, object]) -> int:
    old_key = normalize_url(old_url)
    update_keys = (
        "canonical_url",
        "canonical_url_hash",
        "google_news_url",
        "google_news_decoded",
        "source",
        "image_url",
        "image_candidates",
        "article_published_at",
    )
    updated = 0
    for record in state_records(state):
        if normalize_url(state_record_url(record)) != old_key:
            continue
        for key in update_keys:
            value = repaired.get(key)
            if value not in (None, "", []):
                record[key] = value
        updated += 1

    for cluster in list(state.get("pending_clusters") or []) + list(state.get("published_clusters") or []):
        if not isinstance(cluster, dict):
            continue
        if normalize_url(str(cluster.get("representative_url") or "")) == old_key:
            cluster["representative_url"] = repaired.get("canonical_url") or cluster.get("representative_url")
    return updated


def refresh_seen_url_hashes(state: dict[str, object]) -> None:
    articles = [record for record in state.get("articles", []) if isinstance(record, dict)]
    state["seen_url_hashes"] = sorted(
        {str(record.get("canonical_url_hash")) for record in articles if record.get("canonical_url_hash")}
    )


def enough_runtime_remaining(started_at: float, max_runtime_seconds: float) -> bool:
    if max_runtime_seconds <= 0:
        return True
    return time.monotonic() - started_at < max_runtime_seconds


def decode_state_google_news_urls(args: argparse.Namespace) -> StateGoogleNewsDecodeStats:
    root = Path(args.root).resolve()
    config = load_config(root / "config.yaml")
    state_path = root / args.state_path
    state = load_state(state_path)
    stats = StateGoogleNewsDecodeStats()
    candidates = collect_state_decode_candidates(
        state,
        config,
        limit=int(args.limit),
        include_rejected=bool(args.include_rejected),
        publish_levels_only=not bool(args.include_low_relevance),
    )
    stats.candidates = len(candidates)
    started_at = time.monotonic()
    page_timeout = float(args.page_timeout)
    timeout = httpx.Timeout(page_timeout, connect=min(5.0, page_timeout))
    headers = {"User-Agent": USER_AGENT}

    with httpx.Client(timeout=timeout, headers=headers, follow_redirects=True) as client:
        for old_url, record in candidates:
            if not enough_runtime_remaining(started_at, float(args.max_runtime_minutes) * 60.0):
                stats.timed_out = True
                break
            stats.attempted += 1
            result = decode_google_news_url_online_result(old_url, client)
            if result.rate_limited:
                stats.rate_limited = True
                break
            if not result.decoded_url or google_news_article_id(result.decoded_url):
                stats.failed += 1
            else:
                repaired = apply_decoded_google_news_url(dict(record), result.decoded_url)
                if bool(args.enrich):
                    enriched = enrich_article(repaired, client, config, decode_google_news=False)
                    if enriched.get("canonical_url") and not google_news_article_id(str(enriched.get("canonical_url") or "")):
                        repaired = enriched
                        stats.enriched += 1
                changed = update_state_records_for_url(state, old_url, repaired)
                if changed:
                    stats.decoded += 1
                    stats.updated_records += changed
                else:
                    stats.skipped += 1
            if (
                float(args.sleep_seconds) > 0
                and stats.attempted < len(candidates)
                and enough_runtime_remaining(started_at, float(args.max_runtime_minutes) * 60.0)
            ):
                time.sleep(float(args.sleep_seconds))

    if stats.updated_records and not bool(args.dry_run):
        refresh_seen_url_hashes(state)
        state["last_google_news_decode_at"] = datetime_to_iso(now_in_timezone(str(config.get("timezone") or "Asia/Seoul")))
        save_state(state_path, state)
        if bool(args.sync_remote):
            remote_summary = sync_state_to_remote_api(
                state,
                config,
                now_in_timezone(str(config.get("timezone") or "Asia/Seoul")),
                {
                    "google_news_decode_candidates": stats.candidates,
                    "google_news_decode_attempted": stats.attempted,
                    "google_news_decoded": stats.decoded,
                    "google_news_decode_updated_records": stats.updated_records,
                    "google_news_decode_failed": stats.failed,
                    "google_news_decode_rate_limited": int(stats.rate_limited),
                    "google_news_decode_timed_out": int(stats.timed_out),
                },
            )
            stats.remote_synced = int(remote_summary.get("remote_articles", 0) or 0)
            stats.remote_failed = int(remote_summary.get("remote_failed", 0) or 0)
    return stats


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Decode Google News URLs stored in local state.json.")
    parser.add_argument("--root", default=str(PROJECT_ROOT))
    parser.add_argument("--state-path", default="data/state.json")
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--sleep-seconds", type=float, default=17.0)
    parser.add_argument("--max-runtime-minutes", type=float, default=20.0)
    parser.add_argument("--page-timeout", type=float, default=8.0)
    parser.add_argument("--include-rejected", action="store_true")
    parser.add_argument("--include-low-relevance", action="store_true")
    parser.add_argument("--no-enrich", dest="enrich", action="store_false")
    parser.set_defaults(enrich=True)
    parser.add_argument("--sync-remote", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    stats = decode_state_google_news_urls(args)
    print(
        "google news state decode finished: "
        f"candidates={stats.candidates}, attempted={stats.attempted}, decoded={stats.decoded}, "
        f"enriched={stats.enriched}, updated_records={stats.updated_records}, failed={stats.failed}, "
        f"skipped={stats.skipped}, rate_limited={int(stats.rate_limited)}, timed_out={int(stats.timed_out)}, "
        f"remote_synced={stats.remote_synced}, remote_failed={stats.remote_failed}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
