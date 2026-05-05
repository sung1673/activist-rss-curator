from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, time as datetime_time, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote_plus, urlencode, urlsplit

import httpx

from .cluster import cluster_articles
from .config import article_domain_is_excluded, configured_feeds, load_config
from .dates import choose_publication_datetime, datetime_to_iso, get_timezone, now_in_timezone, parse_datetime
from .dedupe import dedupe_articles
from .fetch import fetch_google_alerts_articles
from .main import PROJECT_ROOT, prepare_article
from .priority import annotate_state_priorities, load_priority_overrides, priority_overrides_path
from .remote_api import post_remote_action, remote_api_configured, shrink_snapshot_payload, snapshot_payload
from .state import default_state, load_state, remember_article, remember_rejected, save_state


GOOGLE_NEWS_HOST = "news.google.com"
DATE_OPERATOR_RE = re.compile(r"(?i)\b(?:when:\S+|after:\d{4}-\d{2}-\d{2}|before:\d{4}-\d{2}-\d{2})\b")
DEFAULT_PROGRESS_PATH = Path("data/backfill_progress.json")
DEFAULT_STATE_PATH = Path("data/backfill_state.json")
DEFAULT_DAYS = 180
DEFAULT_CHUNK_DAYS = 7
DEFAULT_MAX_ENTRIES_PER_FEED = 100
DEFAULT_FEED_WORKERS = 24
DEFAULT_ENRICH_WORKERS = 12
DEFAULT_MAX_ENRICH_ARTICLES = 1000
DEFAULT_GOOGLE_NEWS_DECODE_LIMIT = -1
DEFAULT_GOOGLE_NEWS_DECODE_SLEEP_SECONDS = 0.35
DEFAULT_API_TIMEOUT_SECONDS = 60.0
DEFAULT_MAX_PAYLOAD_BYTES = 1_750_000


@dataclass(frozen=True)
class QuerySpec:
    name: str
    query: str
    category: str = "backfill"
    hl: str = "ko"
    gl: str = "KR"
    ceid: str = "KR:ko"


@dataclass(frozen=True)
class DateWindow:
    start: date
    end: date

    @property
    def key(self) -> str:
        return f"{self.start.isoformat()}:{self.end.isoformat()}"


@dataclass
class BackfillTotals:
    chunks: int = 0
    feeds: int = 0
    fetched: int = 0
    accepted: int = 0
    duplicates: int = 0
    rejected: int = 0
    ignored_outside_window: int = 0
    synced_articles: int = 0
    synced_raw_records: int = 0
    synced_stories: int = 0
    failed_chunks: int = 0
    started_at_monotonic: float = field(default_factory=time.monotonic)

    def add_chunk(self, summary: dict[str, int]) -> None:
        self.chunks += 1
        self.feeds += int(summary.get("feeds") or 0)
        self.fetched += int(summary.get("fetched") or 0)
        self.accepted += int(summary.get("accepted") or 0)
        self.duplicates += int(summary.get("duplicates") or 0)
        self.rejected += int(summary.get("rejected") or 0)
        self.ignored_outside_window += int(summary.get("ignored_outside_window") or 0)
        self.synced_articles += int(summary.get("remote_articles") or 0)
        self.synced_raw_records += int(summary.get("remote_raw_records") or 0)
        self.synced_stories += int(summary.get("remote_stories") or 0)


DEFAULT_BROAD_QUERIES: tuple[QuerySpec, ...] = (
    QuerySpec(
        "backfill-kr-shareholder-activism-events",
        '(행동주의 OR "행동주의 주주" OR 주주제안 OR 공개서한 OR 위임장 OR 표대결 OR "경영권 분쟁")',
        "core",
    ),
    QuerySpec(
        "backfill-kr-minority-shareholders",
        '(소액주주 OR 소액주주연대 OR 주주행동 OR 주주권 OR 주주명부 OR 임시주총)',
        "core",
    ),
    QuerySpec(
        "backfill-kr-board-audit-governance",
        '(이사회 OR 사외이사 OR 감사위원 OR 감사선임 OR "이사회 교체" OR 지배구조 OR 거버넌스)',
        "capital_market",
    ),
    QuerySpec(
        "backfill-kr-valueup-return",
        '(밸류업 OR 벨류업 OR 주주환원 OR 자사주 OR 배당확대 OR "자사주 소각" OR "저PBR")',
        "capital_market",
    ),
    QuerySpec(
        "backfill-kr-capital-market-rules",
        '(자본시장법 OR 상법 OR 일반주주 OR 의무공개매수 OR 물적분할 OR 중복상장 OR 합병비율 OR 주식매수청구권)',
        "capital_market",
    ),
    QuerySpec(
        "backfill-kr-delisting-disclosure",
        '(상장폐지 OR 거래정지 OR 상장적격성 OR 개선기간 OR 불성실공시 OR 정정신고서 OR 감사의견거절)',
        "capital_market",
    ),
    QuerySpec(
        "backfill-kr-convertibles-dilution",
        '(전환사채 OR CB OR EB OR 리픽싱 OR 콜옵션 OR 제3자배정 OR 유상증자 OR 주주가치)',
        "capital_market",
    ),
    QuerySpec(
        "backfill-en-global-activism",
        '"shareholder activism" OR "activist investor" OR "proxy fight" OR "proxy contest" OR "board seats" OR "open letter"',
        "global",
        "en-US",
        "US",
        "US:en",
    ),
    QuerySpec(
        "backfill-en-governance-rights",
        '"corporate governance" OR "shareholder rights" OR "stewardship code" OR "say on pay" OR "universal proxy"',
        "global",
        "en-US",
        "US",
        "US:en",
    ),
    QuerySpec(
        "backfill-en-korea-market-reform",
        '"South Korea" ("Value-up Program" OR "Korea discount" OR "shareholder returns" OR "corporate governance" OR "capital market reform")',
        "global",
        "en-US",
        "US",
        "US:en",
    ),
    QuerySpec(
        "backfill-en-japan-asia-activism",
        '(Japan OR Asia) ("shareholder activism" OR "corporate governance" OR "proxy fight" OR "minority shareholders")',
        "global",
        "en-US",
        "US",
        "US:en",
    ),
)


def project_path(project_root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else project_root / path


def load_env_files(project_root: Path, names: tuple[str, ...] = (".env", ".env.local", ".env.api")) -> list[Path]:
    loaded: list[Path] = []
    for name in names:
        path = project_root / name
        if not path.exists():
            continue
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
        loaded.append(path)
    return loaded


def clean_query(value: str) -> str:
    query = DATE_OPERATOR_RE.sub(" ", value or "")
    query = re.sub(r"\s+", " ", query).strip()
    return query


def query_key(spec: QuerySpec) -> tuple[str, str, str, str]:
    return (spec.query.casefold(), spec.hl.casefold(), spec.gl.casefold(), spec.ceid.casefold())


def query_specs_from_config(config: dict[str, Any]) -> list[QuerySpec]:
    specs: list[QuerySpec] = []
    for feed in configured_feeds(config):
        parsed = urlsplit(str(feed.get("url") or ""))
        if parsed.hostname != GOOGLE_NEWS_HOST or "/rss/search" not in parsed.path:
            continue
        params = parse_qs(parsed.query)
        raw_query = (params.get("q") or [""])[0]
        query = clean_query(raw_query)
        if not query:
            continue
        specs.append(
            QuerySpec(
                name=str(feed.get("name") or f"google-news-{len(specs) + 1}"),
                query=query,
                category=str(feed.get("category") or "backfill"),
                hl=(params.get("hl") or ["ko"])[0],
                gl=(params.get("gl") or ["KR"])[0],
                ceid=(params.get("ceid") or ["KR:ko"])[0],
            )
        )
    return specs


def build_query_specs(config: dict[str, Any], *, include_defaults: bool, max_queries: int = 0) -> list[QuerySpec]:
    combined = query_specs_from_config(config)
    if include_defaults:
        combined.extend(DEFAULT_BROAD_QUERIES)

    deduped: list[QuerySpec] = []
    seen: set[tuple[str, str, str, str]] = set()
    for spec in combined:
        cleaned = clean_query(spec.query)
        if not cleaned:
            continue
        normalized = QuerySpec(spec.name, cleaned, spec.category, spec.hl, spec.gl, spec.ceid)
        key = query_key(normalized)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(normalized)
        if max_queries > 0 and len(deduped) >= max_queries:
            break
    return deduped


def google_news_search_url(spec: QuerySpec, window: DateWindow) -> str:
    query = f"{clean_query(spec.query)} after:{window.start.isoformat()} before:{window.end.isoformat()}"
    params = urlencode(
        {"q": query, "hl": spec.hl, "gl": spec.gl, "ceid": spec.ceid},
        quote_via=quote_plus,
    )
    return f"https://news.google.com/rss/search?{params}"


def feeds_for_window(specs: list[QuerySpec], window: DateWindow) -> list[dict[str, str]]:
    return [
        {
            "name": spec.name,
            "category": spec.category,
            "url": google_news_search_url(spec, window),
        }
        for spec in specs
    ]


def build_date_windows(start: date, end: date, chunk_days: int) -> list[DateWindow]:
    windows: list[DateWindow] = []
    cursor = start
    while cursor < end:
        window_end = min(cursor + timedelta(days=chunk_days), end)
        windows.append(DateWindow(cursor, window_end))
        cursor = window_end
    return windows


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def kst_now_text() -> str:
    return now_in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S KST")


def format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes:02d}m {secs:02d}s"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def log(message: str) -> None:
    print(f"[{kst_now_text()}] {message}", flush=True)


def load_progress(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"completed_windows": [], "failed_windows": [], "chunks": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"completed_windows": [], "failed_windows": [], "chunks": []}
    return data if isinstance(data, dict) else {"completed_windows": [], "failed_windows": [], "chunks": []}


def save_progress(path: Path, progress: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(progress, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def datetime_for_window_end(window: DateWindow, timezone_name: str) -> datetime:
    timezone = get_timezone(timezone_name)
    return datetime.combine(window.end, datetime_time.min, timezone) - timedelta(seconds=1)


def article_datetime_for_filter(article: dict[str, object], timezone_name: str) -> datetime | None:
    parsed, _status = choose_publication_datetime(
        article.get("article_published_at"),
        article.get("feed_published_at") or article.get("published_at"),
        article.get("feed_updated_at"),
        timezone_name,
    )
    return parsed


def article_in_window(article: dict[str, object], window: DateWindow, timezone_name: str) -> bool:
    article_dt = article_datetime_for_filter(article, timezone_name)
    if not article_dt:
        return True
    article_date = article_dt.astimezone(get_timezone(timezone_name)).date()
    return window.start <= article_date < window.end


def backfill_config(base_config: dict[str, Any], args: argparse.Namespace, feeds: list[dict[str, str]]) -> dict[str, Any]:
    config = deepcopy(base_config)
    config["feeds"] = feeds
    fetch_config = config.setdefault("fetch", {})
    fetch_config["max_entries_per_feed"] = args.max_entries_per_feed
    fetch_config["feed_fetch_workers"] = args.feed_workers
    fetch_config["enrich_workers"] = args.enrich_workers
    fetch_config["max_enrich_articles"] = args.max_enrich_articles
    fetch_config["google_news_decode_limit"] = args.google_news_decode_limit
    fetch_config["google_news_decode_sleep_seconds"] = args.google_news_decode_sleep
    fetch_config["google_news_decode_stop_on_rate_limit"] = True
    fetch_config["feed_timeout_seconds"] = args.feed_timeout
    fetch_config["page_timeout_seconds"] = args.page_timeout
    date_filter = config.setdefault("date_filter", {})
    date_filter["max_article_age_days"] = max(args.days + args.chunk_days + 30, 365)
    date_filter["exclude_before_previous_day"] = False
    state_config = config.setdefault("state", {})
    state_config["retention_days"] = max(args.days + args.chunk_days + 30, 365)
    state_config["max_articles"] = args.max_state_articles
    state_config["max_rejected_articles"] = args.max_state_articles
    cluster_config = config.setdefault("cluster", {})
    cluster_config["buffer_minutes_default"] = 0
    cluster_config["buffer_minutes_high"] = 0
    cluster_config["max_pending_hours"] = 0
    ai_config = config.setdefault("ai", {})
    if not args.enable_ai_judge:
        ai_config["enabled"] = False
        ai_config["story_judge_enabled"] = False
    return config


def remote_sync_chunk(
    chunk_state: dict[str, object],
    config: dict[str, object],
    now: datetime,
    run_summary: dict[str, int],
    args: argparse.Namespace,
) -> dict[str, int | str]:
    if args.dry_run:
        return {"remote_status": "dry_run", "remote_articles": 0, "remote_raw_records": 0, "remote_stories": 0}
    if not remote_api_configured():
        raise RuntimeError("ACTIVIST_API_URL/ACTIVIST_API_SECRET is not configured. Fill .env.api or environment variables.")
    payload = shrink_snapshot_payload(snapshot_payload(chunk_state, config, now, run_summary), max_bytes=args.max_payload_bytes)
    response = post_remote_action("upsert_snapshot", payload, timeout=args.api_timeout)
    if not response.get("ok"):
        raise RuntimeError(f"remote API failed: {response}")
    return {
        "remote_status": "ok",
        "remote_articles": int(response.get("articles") or 0),
        "remote_raw_records": int(response.get("raw_records") or 0),
        "remote_stories": int(response.get("stories") or 0),
    }


def process_window(
    window: DateWindow,
    base_config: dict[str, Any],
    query_specs: list[QuerySpec],
    dedupe_state: dict[str, object],
    args: argparse.Namespace,
) -> dict[str, int | str]:
    timezone_name = str(base_config.get("timezone") or "Asia/Seoul")
    now = datetime_for_window_end(window, timezone_name)
    feeds = feeds_for_window(query_specs, window)
    config = backfill_config(base_config, args, feeds)
    chunk_start = time.monotonic()

    fetched_articles = fetch_google_alerts_articles(config)
    publish_levels = set(config.get("publish", {}).get("publish_levels", ["high", "medium"]))  # type: ignore[union-attr]
    article_start_index = len(list(dedupe_state.get("articles") or []))

    candidates: list[dict[str, object]] = []
    rejected_count = 0
    ignored_outside_window = 0
    for raw_article in fetched_articles:
        article = prepare_article(raw_article, config)
        if args.strict_date_window and not article_in_window(article, window, timezone_name):
            ignored_outside_window += 1
            continue
        if article_domain_is_excluded(article, config):
            if args.include_rejected:
                remember_rejected(dedupe_state, article, now, "excluded_domain")
            rejected_count += 1
            continue
        if article.get("relevance_level") not in publish_levels:
            if args.include_rejected:
                remember_rejected(dedupe_state, article, now, "low_relevance")
            rejected_count += 1
            continue
        candidates.append(article)

    unique_articles, duplicates = dedupe_articles(candidates, dedupe_state, config, now)
    for duplicate in duplicates:
        remember_article(dedupe_state, duplicate, "duplicate", now, str(duplicate.get("duplicate_reason") or "duplicate"))
    for article in unique_articles:
        remember_article(dedupe_state, article, "accepted", now)

    chunk_cluster_state = default_state()
    cluster_articles(unique_articles, chunk_cluster_state, config, now)
    chunk_records = list(dedupe_state.get("articles") or [])[article_start_index:]
    chunk_state = {
        "articles": chunk_records,
        "rejected_articles": [],
        "pending_clusters": [],
        "published_clusters": list(chunk_cluster_state.get("published_clusters") or []),
        "last_run_at": datetime_to_iso(now),
    }
    overrides = load_priority_overrides(priority_overrides_path(PROJECT_ROOT, config))
    prioritized = annotate_state_priorities(chunk_state, config, now, overrides)
    dedupe_state["articles"] = list(dedupe_state.get("articles") or [])[:article_start_index] + list(chunk_state.get("articles") or [])

    run_summary = {
        "fetched": len(fetched_articles),
        "accepted": len(unique_articles),
        "duplicates": len(duplicates),
        "rejected": rejected_count,
        "ignored_outside_window": ignored_outside_window,
        "published_now": len(chunk_state.get("published_clusters") or []),
        "prioritized": prioritized,
        "feeds": len(feeds),
    }
    remote_summary = remote_sync_chunk(chunk_state, config, now, run_summary, args)
    elapsed = int(time.monotonic() - chunk_start)
    return {
        **run_summary,
        **remote_summary,
        "elapsed_seconds": elapsed,
    }


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill historical governance/capital-market news into the remote DB.")
    parser.add_argument("--days", type=positive_int, default=DEFAULT_DAYS, help="Number of days to backfill ending at --to date. Default: 180.")
    parser.add_argument("--from", dest="from_date", help="Start date, YYYY-MM-DD. Overrides --days when provided with --to.")
    parser.add_argument("--to", dest="to_date", help="End date, YYYY-MM-DD, exclusive. Default: tomorrow in Asia/Seoul.")
    parser.add_argument("--chunk-days", type=positive_int, default=DEFAULT_CHUNK_DAYS, help="Date window size per batch. Default: 7.")
    parser.add_argument("--max-queries", type=int, default=0, help="Limit query count for smoke tests. 0 means all.")
    parser.add_argument("--config-only", action="store_true", help="Use only config.yaml Google News queries, without broad default backfill queries.")
    parser.add_argument("--max-entries-per-feed", type=positive_int, default=DEFAULT_MAX_ENTRIES_PER_FEED)
    parser.add_argument("--feed-workers", type=positive_int, default=DEFAULT_FEED_WORKERS)
    parser.add_argument("--enrich-workers", type=positive_int, default=DEFAULT_ENRICH_WORKERS)
    parser.add_argument("--max-enrich-articles", type=int, default=DEFAULT_MAX_ENRICH_ARTICLES, help="0 means unlimited page enrichment per chunk.")
    parser.add_argument(
        "--google-news-decode-limit",
        type=int,
        default=DEFAULT_GOOGLE_NEWS_DECODE_LIMIT,
        help="0 disables online Google News decoding; -1 means unlimited. Default: -1.",
    )
    parser.add_argument(
        "--google-news-decode-sleep",
        type=float,
        default=DEFAULT_GOOGLE_NEWS_DECODE_SLEEP_SECONDS,
        help="Seconds to wait between Google News decode attempts. Default: 0.35.",
    )
    parser.add_argument("--feed-timeout", type=float, default=20.0)
    parser.add_argument("--page-timeout", type=float, default=6.0)
    parser.add_argument("--api-timeout", type=float, default=DEFAULT_API_TIMEOUT_SECONDS)
    parser.add_argument("--max-payload-bytes", type=positive_int, default=DEFAULT_MAX_PAYLOAD_BYTES)
    parser.add_argument("--max-state-articles", type=positive_int, default=200_000)
    parser.add_argument("--sleep", type=float, default=1.0, help="Seconds to sleep between chunks.")
    parser.add_argument("--progress-path", default=str(DEFAULT_PROGRESS_PATH))
    parser.add_argument("--state-path", default=str(DEFAULT_STATE_PATH))
    parser.add_argument("--restart", action="store_true", help="Ignore existing backfill progress/state files.")
    parser.add_argument("--dry-run", action="store_true", help="Run collection and processing without writing to remote API.")
    parser.add_argument("--include-rejected", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--strict-date-window", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--enable-ai-judge", action="store_true", help="Enable AI story judge during clustering. Off by default for speed/cost.")
    return parser


def date_range_from_args(args: argparse.Namespace, timezone_name: str) -> tuple[date, date]:
    timezone = get_timezone(timezone_name)
    if args.to_date:
        end = parse_date(args.to_date)
    else:
        end = datetime.now(timezone).date() + timedelta(days=1)
    if args.from_date:
        start = parse_date(args.from_date)
    else:
        start = end - timedelta(days=args.days)
    if start >= end:
        raise ValueError("--from date must be earlier than --to date")
    return start, end


def print_plan(
    *,
    query_specs: list[QuerySpec],
    windows: list[DateWindow],
    args: argparse.Namespace,
    completed_windows: set[str],
    env_files: list[Path],
) -> None:
    remaining = [window for window in windows if args.restart or window.key not in completed_windows]
    log("백필 계획")
    log(f"- env files: {', '.join(str(path) for path in env_files) or 'none'}")
    log(f"- 기간: {windows[0].start.isoformat()} ~ {windows[-1].end.isoformat()} exclusive")
    log(f"- chunk: {len(windows)}개 중 남은 {len(remaining)}개, chunk_days={args.chunk_days}")
    log(f"- queries: {len(query_specs)}개, 예상 RSS 요청={len(remaining) * len(query_specs):,}건")
    log(
        "- parallel: "
        f"feed_workers={args.feed_workers}, enrich_workers={args.enrich_workers}, "
        f"max_entries/feed={args.max_entries_per_feed}, max_enrich/chunk={args.max_enrich_articles}"
    )
    log(f"- remote write: {'dry-run' if args.dry_run else 'enabled'}")
    log("초기 ETA는 첫 chunk 완료 후 계산합니다.")


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    project_root = PROJECT_ROOT
    env_files = load_env_files(project_root)
    config = load_config(project_root / "config.yaml")
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    start_date, end_date = date_range_from_args(args, timezone_name)
    windows = build_date_windows(start_date, end_date, args.chunk_days)
    query_specs = build_query_specs(config, include_defaults=not args.config_only, max_queries=max(0, args.max_queries))
    if not query_specs:
        raise RuntimeError("No Google News RSS query specs were found.")
    if not args.dry_run and not remote_api_configured():
        raise RuntimeError("ACTIVIST_API_URL/ACTIVIST_API_SECRET is required. Put them in .env.api or environment variables.")

    progress_path = project_path(project_root, args.progress_path)
    state_path = project_path(project_root, args.state_path)
    progress = {"completed_windows": [], "failed_windows": [], "chunks": []} if args.restart else load_progress(progress_path)
    completed_windows = {str(value) for value in progress.get("completed_windows", [])}
    if args.restart and state_path.exists():
        state_path.unlink()
    dedupe_state = default_state() if args.restart or not state_path.exists() else load_state(state_path)
    totals = BackfillTotals()
    print_plan(query_specs=query_specs, windows=windows, args=args, completed_windows=completed_windows, env_files=env_files)

    remaining_windows = [window for window in windows if args.restart or window.key not in completed_windows]
    total_remaining = len(remaining_windows)
    for index, window in enumerate(remaining_windows, start=1):
        chunk_label = f"{window.start.isoformat()}..{window.end.isoformat()}"
        log(f"[chunk {index}/{total_remaining}] 시작: {chunk_label}, feeds={len(query_specs)}")
        try:
            summary = process_window(window, config, query_specs, dedupe_state, args)
        except (httpx.HTTPError, RuntimeError, OSError, ValueError) as exc:
            totals.failed_chunks += 1
            failure = {"window": window.key, "error": str(exc), "failed_at": datetime_to_iso(now_in_timezone(timezone_name))}
            progress.setdefault("failed_windows", []).append(failure)
            save_progress(progress_path, progress)
            save_state(state_path, dedupe_state)
            log(f"[chunk {index}/{total_remaining}] 실패: {chunk_label} error={exc}")
            if totals.failed_chunks >= 3:
                log("연속/누적 실패가 3회에 도달해 중단합니다. 문제 해결 후 같은 명령으로 재개할 수 있습니다.")
                return 2
            continue

        totals.add_chunk({key: int(value) for key, value in summary.items() if isinstance(value, int)})
        completed_windows.add(window.key)
        progress.setdefault("completed_windows", []).append(window.key)
        progress.setdefault("chunks", []).append(
            {
                "window": window.key,
                "completed_at": datetime_to_iso(now_in_timezone(timezone_name)),
                **summary,
            }
        )
        save_progress(progress_path, progress)
        save_state(state_path, dedupe_state)

        elapsed_total = time.monotonic() - totals.started_at_monotonic
        avg_per_chunk = elapsed_total / max(1, totals.chunks)
        eta = avg_per_chunk * max(0, total_remaining - index)
        log(
            f"[chunk {index}/{total_remaining}] 완료: {chunk_label} "
            f"fetched={summary.get('fetched')} accepted={summary.get('accepted')} "
            f"dup={summary.get('duplicates')} rejected={summary.get('rejected')} "
            f"outside={summary.get('ignored_outside_window')} "
            f"remote_articles={summary.get('remote_articles')} remote_stories={summary.get('remote_stories')} "
            f"chunk_elapsed={format_duration(float(summary.get('elapsed_seconds') or 0))} "
            f"total_elapsed={format_duration(elapsed_total)} eta={format_duration(eta)}"
        )
        if args.sleep > 0 and index < total_remaining:
            time.sleep(args.sleep)

    log(
        "백필 종료: "
        f"chunks={totals.chunks}, failed={totals.failed_chunks}, feeds={totals.feeds:,}, fetched={totals.fetched:,}, "
        f"accepted={totals.accepted:,}, duplicates={totals.duplicates:,}, rejected={totals.rejected:,}, "
        f"outside={totals.ignored_outside_window:,}, remote_articles={totals.synced_articles:,}, "
        f"remote_raw={totals.synced_raw_records:,}, remote_stories={totals.synced_stories:,}, "
        f"elapsed={format_duration(time.monotonic() - totals.started_at_monotonic)}"
    )
    log(f"progress={progress_path}")
    log(f"dedupe_state={state_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted. Re-run the same command to resume from the last completed chunk.", file=sys.stderr)
        raise SystemExit(130)
