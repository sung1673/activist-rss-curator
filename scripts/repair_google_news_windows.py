from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from curator.google_news_repair import db_connect, load_local_env, repair_google_news_urls  # noqa: E402


def print_stats() -> None:
    load_local_env(PROJECT_ROOT)
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            queries = [
                ("all", ""),
                ("active", "AND status <> 'rejected'"),
                ("rejected", "AND status = 'rejected'"),
                ("active_7d", "AND status <> 'rejected' AND sort_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 7 DAY)"),
                ("active_30d", "AND status <> 'rejected' AND sort_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 DAY)"),
                ("active_180d", "AND status <> 'rejected' AND sort_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 180 DAY)"),
            ]
            print("Google News URL repair remaining")
            print("--------------------------------")
            for label, extra in queries:
                cur.execute(
                    f"""
                    SELECT COUNT(*) AS cnt
                    FROM activist_articles
                    WHERE canonical_url LIKE 'https://news.google.com/%'
                    {extra}
                    """
                )
                print(f"{label}: {cur.fetchone()['cnt']}")
            print("")
            cur.execute(
                """
                SELECT status, COUNT(*) AS cnt
                FROM activist_articles
                WHERE canonical_url LIKE 'https://news.google.com/%'
                GROUP BY status
                ORDER BY cnt DESC
                """
            )
            print("by_status:")
            for row in cur.fetchall():
                print(f"- {row['status']}: {row['cnt']}")
    finally:
        conn.close()


def build_repair_args(args: argparse.Namespace) -> SimpleNamespace:
    sleep_min = args.sleep_seconds if args.sleep_max_seconds > 0 else None
    sleep_max = args.sleep_max_seconds if args.sleep_max_seconds > 0 else None
    return SimpleNamespace(
        root=str(PROJECT_ROOT),
        limit=args.limit,
        sleep=args.sleep_seconds,
        sleep_min=sleep_min,
        sleep_max=sleep_max,
        page_timeout=args.page_timeout,
        apply=args.mode == "apply",
        include_rejected=args.include_rejected,
        mark_duplicates=args.mark_duplicates,
        update_published_at=args.update_published_at,
        state_path="",
    )


def run(args: argparse.Namespace) -> int:
    print_stats()
    if args.mode == "stats":
        return 0

    for index in range(1, args.repeat + 1):
        print("")
        print(f"[{index}/{args.repeat}] Google News repair {args.mode} started")
        stats = repair_google_news_urls(build_repair_args(args))
        print(
            "google news repair finished: "
            f"scanned={stats.scanned}, decoded={stats.decoded}, enriched={stats.enriched}, "
            f"updated={stats.updated}, state_updated={stats.state_updated}, conflicts={stats.conflicts}, "
            f"failed={stats.failed}, rate_limited={int(stats.rate_limited)}"
        )
        if stats.rate_limited:
            print("")
            print("Google News returned rate limit. Stop now and retry later with a larger sleep.")
            break
        if index < args.repeat and args.pause_minutes > 0:
            seconds = args.pause_minutes * 60
            print("")
            print(f"Waiting {args.pause_minutes} minutes before next batch...")
            time.sleep(seconds)

    print("")
    print_stats()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Windows-friendly Google News URL repair helper.")
    parser.add_argument("--mode", choices=["stats", "dry-run", "apply"], default="stats")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--sleep-seconds", type=float, default=20.0)
    parser.add_argument("--sleep-max-seconds", type=float, default=0.0)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--pause-minutes", type=int, default=30)
    parser.add_argument("--page-timeout", type=float, default=8.0)
    parser.add_argument("--include-rejected", action="store_true")
    parser.add_argument("--no-mark-duplicates", dest="mark_duplicates", action="store_false")
    parser.add_argument("--update-published-at", action="store_true")
    parser.set_defaults(mark_duplicates=True)
    return run(parser.parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
