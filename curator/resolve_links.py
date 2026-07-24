from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any

import httpx

from .fetch import decode_google_news_url_online_result
from .remote_api import post_remote_action, remote_api_configured


def _discoveries(response: dict[str, Any]) -> list[dict[str, object]]:
    value = response.get("discoveries") or response.get("items") or []
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def resolve_remote_links(
    *,
    limit: int = 200,
    timeout_seconds: float = 8.0,
    max_runtime_seconds: float = 1200.0,
) -> dict[str, int]:
    worker_id = f"github:{os.environ.get('GITHUB_RUN_ID', 'local')}:{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
    if not remote_api_configured():
        return {"links_claimed": 0, "links_resolved": 0, "links_retry": 0, "links_expired": 0, "links_failed": 1}
    overall_limit = max(1, min(1000, limit))
    deadline = time.monotonic() + max(30.0, max_runtime_seconds)
    claimed = resolved = retry = expired = failed = 0
    timeout = httpx.Timeout(timeout_seconds, connect=min(5.0, timeout_seconds))
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        while claimed < overall_limit:
            remaining_seconds = deadline - time.monotonic()
            # Each claimed row must have enough time for both URL decoding and
            # its durable ACK. Never lease work that this run cannot finish.
            budget_limit = int(max(0.0, remaining_seconds - 5.0) // (timeout_seconds + 5.0))
            if budget_limit < 1:
                break
            batch_limit = min(25, overall_limit - claimed, budget_limit)
            try:
                claim = post_remote_action(
                    "claim_link_discoveries",
                    {
                        "limit": batch_limit,
                        "worker_id": worker_id,
                        "lease_seconds": 300,
                    },
                    timeout=5.0,
                )
            except Exception:
                claim = {"ok": False}
            if not claim.get("ok"):
                failed += 1
                break
            items = _discoveries(claim)
            if not items:
                break
            claimed += len(items)
            lease_token = str(claim.get("lease_token") or "")
            for item in items:
                discovery_id = str(item.get("discovery_id") or "")
                url = str(item.get("discovered_url") or "")
                attempts = int(item.get("attempt_count") or 1)
                result = decode_google_news_url_online_result(url, client)
                if result.decoded_url:
                    outcome = "resolved"
                    resolved += 1
                elif attempts >= 5:
                    outcome = "expired"
                    expired += 1
                else:
                    outcome = "retry"
                    retry += 1
                try:
                    response = post_remote_action(
                        "resolve_link_discovery",
                        {
                            "discovery_id": discovery_id,
                            "lease_token": lease_token,
                            "outcome": outcome,
                            "resolved_url": result.decoded_url,
                            "error": result.error,
                            "retry_after_seconds": (
                                3600 if not result.rate_limited else 7200
                            ),
                            "max_attempts": 5,
                        },
                        timeout=5.0,
                    )
                except Exception:
                    response = {"ok": False}
                if not response.get("ok"):
                    failed += 1
            if len(items) < batch_limit:
                break
    return {
        "links_claimed": claimed,
        "links_resolved": resolved,
        "links_retry": retry,
        "links_expired": expired,
        "links_failed": failed,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Resolve MySQL-backed Google News discovery URLs.")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--timeout", type=float, default=8.0)
    parser.add_argument("--max-runtime", type=float, default=1200.0)
    parser.add_argument("--root", default=".")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    # Root is accepted for a consistent operational CLI even though persistence
    # is remote-only.
    Path(args.root).resolve()
    summary = resolve_remote_links(
        limit=args.limit,
        timeout_seconds=args.timeout,
        max_runtime_seconds=args.max_runtime,
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 1 if summary["links_failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
