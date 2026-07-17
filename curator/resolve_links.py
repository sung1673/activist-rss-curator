from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import httpx

from .fetch import decode_google_news_url_online_result
from .remote_api import post_remote_action, remote_api_configured


def _discoveries(response: dict[str, Any]) -> list[dict[str, object]]:
    value = response.get("discoveries") or response.get("items") or []
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def resolve_remote_links(*, limit: int = 100, timeout_seconds: float = 8.0) -> dict[str, int]:
    worker_id = f"github:{os.environ.get('GITHUB_RUN_ID', 'local')}:{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
    if not remote_api_configured():
        return {"links_claimed": 0, "links_resolved": 0, "links_retry": 0, "links_expired": 0, "links_failed": 1}
    try:
        claim = post_remote_action(
            "claim_link_discoveries",
            {"limit": max(1, min(100, limit)), "worker_id": worker_id, "lease_seconds": 300},
        )
    except Exception:
        claim = {"ok": False}
    if not claim.get("ok"):
        return {"links_claimed": 0, "links_resolved": 0, "links_retry": 0, "links_expired": 0, "links_failed": 1}

    items = _discoveries(claim)
    lease_token = str(claim.get("lease_token") or "")
    resolved = retry = expired = failed = 0
    timeout = httpx.Timeout(timeout_seconds, connect=min(5.0, timeout_seconds))
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
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
                        "retry_after_seconds": 3600 if not result.rate_limited else 7200,
                        "max_attempts": 5,
                    },
                )
            except Exception:
                response = {"ok": False}
            if not response.get("ok"):
                failed += 1
    return {
        "links_claimed": len(items),
        "links_resolved": resolved,
        "links_retry": retry,
        "links_expired": expired,
        "links_failed": failed,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Resolve MySQL-backed Google News discovery URLs.")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=float, default=8.0)
    parser.add_argument("--root", default=".")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    # Root is accepted for a consistent operational CLI even though persistence
    # is remote-only.
    Path(args.root).resolve()
    summary = resolve_remote_links(limit=args.limit, timeout_seconds=args.timeout)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 1 if summary["links_failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
