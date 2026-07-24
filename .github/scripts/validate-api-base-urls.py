#!/usr/bin/env python3
"""Fail closed when privileged and public governance API routes diverge."""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import Sequence
from urllib.parse import SplitResult, urlsplit, urlunsplit


class ApiRoutingError(ValueError):
    """An operational API URL is absent, unsafe, or inconsistent."""


_DNS_HOST_RE = re.compile(
    r"(?=.{1,253}\Z)"
    r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?"
)


def canonical_api_base(value: str, *, name: str) -> str:
    candidate = value.strip()
    if not candidate:
        raise ApiRoutingError(f"{name} is required")
    if (
        any(ord(character) <= 32 or ord(character) == 127 for character in candidate)
        or "\\" in candidate
        or "%" in candidate
    ):
        raise ApiRoutingError(f"{name} contains an unsafe URL character")

    try:
        parsed = urlsplit(candidate)
        port = parsed.port
    except ValueError as exc:
        raise ApiRoutingError(f"{name} is not a valid URL") from exc
    if (
        parsed.scheme.lower() != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ApiRoutingError(
            f"{name} must be credential-free, query-free, fragment-free HTTPS"
        )
    hostname = parsed.hostname.lower()
    if _DNS_HOST_RE.fullmatch(hostname) is None or port not in (None, 443):
        raise ApiRoutingError(f"{name} must use a canonical DNS host on HTTPS port 443")

    path = parsed.path.rstrip("/")
    segments = path.split("/")
    if (
        not path.startswith("/")
        or not path.endswith("/api/v1")
        or "//" in path
        or any(segment in {".", ".."} for segment in segments)
    ):
        raise ApiRoutingError(f"{name} must end with /api/v1")

    return urlunsplit(SplitResult("https", hostname, path, "", ""))


def validate_api_routing(*, privileged: str, public: str) -> str:
    privileged_base = canonical_api_base(
        privileged,
        name="BSIDE_API_BASE_URL",
    )
    public_base = canonical_api_base(
        public,
        name="GOVERNANCE_API_BASE_URL",
    )
    if privileged_base != public_base:
        raise ApiRoutingError("operational API base URL mismatch")
    return privileged_base


def _write_github_env(path: Path, canonical_base: str) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(f"BSIDE_API_BASE_URL={canonical_base}\n")
        stream.write(f"GOVERNANCE_API_BASE_URL={canonical_base}\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate and canonicalize the privileged/public governance API binding."
    )
    parser.add_argument(
        "--github-env",
        type=Path,
        help="append canonical URL values to this GitHub Actions environment file",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        canonical_base = validate_api_routing(
            privileged=os.environ.get("BSIDE_API_BASE_URL", ""),
            public=os.environ.get("GOVERNANCE_API_BASE_URL", ""),
        )
    except ApiRoutingError as exc:
        print(f"::error::{exc}")
        return 1
    if args.github_env is not None:
        _write_github_env(args.github_env, canonical_base)
    print("Operational API routing verified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
