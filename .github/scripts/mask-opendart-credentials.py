#!/usr/bin/env python3
"""Validate OpenDART credentials and mask each key without logging it."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from curator.opendart_credentials import load_opendart_credentials  # noqa: E402


def _append_output(path: Path, *, mode: str, count: int) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as output:
        output.write(f"credential_mode={mode}\n")
        output.write(f"credential_count={count}\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()

    try:
        credentials = load_opendart_credentials()
    except Exception:
        print(
            "OpenDART credential configuration is invalid.",
            file=sys.stderr,
        )
        return 1
    if not credentials:
        print(
            "OpenDART credential configuration is missing.",
            file=sys.stderr,
        )
        return 1

    # GitHub masks a multiline secret as one value. Register each parsed key
    # separately before any collector step can render an exception or URL.
    for credential in credentials:
        print(f"::add-mask::{credential.key}")

    mode = (
        "pool"
        if os.environ.get("OPENDART_API_KEYS", "").strip()
        else "legacy"
    )
    if args.github_output is not None:
        _append_output(
            args.github_output,
            mode=mode,
            count=len(credentials),
        )
    print(
        f"OpenDART credential configuration validated "
        f"(mode={mode}, count={len(credentials)})."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
