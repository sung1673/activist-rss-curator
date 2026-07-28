#!/usr/bin/env python3
"""Validate an empty official-backfill checkpoint artifact before a fresh start."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Mapping, Sequence


class CheckpointMetadataError(ValueError):
    """Checkpoint metadata is absent, malformed, or does not match this dispatch."""


_EXPECTED_KEYS = frozenset(
    {
        "mode",
        "source",
        "from_date",
        "to_date",
        "checkpoint_present",
    }
)


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    payload: dict[str, object] = {}
    for key, value in pairs:
        if key in payload:
            raise CheckpointMetadataError(f"metadata contains duplicate key: {key}")
        payload[key] = value
    return payload


def load_checkpoint_metadata(path: Path) -> object:
    return json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=_reject_duplicate_keys,
    )


def validate_empty_checkpoint_metadata(
    payload: object,
    *,
    mode: str,
    source: str,
    from_date: str,
    to_date: str,
) -> None:
    """Accept only the exact empty marker emitted for this workflow dispatch."""
    if not isinstance(payload, Mapping):
        raise CheckpointMetadataError("metadata must be a JSON object")
    keys = frozenset(payload)
    if keys != _EXPECTED_KEYS:
        missing = sorted(_EXPECTED_KEYS - keys)
        unexpected = sorted(keys - _EXPECTED_KEYS)
        raise CheckpointMetadataError(
            f"metadata keys do not match (missing={missing}, unexpected={unexpected})"
        )

    expected_values = {
        "mode": mode,
        "source": source,
        "from_date": from_date,
        "to_date": to_date,
    }
    for name, expected in expected_values.items():
        actual = payload[name]
        if not isinstance(actual, str) or actual != expected:
            raise CheckpointMetadataError(f"{name} does not match this dispatch")
    if payload["checkpoint_present"] is not False:
        raise CheckpointMetadataError(
            "checkpoint_present must be false when the checkpoint file is absent"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Verify that a downloaded official-backfill artifact is an exact, "
            "non-resumable marker for the current dispatch."
        )
    )
    parser.add_argument("--metadata", required=True, type=Path)
    parser.add_argument("--mode", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        payload = load_checkpoint_metadata(args.metadata)
        validate_empty_checkpoint_metadata(
            payload,
            mode=args.mode,
            source=args.source,
            from_date=args.from_date,
            to_date=args.to_date,
        )
    except (OSError, UnicodeError, json.JSONDecodeError, CheckpointMetadataError) as exc:
        print(f"::error::Unsafe empty checkpoint artifact: {exc}")
        return 1
    print(
        "Verified an exact empty checkpoint marker; "
        "the idempotent backfill may start without a local resume file."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
