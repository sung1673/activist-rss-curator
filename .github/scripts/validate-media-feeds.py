#!/usr/bin/env python3
"""Validate the structured CURATOR_FEEDS GitHub secret."""

from __future__ import annotations

import json
import os
from typing import Mapping
from urllib.parse import urlsplit


ALLOWED_SCOPES = {"korean_governance", "korean_governance_context"}


class ValidationError(RuntimeError):
    """A CURATOR_FEEDS configuration failure."""


def _is_http_url(value: object) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    parsed = urlsplit(value.strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def validate_feeds(raw_value: str | None) -> int:
    """Return the number of enabled feeds after strict JSON validation."""

    if raw_value is None or not raw_value.strip():
        return 0
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValidationError(
            "CURATOR_FEEDS must be a JSON array; legacy newline/comma URL lists are not supported"
        ) from exc
    if not isinstance(payload, list):
        raise ValidationError("CURATOR_FEEDS must be a JSON array")

    enabled_count = 0
    for index, item in enumerate(payload, start=1):
        if not isinstance(item, Mapping):
            raise ValidationError(f"CURATOR_FEEDS item {index} must be an object")
        enabled = item.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValidationError(f"CURATOR_FEEDS item {index} enabled must be boolean")
        if not enabled:
            continue
        enabled_count += 1
        name = item.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValidationError(f"enabled CURATOR_FEEDS item {index} requires name")
        if not _is_http_url(item.get("url")):
            raise ValidationError(f"enabled CURATOR_FEEDS item {index} requires an HTTP(S) url")
        scope = item.get("scope")
        if scope not in ALLOWED_SCOPES:
            raise ValidationError(
                f"enabled CURATOR_FEEDS item {index} requires an approved governance scope"
            )
    return enabled_count


def main() -> int:
    try:
        enabled_count = validate_feeds(os.environ.get("CURATOR_FEEDS"))
    except ValidationError as exc:
        print(f"::error::Media feed validation failed: {exc}")
        return 1
    if enabled_count:
        print(f"Media feed validation passed ({enabled_count} enabled feeds).")
    else:
        print("Media feed validation passed (no enabled environment feeds).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
