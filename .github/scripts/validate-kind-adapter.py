#!/usr/bin/env python3
"""Fail-closed preflight for the deploy-configured KIND JSON adapter.

The validator deliberately avoids including the endpoint, API key, exception
text, or response body in its output.  Those values may contain credentials or
third-party data and do not belong in a public GitHub Actions log.
"""

from __future__ import annotations

import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Mapping
from urllib.parse import urlsplit

import httpx


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from curator.official_sources import (  # noqa: E402
    OfficialSourceError,
    normalize_kind_datetime,
    parse_kind_list_payload,
)


RECEIPT_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,180}")
CORP_CODE_PATTERN = re.compile(r"\d{8}")
REQUEST_PAGE_SIZE = 10
LOOKBACK_DAYS = 7


class ValidationError(RuntimeError):
    """A log-safe KIND adapter validation failure."""


def _first_text(row: Mapping[str, object], fields: tuple[str, ...]) -> str:
    for field in fields:
        value = row.get(field)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def validate_endpoint(endpoint: str) -> None:
    try:
        parsed = urlsplit(endpoint)
    except ValueError as exc:
        raise ValidationError("KIND_DISCLOSURE_ENDPOINT must be an HTTP(S) URL") from exc
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValidationError("KIND_DISCLOSURE_ENDPOINT must be an HTTP(S) URL")


def validate_payload(payload: object) -> int:
    """Validate one requested page and return its accepted row count."""

    try:
        rows, page, total_pages = parse_kind_list_payload(payload)
    except (OfficialSourceError, TypeError, ValueError) as exc:
        raise ValidationError(
            "response does not satisfy the KIND list and pagination contract"
        ) from exc

    if (page, total_pages) == (0, 0):
        # parse_kind_list_payload only produces this sentinel for an explicit
        # no-data response.  It is valid for the requested first page.
        return 0
    if page != 1:
        raise ValidationError("KIND adapter did not return the requested first page")
    if not rows:
        raise ValidationError("KIND adapter returned an empty success page without no-data status")

    for index, row in enumerate(rows, start=1):
        title = _first_text(row, ("title", "report_nm", "disclosure_title"))
        receipt = _first_text(row, ("acptno", "receipt_no", "rcept_no"))
        corp_code = _first_text(row, ("corp_code", "dart_corp_code"))
        corp_name = _first_text(row, ("corp_name", "company_name"))
        received_at = _first_text(row, ("received_at", "rcept_dt", "date"))
        if not title:
            raise ValidationError(f"KIND row {index} is missing a title")
        if RECEIPT_PATTERN.fullmatch(receipt) is None:
            raise ValidationError(f"KIND row {index} is missing a stable receipt number")
        if CORP_CODE_PATTERN.fullmatch(corp_code) is None:
            raise ValidationError(f"KIND row {index} is missing an 8-digit DART corp_code")
        if not corp_name:
            raise ValidationError(f"KIND row {index} is missing corp_name")
        if not received_at:
            raise ValidationError(f"KIND row {index} is missing received_at")
        try:
            normalize_kind_datetime(received_at)
        except ValueError as exc:
            raise ValidationError(f"KIND row {index} has invalid received_at") from exc
    return len(rows)


def request_and_validate(
    endpoint: str,
    *,
    api_key: str = "",
    end_date: date | None = None,
    client: httpx.Client | None = None,
) -> int:
    """GET and validate page one without ever logging request or response data."""

    validate_endpoint(endpoint)
    effective_end = end_date or date.today()
    params: dict[str, str | int] = {
        "start_date": (effective_end - timedelta(days=LOOKBACK_DAYS)).isoformat(),
        "end_date": effective_end.isoformat(),
        "page": 1,
        "page_size": REQUEST_PAGE_SIZE,
    }
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else None

    try:
        if client is None:
            response = httpx.get(
                endpoint,
                params=params,
                headers=headers,
                timeout=20.0,
                follow_redirects=True,
            )
        else:
            response = client.get(endpoint, params=params, headers=headers)
    except httpx.HTTPError as exc:
        raise ValidationError("KIND adapter request failed") from exc
    if not response.is_success:
        raise ValidationError("KIND adapter returned an unsuccessful HTTP status")
    try:
        payload = response.json()
    except ValueError as exc:
        raise ValidationError("KIND adapter response is not valid JSON") from exc
    return validate_payload(payload)


def main() -> int:
    endpoint = os.environ.get("KIND_DISCLOSURE_ENDPOINT", "").strip()
    api_key = os.environ.get("KIND_API_KEY", "").strip()
    if not endpoint:
        print("::error::KIND adapter validation failed: KIND_DISCLOSURE_ENDPOINT is required")
        return 1
    try:
        rows = request_and_validate(endpoint, api_key=api_key)
    except ValidationError as exc:
        print(f"::error::KIND adapter validation failed: {exc}")
        return 1
    except Exception:
        # Keep even unexpected client/runtime exception text out of the public
        # log because it can embed a request URL or response fragment.
        print("::error::KIND adapter validation failed: unexpected validator failure")
        return 1
    if rows:
        print(f"KIND adapter validation passed (page 1, {rows} rows checked).")
    else:
        print("KIND adapter validation passed (explicit no-data response on page 1).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
