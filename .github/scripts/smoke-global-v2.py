#!/usr/bin/env python3
"""Verify that an operational URL is the exact expected BSIDE API v2 build."""

from __future__ import annotations

import argparse
import json
import os
import re
import ssl
import sys
from dataclasses import dataclass
from typing import Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import (
    HTTPRedirectHandler,
    HTTPSHandler,
    Request,
    build_opener,
)


EXPECTED_SERVICE = "bside-global-market-terminal"
EXPECTED_SCHEMA_VERSION = 11
EXPECTED_API_HEADER = "v2"
EXPECTED_OPENAPI_MARKERS = (
    "openapi: 3.1.0",
    "title: BSIDE Global Market Terminal API",
    "x-schema-version: 11",
    "/health:",
)
SHA_RE = re.compile(r"^[0-9a-f]{40}$")


class SmokeFailure(RuntimeError):
    """The deployed HTTP surface does not satisfy the v2 release contract."""


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Request,
        fp: object,
        code: int,
        msg: str,
        headers: object,
        newurl: str,
    ) -> None:
        del req, fp, code, msg, headers, newurl
        return None


@dataclass(frozen=True)
class Response:
    status: int
    headers: Mapping[str, str]
    body: bytes

    def header(self, name: str) -> str:
        return self.headers.get(name.casefold(), "")


def _fail(message: str) -> None:
    raise SmokeFailure(message)


def _request(
    url: str,
    *,
    token: str = "",
    timeout: float,
    insecure: bool,
) -> Response:
    headers = {
        "Accept": "application/json, application/yaml",
        "User-Agent": "BSIDE-Global-V2-Operational-Smoke/1.0",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(url, headers=headers, method="GET")
    context = ssl._create_unverified_context() if insecure else None
    handlers: list[object] = [_RejectRedirects()]
    if context is not None:
        handlers.append(HTTPSHandler(context=context))
    opener = build_opener(*handlers)
    try:
        response = opener.open(request, timeout=timeout)
    except HTTPError as error:
        response = error
    except (OSError, URLError) as error:
        _fail(f"request failed for {url}: {error}")
    try:
        if 300 <= int(response.status) < 400:
            _fail(f"redirects are forbidden for {url}")
        return Response(
            status=int(response.status),
            headers={
                key.casefold(): value.strip()
                for key, value in response.headers.items()
            },
            body=response.read(),
        )
    finally:
        response.close()


def _json(response: Response, *, label: str) -> dict[str, object]:
    content_type = response.header("content-type").casefold()
    if not content_type.startswith("application/json"):
        _fail(f"{label}: expected application/json, got {content_type!r}")
    try:
        payload = json.loads(response.body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        _fail(f"{label}: invalid UTF-8 JSON: {error}")
    if not isinstance(payload, dict):
        _fail(f"{label}: JSON body must be an object")
    return payload


def _require_v2_header(response: Response, *, label: str) -> None:
    actual = response.header("x-bside-api-version")
    if actual != EXPECTED_API_HEADER:
        _fail(
            f"{label}: X-BSIDE-API-Version must be "
            f"{EXPECTED_API_HEADER!r}, got {actual!r}"
        )


def _require_status(response: Response, expected: int, *, label: str) -> None:
    if response.status != expected:
        _fail(f"{label}: expected HTTP {expected}, got {response.status}")


def _require_v2_json(response: Response, *, label: str) -> dict[str, object]:
    _require_v2_header(response, label=label)
    payload = _json(response, label=label)
    if payload.get("api_version") != "v2":
        _fail(f"{label}: JSON api_version must be 'v2'")
    return payload


def verify_global_v2(
    *,
    base_url: str,
    expected_sha: str,
    release_state: str,
    preview_token: str = "",
    timeout: float = 20.0,
    allow_http: bool = False,
    insecure: bool = False,
) -> None:
    base = base_url.strip().rstrip("/")
    parsed = urlsplit(base)
    allowed_schemes = {"https", "http"} if allow_http else {"https"}
    if (
        parsed.scheme.casefold() not in allowed_schemes
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or not parsed.path.endswith("/api/v2")
    ):
        _fail("base URL must be a credential-free /api/v2 HTTPS URL")
    if SHA_RE.fullmatch(expected_sha) is None:
        _fail("expected SHA must be exactly 40 lowercase hexadecimal characters")
    if release_state not in {"closed", "preview", "live"}:
        _fail("release state must be closed, preview, or live")
    if release_state == "preview" and not preview_token:
        _fail("preview release-state smoke requires a preview token")

    health = _request(f"{base}/health", timeout=timeout, insecure=insecure)
    _require_status(health, 200, label="health")
    health_payload = _require_v2_json(health, label="health")
    expected_health = {
        "ok": True,
        "service": EXPECTED_SERVICE,
        "code_revision": expected_sha,
        "schema_version": EXPECTED_SCHEMA_VERSION,
        "api_version": "v2",
    }
    for field, expected in expected_health.items():
        if health_payload.get(field) != expected:
            _fail(
                f"health: {field} must be {expected!r}, "
                f"got {health_payload.get(field)!r}"
            )

    specification = _request(
        f"{base}/openapi.yaml", timeout=timeout, insecure=insecure
    )
    _require_status(specification, 200, label="OpenAPI")
    _require_v2_header(specification, label="OpenAPI")
    openapi_content_type = specification.header("content-type").casefold()
    if not openapi_content_type.startswith("application/yaml"):
        _fail(
            "OpenAPI: expected application/yaml content type, "
            f"got {openapi_content_type!r}"
        )
    try:
        openapi_body = specification.body.decode("utf-8")
    except UnicodeDecodeError as error:
        _fail(f"OpenAPI: invalid UTF-8 YAML: {error}")
    for marker in EXPECTED_OPENAPI_MARKERS:
        if marker not in openapi_body:
            _fail(f"OpenAPI: required marker is absent: {marker!r}")

    unknown = _request(
        f"{base}/__bside_v2_operational_smoke_not_found__",
        timeout=timeout,
        insecure=insecure,
    )
    _require_status(unknown, 404, label="unknown route")
    unknown_payload = _require_v2_json(unknown, label="unknown route")
    if unknown_payload.get("ok") is not False or unknown_payload.get("error") != "not_found":
        _fail("unknown route: expected the v2 not_found error envelope")

    missing_admin = _request(
        f"{base}/admin/release-state", timeout=timeout, insecure=insecure
    )
    _require_status(
        missing_admin,
        401,
        label="unauthenticated admin route",
    )
    missing_admin_payload = _require_v2_json(
        missing_admin, label="unauthenticated admin route"
    )
    if (
        missing_admin_payload.get("ok") is not False
        or missing_admin_payload.get("error") != "bearer_token_required"
    ):
        _fail("unauthenticated admin route: unexpected v2 error envelope")

    invalid_admin = _request(
        f"{base}/admin/release-state",
        token=f"bside-invalid-operational-smoke-{expected_sha}",
        timeout=timeout,
        insecure=insecure,
    )
    if invalid_admin.status not in {401, 403}:
        _fail(
            "invalid bearer admin route: expected HTTP 401 or 403, "
            f"got {invalid_admin.status}"
        )
    invalid_admin_payload = _require_v2_json(
        invalid_admin, label="invalid bearer admin route"
    )
    if (
        invalid_admin_payload.get("ok") is not False
        or invalid_admin_payload.get("error")
        not in {"bearer_token_required", "insufficient_role"}
    ):
        _fail("invalid bearer admin route: unexpected v2 error envelope")

    if release_state == "preview":
        anonymous_events = _request(
            f"{base}/events?limit=1",
            timeout=timeout,
            insecure=insecure,
        )
        if anonymous_events.status not in {401, 403}:
            _fail(
                "preview anonymous events: expected HTTP 401 or 403, "
                f"got {anonymous_events.status}"
            )
        anonymous_events_payload = _require_v2_json(
            anonymous_events,
            label="preview anonymous events",
        )
        if (
            anonymous_events_payload.get("ok") is not False
            or anonymous_events_payload.get("error")
            not in {"preview_token_required", "invalid_preview_token"}
        ):
            _fail("preview anonymous events: unexpected preview error envelope")

    events_token = preview_token if release_state == "preview" else ""
    events = _request(
        f"{base}/events?limit=1",
        token=events_token,
        timeout=timeout,
        insecure=insecure,
    )
    events_payload = _require_v2_json(events, label="events")
    if release_state == "closed":
        _require_status(events, 503, label="closed events")
        if (
            events_payload.get("ok") is not False
            or events_payload.get("error")
            != "global_terminal_release_closed"
        ):
            _fail("closed events: expected global_terminal_release_closed")
    else:
        _require_status(events, 200, label=f"{release_state} events")
        if events_payload.get("ok") is not True:
            _fail(f"{release_state} events: expected a successful v2 envelope")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Fail closed unless an operational endpoint is the exact expected "
            "BSIDE global terminal API v2 build."
        )
    )
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument(
        "--release-state",
        required=True,
        choices=("closed", "preview", "live"),
    )
    parser.add_argument(
        "--preview-token-env",
        default="",
        help="environment variable containing the preview Bearer token",
    )
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument(
        "--allow-http",
        action="store_true",
        help="allow HTTP for isolated local contract tests only",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="disable TLS certificate verification for an isolated test server",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    preview_token = (
        os.environ.get(args.preview_token_env, "")
        if args.preview_token_env
        else ""
    )
    try:
        verify_global_v2(
            base_url=args.base_url,
            expected_sha=args.expected_sha,
            release_state=args.release_state,
            preview_token=preview_token,
            timeout=args.timeout,
            allow_http=args.allow_http,
            insecure=args.insecure,
        )
    except SmokeFailure as error:
        print(f"::error::{error}", file=sys.stderr)
        return 1
    print(
        "Operational API v2 verified: exact service, revision, schema, routing, "
        f"authentication, and {args.release_state} release boundary."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
