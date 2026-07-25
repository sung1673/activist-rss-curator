from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator
from urllib.parse import urlsplit

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
SMOKE = ROOT / ".github" / "scripts" / "smoke-global-v2.py"
REVISION = "a" * 40
PREVIEW_TOKEN = "preview-contract-token-" + "x" * 32
OPS_TOKEN = "ops-contract-token-" + "x" * 32


class ContractHandler(BaseHTTPRequestHandler):
    release_state = "closed"
    failure = ""

    def log_message(self, format: str, *args: object) -> None:
        return

    def send_payload(
        self,
        status: int,
        payload: dict[str, object],
        *,
        api_header: bool = True,
    ) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        if api_header:
            self.send_header("X-BSIDE-API-Version", "v2")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        if self.failure == "generic_legacy":
            self.send_payload(
                200,
                {"ok": True, "service": "activist", "time": "2026-07-25T00:00:00Z"},
                api_header=False,
            )
            return
        if path.endswith("/health"):
            service = (
                "activist" if self.failure == "wrong_service"
                else "bside-global-market-terminal"
            )
            revision = "b" * 40 if self.failure == "wrong_sha" else REVISION
            schema = 10 if self.failure == "wrong_schema" else 11
            self.send_payload(
                200,
                {
                    "ok": True,
                    "service": service,
                    "code_revision": revision,
                    "schema_version": schema,
                    "time": "2026-07-25T00:00:00Z",
                    "api_version": "v2",
                },
                api_header=self.failure != "missing_header",
            )
            return
        if path.endswith("/openapi.yaml"):
            body = (
                "openapi: 3.1.0\n"
                "info:\n"
                "  title: BSIDE Global Market Terminal API\n"
                "x-schema-version: 11\n"
                "paths:\n"
                "  /health:\n"
                "    get: {}\n"
            ).encode()
            self.send_response(200)
            self.send_header(
                "Content-Type",
                "text/plain" if self.failure == "wrong_openapi" else "application/yaml",
            )
            self.send_header("X-BSIDE-API-Version", "v2")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if path.endswith("/__bside_v2_operational_smoke_not_found__"):
            self.send_payload(
                200 if self.failure == "unknown_200" else 404,
                (
                    {"ok": True, "api_version": "v2"}
                    if self.failure == "unknown_200"
                    else {"ok": False, "error": "not_found", "api_version": "v2"}
                ),
            )
            return
        if path.endswith("/admin/release-state"):
            authorization = self.headers.get("Authorization", "")
            if self.failure == "admin_accepts_invalid" and authorization:
                self.send_payload(
                    200,
                    {"ok": True, "api_version": "v2"},
                )
                return
            if authorization:
                self.send_payload(
                    403,
                    {
                        "ok": False,
                        "error": "insufficient_role",
                        "api_version": "v2",
                    },
                )
                return
            self.send_payload(
                200 if self.failure == "admin_200" else 401,
                (
                    {"ok": True, "api_version": "v2"}
                    if self.failure == "admin_200"
                    else {
                        "ok": False,
                        "error": "bearer_token_required",
                        "api_version": "v2",
                    }
                ),
            )
            return
        if path.endswith("/ops/release-state"):
            authorization = self.headers.get("Authorization", "")
            if self.failure == "authorization_stripped":
                authorization = ""
            if authorization != f"Bearer {OPS_TOKEN}":
                self.send_payload(
                    401,
                    {
                        "ok": False,
                        "error": "bearer_token_required",
                        "api_version": "v2",
                    },
                )
                return
            self.send_payload(
                200,
                {
                    "ok": True,
                    "data": {
                        "release_state": (
                            "live"
                            if self.failure == "protected_wrong_state"
                            else self.release_state
                        ),
                        "state_version": 0,
                    },
                    "api_version": "v2",
                },
            )
            return
        if path.endswith("/redirected-events"):
            self.send_payload(
                200,
                {"ok": True, "data": {"items": []}, "api_version": "v2"},
            )
            return
        if path.endswith("/events"):
            if self.release_state == "closed":
                self.send_payload(
                    200 if self.failure == "closed_events_200" else 503,
                    (
                        {"ok": True, "data": {"items": []}, "api_version": "v2"}
                        if self.failure == "closed_events_200"
                        else {
                            "ok": False,
                            "error": "global_terminal_release_closed",
                            "api_version": "v2",
                        }
                    ),
                )
                return
            if self.release_state == "preview":
                authorization = self.headers.get("Authorization", "")
                if authorization != f"Bearer {PREVIEW_TOKEN}":
                    self.send_payload(
                        401,
                        {
                            "ok": False,
                            "error": "preview_token_required",
                            "api_version": "v2",
                        },
                    )
                    return
                if self.failure == "preview_token_redirect":
                    self.send_response(302)
                    self.send_header(
                        "Location",
                        "/api.php/api/v2/redirected-events",
                    )
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
            self.send_payload(
                200,
                {"ok": True, "data": {"items": []}, "api_version": "v2"},
            )
            return
        self.send_payload(
            404, {"ok": False, "error": "not_found", "api_version": "v2"}
        )


@contextmanager
def contract_server(
    *, release_state: str = "closed", failure: str = ""
) -> Iterator[str]:
    handler = type(
        "ConfiguredContractHandler",
        (ContractHandler,),
        {"release_state": release_state, "failure": failure},
    )
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}/api.php/api/v2"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def run_smoke(
    base_url: str,
    *,
    release_state: str = "closed",
    expected_sha: str = REVISION,
    include_ops_token: bool = True,
) -> subprocess.CompletedProcess[str]:
    environment = {
        **os.environ,
        "TEST_PREVIEW_TOKEN": PREVIEW_TOKEN,
    }
    if include_ops_token:
        environment["TEST_OPS_TOKEN"] = OPS_TOKEN
    else:
        environment.pop("TEST_OPS_TOKEN", None)
    return subprocess.run(
        [
            sys.executable,
            str(SMOKE),
            "--base-url",
            base_url,
            "--expected-sha",
            expected_sha,
            "--release-state",
            release_state,
            "--preview-token-env",
            "TEST_PREVIEW_TOKEN",
            "--privileged-token-env",
            "TEST_OPS_TOKEN",
            "--allow-http",
            "--timeout",
            "2",
        ],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize("release_state", ["closed", "preview", "live"])
def test_smoke_accepts_exact_v2_contract(release_state: str) -> None:
    with contract_server(release_state=release_state) as base_url:
        result = run_smoke(base_url, release_state=release_state)
    assert result.returncode == 0, result.stderr
    assert "Operational API v2 verified" in result.stdout
    assert PREVIEW_TOKEN not in result.stdout
    assert PREVIEW_TOKEN not in result.stderr
    assert OPS_TOKEN not in result.stdout
    assert OPS_TOKEN not in result.stderr


@pytest.mark.parametrize(
    ("failure", "message"),
    [
        ("generic_legacy", "X-BSIDE-API-Version"),
        ("wrong_service", "health: service"),
        ("wrong_sha", "health: code_revision"),
        ("wrong_schema", "health: schema_version"),
        ("missing_header", "X-BSIDE-API-Version"),
        ("wrong_openapi", "expected application/yaml"),
        ("unknown_200", "unknown route: expected HTTP 404"),
        ("closed_events_200", "closed events: expected HTTP 503"),
        ("admin_200", "unauthenticated admin route: expected HTTP 401"),
        (
            "admin_accepts_invalid",
            "invalid bearer admin route: expected HTTP 401 or 403",
        ),
        (
            "protected_wrong_state",
            "authenticated protected route: expected the exact release state",
        ),
        (
            "authorization_stripped",
            "authenticated protected route: expected HTTP 200",
        ),
    ],
)
def test_smoke_rejects_wrong_or_generic_surfaces(
    failure: str, message: str
) -> None:
    with contract_server(failure=failure) as base_url:
        result = run_smoke(base_url)
    assert result.returncode == 1
    assert message in result.stderr


def test_smoke_rejects_noncanonical_revision_before_network_access() -> None:
    result = run_smoke(
        "http://127.0.0.1:1/api/v2",
        expected_sha="short",
    )
    assert result.returncode == 1
    assert "exactly 40 lowercase hexadecimal" in result.stderr


def test_smoke_fails_closed_without_a_protected_token() -> None:
    with contract_server() as base_url:
        result = run_smoke(base_url, include_ops_token=False)
    assert result.returncode == 1
    assert "requires a protected token" in result.stderr
    assert OPS_TOKEN not in result.stderr


def test_preview_smoke_rejects_an_already_live_release() -> None:
    with contract_server(release_state="live") as base_url:
        result = run_smoke(base_url, release_state="preview")
    assert result.returncode == 1
    assert "authenticated protected route: expected the exact release state" in (
        result.stderr
    )


def test_preview_smoke_never_follows_a_credentialed_redirect() -> None:
    with contract_server(
        release_state="preview",
        failure="preview_token_redirect",
    ) as base_url:
        result = run_smoke(base_url, release_state="preview")
    assert result.returncode == 1
    assert "redirects are forbidden" in result.stderr
    assert PREVIEW_TOKEN not in result.stdout
    assert PREVIEW_TOKEN not in result.stderr
    assert OPS_TOKEN not in result.stdout
    assert OPS_TOKEN not in result.stderr


def test_php_and_openapi_expose_the_operational_identity_contract() -> None:
    php = (ROOT / "deploy" / "activist" / "governance_v2.php").read_text(
        encoding="utf-8"
    )
    openapi = yaml.safe_load(
        (ROOT / "deploy" / "activist" / "openapi-v2.yaml").read_text(
            encoding="utf-8"
        )
    )
    assert "'schema_version' => GOV_V2_SCHEMA_VERSION" in php
    assert "header('X-BSIDE-API-Version: v2');" in php[
        php.index("function v2_serve_openapi") : php.index(
            "function v2_valid_country"
        )
    ]
    assert php.index("if (!v2_path_is_defined($path))") < php.index(
        "$role = null;"
    )
    health = openapi["components"]["schemas"]["HealthEnvelope"]
    assert "schema_version" in health["required"]
    assert health["properties"]["schema_version"] == {"const": 11}


def test_workflows_reuse_the_same_operational_smoke() -> None:
    deployment_path = (
        ROOT
        / ".github"
        / "workflows"
        / "global-alpha-api-deployment-smoke.yml"
    )
    deployment = deployment_path.read_text(encoding="utf-8")
    backfill = (
        ROOT / ".github" / "workflows" / "global-backfill.yml"
    ).read_text(encoding="utf-8")
    preview = (
        ROOT / ".github" / "workflows" / "global-alpha-preview-smoke.yml"
    ).read_text(encoding="utf-8")
    cutover = (
        ROOT / ".github" / "workflows" / "governance-cutover.yml"
    ).read_text(encoding="utf-8")
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert "--release-state closed" in deployment
    assert "--release-state closed" in backfill
    assert "--release-state preview" in preview
    assert cutover.count(".github/scripts/smoke-global-v2.py") >= 3
    assert "--release-state preview" in cutover
    assert "--release-state live" in cutover
    assert "--release-state closed" in ci
    for workflow in (deployment, backfill, preview, cutover, ci):
        assert ".github/scripts/smoke-global-v2.py" in workflow
        assert "--privileged-token-env" in workflow
    assert deployment.count("--privileged-token-env BSIDE_OPS_TOKEN") == 1
    assert backfill.count("--privileged-token-env BSIDE_OPS_TOKEN") == 1
    assert preview.count("--privileged-token-env BSIDE_OPS_TOKEN") == 1
    assert cutover.count("--privileged-token-env BSIDE_ADMIN_TOKEN") >= 3
    assert ci.count("--privileged-token-env PHP73_CI_OPS_TOKEN") == 1
