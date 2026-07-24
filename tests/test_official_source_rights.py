from __future__ import annotations

import httpx
import pytest

from curator.official_source_rights import (
    OfficialSourceRightClient,
    OfficialSourceRightError,
    source_right_api_base_url,
)


REVISION = "a" * 64


def test_source_right_api_base_url_uses_fail_closed_precedence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ACTIVIST_API_URL", "https://legacy.example/api.php")
    monkeypatch.setenv("GOVERNANCE_API_BASE_URL", "https://governance.example/api.php/api/v1/")
    monkeypatch.setenv("BSIDE_API_BASE_URL", "https://ops.example/activist/api.php")
    assert source_right_api_base_url() == "https://ops.example/activist/api.php/api/v1"

    monkeypatch.delenv("BSIDE_API_BASE_URL")
    assert source_right_api_base_url() == "https://governance.example/api.php/api/v1"

    monkeypatch.delenv("GOVERNANCE_API_BASE_URL")
    assert source_right_api_base_url() == "https://legacy.example/api.php/api/v1"


@pytest.mark.parametrize(
    "url",
    (
        "http://ops.example/api/v1",
        "ops.example/api/v1",
        "https://user:password@ops.example/api/v1",
        "https://ops.example/api/v1?token=secret",
        "https://ops.example/api/v1#fragment",
    ),
)
def test_source_right_client_rejects_unsafe_api_base_urls(url: str) -> None:
    with pytest.raises(OfficialSourceRightError, match="API base URL"):
        OfficialSourceRightClient(base_url=url, token="ops-token")


def test_source_right_client_requires_base_url_and_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "BSIDE_API_BASE_URL",
        "GOVERNANCE_API_BASE_URL",
        "ACTIVIST_API_URL",
        "BSIDE_OPS_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(OfficialSourceRightError, match="BSIDE_OPS_TOKEN"):
        OfficialSourceRightClient()


def test_kind_ingest_preflight_sends_bearer_and_accepts_exact_revision() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/api.php/api/v1/ops/source-right-eligibility"
        assert dict(request.url.params) == {
            "source_right_id": "official:kind",
            "use": "ingest",
        }
        assert request.headers["Authorization"] == "Bearer ops-token"
        assert request.headers["Accept"] == "application/json"
        return httpx.Response(
            200,
            json={
                "ok": True,
                "source_right_id": "official:kind",
                "use": "ingest",
                "eligible": True,
                "rights_revision": REVISION,
                "checked_at": "2026-07-22T15:00:00+00:00",
            },
        )

    eligibility = OfficialSourceRightClient(
        base_url="https://ops.example/api.php",
        token="ops-token",
        transport=httpx.MockTransport(handler),
    ).check_kind_ingest()

    assert eligibility.source_right_id == "official:kind"
    assert eligibility.use == "ingest"
    assert eligibility.rights_revision == REVISION
    assert eligibility.checked_at == "2026-07-22T15:00:00+00:00"


def test_kind_ingest_preflight_rejects_registered_but_ineligible_right() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            json={
                "ok": False,
                "error": "source_right_ineligible",
                "source_right_id": "official:kind",
                "use": "ingest",
                "eligible": False,
                "rights_revision": None,
                "reasons": ["permission_expired", "redistribution_not_allowed"],
                "checked_at": "2026-07-22T15:00:00+00:00",
            },
        )

    client = OfficialSourceRightClient(
        base_url="https://ops.example/api/v1",
        token="ops-token",
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(OfficialSourceRightError, match="permission_expired"):
        client.check_kind_ingest()


@pytest.mark.parametrize(
    ("status", "content", "match"),
    (
        (200, b"not-json", "invalid JSON"),
        (200, b"[]", "JSON object"),
        (500, b'{"ok":false,"error":"db_unavailable"}', "HTTP 500"),
        (
            200,
            b'{"ok":true,"source_right_id":"official:kind","use":"ingest",'
            b'"eligible":true,"rights_revision":"NOT-A-REVISION"}',
            "exact KIND ingest eligibility contract",
        ),
    ),
)
def test_kind_ingest_preflight_rejects_invalid_http_contract(
    status: int,
    content: bytes,
    match: str,
) -> None:
    client = OfficialSourceRightClient(
        base_url="https://ops.example/api/v1",
        token="ops-token",
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(status, content=content)
        ),
    )
    with pytest.raises(OfficialSourceRightError, match=match):
        client.check_kind_ingest()


def test_kind_ingest_preflight_wraps_network_failure_without_leaking_token() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("offline", request=request)

    client = OfficialSourceRightClient(
        base_url="https://ops.example/api/v1",
        token="super-secret-ops-token",
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(OfficialSourceRightError, match="ConnectError") as raised:
        client.check_kind_ingest()
    assert "super-secret-ops-token" not in str(raised.value)
