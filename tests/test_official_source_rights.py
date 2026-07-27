from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from curator.official_source_contracts import (
    DART_SOURCE_RIGHT_CONTRACT_REVISION,
    source_right_contract_payload,
    source_right_contract_revision,
)
from curator.official_source_rights import (
    DartOfficialSourceRightClient,
    OfficialSourceRightClient,
    OfficialSourceRightError,
    source_right_api_base_url,
)


REVISION = "a" * 64
RELEASE_SHA = "b" * 40
PRODUCTION_BASE = "https://alignpe.gabia.io/activist/api.php/api/v1"
FIXTURE = (
    Path(__file__).parent / "fixtures" / "dart_source_right_contract_v1.json"
)


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


def test_dart_contract_fixture_matches_independent_python_canonicalization() -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert fixture["schema"] == "source-right-contract-v1"
    assert fixture["contract_payload"] == source_right_contract_payload(
        fixture["source_right"]
    )
    assert fixture["contract_payload"] == source_right_contract_payload()
    assert (
        source_right_contract_revision(fixture["contract_payload"])
        == fixture["expected_revision"]
        == DART_SOURCE_RIGHT_CONTRACT_REVISION
    )


def _v2_response(status: int, payload: dict[str, object]) -> httpx.Response:
    return httpx.Response(
        status,
        json=payload,
        headers={"X-BSIDE-API-Version": "v2"},
    )


def _v1_response(status: int, payload: dict[str, object]) -> httpx.Response:
    return httpx.Response(
        status,
        json=payload,
        headers={"X-BSIDE-API-Version": "v1"},
    )


def test_dart_apply_preflight_verifies_release_state_and_exact_contract() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        assert request.headers["Cache-Control"] == "no-cache"
        if request.url.path.endswith("/api/v1/health"):
            assert "Authorization" not in request.headers
            return _v1_response(
                200,
                {
                    "ok": True,
                    "service": "bside-governance-intelligence",
                    "api_version": "v1",
                },
            )
        if request.url.path.endswith("/api/v1/events"):
            assert "Authorization" not in request.headers
            assert dict(request.url.params) == {"limit": "1"}
            return _v1_response(
                503,
                {
                    "ok": False,
                    "error": "governance_release_closed",
                    "api_version": "v1",
                },
            )
        assert request.headers["Authorization"] == "Bearer ops-token"
        if request.url.path.endswith("/api/v2/health"):
            return _v2_response(
                200,
                {
                    "ok": True,
                    "service": "bside-global-market-terminal",
                    "schema_version": 12,
                    "code_revision": RELEASE_SHA,
                },
            )
        if request.url.path.endswith("/api/v2/ops/release-state"):
            return _v2_response(
                200,
                {"ok": True, "data": {"release_state": "closed"}},
            )
        assert request.url.path.endswith(
            "/api/v2/ops/source-right-eligibility"
        )
        assert dict(request.url.params) == {
            "source_right_id": "official:dart",
            "use": "collect",
        }
        return _v2_response(
            200,
            {
                "ok": True,
                "source_right_id": "official:dart",
                "source_type": "official_disclosure",
                "source_key": "dart",
                "use": "collect",
                "eligible": True,
                "rights_revision": REVISION,
                "contract_revision": DART_SOURCE_RIGHT_CONTRACT_REVISION,
                "redistribution_allowed": True,
                "ai_allowed": False,
                "connector_id": "connector:kr:dart",
                "connector_ready": True,
                "checked_at": "2026-07-27T00:00:00+00:00",
            },
        )

    eligibility = DartOfficialSourceRightClient(
        base_url=PRODUCTION_BASE,
        token="ops-token",
        transport=httpx.MockTransport(handler),
    ).preflight(RELEASE_SHA, "dart_canary")

    assert eligibility.rights_revision == REVISION
    assert eligibility.contract_revision == DART_SOURCE_RIGHT_CONTRACT_REVISION
    assert eligibility.release_state == "closed"
    assert len(calls) == 5


@pytest.mark.parametrize(
    ("pipeline_mode", "release_state", "public_status", "public_payload"),
    (
        (
            "shadow",
            "preview",
            401,
            {
                "ok": False,
                "error": "preview_token_required",
                "api_version": "v1",
            },
        ),
        (
            "live",
            "live",
            200,
            {"ok": True, "data": [], "api_version": "v1"},
        ),
    ),
)
def test_dart_apply_preflight_binds_shadow_and_live_public_contracts(
    pipeline_mode: str,
    release_state: str,
    public_status: int,
    public_payload: dict[str, object],
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/api/v1/health"):
            return _v1_response(
                200,
                {
                    "ok": True,
                    "service": "bside-governance-intelligence",
                    "api_version": "v1",
                },
            )
        if request.url.path.endswith("/api/v1/events"):
            assert "Authorization" not in request.headers
            return _v1_response(public_status, public_payload)
        if request.url.path.endswith("/api/v2/health"):
            return _v2_response(
                200,
                {
                    "ok": True,
                    "service": "bside-global-market-terminal",
                    "schema_version": 12,
                    "code_revision": RELEASE_SHA,
                },
            )
        if request.url.path.endswith("/api/v2/ops/release-state"):
            return _v2_response(
                200,
                {"ok": True, "data": {"release_state": release_state}},
            )
        return _v2_response(
            200,
            {
                "ok": True,
                "source_right_id": "official:dart",
                "source_type": "official_disclosure",
                "source_key": "dart",
                "use": "collect",
                "eligible": True,
                "rights_revision": REVISION,
                "contract_revision": DART_SOURCE_RIGHT_CONTRACT_REVISION,
                "redistribution_allowed": True,
                "ai_allowed": False,
                "connector_id": "connector:kr:dart",
                "connector_ready": True,
            },
        )

    eligibility = DartOfficialSourceRightClient(
        base_url=PRODUCTION_BASE,
        token="ops-token",
        transport=httpx.MockTransport(handler),
    ).preflight(RELEASE_SHA, pipeline_mode)

    assert eligibility.release_state == release_state


@pytest.mark.parametrize("pipeline_mode", ("", "off", "manual", "preview"))
def test_dart_apply_preflight_rejects_unbound_pipeline_modes_before_network(
    pipeline_mode: str,
) -> None:
    def unexpected_request(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("invalid pipeline mode must fail before network")

    client = DartOfficialSourceRightClient(
        base_url=PRODUCTION_BASE,
        token="ops-token",
        transport=httpx.MockTransport(unexpected_request),
    )
    with pytest.raises(
        OfficialSourceRightError,
        match="dart_canary, shadow, or live",
    ):
        client.preflight(RELEASE_SHA, pipeline_mode)


@pytest.mark.parametrize(
    ("mutation", "match"),
    (
        ({"code_revision": "c" * 40}, "exact SHA/schema 12"),
        ({"release_state": "preview"}, "exact closed contract"),
        ({"contract_revision": "d" * 64}, "metadata-only contract"),
        ({"ai_allowed": True}, "metadata-only contract"),
        ({"redistribution_allowed": False}, "metadata-only contract"),
        ({"connector_ready": False}, "configured for collection"),
        ({"connector_id": "connector:kr:other"}, "configured for collection"),
    ),
)
def test_dart_apply_preflight_fails_closed_on_mismatch(
    mutation: dict[str, object],
    match: str,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/api/v1/health"):
            return _v1_response(
                200,
                {
                    "ok": True,
                    "service": "bside-governance-intelligence",
                    "api_version": "v1",
                },
            )
        if request.url.path.endswith("/api/v1/events"):
            return _v1_response(
                503,
                {
                    "ok": False,
                    "error": "governance_release_closed",
                    "api_version": "v1",
                },
            )
        if request.url.path.endswith("/api/v2/health"):
            payload: dict[str, object] = {
                "ok": True,
                "service": "bside-global-market-terminal",
                "schema_version": 12,
                "code_revision": RELEASE_SHA,
            }
            if "code_revision" in mutation:
                payload.update(mutation)
            return _v2_response(200, payload)
        if request.url.path.endswith("/release-state"):
            state = mutation.get("release_state", "closed")
            return _v2_response(
                200,
                {"ok": True, "data": {"release_state": state}},
            )
        payload = {
            "ok": True,
            "source_right_id": "official:dart",
            "source_type": "official_disclosure",
            "source_key": "dart",
            "use": "collect",
            "eligible": True,
            "rights_revision": REVISION,
            "contract_revision": DART_SOURCE_RIGHT_CONTRACT_REVISION,
            "redistribution_allowed": True,
            "ai_allowed": False,
            "connector_id": "connector:kr:dart",
            "connector_ready": True,
        }
        payload.update(
            {
                key: value
                for key, value in mutation.items()
                if key not in {"code_revision", "release_state"}
            }
        )
        return _v2_response(200, payload)

    client = DartOfficialSourceRightClient(
        base_url=PRODUCTION_BASE,
        token="ops-token",
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(OfficialSourceRightError, match=match):
        client.preflight(RELEASE_SHA, "dart_canary")


def test_dart_apply_preflight_rejects_ineligible_before_ack() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/api/v1/health"):
            return _v1_response(
                200,
                {
                    "ok": True,
                    "service": "bside-governance-intelligence",
                    "api_version": "v1",
                },
            )
        if request.url.path.endswith("/api/v1/events"):
            return _v1_response(
                503,
                {
                    "ok": False,
                    "error": "governance_release_closed",
                    "api_version": "v1",
                },
            )
        if request.url.path.endswith("/api/v2/health"):
            return _v2_response(
                200,
                {
                    "ok": True,
                    "service": "bside-global-market-terminal",
                    "schema_version": 12,
                    "code_revision": RELEASE_SHA,
                },
            )
        if request.url.path.endswith("/release-state"):
            return _v2_response(
                200,
                {"ok": True, "data": {"release_state": "closed"}},
            )
        return _v2_response(
            409,
            {
                "ok": False,
                "error": "source_right_ineligible",
                "reasons": ["expired"],
            },
        )

    client = DartOfficialSourceRightClient(
        base_url=PRODUCTION_BASE,
        token="ops-token",
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(OfficialSourceRightError, match="HTTP 409"):
        client.preflight(RELEASE_SHA, "dart_canary")


@pytest.mark.parametrize(
    ("status", "payload"),
    (
        (401, {"ok": False, "error": "preview_token_required", "api_version": "v1"}),
        (200, {"ok": True, "items": [], "api_version": "v1"}),
    ),
)
def test_dart_apply_preflight_requires_independent_v1_closed_state(
    status: int,
    payload: dict[str, object],
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/api/v2/health"):
            return _v2_response(
                200,
                {
                    "ok": True,
                    "service": "bside-global-market-terminal",
                    "schema_version": 12,
                    "code_revision": RELEASE_SHA,
                },
            )
        if request.url.path.endswith("/api/v1/health"):
            assert "Authorization" not in request.headers
            return _v1_response(
                200,
                {
                    "ok": True,
                    "service": "bside-governance-intelligence",
                    "api_version": "v1",
                },
            )
        assert request.url.path.endswith("/api/v1/events")
        assert "Authorization" not in request.headers
        return _v1_response(status, payload)

    client = DartOfficialSourceRightClient(
        base_url=PRODUCTION_BASE,
        token="ops-token",
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(
        OfficialSourceRightError,
        match="exact closed contract",
    ):
        client.preflight(RELEASE_SHA, "dart_canary")


def test_dart_apply_preflight_rejects_nonproduction_endpoint() -> None:
    with pytest.raises(OfficialSourceRightError, match="fixed production"):
        DartOfficialSourceRightClient(
            base_url="https://staging.example/api/v2",
            token="ops-token",
        )
