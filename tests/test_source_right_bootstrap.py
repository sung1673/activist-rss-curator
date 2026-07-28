from __future__ import annotations

import hashlib
import json
import traceback
from datetime import datetime, timezone
from typing import Any

import httpx
import pytest

from curator.source_right_bootstrap import (
    CONFIRMATION,
    CORE_SOURCES,
    SourceRightBootstrapError,
    bootstrap_source_rights,
    bootstrap_sources,
)


RELEASE_SHA = "a" * 40
ADMIN_TOKEN = "bootstrap-admin-token-" + ("x" * 32)
API_BASE = "https://alignpe.gabia.io/activist/api.php/api/v1"


def _selected_links(country: str) -> str:
    if country == "CA":
        hostname = "investors.acme.ca"
        identifier_type = "SEDAR_ISSUER_ID"
        identifier = "00001234"
        source_right_id = "official:ca-issuer-ir"
        event_family = "meeting_and_vote"
        url = "https://investors.acme.ca/governance/meeting-2026"
    else:
        hostname = "asic.gov.au"
        identifier_type = "ACN"
        identifier = "123456789"
        source_right_id = "official:asic-register"
        event_family = "listing_status"
        url = "https://asic.gov.au/online-services/search-registers/company/123456789"
    return json.dumps(
        {
            "schema_version": 1,
            "approved_hosts": [
                {
                    "hostname": hostname,
                    "issuer_identifier_type": identifier_type,
                    "issuer_identifier": identifier,
                    "evidence_sha256": "e" * 64,
                }
            ],
            "records": [
                {
                    "country_code": country,
                    "issuer_identifier_type": identifier_type,
                    "issuer_identifier": identifier,
                    "issuer_name": f"{country} Example Issuer",
                    "source_right_id": source_right_id,
                    "official_host": hostname,
                    "original_url": url,
                    "title": "2026 governance notice",
                    "original_language": "en",
                    "filed_at": "2026-07-25T00:00:00Z",
                    "first_observed_at": "2026-07-25T00:05:00Z",
                    "event_family": event_family,
                }
            ],
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def _active_right(source_index: int) -> dict[str, Any]:
    source = CORE_SOURCES[source_index]
    payload = source.source_right_payload(
        valid_from="2026-07-25T00:00:00Z",
        code_revision=RELEASE_SHA,
    )
    payload["updated_at"] = "2026-07-25T23:58:00Z"
    return payload


class BootstrapApi:
    def __init__(self) -> None:
        self.release_sha = RELEASE_SHA
        self.v1_state = "closed"
        self.v2_state = "closed"
        self.rights: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, str]] = []
        self.posts: list[tuple[str, dict[str, Any]]] = []
        self.connectors: dict[str, dict[str, Any]] = {}
        for source in bootstrap_sources(
            include_ca=True,
            include_au=True,
            environment={
                "CA_OFFICIAL_LINKS_JSON": _selected_links("CA"),
                "AU_OFFICIAL_LINKS_JSON": _selected_links("AU"),
            },
        ):
            self.connectors[source.connector_id] = {
                "connector_id": source.connector_id,
                "country_code": source.country_code,
                "source_key": source.source_key,
                "source_name": source.source_name,
                "source_type": source.source_type,
                "base_url": "https://official.example.com",
                "source_right_id": source.source_right_id,
                "coverage_mode": source.coverage_mode,
                "connector_status": (
                    "active" if source.country_code == "KR" else "pending_rights"
                ),
                "schedule_minutes": 30,
                "last_checked_at": None,
                "last_success_at": None,
                "last_error_class": "source_right_required",
                "code_revision": None,
                "updated_at": "2026-07-25T23:59:00Z",
            }

    @staticmethod
    def _response(version: str, payload: dict[str, Any]) -> httpx.Response:
        return httpx.Response(
            200,
            json=payload,
            headers={"X-BSIDE-API-Version": version},
        )

    def _revision(self, source_right_id: str) -> str:
        payload = json.dumps(
            self.rights[source_right_id],
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def _eligibility(self, connector: dict[str, Any]) -> dict[str, Any]:
        right_id = connector["source_right_id"]
        right = self.rights.get(right_id)
        return {
            "eligible": right is not None,
            "identity_match": bool(
                right
                and right["source_type"] == connector["source_type"]
                and right["source_key"] == connector["source_key"]
            ),
            "ineligible_reasons": [] if right else ["not_registered"],
            "rights_revision": self._revision(right_id) if right else None,
            "right_status": right["status"] if right else None,
            "redistribution_allowed": (
                right["redistribution_allowed"] if right else False
            ),
            "ai_allowed": right["ai_allowed"] if right else False,
        }

    def _connector_view(self, connector_id: str) -> dict[str, Any]:
        connector = dict(self.connectors[connector_id])
        connector["collect_eligibility"] = self._eligibility(connector)
        return connector

    def __call__(self, request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == f"Bearer {ADMIN_TOKEN}"
        method = request.method
        path = request.url.path
        self.calls.append((method, path))
        is_v1 = "/api/v1/" in path
        version = "v1" if is_v1 else "v2"

        if method == "GET" and path.endswith("/api/v2/health"):
            return self._response(
                "v2",
                {
                    "ok": True,
                    "service": "bside-global-market-terminal",
                    "schema_version": 12,
                    "code_revision": self.release_sha,
                    "time": "2026-07-26T00:00:00+00:00",
                },
            )
        if method == "GET" and path.endswith("/api/v1/admin/release-state"):
            return self._response(
                "v1",
                {"ok": True, "release_state": self.v1_state},
            )
        if method == "GET" and path.endswith("/api/v2/admin/release-state"):
            return self._response(
                "v2",
                {
                    "ok": True,
                    "data": {"release_state": self.v2_state},
                },
            )
        if method == "GET" and path.endswith("/api/v1/admin/source-rights"):
            page = int(request.url.params.get("page", "1"))
            limit = int(request.url.params.get("limit", "25"))
            rows = list(self.rights.values())
            start = (page - 1) * limit
            selected = rows[start : start + limit]
            has_more = start + limit < len(rows)
            return self._response(
                "v1",
                {
                    "ok": True,
                    "data": selected,
                    "pagination": {
                        "page": page,
                        "limit": limit,
                        "returned": len(selected),
                        "has_more": has_more,
                        "next_page": page + 1 if has_more else None,
                    },
                },
            )
        if method == "POST" and path.endswith("/api/v1/admin/source-rights"):
            body = json.loads(request.content)
            self.posts.append((path, body))
            existing = self.rights.get(body["source_right_id"])
            if body["expected_status"] == "missing":
                assert existing is None
                assert body["expected_updated_at"] is None
            else:
                assert existing is not None
                assert body["expected_status"] == existing["status"]
                assert body["expected_updated_at"] == existing["updated_at"]
            stored = {
                key: value
                for key, value in body.items()
                if key not in {"expected_status", "expected_updated_at"}
            }
            stored["updated_at"] = "2026-07-26T00:00:00Z"
            self.rights[body["source_right_id"]] = stored
            return self._response(
                "v1",
                {
                    "ok": True,
                    "source_right_id": body["source_right_id"],
                    "status": body["status"],
                },
            )
        if method == "GET" and "/api/v2/ops/source-right-eligibility" in path:
            right_id = request.url.params["source_right_id"]
            use = request.url.params["use"]
            right = self.rights[right_id]
            return self._response(
                "v2",
                {
                    "ok": True,
                    "source_right_id": right_id,
                    "source_type": right["source_type"],
                    "source_key": right["source_key"],
                    "use": use,
                    "eligible": True,
                    "rights_revision": self._revision(right_id),
                    "redistribution_allowed": right["redistribution_allowed"],
                    "ai_allowed": right["ai_allowed"],
                },
            )
        connector_marker = "/api/v2/admin/connectors/"
        if connector_marker in path:
            connector_id = path.split(connector_marker, 1)[1]
            if method == "GET":
                return self._response(
                    "v2",
                    {
                        "ok": True,
                        "data": {
                            "connector": self._connector_view(connector_id),
                            "audit_log": [],
                        },
                    },
                )
            body = json.loads(request.content)
            self.posts.append((path, body))
            connector = self.connectors[connector_id]
            assert body["expected_updated_at"] == connector["updated_at"]
            previous_status = connector["connector_status"]
            connector["connector_status"] = body["target_status"]
            connector["updated_at"] = "2026-07-26T00:00:01Z"
            result = self._connector_view(connector_id)
            result.update(
                {
                    "previous_status": previous_status,
                    "changed": previous_status != body["target_status"],
                    "audit_id": f"connector-audit:{'b' * 64}",
                }
            )
            return self._response("v2", {"ok": True, "data": result})
        raise AssertionError(f"unexpected request: {method} {path} ({version})")


def _run(
    api: BootstrapApi,
    *,
    confirmation: str = CONFIRMATION,
    expected_release_sha: str = RELEASE_SHA,
    code_revision: str = RELEASE_SHA,
    include_ca: bool = False,
    include_au: bool = False,
    environment: dict[str, str] | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    return bootstrap_source_rights(
        base_url=API_BASE,
        admin_token=ADMIN_TOKEN,
        expected_release_sha=expected_release_sha,
        code_revision=code_revision,
        reason="Human approved metadata-only official source registration.",
        confirmation=confirmation,
        include_ca=include_ca,
        include_au=include_au,
        environment=environment,
        transport=httpx.MockTransport(api),
        now=now or datetime(2026, 7, 26, tzinfo=timezone.utc),
    )


def test_core_bootstrap_is_metadata_only_and_verifies_connectors() -> None:
    api = BootstrapApi()

    result = _run(api)

    assert result["source_count"] == 2
    assert result["release_states"] == {"v1": "closed", "v2": "closed"}
    assert {item["source_right_id"] for item in result["sources"]} == {
        "official:dart",
        "official:sec-edgar",
    }
    right_posts = [
        payload
        for path, payload in api.posts
        if path.endswith("/api/v1/admin/source-rights")
    ]
    assert len(right_posts) == 2
    for payload in right_posts:
        assert payload["ai_allowed"] is False
        assert payload["redistribution_allowed"] is True
        assert payload["status"] == "active"
        assert payload["expected_status"] == "missing"
        assert payload["expected_updated_at"] is None
        assert payload["evidence_uri"].startswith("https://")
        assert payload["evidence_hash"] is None
        assert "metadata only" in payload["permission_scope"]
        assert "Full filing text" in payload["permission_scope"]
        assert payload["permission_scope"].endswith("excluded.")
        assert "document bodies are excluded" in payload["notes"]
    assert api.connectors["connector:kr:dart"]["connector_status"] == "active"
    assert (
        api.connectors["connector:us:sec-edgar"]["connector_status"]
        == "configured"
    )
    first_post_index = next(
        index for index, call in enumerate(api.calls) if call[0] == "POST"
    )
    assert sum(
        call[0] == "GET" and "/admin/connectors/" in call[1]
        for call in api.calls[:first_post_index]
    ) == 2
    assert ADMIN_TOKEN not in json.dumps(result)


def test_bootstrap_retries_get_transport_failure_but_not_post() -> None:
    api = BootstrapApi()
    get_attempts = 0

    def transient_get(request: httpx.Request) -> httpx.Response:
        nonlocal get_attempts
        if request.method == "GET" and request.url.path.endswith("/api/v2/health"):
            get_attempts += 1
            if get_attempts == 1:
                raise httpx.RemoteProtocolError(
                    f"transient {ADMIN_TOKEN}",
                    request=request,
                )
        return api(request)

    result = bootstrap_source_rights(
        base_url=API_BASE,
        admin_token=ADMIN_TOKEN,
        expected_release_sha=RELEASE_SHA,
        code_revision=RELEASE_SHA,
        reason="Human approved metadata-only official source registration.",
        confirmation=CONFIRMATION,
        transport=httpx.MockTransport(transient_get),
        now=datetime(2026, 7, 26, tzinfo=timezone.utc),
    )
    assert result["source_count"] == 2
    # The bootstrap reads health both before and after mutation; one additional
    # call is the bounded retry of the first transient failure.
    assert get_attempts == 3

    api = BootstrapApi()
    post_attempts = 0

    def failed_post(request: httpx.Request) -> httpx.Response:
        nonlocal post_attempts
        if request.method == "POST":
            post_attempts += 1
            raise httpx.RemoteProtocolError(
                f"unsafe retry {ADMIN_TOKEN}",
                request=request,
            )
        return api(request)

    with pytest.raises(SourceRightBootstrapError, match="RemoteProtocolError") as error:
        bootstrap_source_rights(
            base_url=API_BASE,
            admin_token=ADMIN_TOKEN,
            expected_release_sha=RELEASE_SHA,
            code_revision=RELEASE_SHA,
            reason="Human approved metadata-only official source registration.",
            confirmation=CONFIRMATION,
            transport=httpx.MockTransport(failed_post),
            now=datetime(2026, 7, 26, tzinfo=timezone.utc),
        )
    assert post_attempts == 1
    assert ADMIN_TOKEN not in str(error.value)


def test_bootstrap_bounds_get_transport_retries_without_token_leak() -> None:
    calls = 0

    def failed_get(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.RemoteProtocolError(
            f"hostile {ADMIN_TOKEN}",
            request=request,
        )

    with pytest.raises(SourceRightBootstrapError, match="RemoteProtocolError") as error:
        bootstrap_source_rights(
            base_url=API_BASE,
            admin_token=ADMIN_TOKEN,
            expected_release_sha=RELEASE_SHA,
            code_revision=RELEASE_SHA,
            reason="Human approved metadata-only official source registration.",
            confirmation=CONFIRMATION,
            transport=httpx.MockTransport(failed_get),
            now=datetime(2026, 7, 26, tzinfo=timezone.utc),
        )
    assert calls == 3
    assert ADMIN_TOKEN not in str(error.value)
    assert ADMIN_TOKEN not in "".join(
        traceback.format_exception(error.type, error.value, error.tb)
    )


def test_selected_link_rights_require_explicit_valid_allowlists() -> None:
    environment = {
        "CA_OFFICIAL_LINKS_JSON": _selected_links("CA"),
        "AU_OFFICIAL_LINKS_JSON": _selected_links("AU"),
    }

    sources = bootstrap_sources(
        include_ca=True,
        include_au=True,
        environment=environment,
    )

    assert [source.country_code for source in sources] == [
        "KR",
        "US",
        "CA",
        "AU",
    ]
    assert not {"JP", "GB"}.intersection(
        source.country_code for source in sources
    )
    for source in sources[2:]:
        assert source.coverage_mode == "link-only"
        assert source.evidence_uri is None
        assert source.evidence_hash is not None
        assert len(source.evidence_hash) == 64
        assert "Source URLs are not fetched" in source.permission_scope

    with pytest.raises(
        SourceRightBootstrapError,
        match="CA selected-link allowlist",
    ):
        bootstrap_sources(
            include_ca=True,
            include_au=False,
            environment={},
        )


def test_selected_rights_are_registered_only_when_requested() -> None:
    api = BootstrapApi()
    environment = {
        "CA_OFFICIAL_LINKS_JSON": _selected_links("CA"),
        "AU_OFFICIAL_LINKS_JSON": _selected_links("AU"),
    }

    result = _run(
        api,
        include_ca=True,
        include_au=True,
        environment=environment,
    )

    assert result["source_count"] == 4
    assert {
        payload["source_right_id"]
        for path, payload in api.posts
        if path.endswith("/api/v1/admin/source-rights")
    } == {
        "official:dart",
        "official:sec-edgar",
        "official:ca-issuer-ir",
        "official:asic-register",
    }


def test_immutable_identity_mismatch_fails_before_any_write() -> None:
    api = BootstrapApi()
    api.rights["official:dart"] = {
        **_active_right(0),
        "source_key": "not-dart",
    }

    with pytest.raises(
        SourceRightBootstrapError,
        match="identity is immutable and mismatched",
    ):
        _run(api)

    assert not api.posts


@pytest.mark.parametrize(
    ("status", "revoked_at"),
    [
        ("expired", None),
        ("revoked", "2026-07-25T23:00:00Z"),
        ("active", "2026-07-25T23:00:00Z"),
    ],
)
def test_revoked_or_expired_right_fails_before_any_write(
    status: str,
    revoked_at: str | None,
) -> None:
    api = BootstrapApi()
    api.rights["official:dart"] = {
        **_active_right(0),
        "status": status,
        "revoked_at": revoked_at,
    }

    with pytest.raises(SourceRightBootstrapError, match="cannot be bootstrapped"):
        _run(api)

    assert not api.posts


def test_inactive_connector_fails_before_any_write() -> None:
    api = BootstrapApi()
    api.connectors["connector:us:sec-edgar"]["connector_status"] = "inactive"

    with pytest.raises(
        SourceRightBootstrapError,
        match="connector status cannot be bootstrapped",
    ):
        _run(api)

    assert not api.posts


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("permission_scope", "broader grant"),
        ("evidence_uri", "https://example.invalid/evidence"),
        ("valid_until", "2026-07-27T00:00:00Z"),
        ("ai_allowed", True),
        ("redistribution_allowed", False),
    ],
)
def test_conflicting_active_grant_fails_before_any_write(
    field: str,
    value: object,
) -> None:
    api = BootstrapApi()
    api.rights["official:dart"] = {
        **_active_right(0),
        field: value,
    }

    with pytest.raises(
        SourceRightBootstrapError,
        match="conflicts with the fixed metadata-only grant",
    ):
        _run(api)

    assert not api.posts


def test_matching_active_grants_and_running_connectors_are_idempotent() -> None:
    api = BootstrapApi()
    api.rights["official:dart"] = _active_right(0)
    api.rights["official:sec-edgar"] = _active_right(1)
    api.connectors["connector:kr:dart"]["connector_status"] = "active"
    api.connectors["connector:us:sec-edgar"]["connector_status"] = "configured"

    result = _run(api)

    assert api.posts == []
    assert {
        item["country_code"]: item["connector_status"]
        for item in result["sources"]
    } == {"KR": "active", "US": "configured"}


def test_pending_grant_is_safely_replaced_by_the_fixed_grant() -> None:
    api = BootstrapApi()
    api.rights["official:sec-edgar"] = {
        **_active_right(1),
        "permission_scope": "Unapproved migration placeholder",
        "redistribution_allowed": False,
        "status": "pending",
    }

    _run(api)

    sec_posts = [
        payload
        for path, payload in api.posts
        if path.endswith("/api/v1/admin/source-rights")
        and payload["source_right_id"] == "official:sec-edgar"
    ]
    assert len(sec_posts) == 1
    assert sec_posts[0]["permission_scope"] == CORE_SOURCES[1].permission_scope
    assert sec_posts[0]["redistribution_allowed"] is True
    assert sec_posts[0]["status"] == "active"
    assert sec_posts[0]["expected_status"] == "pending"
    assert sec_posts[0]["expected_updated_at"] == "2026-07-25T23:58:00Z"


@pytest.mark.parametrize(
    "unsafe_base",
    [
        "https://ops.example.com/activist/api.php/api/v1",
        "https://alignpe.gabia.io/activist/api.php/api/v1/other",
        "https://alignpe.gabia.io/activist/api.php/api/v1.evil",
        "http://alignpe.gabia.io/activist/api.php/api/v1",
        "https://alignpe.gabia.io/other/api.php/api/v1",
    ],
)
def test_admin_token_is_never_sent_to_a_nonproduction_endpoint(
    unsafe_base: str,
) -> None:
    def no_request(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"network request was not expected: {request.url}")

    with pytest.raises(
        SourceRightBootstrapError,
        match="operational API base URL",
    ):
        bootstrap_source_rights(
            base_url=unsafe_base,
            admin_token=ADMIN_TOKEN,
            expected_release_sha=RELEASE_SHA,
            code_revision=RELEASE_SHA,
            reason="Human approved metadata-only official source registration.",
            confirmation=CONFIRMATION,
            transport=httpx.MockTransport(no_request),
            now=datetime(2026, 7, 26, tzinfo=timezone.utc),
        )


@pytest.mark.parametrize(
    ("expected_sha", "actual_sha", "confirmation"),
    [
        ("not-a-sha", RELEASE_SHA, CONFIRMATION),
        (RELEASE_SHA, "b" * 40, CONFIRMATION),
        (RELEASE_SHA, RELEASE_SHA, "YES"),
    ],
)
def test_explicit_release_identity_is_required_before_network_access(
    expected_sha: str,
    actual_sha: str,
    confirmation: str,
) -> None:
    def no_request(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"network request was not expected: {request.url}")

    with pytest.raises(SourceRightBootstrapError):
        bootstrap_source_rights(
            base_url=API_BASE,
            admin_token=ADMIN_TOKEN,
            expected_release_sha=expected_sha,
            code_revision=actual_sha,
            reason="Human approved metadata-only official source registration.",
            confirmation=confirmation,
            transport=httpx.MockTransport(no_request),
            now=datetime(2026, 7, 26, tzinfo=timezone.utc),
        )


def test_naive_bootstrap_time_is_rejected_before_network_access() -> None:
    api = BootstrapApi()

    with pytest.raises(
        SourceRightBootstrapError,
        match="timezone",
    ):
        _run(api, now=datetime(2026, 7, 26))

    assert not api.calls


def test_release_or_deployment_mismatch_fails_closed_without_writes() -> None:
    api = BootstrapApi()
    api.release_sha = "b" * 40

    with pytest.raises(
        SourceRightBootstrapError,
        match="deployed release identity",
    ):
        _run(api)
    assert not api.posts

    api = BootstrapApi()
    api.v2_state = "preview"
    with pytest.raises(
        SourceRightBootstrapError,
        match="must remain closed",
    ):
        _run(api)
    assert not api.posts


def test_errors_and_results_never_echo_admin_token() -> None:
    def reject(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            500,
            json={"ok": False, "token": ADMIN_TOKEN},
            headers={"X-BSIDE-API-Version": "v2"},
        )

    with pytest.raises(SourceRightBootstrapError) as caught:
        bootstrap_source_rights(
            base_url=API_BASE,
            admin_token=ADMIN_TOKEN,
            expected_release_sha=RELEASE_SHA,
            code_revision=RELEASE_SHA,
            reason="Human approved metadata-only official source registration.",
            confirmation=CONFIRMATION,
            transport=httpx.MockTransport(reject),
            now=datetime(2026, 7, 26, tzinfo=timezone.utc),
        )

    assert ADMIN_TOKEN not in str(caught.value)
    assert tuple(source.country_code for source in CORE_SOURCES) == ("KR", "US")
