from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from typing import Callable
from urllib.parse import urlsplit, urlunsplit

import httpx

from .official_source_contracts import (
    DART_SOURCE_RIGHT_CONTRACT_REVISION,
)


_REVISION_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_RELEASE_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_PRODUCTION_V2_BASE = "https://alignpe.gabia.io/activist/api.php/api/v2"
_MAX_RESPONSE_BYTES = 512_000
_IDEMPOTENT_GET_TRANSPORT_ATTEMPTS = 3
_DART_RELEASE_STATE_BY_PIPELINE_MODE = {
    "dart_canary": "closed",
    "shadow": "preview",
    "live": "live",
}


class OfficialSourceRightError(RuntimeError):
    """An official source cannot be used under the registered rights policy."""


@dataclass(frozen=True)
class OfficialSourceRightEligibility:
    source_right_id: str
    use: str
    rights_revision: str
    checked_at: str | None = None
    source_type: str | None = None
    source_key: str | None = None
    redistribution_allowed: bool = False
    ai_allowed: bool = False
    contract_revision: str | None = None
    release_state: str | None = None




def _validated_api_base_url(raw: str) -> str:
    value = raw.strip()
    if not value:
        return ""
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.netloc or not parsed.hostname:
        raise OfficialSourceRightError(
            "source-right API base URL must be an absolute HTTPS URL"
        )
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise OfficialSourceRightError(
            "source-right API base URL must not contain credentials, a query, or a fragment"
        )
    path = parsed.path.rstrip("/")
    if not path.endswith("/api/v1"):
        path += "/api/v1"
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def _validated_v2_api_base_url(raw: str) -> str:
    value = raw.strip()
    if not value:
        return ""
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.netloc or not parsed.hostname:
        raise OfficialSourceRightError(
            "source-right API base URL must be an absolute HTTPS URL"
        )
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise OfficialSourceRightError(
            "source-right API base URL must not contain credentials, a query, or a fragment"
        )
    path = parsed.path.rstrip("/")
    for suffix in ("/api/v1", "/api/v2"):
        if path.endswith(suffix):
            path = path[: -len(suffix)]
            break
    path += "/api/v2"
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def source_right_api_base_url() -> str:
    for name in (
        "BSIDE_API_BASE_URL",
        "GOVERNANCE_API_BASE_URL",
        "ACTIVIST_API_URL",
    ):
        value = os.environ.get(name, "").strip()
        if value:
            return _validated_api_base_url(value)
    return ""


def source_right_api_token() -> str:
    return os.environ.get("BSIDE_OPS_TOKEN", "").strip()


def source_right_api_configured() -> bool:
    return bool(source_right_api_base_url() and source_right_api_token())


class OfficialSourceRightClient:
    def __init__(
        self,
        *,
        base_url: str | None = None,
        token: str | None = None,
        timeout: float = 15.0,
        transport: httpx.BaseTransport | None = None,
        client_factory: Callable[..., httpx.Client] = httpx.Client,
    ) -> None:
        self.base_url = _validated_api_base_url(
            base_url if base_url is not None else source_right_api_base_url()
        ).rstrip("/")
        self.token = (token if token is not None else source_right_api_token()).strip()
        self.timeout = timeout
        self.transport = transport
        self.client_factory = client_factory
        if not self.base_url or not self.token:
            raise OfficialSourceRightError(
                "KIND ingest requires BSIDE_API_BASE_URL (or GOVERNANCE_API_BASE_URL/"
                "ACTIVIST_API_URL) and BSIDE_OPS_TOKEN"
            )

    @staticmethod
    def _json_object(response: httpx.Response) -> dict[str, object]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise OfficialSourceRightError(
                f"source-right API returned invalid JSON (HTTP {response.status_code})"
            ) from exc
        if not isinstance(payload, dict):
            raise OfficialSourceRightError("source-right API response must be a JSON object")
        return payload

    def check(
        self,
        source_right_id: str,
        *,
        use: str,
    ) -> OfficialSourceRightEligibility:
        if source_right_id != "official:kind" or use != "ingest":
            raise OfficialSourceRightError(
                "the official source-right preflight only permits official:kind ingest"
            )
        try:
            with self.client_factory(timeout=self.timeout, transport=self.transport) as client:
                response = client.get(
                    f"{self.base_url}/ops/source-right-eligibility",
                    params={"source_right_id": source_right_id, "use": use},
                    headers={
                        "Accept": "application/json",
                        "Authorization": f"Bearer {self.token}",
                    },
                )
        except httpx.HTTPError as exc:
            raise OfficialSourceRightError(
                f"source-right API request failed: {type(exc).__name__}"
            ) from exc

        payload = self._json_object(response)
        if response.status_code == 409:
            reasons = payload.get("reasons")
            valid_reasons = (
                [reason for reason in reasons if isinstance(reason, str) and reason]
                if isinstance(reasons, list)
                else []
            )
            if (
                payload.get("ok") is False
                and payload.get("error") == "source_right_ineligible"
                and payload.get("eligible") is False
                and payload.get("source_right_id") == source_right_id
                and payload.get("use") == use
            ):
                detail = ",".join(valid_reasons) if valid_reasons else "unspecified"
                raise OfficialSourceRightError(f"KIND SourceRight is ineligible: {detail}")
        if response.status_code != 200:
            raise OfficialSourceRightError(
                f"source-right API eligibility check failed (HTTP {response.status_code}): "
                f"{payload.get('error') or 'invalid_response'}"
            )

        revision = payload.get("rights_revision")
        if (
            payload.get("ok") is not True
            or payload.get("eligible") is not True
            or payload.get("source_right_id") != source_right_id
            or payload.get("use") != use
            or not isinstance(revision, str)
            or _REVISION_PATTERN.fullmatch(revision) is None
        ):
            raise OfficialSourceRightError(
                "source-right API did not acknowledge the exact KIND ingest eligibility contract"
            )
        checked_at = payload.get("checked_at")
        return OfficialSourceRightEligibility(
            source_right_id=source_right_id,
            use=use,
            rights_revision=revision,
            checked_at=checked_at if isinstance(checked_at, str) and checked_at else None,
        )

    def check_kind_ingest(self) -> OfficialSourceRightEligibility:
        return self.check("official:kind", use="ingest")


class GlobalOfficialSourceRightClient:
    """Authenticated fail-closed eligibility client for global official sources."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        token: str | None = None,
        timeout: float = 15.0,
        transport: httpx.BaseTransport | None = None,
        client_factory: Callable[..., httpx.Client] = httpx.Client,
    ) -> None:
        configured_url = (
            base_url if base_url is not None else source_right_api_base_url()
        )
        self.base_url = _validated_v2_api_base_url(configured_url).rstrip("/")
        self.token = (token if token is not None else source_right_api_token()).strip()
        self.timeout = timeout
        self.transport = transport
        self.client_factory = client_factory
        if not self.base_url or not self.token:
            raise OfficialSourceRightError(
                "global official ingest requires a v2 API URL and BSIDE_OPS_TOKEN"
            )

    def _get_with_transport_retry(
        self,
        client: httpx.Client,
        url: str,
        *,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Retry an idempotent GET on a fresh connection after transport failure."""

        last_error: httpx.TransportError | None = None
        for attempt in range(_IDEMPOTENT_GET_TRANSPORT_ATTEMPTS):
            try:
                if attempt == 0:
                    return client.get(url, params=params, headers=headers)
                with self.client_factory(
                    timeout=self.timeout,
                    transport=self.transport,
                    follow_redirects=False,
                ) as retry_client:
                    return retry_client.get(url, params=params, headers=headers)
            except httpx.TransportError as exc:
                last_error = exc
        if last_error is None:  # pragma: no cover - defensive invariant
            raise RuntimeError("idempotent GET retry exhausted without an error")
        raise last_error

    def check(
        self,
        source_right_id: str,
        *,
        use: str = "collect",
    ) -> OfficialSourceRightEligibility:
        if (
            re.fullmatch(r"official:[a-z0-9_.:-]{1,48}", source_right_id) is None
            or use not in {"collect", "public", "ai"}
        ):
            raise OfficialSourceRightError(
                "unsupported global source-right eligibility request"
            )
        try:
            with self.client_factory(
                timeout=self.timeout,
                transport=self.transport,
            ) as client:
                response = client.get(
                    f"{self.base_url}/ops/source-right-eligibility",
                    params={"source_right_id": source_right_id, "use": use},
                    headers={
                        "Accept": "application/json",
                        "Authorization": f"Bearer {self.token}",
                    },
                )
        except httpx.HTTPError as exc:
            raise OfficialSourceRightError(
                f"source-right API request failed: {type(exc).__name__}"
            ) from exc
        payload = OfficialSourceRightClient._json_object(response)
        revision = payload.get("rights_revision")
        if response.status_code != 200:
            detail = payload.get("error") or "invalid_response"
            raise OfficialSourceRightError(
                f"global source-right eligibility check failed "
                f"(HTTP {response.status_code}): {detail}"
            )
        source_type = payload.get("source_type")
        source_key = payload.get("source_key")
        checked_at_value = payload.get("checked_at")
        checked_at = checked_at_value if isinstance(checked_at_value, str) else None
        if (
            payload.get("ok") is not True
            or payload.get("eligible") is not True
            or payload.get("source_right_id") != source_right_id
            or payload.get("use") != use
            or not isinstance(revision, str)
            or _REVISION_PATTERN.fullmatch(revision) is None
            or not isinstance(source_type, str)
            or not source_type
            or not isinstance(source_key, str)
            or not source_key
        ):
            raise OfficialSourceRightError(
                "source-right API did not acknowledge the exact global eligibility contract"
            )
        return OfficialSourceRightEligibility(
            source_right_id=source_right_id,
            use=use,
            rights_revision=revision,
            checked_at=checked_at,
            source_type=source_type,
            source_key=source_key,
            redistribution_allowed=payload.get("redistribution_allowed") is True,
            ai_allowed=payload.get("ai_allowed") is True,
            contract_revision=(
                str(payload["contract_revision"])
                if isinstance(payload.get("contract_revision"), str)
                and _REVISION_PATTERN.fullmatch(
                    str(payload["contract_revision"])
                )
                else None
            ),
        )


class DartOfficialSourceRightClient(GlobalOfficialSourceRightClient):
    """Exact deployed-release and metadata-only OpenDART write preflight."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        token: str | None = None,
        timeout: float = 15.0,
        transport: httpx.BaseTransport | None = None,
        client_factory: Callable[..., httpx.Client] = httpx.Client,
    ) -> None:
        super().__init__(
            base_url=base_url,
            token=token,
            timeout=timeout,
            transport=transport,
            client_factory=client_factory,
        )
        if self.base_url != _PRODUCTION_V2_BASE:
            raise OfficialSourceRightError(
                "OpenDART apply preflight must use the fixed production v2 endpoint"
            )
        self.v1_base_url = (
            self.base_url[: -len("/api/v2")] + "/api/v1"
        )

    @staticmethod
    def expected_release_sha(value: str | None = None) -> str:
        revision = (
            value
            if value is not None
            else (
                os.environ.get("GITHUB_SHA", "")
                or os.environ.get("CURATOR_CODE_REVISION", "")
            )
        ).strip().casefold()
        if _RELEASE_SHA_PATTERN.fullmatch(revision) is None:
            raise OfficialSourceRightError(
                "OpenDART apply requires the exact 40-character deployed release SHA"
            )
        return revision

    @staticmethod
    def expected_release_state(pipeline_mode: str | None = None) -> str:
        mode = (
            pipeline_mode
            if pipeline_mode is not None
            else os.environ.get("GOVERNANCE_PIPELINE_MODE", "")
        ).strip().casefold()
        expected = _DART_RELEASE_STATE_BY_PIPELINE_MODE.get(mode)
        if expected is None:
            raise OfficialSourceRightError(
                "OpenDART apply requires GOVERNANCE_PIPELINE_MODE to resolve "
                "exactly to dart_canary, shadow, or live"
            )
        return expected

    @staticmethod
    def _require_v2_json(
        response: httpx.Response,
        *,
        operation: str,
    ) -> dict[str, object]:
        if len(response.content) > _MAX_RESPONSE_BYTES:
            raise OfficialSourceRightError(
                f"{operation} response exceeded the fail-closed byte limit"
            )
        if response.headers.get("X-BSIDE-API-Version") != "v2":
            raise OfficialSourceRightError(f"{operation} API identity is invalid")
        if "application/json" not in response.headers.get(
            "content-type", ""
        ).casefold():
            raise OfficialSourceRightError(f"{operation} content type is invalid")
        payload = OfficialSourceRightClient._json_object(response)
        if response.status_code != 200 or payload.get("ok") is not True:
            raise OfficialSourceRightError(
                f"{operation} failed (HTTP {response.status_code}): "
                f"{payload.get('error') or 'invalid_response'}"
            )
        return payload

    @staticmethod
    def _require_v1_json(
        response: httpx.Response,
        *,
        operation: str,
    ) -> dict[str, object]:
        if len(response.content) > _MAX_RESPONSE_BYTES:
            raise OfficialSourceRightError(
                f"{operation} response exceeded the fail-closed byte limit"
            )
        if response.headers.get("X-BSIDE-API-Version") != "v1":
            raise OfficialSourceRightError(f"{operation} API identity is invalid")
        if "application/json" not in response.headers.get(
            "content-type", ""
        ).casefold():
            raise OfficialSourceRightError(f"{operation} content type is invalid")
        return OfficialSourceRightClient._json_object(response)

    def preflight(
        self,
        expected_release_sha: str | None = None,
        pipeline_mode: str | None = None,
    ) -> OfficialSourceRightEligibility:
        expected = self.expected_release_sha(expected_release_sha)
        expected_release_state = self.expected_release_state(pipeline_mode)
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
            "Cache-Control": "no-cache",
            "Connection": "close",
        }
        public_headers = {
            "Accept": "application/json",
            "Cache-Control": "no-cache",
            "Connection": "close",
        }
        try:
            with self.client_factory(
                timeout=self.timeout,
                transport=self.transport,
                follow_redirects=False,
            ) as client:
                health_response = self._get_with_transport_retry(
                    client,
                    f"{self.base_url}/health",
                    headers=headers,
                )
                health = self._require_v2_json(
                    health_response,
                    operation="OpenDART deployed-release preflight",
                )
                if (
                    health.get("service") != "bside-global-market-terminal"
                    or health.get("schema_version") != 12
                    or health.get("code_revision") != expected
                ):
                    raise OfficialSourceRightError(
                        "OpenDART deployed release does not match the exact SHA/schema 12"
                    )

                v1_health_response = self._get_with_transport_retry(
                    client,
                    f"{self.v1_base_url}/health",
                    headers=public_headers,
                )
                v1_health = self._require_v1_json(
                    v1_health_response,
                    operation="OpenDART v1 identity preflight",
                )
                if (
                    v1_health_response.status_code != 200
                    or v1_health.get("ok") is not True
                    or v1_health.get("service")
                    != "bside-governance-intelligence"
                    or v1_health.get("api_version") != "v1"
                ):
                    raise OfficialSourceRightError(
                        "OpenDART v1 API identity is invalid"
                    )

                # This intentionally has no Authorization header. Its exact
                # public contract independently proves the v1 state without
                # granting the collector admin or preview access.
                v1_events_response = self._get_with_transport_retry(
                    client,
                    f"{self.v1_base_url}/events",
                    params={"limit": "1"},
                    headers=public_headers,
                )
                v1_events = self._require_v1_json(
                    v1_events_response,
                    operation="OpenDART v1 release-state preflight",
                )
                v1_contract_matches = False
                if expected_release_state == "closed":
                    v1_contract_matches = (
                        v1_events_response.status_code == 503
                        and v1_events.get("ok") is False
                        and v1_events.get("error") == "governance_release_closed"
                    )
                elif expected_release_state == "preview":
                    v1_contract_matches = (
                        v1_events_response.status_code in {401, 403}
                        and v1_events.get("ok") is False
                        and v1_events.get("error")
                        in {"preview_token_required", "invalid_preview_token"}
                    )
                elif expected_release_state == "live":
                    v1_contract_matches = (
                        v1_events_response.status_code == 200
                        and v1_events.get("ok") is True
                    )
                if (
                    not v1_contract_matches
                    or v1_events.get("api_version") != "v1"
                ):
                    raise OfficialSourceRightError(
                        "OpenDART v1 release state does not match the exact "
                        f"{expected_release_state} contract"
                    )

                state_response = self._get_with_transport_retry(
                    client,
                    f"{self.base_url}/ops/release-state",
                    headers=headers,
                )
                state = self._require_v2_json(
                    state_response,
                    operation="OpenDART release-state preflight",
                )
                state_data = state.get("data")
                if (
                    not isinstance(state_data, dict)
                    or state_data.get("release_state")
                    != expected_release_state
                ):
                    raise OfficialSourceRightError(
                        "OpenDART v2 release state does not match the exact "
                        f"{expected_release_state} contract"
                    )

                eligibility_response = self._get_with_transport_retry(
                    client,
                    f"{self.base_url}/ops/source-right-eligibility",
                    params={
                        "source_right_id": "official:dart",
                        "use": "collect",
                    },
                    headers=headers,
                )
                eligibility_payload = self._require_v2_json(
                    eligibility_response,
                    operation="OpenDART SourceRight preflight",
                )
        except httpx.HTTPError as exc:
            raise OfficialSourceRightError(
                f"OpenDART apply preflight request failed: {type(exc).__name__}"
            ) from None

        revision = eligibility_payload.get("rights_revision")
        contract_revision = eligibility_payload.get("contract_revision")
        connector_ready = eligibility_payload.get("connector_ready")
        if (
            eligibility_payload.get("eligible") is not True
            or eligibility_payload.get("source_right_id") != "official:dart"
            or eligibility_payload.get("source_type") != "official_disclosure"
            or eligibility_payload.get("source_key") != "dart"
            or eligibility_payload.get("use") != "collect"
            or eligibility_payload.get("redistribution_allowed") is not True
            or eligibility_payload.get("ai_allowed") is not False
            or not isinstance(revision, str)
            or _REVISION_PATTERN.fullmatch(revision) is None
            or contract_revision != DART_SOURCE_RIGHT_CONTRACT_REVISION
        ):
            raise OfficialSourceRightError(
                "OpenDART SourceRight does not match the protected metadata-only contract"
            )
        if (
            eligibility_payload.get("connector_id") != "connector:kr:dart"
            or connector_ready is not True
        ):
            raise OfficialSourceRightError(
                "OpenDART connector is not configured for collection"
            )
        checked_at = eligibility_payload.get("checked_at")
        return OfficialSourceRightEligibility(
            source_right_id="official:dart",
            use="collect",
            rights_revision=revision,
            checked_at=checked_at if isinstance(checked_at, str) else None,
            source_type="official_disclosure",
            source_key="dart",
            redistribution_allowed=True,
            ai_allowed=False,
            contract_revision=DART_SOURCE_RIGHT_CONTRACT_REVISION,
            release_state=expected_release_state,
        )


def preflight_dart_source_right(
    expected_release_sha: str | None = None,
    pipeline_mode: str | None = None,
) -> OfficialSourceRightEligibility:
    return DartOfficialSourceRightClient().preflight(
        expected_release_sha,
        pipeline_mode,
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Fail-closed exact OpenDART SourceRight apply preflight."
    )
    parser.add_argument("--preflight-dart", action="store_true")
    parser.add_argument("--expected-release-sha")
    parser.add_argument("--pipeline-mode")
    args = parser.parse_args(argv)
    if not args.preflight_dart:
        parser.error("--preflight-dart is required")
    preflight_dart_source_right(
        args.expected_release_sha,
        args.pipeline_mode,
    )


if __name__ == "__main__":
    main()
