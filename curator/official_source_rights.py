from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Callable
from urllib.parse import urlsplit, urlunsplit

import httpx


_REVISION_PATTERN = re.compile(r"^[0-9a-f]{64}$")


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
        )
