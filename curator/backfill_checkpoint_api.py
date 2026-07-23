from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Callable
from urllib.parse import urlsplit, urlunsplit

import httpx


class RemoteCheckpointError(RuntimeError):
    """The authoritative MySQL checkpoint could not be read or acknowledged."""


class RemoteCheckpointConflictError(RemoteCheckpointError):
    def __init__(self, *, expected_version: int, actual_version: int) -> None:
        self.expected_version = expected_version
        self.actual_version = actual_version
        super().__init__(
            "remote backfill checkpoint version conflict: "
            f"expected {expected_version}, actual {actual_version}"
        )


@dataclass(frozen=True)
class RemoteCheckpointSnapshot:
    checkpoint: dict[str, object] | None
    version: int
    payload_hash: str | None = None


@dataclass(frozen=True)
class RemoteCheckpointWrite:
    version: int
    payload_hash: str
    unchanged: bool


def _validated_api_base_url(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return ""
    parsed = urlsplit(raw)
    if parsed.scheme != "https" or not parsed.netloc:
        raise RemoteCheckpointError("checkpoint API base URL must be an absolute HTTPS URL")
    if parsed.query or parsed.fragment or parsed.username or parsed.password:
        raise RemoteCheckpointError(
            "checkpoint API base URL must not contain credentials, a query, or a fragment"
        )
    path = parsed.path.rstrip("/")
    if not path.endswith("/api/v1"):
        path += "/api/v1"
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def checkpoint_api_base_url() -> str:
    explicit = os.environ.get("BSIDE_API_BASE_URL", "").strip()
    legacy = os.environ.get("ACTIVIST_API_URL", "").strip()
    return _validated_api_base_url(explicit or legacy)


def checkpoint_api_token() -> str:
    return os.environ.get("BSIDE_OPS_TOKEN", "").strip()


def checkpoint_api_configured() -> bool:
    return bool(checkpoint_api_base_url() and checkpoint_api_token())


def canonical_checkpoint(checkpoint: dict[str, object]) -> dict[str, object]:
    encoded = json.dumps(
        checkpoint,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    decoded = json.loads(encoded)
    if not isinstance(decoded, dict):  # pragma: no cover - guarded by the input type
        raise RemoteCheckpointError("checkpoint must serialize to a JSON object")
    return decoded


def checkpoint_payload_bytes(
    checkpoint: dict[str, object],
    *,
    sort_keys: bool = False,
) -> bytes:
    return json.dumps(
        checkpoint,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=sort_keys,
    ).encode("utf-8")


def checkpoint_payload_hash(
    checkpoint: dict[str, object],
    *,
    sort_keys: bool = False,
) -> str:
    return hashlib.sha256(
        checkpoint_payload_bytes(checkpoint, sort_keys=sort_keys)
    ).hexdigest()


class RemoteCheckpointClient:
    def __init__(
        self,
        *,
        base_url: str | None = None,
        token: str | None = None,
        timeout: float = 30.0,
        transport: httpx.BaseTransport | None = None,
        client_factory: Callable[..., httpx.Client] = httpx.Client,
    ) -> None:
        self.base_url = _validated_api_base_url(
            base_url if base_url is not None else checkpoint_api_base_url()
        ).rstrip("/")
        self.token = (token if token is not None else checkpoint_api_token()).strip()
        self.timeout = timeout
        self.transport = transport
        self.client_factory = client_factory
        if not self.base_url or not self.token:
            raise RemoteCheckpointError(
                "remote checkpoint requires BSIDE_API_BASE_URL (or ACTIVIST_API_URL) "
                "and BSIDE_OPS_TOKEN"
            )

    @staticmethod
    def _validate_fingerprint(fingerprint: str) -> None:
        if len(fingerprint) != 64 or any(char not in "0123456789abcdef" for char in fingerprint):
            raise RemoteCheckpointError("backfill job fingerprint must be 64 lowercase hex characters")

    def _url(self, fingerprint: str) -> str:
        self._validate_fingerprint(fingerprint)
        return f"{self.base_url}/ops/backfill-checkpoints/{fingerprint}"

    def _headers(self, *, content: bool = False) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        if content:
            headers["Content-Type"] = "application/json; charset=utf-8"
        return headers

    @staticmethod
    def _json_object(response: httpx.Response) -> dict[str, object]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise RemoteCheckpointError(
                f"checkpoint API returned invalid JSON (HTTP {response.status_code})"
            ) from exc
        if not isinstance(payload, dict):
            raise RemoteCheckpointError("checkpoint API response must be a JSON object")
        return payload

    def get(self, fingerprint: str) -> RemoteCheckpointSnapshot:
        with self.client_factory(timeout=self.timeout, transport=self.transport) as client:
            response = client.get(self._url(fingerprint), headers=self._headers())
        payload = self._json_object(response)
        if response.status_code == 404 and payload.get("error") == "backfill_checkpoint_not_found":
            return RemoteCheckpointSnapshot(checkpoint=None, version=0)
        if response.status_code != 200 or payload.get("ok") is not True:
            raise RemoteCheckpointError(
                f"checkpoint GET failed (HTTP {response.status_code}): "
                f"{payload.get('error') or 'unknown_error'}"
            )
        if payload.get("job_fingerprint") != fingerprint:
            raise RemoteCheckpointError("checkpoint GET acknowledged a different job fingerprint")
        checkpoint = payload.get("checkpoint")
        version = payload.get("checkpoint_version")
        payload_hash = str(payload.get("payload_hash") or "")
        if not isinstance(checkpoint, dict) or not isinstance(version, int) or version < 1:
            raise RemoteCheckpointError("checkpoint GET response is missing checkpoint/version")
        if len(payload_hash) != 64 or checkpoint_payload_hash(checkpoint) != payload_hash:
            raise RemoteCheckpointError("checkpoint GET payload hash does not match the checkpoint")
        return RemoteCheckpointSnapshot(
            checkpoint=checkpoint,
            version=version,
            payload_hash=payload_hash,
        )

    def put(
        self,
        fingerprint: str,
        *,
        expected_version: int,
        checkpoint: dict[str, object],
    ) -> RemoteCheckpointWrite:
        if expected_version < 0:
            raise RemoteCheckpointError("expected checkpoint version cannot be negative")
        normalized = canonical_checkpoint(checkpoint)
        local_hash = checkpoint_payload_hash(normalized)
        request_payload = {
            "expected_version": expected_version,
            "checkpoint": normalized,
        }
        body = json.dumps(
            request_payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        with self.client_factory(timeout=self.timeout, transport=self.transport) as client:
            response = client.put(
                self._url(fingerprint),
                content=body,
                headers=self._headers(content=True),
            )
        payload = self._json_object(response)
        if response.status_code == 409 and payload.get("error") == "backfill_checkpoint_version_conflict":
            actual = payload.get("actual_version")
            if not isinstance(actual, int) or actual < 0:
                raise RemoteCheckpointError("checkpoint conflict response is missing actual_version")
            raise RemoteCheckpointConflictError(
                expected_version=expected_version,
                actual_version=actual,
            )
        if response.status_code not in {200, 201} or payload.get("ok") is not True:
            raise RemoteCheckpointError(
                f"checkpoint PUT failed (HTTP {response.status_code}): "
                f"{payload.get('error') or 'unknown_error'}"
            )
        if payload.get("job_fingerprint") != fingerprint:
            raise RemoteCheckpointError("checkpoint PUT acknowledged a different job fingerprint")
        version = payload.get("checkpoint_version")
        remote_hash = str(payload.get("payload_hash") or "")
        unchanged = payload.get("unchanged")
        if not isinstance(version, int) or not isinstance(unchanged, bool):
            raise RemoteCheckpointError("checkpoint PUT response is missing version/unchanged")
        expected_ack_version = expected_version if unchanged else expected_version + 1
        if version != expected_ack_version:
            raise RemoteCheckpointError(
                "checkpoint PUT returned an inconsistent checkpoint version: "
                f"expected {expected_ack_version}, got {version}"
            )
        if remote_hash != local_hash:
            raise RemoteCheckpointError("checkpoint PUT payload hash acknowledgment mismatch")
        return RemoteCheckpointWrite(
            version=version,
            payload_hash=remote_hash,
            unchanged=unchanged,
        )
