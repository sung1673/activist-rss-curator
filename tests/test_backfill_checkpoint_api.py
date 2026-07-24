from __future__ import annotations

import hashlib
import json

import httpx
import pytest

from curator.backfill_checkpoint_api import (
    RemoteCheckpointClient,
    RemoteCheckpointConflictError,
    RemoteCheckpointError,
    checkpoint_api_base_url,
)


FINGERPRINT = "a" * 64
TOKEN = "ops-token-for-tests"


def checkpoint() -> dict[str, object]:
    return {
        "schema_version": 1,
        "job": {"fingerprint": FINGERPRINT, "range_start": "2021-01-01"},
        "created_at": "2026-07-22T00:00:00+00:00",
        "updated_at": "2026-07-22T00:00:00+00:00",
        "company_master_synced": False,
        "completed_windows": {},
        "failed_windows": {},
    }


def php_style_hash(value: dict[str, object]) -> str:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def test_remote_checkpoint_create_get_and_same_payload_are_idempotent() -> None:
    state: dict[str, object] = {}
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["Authorization"] == f"Bearer {TOKEN}"
        assert request.url.path.endswith(f"/ops/backfill-checkpoints/{FINGERPRINT}")
        if request.method == "GET":
            if not state:
                return httpx.Response(
                    404,
                    json={
                        "ok": False,
                        "error": "backfill_checkpoint_not_found",
                        "job_fingerprint": FINGERPRINT,
                    },
                )
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "job_fingerprint": FINGERPRINT,
                    "checkpoint_version": state["version"],
                    "payload_hash": state["payload_hash"],
                    "checkpoint": state["checkpoint"],
                },
            )
        payload = json.loads(request.content)
        expected = payload["expected_version"]
        actual = int(state.get("version") or 0)
        if expected != actual:
            return httpx.Response(
                409,
                json={
                    "ok": False,
                    "error": "backfill_checkpoint_version_conflict",
                    "expected_version": expected,
                    "actual_version": actual,
                },
            )
        submitted = payload["checkpoint"]
        submitted_hash = php_style_hash(submitted)
        if state and state["payload_hash"] == submitted_hash:
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "job_fingerprint": FINGERPRINT,
                    "checkpoint_version": actual,
                    "payload_hash": submitted_hash,
                    "unchanged": True,
                },
            )
        state.update(
            version=actual + 1,
            checkpoint=submitted,
            payload_hash=submitted_hash,
        )
        return httpx.Response(
            201 if actual == 0 else 200,
            json={
                "ok": True,
                "job_fingerprint": FINGERPRINT,
                "checkpoint_version": actual + 1,
                "payload_hash": submitted_hash,
                "unchanged": False,
            },
        )

    client = RemoteCheckpointClient(
        base_url="https://api.example.test/activist/api.php/api/v1",
        token=TOKEN,
        transport=httpx.MockTransport(handler),
    )
    missing = client.get(FINGERPRINT)
    assert missing.checkpoint is None and missing.version == 0

    created = client.put(FINGERPRINT, expected_version=0, checkpoint=checkpoint())
    assert created.version == 1 and created.unchanged is False
    unchanged = client.put(FINGERPRINT, expected_version=1, checkpoint=checkpoint())
    assert unchanged.version == 1 and unchanged.unchanged is True

    loaded = client.get(FINGERPRINT)
    assert loaded.version == 1
    assert loaded.checkpoint == state["checkpoint"]
    assert [request.method for request in requests] == ["GET", "PUT", "PUT", "GET"]


def test_remote_checkpoint_409_exposes_actual_version() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            json={
                "ok": False,
                "error": "backfill_checkpoint_version_conflict",
                "expected_version": 2,
                "actual_version": 3,
            },
        )

    client = RemoteCheckpointClient(
        base_url="https://api.example.test/api/v1",
        token=TOKEN,
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(RemoteCheckpointConflictError) as raised:
        client.put(FINGERPRINT, expected_version=2, checkpoint=checkpoint())
    assert raised.value.expected_version == 2
    assert raised.value.actual_version == 3


def test_remote_checkpoint_rejects_inexact_payload_hash_ack() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            201,
            json={
                "ok": True,
                "job_fingerprint": FINGERPRINT,
                "checkpoint_version": 1,
                "payload_hash": "0" * 64,
                "unchanged": False,
            },
        )

    client = RemoteCheckpointClient(
        base_url="https://api.example.test/api/v1",
        token=TOKEN,
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(RemoteCheckpointError, match="hash acknowledgment mismatch"):
        client.put(FINGERPRINT, expected_version=0, checkpoint=checkpoint())


def test_checkpoint_api_base_derives_v1_path_without_query_credentials(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.delenv("BSIDE_API_BASE_URL", raising=False)
    monkeypatch.setenv("ACTIVIST_API_URL", "https://api.example.test/activist/api.php")
    assert checkpoint_api_base_url() == "https://api.example.test/activist/api.php/api/v1"

    monkeypatch.setenv("BSIDE_API_BASE_URL", "https://api.example.test/api/v1?token=bad")
    with pytest.raises(RemoteCheckpointError, match="query"):
        checkpoint_api_base_url()


@pytest.mark.parametrize(
    "base_url",
    (
        "http://api.example.test/activist/api.php/api/v1",
        "https://user:pass@api.example.test/activist/api.php/api/v1",
        "https://api.example.test/activist/api.php?debug=1",
    ),
)
def test_explicit_checkpoint_client_base_url_cannot_bypass_validation(base_url: str) -> None:
    with pytest.raises(RemoteCheckpointError):
        RemoteCheckpointClient(base_url=base_url, token=TOKEN)


def test_explicit_checkpoint_client_base_url_is_normalized_to_v1() -> None:
    client = RemoteCheckpointClient(
        base_url="https://api.example.test/activist/api.php",
        token=f"  {TOKEN}  ",
    )
    assert client.base_url == "https://api.example.test/activist/api.php/api/v1"
    assert client.token == TOKEN
