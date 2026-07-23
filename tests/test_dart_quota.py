from __future__ import annotations

import json
from datetime import datetime, timezone

import httpx
import pytest

from curator.dart_quota import (
    DartQuotaClient,
    DartQuotaLedgerError,
    durable_dart_quota_configured,
    durable_dart_quota_required,
)
from curator.official_sources import DartConnector


REVISION = "a" * 40
NOW = datetime(2026, 7, 23, 3, 0, tzinfo=timezone.utc)  # 2026-07-23 KST


def _ack(body: dict[str, object], *, used: int, duplicate: bool = False) -> dict[str, object]:
    return {
        "ok": True,
        "action": body["action"],
        "attempt_id": body["attempt_id"],
        "quota_day": body["quota_day"],
        "accepted": 1,
        "limit_count": 10_000,
        "used_count": used,
        "remaining_count": 10_000 - used,
        "duplicate": duplicate,
        "blocked_until": (
            "2026-07-24T00:00:00+09:00" if body["action"] == "block_020" else None
        ),
    }


def _client(handler, **overrides: object) -> DartQuotaClient:
    return DartQuotaClient(
        base_url="https://api.example.test/activist/api.php/api/v1",
        token="ops-token",
        code_revision=REVISION,
        phase="test",
        transport=httpx.MockTransport(handler),
        now_provider=lambda: NOW,
        sleeper=lambda _delay: None,
        run_prefix="gha-123-2-ingest-test",
        **overrides,
    )


def test_consume_retries_lost_ack_with_same_attempt_id() -> None:
    bodies: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        bodies.append(body)
        if len(bodies) == 1:
            return httpx.Response(503, json={"error": "temporary"})
        return httpx.Response(200, json=_ack(body, used=1, duplicate=True))

    client = _client(handler)
    permit = client.consume(operation="list")

    assert len(bodies) == 2
    assert bodies[0] == bodies[1]
    assert bodies[0] == {
        "action": "consume",
        "attempt_id": "gha-123-2-ingest-test-00000001",
        "quota_day": "2026-07-23",
        "operation": "list",
        "code_revision": REVISION,
    }
    assert permit.attempt_id == bodies[0]["attempt_id"]
    assert permit.duplicate is True
    assert client.used == 1


def test_each_physical_dart_retry_consumes_a_new_global_attempt() -> None:
    quota_bodies: list[dict[str, object]] = []
    dart_calls = 0

    def quota_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        quota_bodies.append(body)
        return httpx.Response(200, json=_ack(body, used=len(quota_bodies)))

    def dart_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal dart_calls
        dart_calls += 1
        if dart_calls == 1:
            return httpx.Response(503)
        return httpx.Response(
            200,
            json={"status": "013", "message": "no data"},
        )

    quota = _client(quota_handler)
    with httpx.Client(transport=httpx.MockTransport(dart_handler)) as dart_http:
        connector = DartConnector(
            "x" * 40,
            client=dart_http,
            governance_detail_codes=(),
            request_budget=quota,
            max_retries=1,
            sleeper=lambda _delay: None,
        )
        assert list(
            connector.iter_disclosure_rows(
                datetime(2026, 7, 22).date(), datetime(2026, 7, 22).date()
            )
        ) == []

    assert dart_calls == 2
    assert [row["attempt_id"] for row in quota_bodies] == [
        "gha-123-2-ingest-test-00000001",
        "gha-123-2-ingest-test-00000002",
    ]
    assert quota.used == 2


def test_quota_api_failure_prevents_physical_dart_request() -> None:
    dart_calls = 0

    def quota_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": "database_unavailable"})

    def dart_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal dart_calls
        dart_calls += 1
        return httpx.Response(200, json={"status": "013"})

    quota = _client(quota_handler, max_ack_retries=1)
    with httpx.Client(transport=httpx.MockTransport(dart_handler)) as dart_http:
        connector = DartConnector(
            "x" * 40,
            client=dart_http,
            governance_detail_codes=(),
            request_budget=quota,
        )
        with pytest.raises(DartQuotaLedgerError, match="was not sent"):
            list(
                connector.iter_disclosure_rows(
                    datetime(2026, 7, 22).date(), datetime(2026, 7, 22).date()
                )
            )

    assert dart_calls == 0
    assert quota.used == 0


def test_status_020_must_receive_durable_block_ack() -> None:
    actions: list[str] = []

    def quota_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        actions.append(str(body["action"]))
        used = 1
        return httpx.Response(200, json=_ack(body, used=used))

    quota = _client(quota_handler)
    with httpx.Client(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200, json={"status": "020", "message": "quota exceeded"}
            )
        )
    ) as dart_http:
        connector = DartConnector(
            "x" * 40,
            client=dart_http,
            governance_detail_codes=(),
            request_budget=quota,
        )
        with pytest.raises(Exception, match="quota exhausted"):
            list(
                connector.iter_disclosure_rows(
                    datetime(2026, 7, 22).date(), datetime(2026, 7, 22).date()
                )
            )

    assert actions == ["consume", "block_020"]


def test_incomplete_ack_fails_closed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        payload = _ack(body, used=1)
        payload.pop("accepted")
        return httpx.Response(200, json=payload)

    with pytest.raises(DartQuotaLedgerError, match="accepted=1"):
        _client(handler).consume(operation="corp_code")


def test_generated_attempt_id_always_fits_server_column(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        observed.append(str(body["attempt_id"]))
        return httpx.Response(200, json=_ack(body, used=1))

    monkeypatch.setenv("GITHUB_RUN_ID", "9" * 30)
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "123")
    monkeypatch.setenv("GITHUB_JOB", "very-long-job-" * 8)
    quota = DartQuotaClient(
        base_url="https://api.example.test/api/v1",
        token="ops-token",
        code_revision=REVISION,
        phase="very-long-phase-" * 8,
        transport=httpx.MockTransport(handler),
        now_provider=lambda: NOW,
    )
    quota.consume()

    assert len(observed[0]) <= 96
    assert observed[0].endswith("-00000001")


def test_restarted_client_in_same_github_job_gets_a_new_process_nonce(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        observed.append(str(body["attempt_id"]))
        return httpx.Response(200, json=_ack(body, used=len(observed)))

    monkeypatch.setenv("GITHUB_RUN_ID", "123456")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    monkeypatch.setenv("GITHUB_JOB", "ingest")
    kwargs = {
        "base_url": "https://api.example.test/api/v1",
        "token": "ops-token",
        "code_revision": REVISION,
        "phase": "official-ingest",
        "transport": httpx.MockTransport(handler),
        "now_provider": lambda: NOW,
    }
    DartQuotaClient(**kwargs).consume()  # type: ignore[arg-type]
    DartQuotaClient(**kwargs).consume()  # type: ignore[arg-type]

    assert len(observed) == 2
    assert observed[0] != observed[1]
    assert all(value.startswith("gha-123456-1-ingest-official-ingest-") for value in observed)
    assert all(value.endswith("-00000001") for value in observed)


def test_github_actions_and_partial_api_config_force_durable_fail_closed_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CURATOR_REQUIRE_DURABLE_DART_QUOTA", raising=False)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    assert durable_dart_quota_required() is True

    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.setenv("BSIDE_OPS_TOKEN", "partial-config")
    assert durable_dart_quota_configured() is True


@pytest.mark.parametrize(
    "base_url",
    (
        "http://api.example.test/api/v1",
        "https://user:secret@api.example.test/api/v1",
        "https://api.example.test/api/v1?token=secret",
    ),
)
def test_quota_url_must_be_credential_safe_https(base_url: str) -> None:
    with pytest.raises(DartQuotaLedgerError, match="absolute HTTPS"):
        DartQuotaClient(
            base_url=base_url,
            token="ops-token",
            code_revision=REVISION,
        )
