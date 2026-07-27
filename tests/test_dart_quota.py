from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

import curator.dart_quota as dart_quota_module
from curator.dart_quota import (
    DartCredentialUnavailableError,
    DartGlobalQuotaExceededError,
    DartQuotaClient,
    DartQuotaLedgerError,
    DartQuotaLedgerRejectedError,
    durable_dart_quota_configured,
    durable_dart_quota_required,
)
from curator.official_sources import DartConnector


REVISION = "a" * 40
BINDING_ID = "b" * 64
CREDENTIAL_ID = "c" * 64
NOW = datetime(2026, 7, 23, 3, 0, tzinfo=timezone.utc)  # 2026-07-23 KST


def _ack(body: dict[str, object], *, used: int, duplicate: bool = False) -> dict[str, object]:
    return {
        "ok": True,
        "action": body["action"],
        "attempt_id": body["attempt_id"],
        "quota_day": body["quota_day"],
        "credential_id": body["credential_id"],
        "backend_binding_id": BINDING_ID,
        "accepted": 1,
        "limit_count": 40_000,
        "used_count": used,
        "remaining_count": 40_000 - used,
        "credential_limit_count": 40_000,
        "credential_used_count": used,
        "credential_remaining_count": 40_000 - used,
        "credential_status": (
            "disabled_901" if body["action"] == "disable_901" else "active"
        ),
        "credential_blocked_until": (
            "2026-07-24T00:00:00+09:00"
            if body["action"] == "block_020"
            else None
        ),
        "duplicate": duplicate,
        "blocked_until": (
            "2026-07-24T00:00:00+09:00" if body["action"] == "block_020" else None
        ),
    }


class _DurableAckServer:
    def __init__(self) -> None:
        self.bodies: list[dict[str, object]] = []
        self._seen: set[tuple[str, str]] = set()
        self.used = 0

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body: dict[str, object] = json.loads(request.content)
        self.bodies.append(body)
        identity = (str(body["action"]), str(body["attempt_id"]))
        duplicate = identity in self._seen
        if not duplicate:
            self._seen.add(identity)
            if body["action"] == "consume":
                self.used += 1
        return httpx.Response(
            200,
            json=_ack(body, used=self.used, duplicate=duplicate),
        )


def _client(handler, **overrides: object) -> DartQuotaClient:
    return DartQuotaClient(
        base_url="https://api.example.test/activist/api.php/api/v1",
        token="ops-token",
        backend_binding_id=BINDING_ID,
        credential_id=CREDENTIAL_ID,
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

    assert len(bodies) == 3
    assert bodies[0] == bodies[1] == bodies[2]
    assert bodies[0] == {
        "action": "consume",
        "attempt_id": "gha-123-2-ingest-test-00000001",
        "quota_day": "2026-07-23",
        "credential_id": CREDENTIAL_ID,
        "operation": "list",
        "code_revision": REVISION,
        "expected_backend_binding_id": BINDING_ID,
    }
    assert permit.attempt_id == bodies[0]["attempt_id"]
    assert permit.duplicate is True
    assert client.used == 1


def test_consume_requires_an_independent_duplicate_replay_before_dart_request() -> None:
    quota_bodies: list[dict[str, object]] = []
    dart_calls = 0

    def quota_handler(request: httpx.Request) -> httpx.Response:
        body: dict[str, object] = json.loads(request.content)
        quota_bodies.append(body)
        return httpx.Response(200, json=_ack(body, used=1, duplicate=False))

    def dart_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal dart_calls
        dart_calls += 1
        return httpx.Response(200, json={"status": "013"})

    quota = _client(quota_handler)
    with httpx.Client(transport=httpx.MockTransport(dart_handler)) as dart_http:
        connector = DartConnector(
            "x" * 40,
            client=dart_http,
            governance_detail_codes=(),
            request_budget=quota,
        )
        with pytest.raises(DartQuotaLedgerError, match="explicit replay"):
            list(
                connector.iter_disclosure_rows(
                    datetime(2026, 7, 22).date(),
                    datetime(2026, 7, 22).date(),
                )
            )

    assert len(quota_bodies) == 2
    assert quota_bodies[0] == quota_bodies[1]
    assert quota.used == 0
    assert dart_calls == 0


@pytest.mark.parametrize("malformed_ack_number", (1, 2))
@pytest.mark.parametrize(
    ("malformed_field", "malformed_value"),
    (
        ("credential_status", "disabled_901"),
        ("blocked_until", "2026-07-24T00:00:00+09:00"),
        ("credential_blocked_until", "2026-07-24T00:00:00+09:00"),
    ),
)
def test_malformed_consume_credential_ack_prevents_physical_dart_request(
    malformed_ack_number: int,
    malformed_field: str,
    malformed_value: str,
) -> None:
    quota_calls = 0
    dart_calls = 0

    def quota_handler(request: httpx.Request) -> httpx.Response:
        nonlocal quota_calls
        quota_calls += 1
        body: dict[str, object] = json.loads(request.content)
        payload = _ack(body, used=1, duplicate=quota_calls > 1)
        if quota_calls == malformed_ack_number:
            payload[malformed_field] = malformed_value
        return httpx.Response(200, json=payload)

    def dart_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal dart_calls
        dart_calls += 1
        return httpx.Response(200, json={"status": "013"})

    quota = _client(quota_handler)
    with httpx.Client(transport=httpx.MockTransport(dart_handler)) as dart_http:
        connector = DartConnector(
            "x" * 40,
            client=dart_http,
            governance_detail_codes=(),
            request_budget=quota,
        )
        with pytest.raises(DartQuotaLedgerError):
            list(
                connector.iter_disclosure_rows(
                    datetime(2026, 7, 22).date(),
                    datetime(2026, 7, 22).date(),
                )
            )

    assert quota_calls == malformed_ack_number
    assert quota.used == 0
    assert dart_calls == 0


def test_consume_rejects_counter_regression_during_explicit_replay() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        body: dict[str, object] = json.loads(request.content)
        return httpx.Response(
            200,
            json=_ack(
                body,
                used=2 if calls == 1 else 1,
                duplicate=calls == 2,
            ),
        )

    quota = _client(handler)
    with pytest.raises(DartQuotaLedgerError, match="counters regressed"):
        quota.consume()
    assert quota.used == 0


def test_each_physical_dart_retry_consumes_a_new_global_attempt() -> None:
    quota_server = _DurableAckServer()
    dart_calls = 0

    def dart_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal dart_calls
        dart_calls += 1
        if dart_calls == 1:
            return httpx.Response(503)
        return httpx.Response(
            200,
            json={"status": "013", "message": "no data"},
        )

    quota = _client(quota_server)
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
    assert [row["attempt_id"] for row in quota_server.bodies] == [
        "gha-123-2-ingest-test-00000001",
        "gha-123-2-ingest-test-00000001",
        "gha-123-2-ingest-test-00000002",
        "gha-123-2-ingest-test-00000002",
    ]
    assert quota_server.used == 2
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


@pytest.mark.parametrize("remote_binding_id", ("c" * 64, "한" * 64))
def test_backend_binding_mismatch_prevents_physical_dart_request(
    remote_binding_id: str,
) -> None:
    dart_calls = 0

    def quota_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        payload = _ack(body, used=1)
        payload["backend_binding_id"] = remote_binding_id
        return httpx.Response(200, json=payload)

    def dart_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal dart_calls
        dart_calls += 1
        return httpx.Response(200, json={"status": "013"})

    quota = _client(quota_handler)
    with httpx.Client(transport=httpx.MockTransport(dart_handler)) as dart_http:
        connector = DartConnector(
            "x" * 40,
            client=dart_http,
            governance_detail_codes=(),
            request_budget=quota,
        )
        with pytest.raises(DartQuotaLedgerError, match="backend binding"):
            list(
                connector.iter_disclosure_rows(
                    datetime(2026, 7, 22).date(), datetime(2026, 7, 22).date()
                )
            )

    assert dart_calls == 0
    assert quota.used == 0


def test_status_020_must_receive_durable_block_ack() -> None:
    quota_server = _DurableAckServer()
    quota = _client(quota_server)
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

    assert [str(body["action"]) for body in quota_server.bodies] == [
        "consume",
        "consume",
        "block_020",
        "block_020",
    ]
    assert quota_server.used == 1
    assert quota.used == 1


def test_status_901_permanently_disables_only_permit_credential() -> None:
    quota_server = _DurableAckServer()
    quota = _client(quota_server)
    permit = quota.consume(operation="list")
    quota.disable_901(permit)

    assert [body["action"] for body in quota_server.bodies] == [
        "consume",
        "consume",
        "disable_901",
        "disable_901",
    ]
    assert quota_server.bodies[2] == quota_server.bodies[3]
    assert quota_server.bodies[2]["credential_id"] == CREDENTIAL_ID
    assert quota_server.bodies[2]["reason"] == "opendart_status_901"
    assert quota_server.bodies[2]["attempt_id"] == permit.attempt_id
    assert quota_server.used == 1
    assert quota.used == 1


@pytest.mark.parametrize("action", ("block_020", "disable_901"))
def test_credential_mutation_requires_an_independent_duplicate_replay(
    action: str,
) -> None:
    seen: set[tuple[str, str]] = set()
    bodies: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body: dict[str, object] = json.loads(request.content)
        bodies.append(body)
        identity = (str(body["action"]), str(body["attempt_id"]))
        duplicate = identity in seen
        seen.add(identity)
        if body["action"] == action:
            duplicate = False
        return httpx.Response(
            200,
            json=_ack(body, used=1, duplicate=duplicate),
        )

    quota = _client(handler)
    permit = quota.consume()
    with pytest.raises(DartQuotaLedgerError, match="explicit replay"):
        if action == "block_020":
            quota.block_020(permit)
        else:
            quota.disable_901(permit)

    assert [str(body["action"]) for body in bodies] == [
        "consume",
        "consume",
        action,
        action,
    ]
    assert quota.used == 1


@pytest.mark.parametrize("malformed_ack_number", (1, 2))
@pytest.mark.parametrize(
    ("action", "mutations"),
    (
        ("block_020", {"credential_status": "unknown"}),
        ("block_020", {"credential_blocked_until": None}),
        (
            "block_020",
            {"credential_blocked_until": "2026-07-25T00:00:00+09:00"},
        ),
        (
            "block_020",
            {
                "blocked_until": "not-a-timestamp",
                "credential_blocked_until": "not-a-timestamp",
            },
        ),
        ("disable_901", {"credential_status": "active"}),
    ),
)
def test_credential_mutation_rejects_malformed_effect_ack(
    malformed_ack_number: int,
    action: str,
    mutations: dict[str, object],
) -> None:
    seen: set[tuple[str, str]] = set()
    action_ack_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal action_ack_count
        body: dict[str, object] = json.loads(request.content)
        identity = (str(body["action"]), str(body["attempt_id"]))
        duplicate = identity in seen
        seen.add(identity)
        payload = _ack(body, used=1, duplicate=duplicate)
        if body["action"] == action:
            action_ack_count += 1
            if action_ack_count == malformed_ack_number:
                payload.update(mutations)
        return httpx.Response(200, json=payload)

    quota = _client(handler)
    permit = quota.consume()
    with pytest.raises(DartQuotaLedgerError):
        if action == "block_020":
            quota.block_020(permit)
        else:
            quota.disable_901(permit)

    assert action_ack_count == malformed_ack_number
    assert quota.used == 1


def test_block_ack_allows_disabled_credential_when_block_timestamps_match() -> None:
    seen: set[tuple[str, str]] = set()

    def handler(request: httpx.Request) -> httpx.Response:
        body: dict[str, object] = json.loads(request.content)
        identity = (str(body["action"]), str(body["attempt_id"]))
        duplicate = identity in seen
        seen.add(identity)
        payload = _ack(body, used=1, duplicate=duplicate)
        if body["action"] == "block_020":
            payload["credential_status"] = "disabled_901"
        return httpx.Response(200, json=payload)

    quota = _client(handler)
    permit = quota.consume()
    quota.block_020(permit)

    assert quota.used == 1


@pytest.mark.parametrize(
    ("error_code", "reason"),
    (
        ("dart_credential_blocked", "blocked_020"),
        ("dart_credential_disabled", "disabled_901"),
    ),
)
def test_one_unavailable_credential_has_stable_skip_error(
    error_code: str,
    reason: str,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            json={
                "ok": False,
                "error": {
                    "code": error_code,
                    "credential_id": CREDENTIAL_ID,
                    "credential_reason": reason,
                },
            },
        )

    with pytest.raises(DartCredentialUnavailableError) as caught:
        _client(handler).consume()

    assert caught.value.reason == reason
    assert caught.value.credential_id == CREDENTIAL_ID


def test_global_pool_exhaustion_is_not_a_credential_skip() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            json={"ok": False, "error": {"code": "dart_quota_exhausted"}},
        )

    with pytest.raises(DartGlobalQuotaExceededError) as caught:
        _client(handler).consume()
    assert not isinstance(caught.value, DartCredentialUnavailableError)


def test_non_exhaustion_conflict_is_not_classified_as_global_quota() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            json={"ok": False, "error": {"code": "backend_binding_mismatch"}},
        )

    with pytest.raises(DartQuotaLedgerRejectedError) as caught:
        _client(handler).consume()
    assert not isinstance(caught.value, DartGlobalQuotaExceededError)


@pytest.mark.parametrize(
    "safe_detail",
    (
        "transaction_commit_failed",
        "transaction_readback_attempt_failed",
        "transaction_readback_binding_failed",
        "transaction_readback_connection_failed",
        "transaction_readback_credential_failed",
        "transaction_readback_day_failed",
        "transaction_state_invalid",
    ),
)
def test_rejected_ack_exposes_only_allowlisted_persistence_detail(
    safe_detail: str,
) -> None:
    def safe_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            503,
            json={
                "ok": False,
                "error": {
                    "code": "dart_quota_persistence_failed",
                    "detail": safe_detail,
                },
            },
        )

    with pytest.raises(DartQuotaLedgerError) as safe_error:
        _client(safe_handler, max_ack_retries=0).consume()
    assert "dart_quota_persistence_failed" in str(safe_error.value)
    assert f"detail={safe_detail}" in str(safe_error.value)

    secret = "f" * 40

    def unsafe_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            500,
            json={"ok": False, "error": {"code": secret, "detail": secret}},
        )

    with pytest.raises(DartQuotaLedgerError) as unsafe_error:
        _client(unsafe_handler, max_ack_retries=0).consume()
    assert "unknown_error" in str(unsafe_error.value)
    assert secret not in str(unsafe_error.value)


def test_python_allowlist_exactly_matches_php_persistence_phase_contract() -> None:
    php = (
        Path(__file__).resolve().parents[1]
        / "deploy"
        / "activist"
        / "governance_v1.php"
    ).read_text(encoding="utf-8")
    start = php.index("function v1_dart_quota_persistence_phase")
    end = php.index("function v1_dart_quota_persistence_outcome", start)
    returned_details = frozenset(
        re.findall(r"return '([^']+)';", php[start:end])
    )
    assert returned_details == dart_quota_module._SAFE_PERSISTENCE_DETAILS


def test_per_call_credential_id_overrides_default_without_key_material() -> None:
    selected = "d" * 64
    quota_server = _DurableAckServer()
    permit = _client(quota_server).consume(credential_id=selected)
    assert permit.credential_id == selected
    assert len(quota_server.bodies) == 2


def test_incomplete_ack_fails_closed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        payload = _ack(body, used=1)
        payload.pop("accepted")
        return httpx.Response(200, json=payload)

    with pytest.raises(DartQuotaLedgerError, match="accepted=1"):
        _client(handler).consume(operation="corp_code")


@pytest.mark.parametrize(
    "credential_id",
    ("", "legacy-single", "C" * 64, "f" * 63, "key-01"),
)
def test_new_physical_attempt_requires_full_lowercase_sha256_credential_id(
    credential_id: str,
) -> None:
    with pytest.raises(DartQuotaLedgerError, match="credential_id"):
        _client(lambda _request: httpx.Response(500)).consume(
            credential_id=credential_id
        )


def test_generated_attempt_id_always_fits_server_column(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quota_server = _DurableAckServer()

    monkeypatch.setenv("GITHUB_RUN_ID", "9" * 30)
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "123")
    monkeypatch.setenv("GITHUB_JOB", "very-long-job-" * 8)
    quota = DartQuotaClient(
        base_url="https://api.example.test/api/v1",
        token="ops-token",
        backend_binding_id=BINDING_ID,
        credential_id=CREDENTIAL_ID,
        code_revision=REVISION,
        phase="very-long-phase-" * 8,
        transport=httpx.MockTransport(quota_server),
        now_provider=lambda: NOW,
    )
    quota.consume()

    observed = [str(body["attempt_id"]) for body in quota_server.bodies]
    assert len(observed) == 2
    assert observed[0] == observed[1]
    assert len(observed[0]) <= 96
    assert observed[0].endswith("-00000001")


def test_restarted_client_in_same_github_job_gets_a_new_process_nonce(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quota_server = _DurableAckServer()

    monkeypatch.setenv("GITHUB_RUN_ID", "123456")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    monkeypatch.setenv("GITHUB_JOB", "ingest")
    kwargs = {
        "base_url": "https://api.example.test/api/v1",
        "token": "ops-token",
        "backend_binding_id": BINDING_ID,
        "credential_id": CREDENTIAL_ID,
        "code_revision": REVISION,
        "phase": "official-ingest",
        "transport": httpx.MockTransport(quota_server),
        "now_provider": lambda: NOW,
    }
    DartQuotaClient(**kwargs).consume()  # type: ignore[arg-type]
    DartQuotaClient(**kwargs).consume()  # type: ignore[arg-type]

    observed = [str(body["attempt_id"]) for body in quota_server.bodies]
    assert len(observed) == 4
    assert observed[0] == observed[1]
    assert observed[2] == observed[3]
    assert observed[0] != observed[2]
    assert all(value.startswith("gha-123456-1-ingest-official-ingest-") for value in observed)
    assert all(value.endswith("-00000001") for value in observed)
    assert quota_server.used == 2


def test_github_actions_and_partial_api_config_force_durable_fail_closed_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CURATOR_REQUIRE_DURABLE_DART_QUOTA", raising=False)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    assert durable_dart_quota_required() is True

    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.setenv("BSIDE_OPS_TOKEN", "partial-config")
    assert durable_dart_quota_configured() is True

    monkeypatch.delenv("BSIDE_OPS_TOKEN", raising=False)
    monkeypatch.setenv("BSIDE_BACKEND_BINDING_ID", BINDING_ID)
    assert durable_dart_quota_configured() is True


@pytest.mark.parametrize(
    "base_url",
    (
        "http://api.example.test/api/v1",
        "https://user:secret@api.example.test/api/v1",
        "https://api.example.test/api/v1?token=secret",
        "https://api.example.test/api/v1%2f..",
        "https://api.example.test\\api\\v1",
        "https://api.example.test/other",
    ),
)
def test_quota_url_must_be_credential_safe_https(base_url: str) -> None:
    with pytest.raises(DartQuotaLedgerError, match="base URL"):
        DartQuotaClient(
            base_url=base_url,
            token="ops-token",
            backend_binding_id=BINDING_ID,
            code_revision=REVISION,
        )


@pytest.mark.parametrize("binding_id", ("not-a-binding", "B" * 64))
def test_quota_backend_binding_id_must_be_exact_sha256_hex(binding_id: str) -> None:
    with pytest.raises(DartQuotaLedgerError, match="64 lowercase hexadecimal"):
        DartQuotaClient(
            base_url="https://api.example.test/api/v1",
            token="ops-token",
            backend_binding_id=binding_id,
            code_revision=REVISION,
        )
