from __future__ import annotations

import json
from io import StringIO
from pathlib import Path

import httpx
import pytest

from curator.official_slot_claim import (
    OfficialSlotClaimActivationError,
    OfficialSlotClaimClient,
    OfficialSlotClaimError,
    _environment_request,
    append_github_environment,
)


REVISION = "a" * 40


def _request() -> dict[str, object]:
    return {
        "action": "claim",
        "pipeline": "ingest-official",
        "github_run_id": "123456789",
        "github_run_attempt": 1,
        "event_schedule": "0,30 15-21 * * *",
        "trigger_created_at": "2026-07-15T15:02:00+00:00",
        "code_revision": REVISION,
    }


def _ack(
    request: dict[str, object],
    *,
    duplicate: bool = False,
    claimed_at: str = "2026-07-15T15:07:00+00:00",
) -> dict[str, object]:
    return {
        "ok": True,
        "accepted": 1,
        "claim_id": "official-slot:claim-1",
        "pipeline": request["pipeline"],
        "github_run_id": request["github_run_id"],
        "github_run_attempt": request["github_run_attempt"],
        "event_schedule": request["event_schedule"],
        "scheduled_slot_at": "2026-07-15T15:00:00+00:00",
        "trigger_created_at": request["trigger_created_at"],
        "claimed_at": claimed_at,
        "next_cadence_slot_at": "2026-07-15T15:30:00+00:00",
        "trigger_lag_seconds": 120,
        "claim_lag_seconds": 420,
        "late": False,
        "status": "claimed",
        "terminal_reason": None,
        "duplicate": duplicate,
    }


def _client(handler) -> OfficialSlotClaimClient:  # type: ignore[no-untyped-def]
    return OfficialSlotClaimClient(
        base_url="https://api.example.test/activist/api.php/api/v1",
        token="ops-token",
        transport=httpx.MockTransport(handler),
        sleeper=lambda _delay: None,
    )


def test_claim_retries_lost_ack_with_exact_same_request() -> None:
    observed: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        observed.append(body)
        if len(observed) == 1:
            return httpx.Response(503, json={"error": "temporary"})
        return httpx.Response(200, json=_ack(body, duplicate=True))

    claim = _client(handler).claim(_request())

    assert observed == [_request(), _request()]
    assert claim.claim_id == "official-slot:claim-1"
    assert claim.scheduled_slot_at == "2026-07-15T15:00:00+00:00"
    assert claim.late is False
    assert claim.duplicate is True


def test_activation_response_fails_without_attributing_an_ambiguous_run() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            json={
                "ok": False,
                "error": {"code": "official_slot_claim_activated"},
                "active_from": "2026-07-16T15:00:00+00:00",
            },
        )

    with pytest.raises(OfficialSlotClaimActivationError, match="not attributed"):
        _client(handler).claim(_request())


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("next_cadence_slot_at", "2026-07-16T15:00:00+00:00"),
        ("claim_lag_seconds", 419),
        ("late", True),
        ("github_run_id", "999"),
        ("github_run_attempt", True),
        ("accepted", 0),
    ),
)
def test_claim_ack_must_match_authoritative_contract(field: str, value: object) -> None:
    request_payload = _request()
    payload = _ack(request_payload)
    payload[field] = value

    with pytest.raises(OfficialSlotClaimError):
        _client(lambda _request: httpx.Response(200, json=payload)).claim(request_payload)


def test_late_claim_is_measured_against_next_overall_cadence_slot() -> None:
    request_payload = _request()
    payload = _ack(
        request_payload,
        claimed_at="2026-07-15T15:31:00+00:00",
    )
    payload["claim_lag_seconds"] = 1860
    payload["late"] = True

    claim = _client(lambda _request: httpx.Response(200, json=payload)).claim(
        request_payload
    )

    assert claim.next_cadence_slot_at == "2026-07-15T15:30:00+00:00"
    assert claim.late is True


@pytest.mark.parametrize("status", ("claimed", "completed"))
def test_terminal_reason_requires_failed_status(status: str) -> None:
    request_payload = _request()
    payload = _ack(request_payload)
    payload["status"] = status
    payload["terminal_reason"] = "rerun_after_next_cadence"

    with pytest.raises(OfficialSlotClaimError, match="inconsistent"):
        _client(lambda _request: httpx.Response(200, json=payload)).claim(
            request_payload
        )


@pytest.mark.parametrize(
    "terminal_reason",
    ("completion_after_next_cadence", "rerun_after_next_cadence"),
)
def test_failed_terminal_claim_exports_terminal_noop(
    monkeypatch: pytest.MonkeyPatch,
    terminal_reason: str,
) -> None:
    request_payload = _request()
    payload = _ack(request_payload)
    payload["status"] = "failed"
    payload["terminal_reason"] = terminal_reason
    claim = _client(lambda _request: httpx.Response(200, json=payload)).claim(
        request_payload
    )

    class Sink(StringIO):
        def __enter__(self) -> "Sink":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    sink = Sink()
    monkeypatch.setattr(Path, "open", lambda *_args, **_kwargs: sink)
    append_github_environment(Path("github.env"), claim)

    assert "CURATOR_OFFICIAL_SLOT_TERMINAL_NOOP=1" in sink.getvalue().splitlines()


def test_completed_claim_rerun_exports_terminal_noop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_payload = _request()
    payload = _ack(request_payload)
    payload["status"] = "completed"
    claim = _client(lambda _request: httpx.Response(200, json=payload)).claim(
        request_payload
    )

    class Sink(StringIO):
        def __enter__(self) -> "Sink":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    sink = Sink()
    monkeypatch.setattr(Path, "open", lambda *_args, **_kwargs: sink)
    append_github_environment(Path("github.env"), claim)

    assert "CURATOR_OFFICIAL_SLOT_TERMINAL_NOOP=1" in sink.getvalue().splitlines()


def test_environment_request_preserves_run_identity_and_utc_trigger() -> None:
    request_payload = _environment_request(
        {
            "GITHUB_RUN_ID": "123456789",
            "GITHUB_RUN_ATTEMPT": "2",
            "CURATOR_EVENT_SCHEDULE": "0,30 15-21 * * *",
            "CURATOR_GITHUB_RUN_CREATED_AT": "2026-07-16T00:02:00+09:00",
            "GITHUB_SHA": REVISION.upper(),
        }
    )

    assert request_payload == _request() | {
        "github_run_attempt": 2,
        "code_revision": REVISION,
    }


def test_explicit_repair_claims_only_the_exact_operator_named_slot() -> None:
    request_payload = _environment_request(
        {
            "GITHUB_RUN_ID": "123456789",
            "GITHUB_RUN_ATTEMPT": "1",
            "CURATOR_EVENT_SCHEDULE": "0,30 15-21 * * *",
            "CURATOR_GITHUB_RUN_CREATED_AT": "2026-07-15T15:32:00Z",
            "CURATOR_OFFICIAL_SLOT_REPAIR_EXPECTED_AT": "2026-07-15T15:00:00Z",
            "GITHUB_SHA": REVISION,
        }
    )
    payload = _ack(
        request_payload,
        claimed_at="2026-07-15T15:33:00+00:00",
    )
    payload["trigger_lag_seconds"] = 1920
    payload["claim_lag_seconds"] = 1980
    payload["late"] = True

    claim = _client(lambda _request: httpx.Response(200, json=payload)).claim(
        request_payload
    )

    assert request_payload["action"] == "repair"
    assert request_payload["expected_slot_at"] == "2026-07-15T15:00:00+00:00"
    assert claim.scheduled_slot_at == request_payload["expected_slot_at"]
    assert claim.late is True


def test_repair_ack_for_a_different_slot_fails_closed() -> None:
    request_payload = _request() | {
        "action": "repair",
        "expected_slot_at": "2026-07-15T15:30:00+00:00",
    }

    with pytest.raises(OfficialSlotClaimError, match="exact expected slot"):
        _client(
            lambda _request: httpx.Response(200, json=_ack(request_payload))
        ).claim(request_payload)


def test_claim_exports_only_server_acknowledged_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_payload = _request()
    claim = _client(
        lambda _request: httpx.Response(200, json=_ack(request_payload))
    ).claim(request_payload)
    class Sink(StringIO):
        def __enter__(self) -> "Sink":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    sink = Sink()
    monkeypatch.setattr(Path, "open", lambda *_args, **_kwargs: sink)

    append_github_environment(Path("github.env"), claim)

    assert sink.getvalue().splitlines() == [
        "CURATOR_OFFICIAL_SLOT_CLAIM_ID=official-slot:claim-1",
        "CURATOR_OFFICIAL_SCHEDULED_SLOT_AT=2026-07-15T15:00:00+00:00",
        "CURATOR_OFFICIAL_SLOT_CLAIMED_AT=2026-07-15T15:07:00+00:00",
        "CURATOR_OFFICIAL_NEXT_CADENCE_SLOT_AT=2026-07-15T15:30:00+00:00",
        "CURATOR_OFFICIAL_TRIGGER_LAG_SECONDS=120",
        "CURATOR_OFFICIAL_CLAIM_LAG_SECONDS=420",
        "CURATOR_OFFICIAL_SLOT_LATE=0",
        "CURATOR_OFFICIAL_SLOT_TERMINAL_NOOP=0",
        "CURATOR_GITHUB_RUN_ID=123456789",
        "CURATOR_GITHUB_RUN_ATTEMPT=1",
    ]


@pytest.mark.parametrize(
    "base_url",
    (
        "http://api.example.test/api/v1",
        "https://user:secret@api.example.test/api/v1",
        "https://api.example.test/api/v1?token=secret",
    ),
)
def test_slot_claim_url_must_be_credential_safe_https(base_url: str) -> None:
    with pytest.raises(OfficialSlotClaimError, match="absolute HTTPS"):
        OfficialSlotClaimClient(base_url=base_url, token="ops-token")
