from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from urllib.error import HTTPError

import pytest


ROOT = Path(__file__).resolve().parents[1]


def load_recorder() -> ModuleType:
    path = ROOT / ".github" / "scripts" / "record-web-distribution.py"
    spec = importlib.util.spec_from_file_location("bside_web_distribution_recorder", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def observation(recorder: ModuleType, *, succeeded: bool = True):  # type: ignore[no-untyped-def]
    return recorder.build_observation(
        target="pages",
        succeeded=succeeded,
        observed_at="2026-07-22T01:02:03Z",
        completed_at="2026-07-22T01:02:04.250Z",
        build_sha="A" * 40,
        workflow_run_id="123456789",
        workflow_run_attempt="2",
        operation="daily-governance",
    )


def test_success_payload_uses_exact_contract_and_null_failure_timestamp() -> None:
    recorder = load_recorder()
    payload = observation(recorder).as_api_dict()

    assert payload == {
        "observation_id": "github-actions:123456789:2:pages:daily-governance",
        "observed_at": "2026-07-22T01:02:03.000Z",
        "distribution_target": "pages",
        "duration_ms": 1250,
        "succeeded": True,
        "build_sha": "a" * 40,
        "workflow_run_id": 123456789,
        "workflow_run_attempt": 2,
        "failure_detected_at": None,
        "source": "github_actions",
    }


def test_failure_payload_requires_detection_timestamp() -> None:
    recorder = load_recorder()
    payload = observation(recorder, succeeded=False).as_api_dict()

    assert payload["succeeded"] is False
    assert payload["failure_detected_at"] == "2026-07-22T01:02:04.250Z"


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("target", "telegram", "pages or api"),
        ("build_sha", "abc123", "full 40-character"),
        ("workflow_run_id", "0", "positive integer"),
        ("workflow_run_attempt", "false", "positive integer"),
        ("workflow_run_attempt", "10001", "must not exceed"),
        ("operation", "UPPER CASE", "operation must contain"),
        ("completed_at", "2026-07-22T01:02:02Z", "duration must be between"),
    ],
)
def test_build_observation_rejects_invalid_identity_or_timing(
    field: str,
    value: str,
    message: str,
) -> None:
    recorder = load_recorder()
    arguments = {
        "target": "pages",
        "succeeded": True,
        "observed_at": "2026-07-22T01:02:03Z",
        "completed_at": "2026-07-22T01:02:04Z",
        "build_sha": "a" * 40,
        "workflow_run_id": "123",
        "workflow_run_attempt": "1",
        "operation": "daily-governance",
    }
    arguments[field] = value

    with pytest.raises(recorder.DistributionObservationError, match=message):
        recorder.build_observation(**arguments)


class Response:
    def __init__(self, payload: object, status: int = 202) -> None:
        self.status = status
        self.body = json.dumps(payload).encode("utf-8")

    def __enter__(self):  # type: ignore[no-untyped-def]
        return self

    def __exit__(self, *_args):  # type: ignore[no-untyped-def]
        return None

    def read(self, _limit: int) -> bytes:
        return self.body


def acknowledged(*, inserted: int = 1, duplicate: int = 0) -> dict[str, object]:
    return {
        "ok": True,
        "accepted_count": 1,
        "inserted_count": inserted,
        "duplicate_count": duplicate,
    }


def test_submit_posts_one_exact_bearer_authenticated_observation() -> None:
    recorder = load_recorder()
    requests = []

    def opener(request, *, timeout):  # type: ignore[no-untyped-def]
        requests.append((request, timeout))
        return Response(acknowledged())

    result = recorder.submit_observation(
        base_url="https://api.example.test/activist/api.php/api/v1",
        token="x" * 32,
        observation=observation(recorder),
        opener=opener,
        sleeper=lambda _delay: None,
    )

    assert result["duplicate_count"] == 0
    assert len(requests) == 1
    request, timeout = requests[0]
    assert request.full_url.endswith("/api/v1/ops/web-distribution-observations")
    assert request.method == "POST"
    assert request.get_header("Authorization") == f"Bearer {'x' * 32}"
    assert request.get_header("Content-type") == "application/json"
    assert timeout == 15
    body = json.loads(request.data)
    assert body == {"observations": [observation(recorder).as_api_dict()]}


def test_exact_duplicate_acknowledgement_is_successful() -> None:
    recorder = load_recorder()
    result = recorder.submit_observation(
        base_url="https://api.example.test",
        token="x" * 32,
        observation=observation(recorder),
        opener=lambda *_args, **_kwargs: Response(acknowledged(inserted=0, duplicate=1)),
        sleeper=lambda _delay: None,
    )
    assert result["inserted_count"] == 0
    assert result["duplicate_count"] == 1


@pytest.mark.parametrize(
    "payload",
    [
        {"ok": True, "accepted_count": 0, "inserted_count": 0, "duplicate_count": 0},
        {"ok": True, "accepted_count": 1, "inserted_count": 1, "duplicate_count": 1},
        {"ok": True, "accepted_count": 1, "inserted_count": 0, "duplicate_count": 0},
        {"ok": True, "accepted_count": 1, "duplicate_count": 0},
        {"ok": False, "accepted_count": 1, "inserted_count": 1, "duplicate_count": 0},
    ],
)
def test_submit_rejects_unbalanced_or_incomplete_acknowledgements(payload: object) -> None:
    recorder = load_recorder()
    with pytest.raises(recorder.DistributionObservationError):
        recorder.submit_observation(
            base_url="https://api.example.test",
            token="x" * 32,
            observation=observation(recorder),
            opener=lambda *_args, **_kwargs: Response(payload),
            sleeper=lambda _delay: None,
        )


def test_submit_requires_exact_http_202() -> None:
    recorder = load_recorder()
    with pytest.raises(recorder.DistributionObservationError, match="exactly 202"):
        recorder.submit_observation(
            base_url="https://api.example.test",
            token="x" * 32,
            observation=observation(recorder),
            opener=lambda *_args, **_kwargs: Response(acknowledged(), status=200),
            sleeper=lambda _delay: None,
        )


def test_transient_post_is_retried_with_the_same_idempotent_body() -> None:
    recorder = load_recorder()
    requests = []
    sleeps = []

    def opener(request, *, timeout):  # type: ignore[no-untyped-def]
        requests.append((request.full_url, request.data, timeout))
        if len(requests) == 1:
            raise HTTPError(request.full_url, 503, "unavailable", {}, None)
        return Response(acknowledged(inserted=0, duplicate=1))

    result = recorder.submit_observation(
        base_url="https://api.example.test",
        token="x" * 32,
        observation=observation(recorder),
        opener=opener,
        sleeper=sleeps.append,
    )

    assert result["duplicate_count"] == 1
    assert len(requests) == 2
    assert requests[0] == requests[1]
    assert sleeps == [1.0]


def test_endpoint_and_token_validation_fail_closed() -> None:
    recorder = load_recorder()
    with pytest.raises(recorder.DistributionObservationError, match="HTTPS URL"):
        recorder.api_endpoint("http://api.example.test/api/v1")
    with pytest.raises(recorder.DistributionObservationError, match="TOKEN"):
        recorder.submit_observation(
            base_url="https://api.example.test",
            token="short",
            observation=observation(recorder),
            opener=lambda *_args, **_kwargs: Response(acknowledged()),
            sleeper=lambda _delay: None,
        )
