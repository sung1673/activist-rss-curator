from __future__ import annotations

import json

import pytest

from curator import remote_api


class _FakeResponse:
    def __init__(self, payload: object, *, status_code: int, content: bytes) -> None:
        self._payload = payload
        self.status_code = status_code
        self.content = content

    def json(self) -> object:
        return self._payload


class _FakeClient:
    response: _FakeResponse

    def __init__(self, *, timeout: float) -> None:
        assert timeout == 45.0

    def __enter__(self) -> _FakeClient:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def post(
        self,
        _url: str,
        *,
        content: bytes,
        headers: dict[str, str],
    ) -> _FakeResponse:
        assert json.loads(content)["probe"] is True
        assert headers["X-Activist-Signature"].startswith("sha256=")
        return self.response


def test_remote_action_measures_body_bytes_and_overwrites_untrusted_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = '{"ok":false,"error":"internal_error","private":"비밀"}'.encode()
    _FakeClient.response = _FakeResponse(
        {
            "ok": False,
            "error": "internal_error",
            "status_code": 418,
            "_response_body_bytes": 999_999,
        },
        status_code=500,
        content=body,
    )
    monkeypatch.setenv("ACTIVIST_API_URL", "https://api.example.invalid/api.php")
    monkeypatch.setenv("ACTIVIST_API_SECRET", "s" * 32)
    monkeypatch.setattr(remote_api.httpx, "Client", _FakeClient)

    response = remote_api.post_remote_action(
        "upsert_governance_snapshot",
        {"probe": True},
        timeout=45.0,
    )

    assert response["_response_body_bytes"] == len(body)
    assert response["status_code"] == 500
    assert response["ok"] is False


def test_remote_action_rejects_non_object_json_without_retaining_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private_body = '["https://api.invalid/?token=secret","dart:record"]'.encode()
    _FakeClient.response = _FakeResponse(
        ["https://api.invalid/?token=secret", "dart:record"],
        status_code=502,
        content=private_body,
    )
    monkeypatch.setenv("ACTIVIST_API_URL", "https://api.example.invalid/api.php")
    monkeypatch.setenv("ACTIVIST_API_SECRET", "s" * 32)
    monkeypatch.setattr(remote_api.httpx, "Client", _FakeClient)

    response = remote_api.post_remote_action(
        "upsert_governance_snapshot",
        {"probe": True},
        timeout=45.0,
    )

    assert response == {
        "ok": False,
        "error": "invalid_json_response",
        "status_code": 502,
        "_response_body_bytes": len(private_body),
    }
    serialized = json.dumps(response)
    assert "token" not in serialized
    assert "dart:record" not in serialized
