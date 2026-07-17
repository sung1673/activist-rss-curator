from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date
from pathlib import Path
from types import ModuleType

import httpx
import pytest


ROOT = Path(__file__).resolve().parents[1]


def load_script(filename: str, module_name: str) -> ModuleType:
    path = ROOT / ".github" / "scripts" / filename
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def kind_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "title": "Outside director appointment",
        "acptno": "20260716000999",
        "dart_corp_code": "00126380",
        "company_name": "Samsung Electronics",
        "received_at": "2026-07-16T10:30:00+09:00",
    }
    row.update(overrides)
    return row


def test_kind_validator_gets_page_one_with_connector_parameters_and_key() -> None:
    validator = load_script("validate-kind-adapter.py", "validate_kind_adapter_request")
    observed: dict[str, str] = {}
    observed_authorization = ""

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal observed_authorization
        observed.update(dict(request.url.params))
        observed_authorization = request.headers.get("Authorization", "")
        return httpx.Response(
            200,
            json={"page": 1, "total_pages": 1, "items": [kind_row()]},
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert validator.request_and_validate(
            "https://kind-adapter.example.test/disclosures",
            api_key="do-not-log-this-key",
            end_date=date(2026, 7, 16),
            client=client,
        ) == 1

    assert observed == {
        "start_date": "2026-07-09",
        "end_date": "2026-07-16",
        "page": "1",
        "page_size": "10",
    }
    assert observed_authorization == "Bearer do-not-log-this-key"


def test_kind_validator_accepts_explicit_first_page_no_data() -> None:
    validator = load_script("validate-kind-adapter.py", "validate_kind_adapter_no_data")
    assert validator.validate_payload({"status": "no_data", "items": []}) == 0


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"items": [kind_row()]}, "list and pagination contract"),
        ({"page": 2, "total_pages": 2, "items": [kind_row()]}, "requested first page"),
        ({"page": 1, "total_pages": 1, "items": []}, "empty success page"),
        (
            {"page": 1, "total_pages": 1, "items": [kind_row(title="")]},
            "missing a title",
        ),
        (
            {"page": 1, "total_pages": 1, "items": [kind_row(acptno="bad receipt")]},
            "stable receipt number",
        ),
        (
            {
                "page": 1,
                "total_pages": 1,
                "items": [kind_row(dart_corp_code="0123456")],
            },
            "8-digit DART corp_code",
        ),
        (
            {"page": 1, "total_pages": 1, "items": [kind_row(company_name="")]},
            "missing corp_name",
        ),
        (
            {"page": 1, "total_pages": 1, "items": [kind_row(received_at="")]},
            "missing received_at",
        ),
        (
            {"page": 1, "total_pages": 1, "items": [kind_row(received_at="not-a-date")]},
            "invalid received_at",
        ),
    ],
)
def test_kind_validator_rejects_contract_and_row_errors(payload: object, message: str) -> None:
    validator = load_script("validate-kind-adapter.py", f"validate_kind_adapter_{message}")
    with pytest.raises(validator.ValidationError, match=message):
        validator.validate_payload(payload)


def test_kind_validator_does_not_print_key_endpoint_or_response_body(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    validator = load_script("validate-kind-adapter.py", "validate_kind_adapter_redaction")
    secret = "super-secret-value"
    monkeypatch.setenv(
        "KIND_DISCLOSURE_ENDPOINT",
        f"https://kind-adapter.example.test/disclosures?token={secret}",
    )
    monkeypatch.setenv("KIND_API_KEY", secret)

    def fake_get(*_args: object, **_kwargs: object) -> httpx.Response:
        return httpx.Response(200, json={"status": secret, "items": []})

    monkeypatch.setattr(validator.httpx, "get", fake_get)
    assert validator.main() == 1
    output = capsys.readouterr().out
    assert secret not in output
    assert "kind-adapter.example.test" not in output
    assert "list and pagination contract" in output


def test_kind_validator_redacts_unexpected_exception_text(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    validator = load_script("validate-kind-adapter.py", "validate_kind_adapter_exception")
    secret = "unexpected-secret-value"
    monkeypatch.setenv("KIND_DISCLOSURE_ENDPOINT", "https://kind-adapter.example.test")
    monkeypatch.setattr(
        validator,
        "request_and_validate",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError(secret)),
    )

    assert validator.main() == 1
    output = capsys.readouterr().out
    assert secret not in output
    assert "unexpected validator failure" in output


def test_media_feed_validator_allows_empty_or_valid_json_array() -> None:
    validator = load_script("validate-media-feeds.py", "validate_media_feeds_valid")
    assert validator.validate_feeds(None) == 0
    assert validator.validate_feeds("  ") == 0
    assert validator.validate_feeds("[]") == 0
    assert validator.validate_feeds(
        json.dumps(
            [
                {
                    "name": "Governance wire",
                    "url": "https://feeds.example.test/governance.xml",
                    "scope": "korean_governance",
                },
                {"enabled": False},
                {
                    "name": "Context wire",
                    "url": "http://feeds.example.test/context.xml",
                    "scope": "korean_governance_context",
                    "enabled": True,
                },
            ]
        )
    ) == 2


@pytest.mark.parametrize(
    ("raw_value", "message"),
    [
        (
            "https://feeds.example.test/one.xml\nhttps://feeds.example.test/two.xml",
            "legacy newline/comma URL lists",
        ),
        ('{"url":"https://feeds.example.test/one.xml"}', "JSON array"),
        ('["https://feeds.example.test/one.xml"]', "must be an object"),
        ('[{"enabled":"false"}]', "enabled must be boolean"),
        (
            '[{"url":"https://feeds.example.test/a.xml","scope":"korean_governance"}]',
            "requires name",
        ),
        (
            '[{"name":"A","url":"ftp://feeds.example.test/a.xml","scope":"korean_governance"}]',
            "HTTP\\(S\\) url",
        ),
        (
            '[{"name":"A","url":"https://feeds.example.test/a.xml","scope":"markets"}]',
            "approved governance scope",
        ),
    ],
)
def test_media_feed_validator_rejects_legacy_and_malformed_values(
    raw_value: str,
    message: str,
) -> None:
    validator = load_script("validate-media-feeds.py", f"validate_media_feeds_{message}")
    with pytest.raises(validator.ValidationError, match=message):
        validator.validate_feeds(raw_value)
