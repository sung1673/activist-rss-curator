from __future__ import annotations

import hashlib
import json
import pickle

import pytest

from curator.opendart_credentials import (
    OpenDartCredentialConfigurationError,
    load_opendart_credentials,
)


KEY_A = "a" * 40
KEY_B = "b" * 40
KEY_C = "0123456789abcdef" * 2 + "01234567"


@pytest.mark.parametrize(
    "raw",
    (
        f"{KEY_A}\n{KEY_B}\n{KEY_C}",
        f"{KEY_A}\r\n{KEY_B}\r\n{KEY_C}\r\n",
        f"{KEY_A},{KEY_B}, {KEY_C}",
        f"\r\n{KEY_A},\r\n{KEY_B}\n{KEY_C}\n",
    ),
)
def test_pool_parses_newline_crlf_and_comma_without_exposing_keys(raw: str) -> None:
    credentials = load_opendart_credentials({"OPENDART_API_KEYS": raw})

    assert [credential.key for credential in credentials] == [KEY_A, KEY_B, KEY_C]
    assert credentials[0].credential_id == hashlib.sha256(KEY_A.encode("ascii")).hexdigest()
    rendered = repr(credentials)
    assert KEY_A not in rendered
    assert KEY_B not in rendered
    assert KEY_C not in rendered
    with pytest.raises(TypeError):
        json.dumps(credentials[0])
    with pytest.raises(TypeError):
        pickle.dumps(credentials[0])


def test_legacy_key_is_used_only_when_pool_is_absent() -> None:
    credentials = load_opendart_credentials({"DART_API_KEY": KEY_A})

    assert len(credentials) == 1
    assert credentials[0].key == KEY_A


@pytest.mark.parametrize(
    "environment",
    (
        {"OPENDART_API_KEYS": f"{KEY_A}\n{KEY_A}"},
        {"OPENDART_API_KEYS": "not-a-key"},
        {"OPENDART_API_KEYS": "a" * 39},
        {"OPENDART_API_KEYS": "g" * 40},
        {"OPENDART_API_KEYS": "A" * 40},
        {"OPENDART_API_KEYS": ",,\r\n,"},
        {"DART_API_KEY": "not-a-key"},
        {"OPENDART_API_KEYS": KEY_A, "DART_API_KEY": KEY_B},
    ),
)
def test_invalid_duplicate_or_conflicting_configuration_fails_without_secret(
    environment: dict[str, str],
) -> None:
    with pytest.raises(OpenDartCredentialConfigurationError) as captured:
        load_opendart_credentials(environment)

    rendered = str(captured.value)
    assert KEY_A not in rendered
    assert KEY_B not in rendered
    assert "not-a-key" not in rendered


def test_empty_configuration_returns_no_credentials() -> None:
    assert load_opendart_credentials({}) == ()
    assert load_opendart_credentials(
        {"OPENDART_API_KEYS": " \r\n ", "DART_API_KEY": ""}
    ) == ()
