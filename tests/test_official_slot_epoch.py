from __future__ import annotations

import json

import httpx
import pytest

from curator.official_slot_epoch import OfficialSlotEpochError, reset_epoch


REVISION = "a" * 40
TOKEN = "admin-token-" + "x" * 32
CONFIRMATION = "RESET_OFFICIAL_SLOT_EPOCH_AT_NEXT_KST_DAY"
REASON = "Advance the audited slot epoch after an operator-approved repair"


def test_reset_epoch_sends_exact_admin_contract_and_validates_ack() -> None:
    observed: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        observed["url"] = str(request.url)
        observed["authorization"] = request.headers.get("Authorization")
        observed["payload"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "ok": True,
                "pipeline": "ingest-official",
                "epoch_version": 4,
                "active_from": "2026-07-23T15:00:00Z",
                "epoch_id": "official-epoch:test",
                "claims_preserved": True,
            },
        )

    result = reset_epoch(
        base_url="https://api.example.test/activist/api.php/api/v1",
        admin_token=TOKEN,
        expected_epoch_version=3,
        reason=REASON,
        code_revision=REVISION.upper(),
        confirmation=CONFIRMATION,
        transport=httpx.MockTransport(handler),
    )

    assert result["epoch_version"] == 4
    assert observed["url"] == (
        "https://api.example.test/activist/api.php/api/v1/admin/official-slot-epoch"
    )
    assert observed["authorization"] == f"Bearer {TOKEN}"
    assert observed["payload"] == {
        "action": "reset",
        "pipeline": "ingest-official",
        "expected_epoch_version": 3,
        "reason": REASON,
        "code_revision": REVISION,
        "confirmation": CONFIRMATION,
    }


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("claims_preserved", False),
        ("epoch_version", 3),
        ("pipeline", "other"),
        ("epoch_id", None),
        ("active_from", None),
        ("epoch_id", "invalid id"),
        ("active_from", "2026-07-23T15:01:00Z"),
        ("active_from", "2026-07-24T00:00:00+09:00"),
    ),
)
def test_reset_epoch_rejects_inconsistent_ack(field: str, value: object) -> None:
    payload: dict[str, object] = {
        "ok": True,
        "pipeline": "ingest-official",
        "epoch_version": 4,
        "active_from": "2026-07-23T15:00:00Z",
        "epoch_id": "official-epoch:test",
        "claims_preserved": True,
    }
    payload[field] = value

    with pytest.raises(OfficialSlotEpochError, match="incomplete or inconsistent"):
        reset_epoch(
            base_url="https://api.example.test/api/v1",
            admin_token=TOKEN,
            expected_epoch_version=3,
            reason=REASON,
            code_revision=REVISION,
            confirmation=CONFIRMATION,
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(200, json=payload)
            ),
        )


@pytest.mark.parametrize(
    "base_url",
    (
        "http://api.example.test/api/v1",
        "https://user:secret@api.example.test/api/v1",
        "https://api.example.test/api/v1?token=secret",
    ),
)
def test_reset_epoch_requires_credential_free_https(base_url: str) -> None:
    with pytest.raises(OfficialSlotEpochError, match="credential-free HTTPS"):
        reset_epoch(
            base_url=base_url,
            admin_token=TOKEN,
            expected_epoch_version=3,
            reason=REASON,
            code_revision=REVISION,
            confirmation=CONFIRMATION,
        )


def test_reset_epoch_requires_long_token_reason_version_revision_and_confirmation() -> None:
    with pytest.raises(OfficialSlotEpochError, match="inputs are invalid"):
        reset_epoch(
            base_url="https://api.example.test/api/v1",
            admin_token="short",
            expected_epoch_version=0,
            reason="short",
            code_revision="not-a-sha",
            confirmation="NO",
        )
