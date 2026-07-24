from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit, urlunsplit

import httpx


class OfficialSlotEpochError(RuntimeError):
    """An admin epoch reset failed its authenticated, append-only contract."""


_ENTITY_ID_RE = re.compile(r"[A-Za-z0-9_.:-]{1,96}")
_KST = timezone(timedelta(hours=9))


def _endpoint(raw: str) -> str:
    parsed = urlsplit(raw.strip())
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise OfficialSlotEpochError("epoch reset API URL must be credential-free HTTPS")
    path = parsed.path.rstrip("/")
    if not path.endswith("/api/v1"):
        path += "/api/v1"
    return urlunsplit((parsed.scheme, parsed.netloc, path + "/admin/official-slot-epoch", "", ""))


def reset_epoch(
    *,
    base_url: str,
    admin_token: str,
    expected_epoch_version: int,
    reason: str,
    code_revision: str,
    confirmation: str,
    transport: httpx.BaseTransport | None = None,
) -> dict[str, object]:
    token = admin_token.strip()
    revision = code_revision.strip().casefold()
    normalized_reason = reason.strip()
    if (
        len(token) < 32
        or expected_epoch_version < 1
        or not 20 <= len(normalized_reason) <= 500
        or re.fullmatch(r"[0-9a-f]{7,40}", revision) is None
        or confirmation != "RESET_OFFICIAL_SLOT_EPOCH_AT_NEXT_KST_DAY"
    ):
        raise OfficialSlotEpochError("epoch reset inputs are invalid")
    request_payload = {
        "action": "reset",
        "pipeline": "ingest-official",
        "expected_epoch_version": expected_epoch_version,
        "reason": normalized_reason,
        "code_revision": revision,
        "confirmation": confirmation,
    }
    with httpx.Client(timeout=20.0, transport=transport, follow_redirects=False) as client:
        response = client.post(
            _endpoint(base_url),
            content=json.dumps(
                request_payload, separators=(",", ":"), sort_keys=True
            ).encode("utf-8"),
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json; charset=utf-8",
            },
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise OfficialSlotEpochError("epoch reset API returned invalid JSON") from exc
    if response.status_code != 200 or not isinstance(payload, dict) or payload.get("ok") is not True:
        raise OfficialSlotEpochError(f"epoch reset was rejected (HTTP {response.status_code})")
    epoch_id = payload.get("epoch_id")
    active_raw = payload.get("active_from")
    try:
        active_from = datetime.fromisoformat(
            str(active_raw).replace("Z", "+00:00")
        )
    except (TypeError, ValueError):
        active_from = None
    active_kst = active_from.astimezone(_KST) if active_from is not None else None
    if (
        payload.get("pipeline") != "ingest-official"
        or payload.get("epoch_version") != expected_epoch_version + 1
        or payload.get("claims_preserved") is not True
        or not isinstance(epoch_id, str)
        or _ENTITY_ID_RE.fullmatch(epoch_id) is None
        or active_from is None
        or active_from.tzinfo is None
        or active_from.utcoffset() != timedelta(0)
        or active_kst is None
        or (active_kst.hour, active_kst.minute, active_kst.second, active_kst.microsecond)
        != (0, 0, 0, 0)
    ):
        raise OfficialSlotEpochError("epoch reset ACK is incomplete or inconsistent")
    return payload


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Advance the durable official-slot epoch")
    parser.add_argument("--expected-epoch-version", type=int, required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--confirmation", required=True)
    args = parser.parse_args(argv)
    result = reset_epoch(
        base_url=os.environ.get("BSIDE_API_BASE_URL", "")
        or os.environ.get("GOVERNANCE_API_BASE_URL", ""),
        admin_token=os.environ.get("BSIDE_ADMIN_TOKEN", ""),
        expected_epoch_version=args.expected_epoch_version,
        reason=args.reason,
        code_revision=os.environ.get("GITHUB_SHA", ""),
        confirmation=args.confirmation,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
