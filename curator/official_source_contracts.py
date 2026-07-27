"""Fixed semantic contracts for protected official-source grants."""

from __future__ import annotations

import hashlib
import json
from typing import Mapping


DART_METADATA_SOURCE_RIGHT: dict[str, object] = {
    "source_right_id": "official:dart",
    "source_type": "official_disclosure",
    "source_key": "dart",
    "source_name": "OpenDART",
    "permission_scope": (
        "Official OpenDART metadata only: company and filing identifiers, "
        "original filing title and language, filing date and time, official "
        "source URL, filing type, and correction, cancellation, or withdrawal "
        "relationship. Full filing text, document bodies, attachments, media, "
        "and third-party content are excluded."
    ),
    "evidence_uri": "https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS001",
    "evidence_hash": None,
    "valid_until": None,
    "revoked_at": None,
    "ai_allowed": False,
    "redistribution_allowed": True,
    "status": "active",
}


def source_right_contract_payload(
    source: Mapping[str, object] = DART_METADATA_SOURCE_RIGHT,
) -> dict[str, object]:
    """Build the cross-runtime ``source-right-contract-v1`` digest input."""

    evidence_uri = str(source.get("evidence_uri") or "")
    evidence_hash = str(source.get("evidence_hash") or "").strip().lower()
    permission_scope = str(source.get("permission_scope") or "")
    return {
        "contract_version": 1,
        "source_right_id": str(source.get("source_right_id") or ""),
        "source_type": str(source.get("source_type") or ""),
        "source_key": str(source.get("source_key") or ""),
        "source_name": str(source.get("source_name") or ""),
        "permission_scope_sha256": hashlib.sha256(
            permission_scope.encode("utf-8")
        ).hexdigest(),
        "evidence_uri_sha256": (
            hashlib.sha256(evidence_uri.encode("utf-8")).hexdigest()
            if evidence_uri
            else None
        ),
        "evidence_hash": evidence_hash or None,
        # The protected bootstrap chooses valid_from from the API server clock.
        # The stable contract records its eligibility state, not that timestamp.
        "valid_from_state": "eligible",
        "valid_until": source.get("valid_until"),
        "revoked_at": source.get("revoked_at"),
        "ai_allowed": source.get("ai_allowed") is True,
        "redistribution_allowed": source.get("redistribution_allowed") is True,
        "status": str(source.get("status") or ""),
    }


def source_right_contract_revision(
    payload: Mapping[str, object] | None = None,
) -> str:
    canonical = json.dumps(
        dict(source_right_contract_payload() if payload is None else payload),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


DART_SOURCE_RIGHT_CONTRACT_REVISION = source_right_contract_revision()
