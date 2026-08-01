from __future__ import annotations

import argparse
import base64
import binascii
import gzip
import hashlib
import io
import itertools
import json
import os
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping, Sequence
from urllib.parse import quote, urlsplit, urlunsplit

import httpx


SCHEMA_VERSION = 1
CANDIDATE_KIND = "bside-global-alpha-expedited-editorial-candidates"
DECISION_KIND = "bside-global-alpha-expedited-editorial-human-decisions"
HUMAN_REVIEW_KIND = "bside-global-alpha-human-review"
PUBLICATION_KIND = "bside-global-alpha-expedited-editorial-publication"
CARRY_FORWARD_KIND = "bside-global-alpha-expedited-editorial-carry-forward"
CARRY_FORWARD_INTENT_KIND = (
    "bside-global-alpha-expedited-editorial-carry-forward-intent"
)
DISPLAY_TARGET_REPAIR_KIND = (
    "bside-global-alpha-expedited-editorial-display-target-repair"
)
CARRY_FORWARD_INTENT_MAX_FRESH_AGE = timedelta(minutes=30)
APPROVED_CANONICAL_BASIS_KIND = (
    "bside-global-alpha-expedited-approved-canonical-basis"
)
LEGACY_APPROVAL_CANDIDATE_SHA256 = (
    "c24627699633cf02084a2caeb3334c182c404861f85e8f4d27acf116fc6d8f76"
)
LEGACY_APPROVAL_REVIEWER = "bside-owner-20260731"
LEGACY_APPROVAL_ATTESTATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "global-alpha-legacy-approval-c2462769.json"
)
LEGACY_APPROVAL_ATTESTATION_SHA256 = (
    "3c64d0dd3567e1c6ec39f4484805dd1754c97370fa415503fedefb713487591e"
)
LEGACY_APPROVAL_CORRECTION_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "global-alpha-legacy-approval-c2462769-transform-correction-v1.json"
)
LEGACY_APPROVAL_CORRECTION_SHA256 = (
    "5e52208a48d6a118b33956f9802378da13a709c76b8573895b365e177fd525da"
)
LEGACY_APPROVAL_CORRECTION_RAW_SHA256 = (
    "a98a13fb3a4b8ee6a4dd0673b0e73547540707f027366f760484bf14c062ddf7"
)
LEGACY_APPROVAL_CHAIN_SHA256 = (
    "848696ec784ca0af613cd32b0de67b8e8f97b4837d8eb32dff464942266c970f"
)
LEGACY_APPROVAL_CORRECTION_TEXT_SHA256 = (
    "a2aac9ad15a4446b7dcdd90d4a0d56ff3175fd5a81312367553590b2927f8b40"
)
LEGACY_APPROVAL_EVENT_IDS_SHA256 = (
    "fd7421931f90a8c7428c99289963d9456a7e64f2fe4b830c42e6e413a0c941fb"
)
LEGACY_APPROVAL_TEXT_SHA256 = (
    "148a7f5b89fd1edcd5e2b7e548ae2ec747cfee774b8ad0066b4020eae3ba95a6"
)
LEGACY_APPROVAL_SOURCE_DECISION_SHA256 = (
    "0056e948bfbb635362a2244cda5f0f25350112dd6a3e06f4f22cb18badd3523b"
)
LEGACY_APPROVAL_CANDIDATE_ARTIFACT = {
    "run_id": 30581161308,
    "artifact_id": 8774655231,
    "artifact_name": (
        "global-alpha-expedited-editorial-candidates-"
        "b44a1aebc2eb6e7b58e5960b0f8245b87e901052"
    ),
    "artifact_digest": (
        "sha256:"
        "f7eec4481564f52b89fbda166544cb1bc0b79e8ee940a8173ed5859aade40afd"
    ),
}
LEGACY_APPROVAL_PUBLICATION_ARTIFACT = {
    "run_id": 30587485449,
    "artifact_id": 8777083749,
    "artifact_name": (
        "global-alpha-expedited-editorial-publication-"
        "b44a1aebc2eb6e7b58e5960b0f8245b87e901052-30587485449-1"
    ),
    "artifact_digest": (
        "sha256:"
        "95028a16adedfc19b5dfe3c6e0b0c36696b5c2619a44f0040d51ef3b1ffcbbaa"
    ),
}
LEGACY_DISPLAY_TARGET_REPAIR_DISCOVERY_ARTIFACT = {
    "run_id": 30693489935,
    "artifact_id": 8816492404,
    "artifact_name": (
        "global-alpha-expedited-editorial-carry-intent-"
        "e8a3ff6acfdd2233cc32ed3b8890598528cf984b-30693489935-1"
    ),
    "artifact_digest": (
        "sha256:"
        "f1a6ab55e28867139f505fe09a445bd45503ad606cc6f709b9e471be3302d6c8"
    ),
}
LEGACY_DISPLAY_TARGET_REPAIR_EVENT_IDS = (
    "event:6fe8cc863cdb3faa55b667f440204f72",
    "event:241d63264e99fadbfe08cb1f6b219c41",
    "event:e90d2124ba3c3459851db2d9a4ee9408",
    "event:c866f5f7841b74642a636049b92e8c0a",
    "event:961ed4d77655b41b247e9d05d3b458c8",
    "event:dba39b9797cbf6efd554cc07c808aea9",
)
LEGACY_APPROVAL_ACTION_OVERRIDES = {
    3: "rights_issue_price_finalized",
    4: "treasury_convertible_bond_sale",
    7: "trading_suspension_lifted",
    9: "treasury_convertible_bond_sale",
    10: "delisting_objection_filed",
    11: "trading_suspension_for_share_consolidation_or_split",
    12: "capital_issuance_result",
    13: "trading_suspension_lifted",
    16: "delisting_review_pending",
    18: "capital_issuance_result",
    19: "delisting_injunction_requested",
}
LEGACY_APPROVED_TRANSFORM_CONTRACT = {
    "event_decision": "approved",
    "country": "KR",
    "identity_target": "candidate.issuer_name",
    "identity_effective_at": (
        "candidate.identity_effective_at_or_occurred_at"
    ),
    "identity_deadline_at": None,
    "deadline_at": None,
    "summary": "candidate.issuer_name — candidate.title",
    "importance": "candidate.importance",
    "current_status": "candidate.verification_status",
    "verification_status": (
        "preserve_corrected_or_withdrawn_else_official"
    ),
    "actor": "exact_unique_candidate_actor_with_country_KR",
    "official_evidence": "exact_candidate_official_document_basis",
    "same_event_pair_decision": False,
    "top5_decision": "approved",
}
LEGACY_APPROVAL_CORRECTED_TRANSFORM_CONTRACT = {
    "identity_target": "회사명 — 원문 제목",
    "summary": "회사명는 DART에 「원문 제목」을 공시했다.",
    "current_status": {
        "corrected": "corrected_official_disclosure",
        "official": "official_disclosure_confirmed",
    },
}
LEGACY_APPROVAL_PRESERVED_DECISIONS = {
    "event_count": 20,
    "event_decision": "approved",
    "same_event_pair_count": 40,
    "same_event_pair_decision": False,
    "top5_count": 5,
    "top5_decision": "approved",
    "actor": "unchanged",
    "country": "KR",
    "official_evidence": "unchanged",
}
EVENT_COUNT = 20
PAIR_COUNT = 40
TOP5_COUNT = 5
CARRY_FORWARD_OUTCOME_FIELDS = frozenset(
    {
        "event_id",
        "decision",
        "result",
        "final_review_status",
        "final_publication_status",
        "final_identity_status",
        "final_updated_at",
        "source_issuer_name",
        "current_issuer_name",
        "issuer_name_drift",
        "source_event_evidence_sha256",
        "current_event_evidence_sha256",
        "approved_event_basis_sha256",
        "immutable_evidence_basis_sha256",
        "current_snapshot_sha256",
    }
)
MAX_RESPONSE_BYTES = 250_000
MAX_COMPRESSED_DECISIONS_BYTES = 1_000_000
MAX_DECISIONS_BYTES = 5_000_000
SHA40 = re.compile(r"^[0-9a-f]{40}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
ENTITY_ID = re.compile(r"^[A-Za-z0-9_.:\-]{1,96}$")
OFFICIAL_SOURCE_CLASSES = frozenset(
    {
        "official_disclosure",
        "official_register",
        "company_statement",
        "official_issuer",
    }
)
EXPEDITED_DOCUMENT_FIELDS = frozenset(
    {
        "document_id",
        "issuer_id",
        "country_code",
        "source_right_id",
        "source_class",
        "source_key",
        "document_type",
        "original_language",
        "title",
        "original_url",
        "content_hash",
        "filed_at",
        "published_at",
        "retrieved_at",
        "updated_at",
        "relation_type",
        "position_no",
        "connector_id",
        "connector_base_url",
        "coverage_mode",
        "connector_status",
    }
)
SAFE_ACTOR_FIELDS = (
    "actor_id",
    "display_name",
    "actor_type",
    "actor_role",
    "country_code",
)
SOURCE_COVERAGE_MODES = frozenset(
    {
        "market-wide",
        "official-register",
        "selected-issuers",
        "link-only",
        "coverage-unavailable",
    }
)
FORBIDDEN_DOCUMENT_KEYS = frozenset(
    {
        "body",
        "body_text",
        "content",
        "document_body",
        "internal_note",
        "payload_json",
        "queue_state",
    }
)
TELEGRAM_HOSTS = frozenset({"t.me", "telegram.me", "telegram.org"})
EVENT_FAMILIES = frozenset(
    {
        "large_ownership",
        "meeting_and_vote",
        "tender_offer_and_mna",
        "capital_issuance",
        "capital_return",
        "board_and_compensation",
        "listing_status",
        "correction_and_withdrawal",
    }
)
IMPORTANCE = frozenset({"low", "medium", "high", "critical", "market_sensitive"})
COUNTRIES = frozenset({"KR", "US", "JP", "GB", "CA", "AU"})
MAX_HUMAN_REVIEW_AGE = timedelta(hours=72)
CARRY_FORWARD_ARTIFACT_FIELDS = (
    "run_id",
    "artifact_id",
    "artifact_name",
    "artifact_digest",
)


class ExpeditedEditorialError(ValueError):
    """Raised when protected editorial publication cannot fail closed."""


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _mapping(value: object, location: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ExpeditedEditorialError(f"{location}: object required")
    return dict(value)


def _list(value: object, location: str) -> list[object]:
    if not isinstance(value, list):
        raise ExpeditedEditorialError(f"{location}: array required")
    return value


def _text(value: object, field: str, location: str, *, maximum: int = 4096) -> str:
    result = value.strip() if isinstance(value, str) else ""
    if not result or len(result) > maximum or any(ord(char) < 32 for char in result):
        raise ExpeditedEditorialError(f"{location}: invalid {field}")
    return result


def _optional_text(
    value: object,
    field: str,
    location: str,
    *,
    maximum: int = 4096,
) -> str | None:
    if value is None:
        return None
    return _text(value, field, location, maximum=maximum)


def _sha40(value: object, location: str) -> str:
    result = _text(value, "code_revision", location).casefold()
    if SHA40.fullmatch(result) is None:
        raise ExpeditedEditorialError(f"{location}: full Git SHA required")
    return result


def _sha256(value: object, field: str, location: str) -> str:
    result = _text(value, field, location).casefold()
    if result.startswith("sha256:"):
        result = result.removeprefix("sha256:")
    if SHA256.fullmatch(result) is None:
        raise ExpeditedEditorialError(f"{location}: invalid {field}")
    return result


def _positive_int(value: object, field: str, location: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ExpeditedEditorialError(f"{location}: invalid {field}")
    return value


def _timestamp(value: object, field: str, location: str) -> str:
    raw = _text(value, field, location, maximum=64)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ExpeditedEditorialError(f"{location}: invalid {field}") from exc
    if parsed.tzinfo is None:
        raise ExpeditedEditorialError(f"{location}: {field} requires timezone")
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def _optional_timestamp(value: object, field: str, location: str) -> str | None:
    return None if value is None else _timestamp(value, field, location)


def _load_json(path: Path, location: str) -> dict[str, object]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ExpeditedEditorialError(f"{location}: unreadable file") from exc
    if len(raw) > 5_000_000:
        raise ExpeditedEditorialError(f"{location}: file too large")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExpeditedEditorialError(f"{location}: invalid UTF-8 JSON") from exc
    return _mapping(value, location)


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_bytes(value) + b"\n")


def decode_human_decisions_secret(encoded: str, output: Path) -> None:
    compact = encoded.strip()
    if not compact or len(compact) > 1_500_000:
        raise ExpeditedEditorialError("human decisions: invalid encoded size")
    try:
        compressed = base64.b64decode(compact, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ExpeditedEditorialError(
            "human decisions: invalid base64"
        ) from exc
    if not compressed or len(compressed) > MAX_COMPRESSED_DECISIONS_BYTES:
        raise ExpeditedEditorialError("human decisions: compressed size limit")
    expanded = bytearray()
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(compressed), mode="rb") as stream:
            while True:
                chunk = stream.read(
                    min(65_536, MAX_DECISIONS_BYTES + 1 - len(expanded))
                )
                if not chunk:
                    break
                expanded.extend(chunk)
                if len(expanded) > MAX_DECISIONS_BYTES:
                    raise ExpeditedEditorialError(
                        "human decisions: decompressed size limit"
                    )
    except (OSError, EOFError) as exc:
        raise ExpeditedEditorialError("human decisions: invalid gzip") from exc
    try:
        value = json.loads(bytes(expanded).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExpeditedEditorialError(
            "human decisions: invalid UTF-8 JSON"
        ) from exc
    _mapping(value, "human decisions")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(bytes(expanded))
    try:
        output.chmod(0o600)
    except OSError:
        pass


def normalize_api_base(value: str) -> str:
    parsed = urlsplit(value.strip().rstrip("/"))
    if (
        parsed.scheme.casefold() != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ExpeditedEditorialError("invalid_api_base_url")
    path = parsed.path.rstrip("/")
    if path.endswith("/api/v1"):
        path = path[: -len("/api/v1")] + "/api/v2"
    elif not path.endswith("/api/v2"):
        path += "/api/v2"
    return urlunsplit(("https", parsed.netloc, path, "", ""))


class EditorialClient:
    def __init__(self, api_base: str, editor_token: str) -> None:
        self.api_base = normalize_api_base(api_base)
        token = editor_token.strip()
        if not token or any(ord(char) < 33 or ord(char) > 126 for char in token):
            raise ExpeditedEditorialError("invalid_editor_token")
        self._client = httpx.Client(
            timeout=httpx.Timeout(45.0, connect=15.0),
            follow_redirects=False,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {token}",
            },
        )

    def close(self) -> None:
        self._client.close()

    def _json(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, str | int] | None = None,
        payload: Mapping[str, object] | None = None,
        authenticated: bool = True,
    ) -> dict[str, object]:
        headers = None if authenticated else {"Authorization": ""}
        try:
            response = self._client.request(
                method,
                self.api_base + path,
                params=params,
                json=payload,
                headers=headers,
            )
        except httpx.HTTPError as exc:
            raise ExpeditedEditorialError(f"{path}: request failed") from exc
        if 300 <= response.status_code < 400:
            raise ExpeditedEditorialError(f"{path}: redirect forbidden")
        if response.status_code != 200:
            error = ""
            try:
                body = response.json()
                error = str(body.get("error") or "")
            except (ValueError, AttributeError):
                pass
            suffix = f" ({error})" if error else ""
            raise ExpeditedEditorialError(
                f"{path}: HTTP {response.status_code}{suffix}"
            )
        if len(response.content) > MAX_RESPONSE_BYTES:
            raise ExpeditedEditorialError(f"{path}: response budget exceeded")
        try:
            value = json.loads(response.content.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ExpeditedEditorialError(f"{path}: invalid JSON") from exc
        result = _mapping(value, path)
        if result.get("ok") is not True or result.get("api_version") != "v2":
            raise ExpeditedEditorialError(f"{path}: successful API v2 required")
        return result

    def health(self) -> dict[str, object]:
        return self._json("GET", "/health", authenticated=False)

    def candidates(self) -> dict[str, object]:
        return self._json(
            "GET",
            "/admin/expedited-review-candidates",
            params={"limit": 50},
        )

    def event(self, event_id: str) -> dict[str, object]:
        return self._json(
            "GET",
            "/admin/expedited-review-candidates/"
            + quote(event_id, safe=""),
        )

    def review(self, event_id: str, payload: Mapping[str, object]) -> dict[str, object]:
        return self._json(
            "POST",
            "/admin/events/" + quote(event_id, safe="") + "/review",
            payload=payload,
        )

    def publish_brief(self, payload: Mapping[str, object]) -> dict[str, object]:
        return self._json("POST", "/admin/briefs", payload=payload)


def _official_url(value: object, location: str) -> tuple[str, str]:
    url = _text(value, "original_url", location)
    parsed = urlsplit(url)
    host = (parsed.hostname or "").casefold().rstrip(".")
    if (
        parsed.scheme.casefold() != "https"
        or not host
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or any(host == item or host.endswith("." + item) for item in TELEGRAM_HOSTS)
    ):
        raise ExpeditedEditorialError(f"{location}: official HTTPS URL required")
    query = (parsed.query or "").casefold()
    if any(word in query for word in ("token=", "secret=", "signature=", "key=")):
        raise ExpeditedEditorialError(f"{location}: credential URL forbidden")
    return url, host


def _connector_host_allowed(connector_id: str, base_url: str, host: str) -> bool:
    base_host = (urlsplit(base_url).hostname or "").casefold().rstrip(".")
    if not base_host:
        return False
    if connector_id == "connector:kr:dart":
        return host in {"dart.fss.or.kr", "opendart.fss.or.kr"}
    if connector_id == "connector:us:sec-edgar":
        return host == "sec.gov" or host.endswith(".sec.gov")
    return host == base_host or host.endswith("." + base_host)


def _validate_document(value: object, location: str) -> dict[str, object]:
    document = _mapping(value, location)
    forbidden = FORBIDDEN_DOCUMENT_KEYS.intersection(document)
    if forbidden:
        raise ExpeditedEditorialError(
            f"{location}: forbidden document content ({sorted(forbidden)[0]})"
        )
    if set(document) != EXPEDITED_DOCUMENT_FIELDS:
        raise ExpeditedEditorialError(
            f"{location}: exact safe document fields required"
        )
    document_id = _text(document.get("document_id"), "document_id", location)
    if ENTITY_ID.fullmatch(document_id) is None:
        raise ExpeditedEditorialError(f"{location}: invalid document_id")
    issuer_id = _text(document.get("issuer_id"), "issuer_id", location)
    if ENTITY_ID.fullmatch(issuer_id) is None:
        raise ExpeditedEditorialError(f"{location}: invalid issuer_id")
    country = _text(document.get("country_code"), "country_code", location)
    if country not in {"KR", "US"}:
        raise ExpeditedEditorialError(f"{location}: KR/US document required")
    source_right_id = _text(
        document.get("source_right_id"), "source_right_id", location
    )
    if ENTITY_ID.fullmatch(source_right_id) is None:
        raise ExpeditedEditorialError(f"{location}: invalid source_right_id")
    source_class = _text(document.get("source_class"), "source_class", location)
    if source_class not in OFFICIAL_SOURCE_CLASSES or "telegram" in source_class:
        raise ExpeditedEditorialError(f"{location}: official source required")
    connector_id = _text(document.get("connector_id"), "connector_id", location)
    if ENTITY_ID.fullmatch(connector_id) is None:
        raise ExpeditedEditorialError(f"{location}: invalid connector_id")
    connector_base, _ = _official_url(
        document.get("connector_base_url"),
        location + ".connector_base_url",
    )
    original_url, host = _official_url(document.get("original_url"), location)
    if not _connector_host_allowed(connector_id, connector_base, host):
        raise ExpeditedEditorialError(
            f"{location}: URL host does not match connector identity"
        )
    if document.get("connector_status") != "active":
        raise ExpeditedEditorialError(f"{location}: connector must be active")
    coverage_mode = _text(
        document.get("coverage_mode"), "coverage_mode", location
    )
    if coverage_mode not in SOURCE_COVERAGE_MODES:
        raise ExpeditedEditorialError(f"{location}: invalid coverage_mode")
    position_no = document.get("position_no")
    if (
        isinstance(position_no, bool)
        or not isinstance(position_no, int)
        or position_no < 0
    ):
        raise ExpeditedEditorialError(f"{location}: invalid position_no")
    language = _text(
        document.get("original_language"),
        "original_language",
        location,
        maximum=16,
    )
    if re.fullmatch(r"[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})*", language) is None:
        raise ExpeditedEditorialError(f"{location}: invalid original_language")
    normalized: dict[str, object] = {
        "document_id": document_id,
        "issuer_id": issuer_id,
        "country_code": country,
        "source_right_id": source_right_id,
        "source_class": source_class,
        "source_key": _optional_text(
            document.get("source_key"), "source_key", location, maximum=96
        ),
        "document_type": _optional_text(
            document.get("document_type"),
            "document_type",
            location,
            maximum=96,
        ),
        "original_language": language,
        "title": _text(document.get("title"), "title", location, maximum=700),
        "original_url": original_url,
        "content_hash": _sha256(
            document.get("content_hash"), "content_hash", location
        ),
        "relation_type": _text(
            document.get("relation_type"),
            "relation_type",
            location,
            maximum=64,
        ),
        "position_no": position_no,
        "connector_id": connector_id,
        "connector_base_url": connector_base,
        "coverage_mode": coverage_mode,
        "connector_status": "active",
    }
    for field in ("filed_at", "published_at", "retrieved_at"):
        normalized[field] = _optional_timestamp(document.get(field), field, location)
    normalized["updated_at"] = _timestamp(
        document.get("updated_at"), "updated_at", location
    )
    return normalized


def _validate_event(value: object, location: str) -> dict[str, object]:
    event = _mapping(value, location)
    event_id = _text(event.get("event_id"), "event_id", location)
    if ENTITY_ID.fullmatch(event_id) is None:
        raise ExpeditedEditorialError(f"{location}: invalid event_id")
    issuer_id = _text(event.get("issuer_id"), "issuer_id", location)
    if ENTITY_ID.fullmatch(issuer_id) is None:
        raise ExpeditedEditorialError(f"{location}: invalid issuer_id")
    country = _text(event.get("country"), "country", location)
    if country not in {"KR", "US"}:
        raise ExpeditedEditorialError(f"{location}: KR/US event required")
    documents = [
        _validate_document(raw, f"{location}.official_documents[{index}]")
        for index, raw in enumerate(
            _list(event.get("official_documents"), f"{location}.official_documents")
        )
    ]
    if not documents:
        raise ExpeditedEditorialError(f"{location}: official evidence required")
    if len({str(item["document_id"]) for item in documents}) != len(documents):
        raise ExpeditedEditorialError(f"{location}: duplicate document")
    if any(
        item["issuer_id"] != issuer_id or item["country_code"] != country
        for item in documents
    ):
        raise ExpeditedEditorialError(
            f"{location}: document issuer/country binding mismatch"
        )
    evidence_count = event.get("official_evidence_count")
    if (
        isinstance(evidence_count, bool)
        or not isinstance(evidence_count, int)
        or evidence_count != len(documents)
    ):
        raise ExpeditedEditorialError(f"{location}: evidence count mismatch")
    normalized = dict(event)
    normalized["event_id"] = event_id
    normalized["issuer_id"] = issuer_id
    normalized["issuer_name"] = _text(
        event.get("issuer_name"), "issuer_name", location, maximum=255
    )
    normalized["country"] = country
    normalized["event_family"] = _text(
        event.get("event_family"), "event_family", location
    )
    normalized["title"] = _text(event.get("title"), "title", location, maximum=700)
    normalized["original_language"] = _text(
        event.get("original_language"),
        "original_language",
        location,
        maximum=16,
    )
    normalized["updated_at"] = _timestamp(
        event.get("updated_at"), "updated_at", location
    )
    for field in (
        "occurred_at",
        "deadline_at",
        "first_observed_at",
        "identity_effective_at",
        "identity_deadline_at",
    ):
        normalized[field] = _optional_timestamp(event.get(field), field, location)
    normalized["official_documents"] = documents
    claimed = _sha256(
        event.get("event_evidence_sha256"),
        "event_evidence_sha256",
        location,
    )
    calculated = canonical_sha256(
        {
            "event_id": event_id,
            "event_updated_at": normalized["updated_at"],
            "official_documents": documents,
        }
    )
    if claimed != calculated:
        raise ExpeditedEditorialError(f"{location}: evidence digest mismatch")
    normalized["event_evidence_sha256"] = claimed
    return normalized


def _event_basis(event: Mapping[str, object]) -> dict[str, object]:
    result = {
        key: event.get(key)
        for key in (
            "event_id",
            "issuer_id",
            "issuer_name",
            "country",
            "event_family",
            "title",
            "original_language",
            "summary",
            "occurred_at",
            "deadline_at",
            "importance",
            "verification_status",
            "change_type",
            "current_status",
            "first_observed_at",
            "review_status",
            "publication_status",
            "identity_action",
            "identity_target",
            "identity_actor_id",
            "identity_effective_at",
            "identity_deadline_at",
            "identity_status",
            "comparison_key",
            "updated_at",
            "official_documents",
            "official_evidence_count",
            "event_evidence_sha256",
        )
    }
    actors: list[dict[str, object]] = []
    seen: set[tuple[str, str]] = set()
    for index, raw in enumerate(_list(event.get("actors"), "event.actors")):
        location = f"event.actors[{index}]"
        actor = _mapping(raw, location)
        country_code = _optional_text(
            actor.get("country_code"),
            "country_code",
            location,
            maximum=2,
        )
        safe: dict[str, object] = {
            "actor_id": _text(
                actor.get("actor_id"), "actor_id", location, maximum=64
            ),
            "display_name": _text(
                actor.get("display_name"),
                "display_name",
                location,
                maximum=255,
            ),
            "actor_type": _text(
                actor.get("actor_type"), "actor_type", location, maximum=40
            ),
            "actor_role": _text(
                actor.get("actor_role"), "actor_role", location, maximum=40
            ),
            "country_code": country_code,
        }
        if re.fullmatch(
            r"[A-Za-z0-9_.:\-]{1,64}",
            str(safe["actor_id"]),
        ) is None:
            raise ExpeditedEditorialError(f"{location}: invalid actor_id")
        for field in ("actor_type", "actor_role"):
            if re.fullmatch(
                r"[a-z][a-z0-9_]{1,39}",
                str(safe[field]),
            ) is None:
                raise ExpeditedEditorialError(f"{location}: invalid {field}")
        if country_code is not None and country_code not in COUNTRIES:
            raise ExpeditedEditorialError(f"{location}: invalid country_code")
        identity = (str(safe["actor_id"]), str(safe["actor_role"]))
        if identity in seen:
            raise ExpeditedEditorialError(f"{location}: duplicate actor")
        seen.add(identity)
        actors.append(safe)
    result["actors"] = actors
    return result


def _pair(
    *,
    stratum: str,
    left_event: Mapping[str, object],
    right_event: Mapping[str, object],
    revision: str,
) -> dict[str, object]:
    left_doc = _mapping(
        _list(left_event["official_documents"], "left.documents")[0],
        "left.document",
    )
    right_doc = _mapping(
        _list(right_event["official_documents"], "right.documents")[0],
        "right.document",
    )
    ordered = sorted(
        (
            (
                str(left_doc["document_id"]),
                str(left_event["event_id"]),
                left_doc,
                left_event,
            ),
            (
                str(right_doc["document_id"]),
                str(right_event["event_id"]),
                right_doc,
                right_event,
            ),
        )
    )
    left_id, left_event_id, left_doc, left_event = ordered[0]
    right_id, right_event_id, right_doc, right_event = ordered[1]
    pair_id = "pair:" + hashlib.sha256(
        (
            revision
            + "\x1f"
            + stratum
            + "\x1f"
            + left_id
            + "\x1f"
            + right_id
        ).encode("utf-8")
    ).hexdigest()[:40]
    return {
        "pair_id": pair_id,
        "stratum": stratum,
        "left_event_id": left_event_id,
        "right_event_id": right_event_id,
        "left_document_id": left_id,
        "right_document_id": right_id,
        "left_title": left_doc["title"],
        "right_title": right_doc["title"],
        "left_url": left_doc["original_url"],
        "right_url": right_doc["original_url"],
        "left_content_hash": left_doc["content_hash"],
        "right_content_hash": right_doc["content_hash"],
        "left_issuer_id": left_event["issuer_id"],
        "right_issuer_id": right_event["issuer_id"],
        "left_event_family": left_event["event_family"],
        "right_event_family": right_event["event_family"],
    }


def _build_pairs(
    events: Sequence[Mapping[str, object]],
    revision: str,
) -> list[dict[str, object]]:
    hard: list[dict[str, object]] = []
    easy: list[dict[str, object]] = []
    seen_documents: set[tuple[str, str]] = set()
    combinations = list(itertools.combinations(events, 2))
    combinations.sort(
        key=lambda pair: hashlib.sha256(
            (
                revision
                + "\x1f"
                + str(pair[0]["event_id"])
                + "\x1f"
                + str(pair[1]["event_id"])
            ).encode("utf-8")
        ).hexdigest()
    )
    for left, right in combinations:
        left_doc = _mapping(
            _list(left["official_documents"], "left.documents")[0],
            "left.document",
        )
        right_doc = _mapping(
            _list(right["official_documents"], "right.documents")[0],
            "right.document",
        )
        first_document_id, second_document_id = sorted(
            (str(left_doc["document_id"]), str(right_doc["document_id"]))
        )
        key = (first_document_id, second_document_id)
        if key in seen_documents:
            continue
        same_issuer = left["issuer_id"] == right["issuer_id"]
        same_family = left["event_family"] == right["event_family"]
        if same_issuer or same_family:
            hard.append(
                _pair(
                    stratum="hard_same_issuer_or_family",
                    left_event=left,
                    right_event=right,
                    revision=revision,
                )
            )
            seen_documents.add(key)
        elif not same_issuer and not same_family:
            easy.append(
                _pair(
                    stratum="easy_cross_issuer_and_family",
                    left_event=left,
                    right_event=right,
                    revision=revision,
                )
            )
            seen_documents.add(key)
    if len(hard) < PAIR_COUNT // 2 or len(easy) < PAIR_COUNT // 2:
        raise ExpeditedEditorialError(
            "candidate export requires at least 20 hard and 20 easy document pairs"
        )
    result = hard[: PAIR_COUNT // 2] + easy[: PAIR_COUNT // 2]
    if len(result) != PAIR_COUNT:
        raise ExpeditedEditorialError("exactly 40 pair candidates required")
    return sorted(result, key=lambda item: str(item["pair_id"]))


def _select_events(
    rows: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Keep the five highest-ranked rows, then diversify by market and family."""
    selected = [dict(item) for item in rows[:TOP5_COUNT]]
    selected_ids = {str(item["event_id"]) for item in selected}
    strata: dict[
        tuple[str, str],
        dict[str, list[dict[str, object]]],
    ] = {}
    for raw in rows[TOP5_COUNT:]:
        item = dict(raw)
        stratum_key = (
            str(item["country"]),
            str(item["event_family"]),
        )
        issuer_groups = strata.setdefault(stratum_key, {})
        issuer_groups.setdefault(str(item["issuer_id"]), []).append(item)
    queues = [
        [strata[key][issuer_id] for issuer_id in sorted(strata[key])]
        for key in sorted(strata)
    ]
    while queues and len(selected) < EVENT_COUNT:
        remaining: list[list[list[dict[str, object]]]] = []
        for issuer_queues in queues:
            if len(selected) >= EVENT_COUNT:
                break
            selected_item: dict[str, object] | None = None
            while issuer_queues and selected_item is None:
                queue = issuer_queues.pop(0)
                while queue and str(queue[0]["event_id"]) in selected_ids:
                    queue.pop(0)
                if queue:
                    selected_item = queue.pop(0)
                if queue:
                    issuer_queues.append(queue)
            if selected_item is not None:
                selected.append(selected_item)
                selected_ids.add(str(selected_item["event_id"]))
            if issuer_queues:
                remaining.append(issuer_queues)
        queues = remaining
    if len(selected) != EVENT_COUNT:
        raise ExpeditedEditorialError("candidate export requires 20 distinct events")
    return selected


def _brief_id(cutoff_at: str) -> str:
    external = "global|" + cutoff_at.replace("T", " ").removesuffix("Z")
    candidate = "brief:" + external
    if ENTITY_ID.fullmatch(candidate) is not None:
        return candidate
    return "brief:" + hashlib.sha256(external.encode("utf-8")).hexdigest()[:64]


def _decision_template(
    candidate_sha256: str,
    candidate: Mapping[str, object],
) -> dict[str, object]:
    basis = _mapping(candidate.get("basis"), "candidate.basis")
    events = [
        _mapping(item, f"candidate.events[{index}]")
        for index, item in enumerate(
            _list(basis.get("events"), "candidate.basis.events")
        )
    ]
    pairs = [
        _mapping(item, f"candidate.pairs[{index}]")
        for index, item in enumerate(
            _list(basis.get("same_event_pair_candidates"), "candidate.basis.pairs")
        )
    ]
    top5 = [
        _mapping(item, f"candidate.top5[{index}]")
        for index, item in enumerate(
            _list(basis.get("top5_candidates"), "candidate.basis.top5")
        )
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": DECISION_KIND,
        "environment": "production",
        "is_synthetic": False,
        "code_revision": candidate["code_revision"],
        "candidate_artifact": {
            "run_id": None,
            "artifact_id": None,
            "artifact_name": "",
            "artifact_digest": "",
        },
        "candidate_sha256": candidate_sha256,
        "ground_truth_source": None,
        "ai_generated_ground_truth": False,
        "human_attestation": False,
        "reviewer_reference": None,
        "reviewed_at": None,
        "event_reviews": [
            {
                "event_id": event["event_id"],
                "decision": None,
                "reviewer_type": None,
                "reviewer_reference": None,
                "reviewed_at": None,
                "reason": None,
                "review_payload": {
                    "decision": None,
                    "expected_updated_at": event["updated_at"],
                    "expected_evidence_sha256": event[
                        "event_evidence_sha256"
                    ],
                    "reason": None,
                    "event_family": (
                        None
                        if event["event_family"] == "unclassified"
                        else event["event_family"]
                    ),
                    "identity_action": event.get("identity_action"),
                    "identity_target": event.get("identity_target"),
                    "identity_effective_at": (
                        event.get("identity_effective_at")
                        or event.get("occurred_at")
                    ),
                    "identity_deadline_at": event.get("identity_deadline_at"),
                    "importance": event.get("importance"),
                    "summary": event.get("summary"),
                    "current_status": event.get("current_status"),
                    "actor": None,
                    "merge_into_event_id": None,
                },
            }
            for event in events
        ],
        "same_event_pair_reviews": [
            {
                "pair_id": pair["pair_id"],
                "left_document_id": pair["left_document_id"],
                "right_document_id": pair["right_document_id"],
                "decision": None,
                "reviewer_type": None,
                "reviewer_reference": None,
                "reviewed_at": None,
            }
            for pair in pairs
        ],
        "top5_reviews": [
            {
                "edition_id": item["edition_id"],
                "event_id": item["event_id"],
                "position_no": item["position_no"],
                "decision": None,
                "selection_reason": None,
                "reviewer_type": None,
                "reviewer_reference": None,
                "reviewed_at": None,
            }
            for item in top5
        ],
    }


def _markdown_text(value: object) -> str:
    return (
        str(value or "")
        .replace("\\", "\\\\")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("[", "\\[")
        .replace("]", "\\]")
        .replace("(", "\\(")
        .replace(")", "\\)")
        .replace("`", "\\`")
        .replace("|", "\\|")
        .replace("\r", " ")
        .replace("\n", " ")
    )


def _markdown_url(value: object) -> str:
    return quote(
        str(value or ""),
        safe="/:?&=#%+,-._~@!$'",
    )


def _review_pack(candidate: Mapping[str, object]) -> str:
    basis = _mapping(candidate.get("basis"), "candidate.basis")
    candidate_sha = _sha256(
        candidate.get("candidate_sha256"), "candidate_sha256", "candidate"
    )
    lines = [
        "# BSIDE Production Alpha · Early Access 검수 묶음",
        "",
        f"- 출시 SHA: `{candidate['code_revision']}`",
        f"- 기계 원본 후보 SHA-256: `{candidate_sha}`",
        "- 이 문서는 읽기 전용입니다. 판단은 JSON 양식에 사람이 직접 기록합니다.",
        "",
        "## 사건 20건",
        "",
        "| # | 회사 | 유형 | 원문 제목 | 공식 근거 | 현재 제안 |",
        "|---:|---|---|---|---|---|",
    ]
    lines.insert(
        4,
        "- 당사자 국가가 `국가 미확인`이면 사건 국가로 추론하지 말고, "
        "공식 근거로 확인한 국가만 승인 JSON에 입력합니다. 확인할 수 없으면 사건을 거절합니다.",
    )
    events = _list(basis.get("events"), "basis.events")
    for index, raw in enumerate(events, start=1):
        event = _mapping(raw, f"events[{index}]")
        documents = _list(event.get("official_documents"), "event.documents")
        links = "<br>".join(
            f"[{_markdown_text(_mapping(doc, 'document').get('title'))}]"
            f"({_markdown_url(_mapping(doc, 'document').get('original_url'))})"
            for doc in documents
        )
        proposal = " / ".join(
            filter(
                None,
                (
                    _markdown_text(event.get("identity_action")),
                    _markdown_text(event.get("identity_target")),
                    _markdown_text(event.get("current_status")),
                ),
            )
        )
        lines.append(
            f"| {index} | {_markdown_text(event.get('issuer_name'))} | "
            f"{_markdown_text(event.get('event_family'))} | "
            f"{_markdown_text(event.get('title'))} | {links} | {proposal or '사람 입력 필요'} |"
        )
    lines.extend(
        [
            "",
            "## 동일 사건 후보 40쌍",
            "",
            "| # | 난이도 | 좌측 문서 | 우측 문서 | 사람 판단 |",
            "|---:|---|---|---|---|",
        ]
    )
    for index, raw in enumerate(
        _list(
            basis.get("same_event_pair_candidates"),
            "basis.same_event_pair_candidates",
        ),
        start=1,
    ):
        pair = _mapping(raw, f"pairs[{index}]")
        lines.append(
            f"| {index} | {_markdown_text(pair.get('stratum'))} | "
            f"[{_markdown_text(pair.get('left_title'))}]({_markdown_url(pair.get('left_url'))}) | "
            f"[{_markdown_text(pair.get('right_title'))}]({_markdown_url(pair.get('right_url'))}) | "
            "merge / separate |"
        )
    lines.extend(
        [
            "",
            "## Top 5 후보",
            "",
            "| 순위 | 회사 | 원문 제목 | 공식 근거 수 | 선정 근거 |",
            "|---:|---|---|---:|---|",
        ]
    )
    by_id = {
        str(_mapping(item, "event").get("event_id")): _mapping(item, "event")
        for item in events
    }
    for raw in _list(basis.get("top5_candidates"), "basis.top5"):
        item = _mapping(raw, "top5")
        event = by_id[str(item["event_id"])]
        lines.append(
            f"| {item['position_no']} | {_markdown_text(event.get('issuer_name'))} | "
            f"{_markdown_text(event.get('title'))} | "
            f"{event.get('official_evidence_count')} | 사람 입력 필요 |"
        )
    return "\n".join(lines) + "\n"


def export_candidates(
    client: EditorialClient,
    *,
    expected_revision: str,
) -> tuple[dict[str, object], dict[str, object], str]:
    health = client.health()
    revision = _sha40(health.get("code_revision"), "health")
    if revision != expected_revision:
        raise ExpeditedEditorialError("health: code_revision mismatch")
    if health.get("service") != "bside-global-market-terminal":
        raise ExpeditedEditorialError("health: unexpected service")
    payload = client.candidates()
    meta = _mapping(payload.get("meta"), "candidates.meta")
    if _sha40(meta.get("code_revision"), "candidates.meta") != revision:
        raise ExpeditedEditorialError("candidates: code_revision mismatch")
    rows = [
        _validate_event(item, f"candidates.items[{index}]")
        for index, item in enumerate(
            _list(
                _mapping(payload.get("data"), "candidates.data").get("items"),
                "candidates.data.items",
            )
        )
    ]
    invalid_countries = sorted(
        {str(item["country"]) for item in rows if item["country"] not in {"KR", "US"}}
    )
    if invalid_countries:
        raise ExpeditedEditorialError(
            "candidate export is restricted to KR/US official events"
        )
    if len(rows) < EVENT_COUNT:
        raise ExpeditedEditorialError(
            f"candidate export requires 20 events (available={len(rows)})"
        )
    events = [_event_basis(item) for item in _select_events(rows)]
    if len({str(item["event_id"]) for item in events}) != EVENT_COUNT:
        raise ExpeditedEditorialError("candidate export contains duplicate events")
    pairs = _build_pairs(events, revision)
    collected_at = _timestamp(health.get("time"), "time", "health")
    cutoff_at = collected_at
    edition_id = _brief_id(cutoff_at)
    top5 = [
        {
            "edition_id": edition_id,
            "event_id": event["event_id"],
            "position_no": index,
            "official_evidence_count": event["official_evidence_count"],
            "event_evidence_sha256": event["event_evidence_sha256"],
            "public_eligible": True,
        }
        for index, event in enumerate(events[:TOP5_COUNT], start=1)
    ]
    basis = {
        "source_snapshot_sha256": _sha256(
            meta.get("snapshot_sha256"),
            "snapshot_sha256",
            "candidates.meta",
        ),
        "brief_cutoff_at": cutoff_at,
        "events": events,
        "same_event_pair_candidates": pairs,
        "top5_candidates": top5,
    }
    candidate_sha = canonical_sha256(basis)
    candidate = {
        "schema_version": SCHEMA_VERSION,
        "kind": CANDIDATE_KIND,
        "environment": "production",
        "evidence_source": "production_editor_api_v2",
        "is_synthetic": False,
        "code_revision": revision,
        "collected_at": collected_at,
        "candidate_sha256": candidate_sha,
        "raw_counts": {
            "event_candidate_count": EVENT_COUNT,
            "same_event_pair_candidate_count": PAIR_COUNT,
            "top5_candidate_count": TOP5_COUNT,
        },
        "basis": basis,
    }
    template = _decision_template(candidate_sha, candidate)
    return candidate, template, _review_pack(candidate)


def _validate_candidate(candidate: Mapping[str, object], revision: str) -> dict[str, object]:
    if candidate.get("schema_version") != SCHEMA_VERSION:
        raise ExpeditedEditorialError("candidate: schema_version mismatch")
    if candidate.get("kind") != CANDIDATE_KIND:
        raise ExpeditedEditorialError("candidate: kind mismatch")
    if candidate.get("environment") != "production" or candidate.get("is_synthetic"):
        raise ExpeditedEditorialError("candidate: production evidence required")
    if _sha40(candidate.get("code_revision"), "candidate") != revision:
        raise ExpeditedEditorialError("candidate: code_revision mismatch")
    basis = _mapping(candidate.get("basis"), "candidate.basis")
    events = _list(basis.get("events"), "candidate.basis.events")
    pairs = _list(
        basis.get("same_event_pair_candidates"),
        "candidate.basis.same_event_pair_candidates",
    )
    top5 = _list(basis.get("top5_candidates"), "candidate.basis.top5_candidates")
    if (len(events), len(pairs), len(top5)) != (
        EVENT_COUNT,
        PAIR_COUNT,
        TOP5_COUNT,
    ):
        raise ExpeditedEditorialError("candidate: exact 20/40/5 required")
    claimed = _sha256(
        candidate.get("candidate_sha256"), "candidate_sha256", "candidate"
    )
    if claimed != canonical_sha256(basis):
        raise ExpeditedEditorialError("candidate: candidate_sha256 mismatch")
    return dict(candidate)


def _review_time(value: object, location: str, now: datetime) -> str:
    normalized = _timestamp(value, "reviewed_at", location)
    parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    if parsed > now + timedelta(minutes=5) or now - parsed > MAX_HUMAN_REVIEW_AGE:
        raise ExpeditedEditorialError(f"{location}: stale or future human review")
    return normalized


def _validate_artifact_binding(
    decisions: Mapping[str, object],
    *,
    run_id: int,
    artifact_id: int,
    artifact_name: str,
    artifact_digest: str,
) -> dict[str, object]:
    binding = _mapping(decisions.get("candidate_artifact"), "decisions.candidate_artifact")
    actual = {
        "run_id": _positive_int(binding.get("run_id"), "run_id", "candidate_artifact"),
        "artifact_id": _positive_int(
            binding.get("artifact_id"), "artifact_id", "candidate_artifact"
        ),
        "artifact_name": _text(
            binding.get("artifact_name"), "artifact_name", "candidate_artifact"
        ),
        "artifact_digest": "sha256:"
        + _sha256(
            binding.get("artifact_digest"),
            "artifact_digest",
            "candidate_artifact",
        ),
    }
    expected = {
        "run_id": run_id,
        "artifact_id": artifact_id,
        "artifact_name": artifact_name,
        "artifact_digest": "sha256:" + _sha256(
            artifact_digest, "artifact_digest", "workflow"
        ),
    }
    if actual != expected:
        raise ExpeditedEditorialError("decisions: candidate artifact binding mismatch")
    return actual


def _validate_actor(value: object, location: str) -> dict[str, object]:
    actor = _mapping(value, location)
    required = ("actor_id", "display_name", "actor_type", "actor_role", "country_code")
    if set(actor) != set(required):
        raise ExpeditedEditorialError(f"{location}: exact actor fields required")
    result: dict[str, object] = {
        "actor_id": _text(
            actor.get("actor_id"), "actor_id", location, maximum=64
        ),
        "display_name": _text(
            actor.get("display_name"),
            "display_name",
            location,
            maximum=255,
        ),
        "actor_type": _text(
            actor.get("actor_type"), "actor_type", location, maximum=40
        ),
        "actor_role": _text(
            actor.get("actor_role"), "actor_role", location, maximum=40
        ),
        "country_code": _text(
            actor.get("country_code"),
            "country_code",
            location,
            maximum=2,
        ),
    }
    if re.fullmatch(r"[A-Za-z0-9_.:\-]{1,64}", str(result["actor_id"])) is None:
        raise ExpeditedEditorialError(f"{location}: invalid actor_id")
    for field in ("actor_type", "actor_role"):
        if re.fullmatch(
            r"[a-z][a-z0-9_]{1,39}",
            str(result[field]),
        ) is None:
            raise ExpeditedEditorialError(f"{location}: invalid {field}")
    if result["country_code"] not in COUNTRIES:
        raise ExpeditedEditorialError(f"{location}: invalid country_code")
    if any(actor.get(field) != result[field] for field in SAFE_ACTOR_FIELDS):
        raise ExpeditedEditorialError(
            f"{location}: exact actor field values required"
        )
    return result


def _validate_candidate_actor(
    value: object,
    location: str,
) -> dict[str, object]:
    actor = _mapping(value, location)
    if set(actor) != set(SAFE_ACTOR_FIELDS):
        raise ExpeditedEditorialError(
            f"{location}: exact candidate actor fields required"
        )
    country_code = _optional_text(
        actor.get("country_code"),
        "country_code",
        location,
        maximum=2,
    )
    result: dict[str, object] = {
        "actor_id": _text(
            actor.get("actor_id"), "actor_id", location, maximum=64
        ),
        "display_name": _text(
            actor.get("display_name"),
            "display_name",
            location,
            maximum=255,
        ),
        "actor_type": _text(
            actor.get("actor_type"), "actor_type", location, maximum=40
        ),
        "actor_role": _text(
            actor.get("actor_role"), "actor_role", location, maximum=40
        ),
        "country_code": country_code,
    }
    if re.fullmatch(r"[A-Za-z0-9_.:\-]{1,64}", str(result["actor_id"])) is None:
        raise ExpeditedEditorialError(f"{location}: invalid actor_id")
    for field in ("actor_type", "actor_role"):
        if re.fullmatch(
            r"[a-z][a-z0-9_]{1,39}",
            str(result[field]),
        ) is None:
            raise ExpeditedEditorialError(f"{location}: invalid {field}")
    if country_code is not None and country_code not in COUNTRIES:
        raise ExpeditedEditorialError(f"{location}: invalid country_code")
    if any(actor.get(field) != result[field] for field in SAFE_ACTOR_FIELDS):
        raise ExpeditedEditorialError(
            f"{location}: exact candidate actor field values required"
        )
    return result


def _bind_review_actor(
    actor: Mapping[str, object],
    event: Mapping[str, object],
    location: str,
) -> dict[str, object]:
    candidates = [
        _validate_candidate_actor(raw, f"{location}.candidate_actors[{index}]")
        for index, raw in enumerate(
            _list(event.get("actors"), f"{location}.candidate_actors")
        )
    ]
    identity_fields = (
        "actor_id",
        "display_name",
        "actor_type",
        "actor_role",
    )
    matches = [
        candidate
        for candidate in candidates
        if all(candidate[field] == actor[field] for field in identity_fields)
    ]
    if not candidates:
        raise ExpeditedEditorialError(
            f"{location}: candidate actor required"
        )
    if len(matches) == 0:
        raise ExpeditedEditorialError(
            f"{location}: candidate actor binding mismatch"
        )
    if len(matches) != 1:
        raise ExpeditedEditorialError(
            f"{location}: ambiguous candidate actor binding"
        )
    candidate_country = matches[0]["country_code"]
    if (
        candidate_country is not None
        and candidate_country != actor["country_code"]
    ):
        raise ExpeditedEditorialError(
            f"{location}: candidate actor country mismatch"
        )
    return dict(actor)


def _validate_review_payload(
    value: object,
    *,
    event: Mapping[str, object],
    human_decision: str,
    marker: str,
    location: str,
) -> dict[str, object]:
    payload = _mapping(value, location)
    api_decision = "approve" if human_decision == "approved" else "reject"
    if payload.get("decision") != api_decision:
        raise ExpeditedEditorialError(f"{location}: decision mismatch")
    if (
        _timestamp(payload.get("expected_updated_at"), "expected_updated_at", location)
        != event["updated_at"]
    ):
        raise ExpeditedEditorialError(f"{location}: event timestamp binding mismatch")
    if (
        _sha256(
            payload.get("expected_evidence_sha256"),
            "expected_evidence_sha256",
            location,
        )
        != event["event_evidence_sha256"]
    ):
        raise ExpeditedEditorialError(f"{location}: evidence binding mismatch")
    reason = _text(payload.get("reason"), "reason", location, maximum=1800)
    result: dict[str, object] = {
        "decision": api_decision,
        "expected_updated_at": event["updated_at"],
        "expected_evidence_sha256": event["event_evidence_sha256"],
        "reason": reason + " " + marker,
    }
    if api_decision == "reject":
        allowed = {
            "decision",
            "expected_updated_at",
            "expected_evidence_sha256",
            "reason",
        }
        if any(key not in allowed and payload.get(key) is not None for key in payload):
            raise ExpeditedEditorialError(f"{location}: rejection has approval fields")
        return result
    family = _text(payload.get("event_family"), "event_family", location)
    if family not in EVENT_FAMILIES:
        raise ExpeditedEditorialError(f"{location}: reviewed event_family required")
    importance = _text(payload.get("importance"), "importance", location)
    if importance not in IMPORTANCE:
        raise ExpeditedEditorialError(f"{location}: invalid importance")
    reviewed_actor = _validate_actor(
        payload.get("actor"),
        location + ".actor",
    )
    reviewed_actor = _bind_review_actor(
        reviewed_actor,
        event,
        location + ".actor",
    )
    result.update(
        {
            "event_family": family,
            "identity_action": _text(
                payload.get("identity_action"),
                "identity_action",
                location,
                maximum=255,
            ),
            "identity_target": _text(
                payload.get("identity_target"),
                "identity_target",
                location,
                maximum=700,
            ),
            "identity_effective_at": _timestamp(
                payload.get("identity_effective_at"),
                "identity_effective_at",
                location,
            ),
            "identity_deadline_at": _optional_timestamp(
                payload.get("identity_deadline_at"),
                "identity_deadline_at",
                location,
            ),
            "importance": importance,
            "summary": _text(
                payload.get("summary"), "summary", location, maximum=4000
            ),
            "current_status": _text(
                payload.get("current_status"),
                "current_status",
                location,
                maximum=64,
            ),
            "actor": reviewed_actor,
        }
    )
    merge_target = payload.get("merge_into_event_id")
    if merge_target is not None:
        raise ExpeditedEditorialError(
            f"{location}: expedited publication never merges events"
        )
    return result


def validate_decisions(
    candidate: Mapping[str, object],
    decisions: Mapping[str, object],
    *,
    revision: str,
    candidate_run_id: int,
    candidate_artifact_id: int,
    candidate_artifact_name: str,
    candidate_artifact_digest: str,
    now: datetime,
) -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    candidate = _validate_candidate(candidate, revision)
    if decisions.get("schema_version") != SCHEMA_VERSION or decisions.get("kind") != DECISION_KIND:
        raise ExpeditedEditorialError("decisions: schema or kind mismatch")
    if decisions.get("environment") != "production" or decisions.get("is_synthetic"):
        raise ExpeditedEditorialError("decisions: production human input required")
    if _sha40(decisions.get("code_revision"), "decisions") != revision:
        raise ExpeditedEditorialError("decisions: revision mismatch")
    if decisions.get("ground_truth_source") != "human":
        raise ExpeditedEditorialError("decisions: human ground truth required")
    if decisions.get("ai_generated_ground_truth") is not False:
        raise ExpeditedEditorialError("decisions: AI ground truth forbidden")
    if decisions.get("human_attestation") is not True:
        raise ExpeditedEditorialError("decisions: human attestation required")
    candidate_sha = _sha256(
        candidate.get("candidate_sha256"), "candidate_sha256", "candidate"
    )
    if (
        _sha256(
            decisions.get("candidate_sha256"),
            "candidate_sha256",
            "decisions",
        )
        != candidate_sha
    ):
        raise ExpeditedEditorialError("decisions: candidate_sha256 mismatch")
    artifact = _validate_artifact_binding(
        decisions,
        run_id=candidate_run_id,
        artifact_id=candidate_artifact_id,
        artifact_name=candidate_artifact_name,
        artifact_digest=candidate_artifact_digest,
    )
    reviewer = _text(
        decisions.get("reviewer_reference"),
        "reviewer_reference",
        "decisions",
        maximum=191,
    )
    reviewed_at = _review_time(decisions.get("reviewed_at"), "decisions", now)
    basis = _mapping(candidate.get("basis"), "candidate.basis")
    event_candidates = {
        str(_mapping(item, "candidate.event").get("event_id")): _mapping(
            item, "candidate.event"
        )
        for item in _list(basis.get("events"), "candidate.events")
    }
    expected_event_ids = list(event_candidates)
    marker = "[expedited-candidate:" + candidate_sha + "]"
    event_reviews: list[dict[str, object]] = []
    raw_event_reviews = _list(decisions.get("event_reviews"), "decisions.event_reviews")
    if len(raw_event_reviews) != EVENT_COUNT:
        raise ExpeditedEditorialError("decisions: exactly 20 event reviews required")
    for index, raw in enumerate(raw_event_reviews):
        location = f"decisions.event_reviews[{index}]"
        review = _mapping(raw, location)
        event_id = _text(review.get("event_id"), "event_id", location)
        if index >= len(expected_event_ids) or event_id != expected_event_ids[index]:
            raise ExpeditedEditorialError("decisions: event order/identity changed")
        human_decision = _text(review.get("decision"), "decision", location)
        if human_decision not in {"approved", "rejected"}:
            raise ExpeditedEditorialError(f"{location}: invalid decision")
        if review.get("reviewer_type") != "human":
            raise ExpeditedEditorialError(f"{location}: human reviewer required")
        if _text(review.get("reviewer_reference"), "reviewer_reference", location) != reviewer:
            raise ExpeditedEditorialError(f"{location}: reviewer mismatch")
        item_reviewed_at = _review_time(review.get("reviewed_at"), location, now)
        if item_reviewed_at != reviewed_at:
            raise ExpeditedEditorialError(f"{location}: reviewed_at mismatch")
        payload = _validate_review_payload(
            review.get("review_payload"),
            event=event_candidates[event_id],
            human_decision=human_decision,
            marker=marker,
            location=location + ".review_payload",
        )
        event_reviews.append(
            {
                "event_id": event_id,
                "decision": human_decision,
                "reviewer_type": "human",
                "reviewer_reference": reviewer,
                "reviewed_at": reviewed_at,
                "api_payload": payload,
            }
        )
    pair_candidates = {
        str(_mapping(item, "candidate.pair").get("pair_id")): _mapping(
            item, "candidate.pair"
        )
        for item in _list(
            basis.get("same_event_pair_candidates"), "candidate.pairs"
        )
    }
    pair_reviews: list[dict[str, object]] = []
    raw_pair_reviews = _list(
        decisions.get("same_event_pair_reviews"), "decisions.pair_reviews"
    )
    if len(raw_pair_reviews) != PAIR_COUNT:
        raise ExpeditedEditorialError("decisions: exactly 40 pair reviews required")
    if [str(_mapping(item, "pair").get("pair_id")) for item in raw_pair_reviews] != list(
        pair_candidates
    ):
        raise ExpeditedEditorialError("decisions: pair order/identity changed")
    for index, raw in enumerate(raw_pair_reviews):
        location = f"decisions.same_event_pair_reviews[{index}]"
        review = _mapping(raw, location)
        pair_id = str(review["pair_id"])
        candidate_pair = pair_candidates[pair_id]
        decision = review.get("decision")
        if not isinstance(decision, bool):
            raise ExpeditedEditorialError(f"{location}: boolean decision required")
        for field in ("left_document_id", "right_document_id"):
            if review.get(field) != candidate_pair.get(field):
                raise ExpeditedEditorialError(f"{location}: document binding mismatch")
        if review.get("reviewer_type") != "human":
            raise ExpeditedEditorialError(f"{location}: human reviewer required")
        if _text(review.get("reviewer_reference"), "reviewer_reference", location) != reviewer:
            raise ExpeditedEditorialError(f"{location}: reviewer mismatch")
        if _review_time(review.get("reviewed_at"), location, now) != reviewed_at:
            raise ExpeditedEditorialError(f"{location}: reviewed_at mismatch")
        pair_reviews.append(
            {
                "pair_id": pair_id,
                "left_document_id": candidate_pair["left_document_id"],
                "right_document_id": candidate_pair["right_document_id"],
                "decision": decision,
                "reviewer_type": "human",
                "reviewer_reference": reviewer,
                "reviewed_at": reviewed_at,
            }
        )
    top_candidates = [
        _mapping(item, "candidate.top5")
        for item in _list(basis.get("top5_candidates"), "candidate.top5")
    ]
    raw_top_reviews = _list(decisions.get("top5_reviews"), "decisions.top5_reviews")
    if len(raw_top_reviews) != TOP5_COUNT:
        raise ExpeditedEditorialError("decisions: exactly five Top reviews required")
    approved_events = {
        str(item["event_id"])
        for item in event_reviews
        if item["decision"] == "approved"
    }
    top_reviews: list[dict[str, object]] = []
    for index, (raw, top) in enumerate(zip(raw_top_reviews, top_candidates, strict=True)):
        location = f"decisions.top5_reviews[{index}]"
        review = _mapping(raw, location)
        for field in ("edition_id", "event_id", "position_no"):
            if review.get(field) != top.get(field):
                raise ExpeditedEditorialError(f"{location}: candidate binding mismatch")
        if review.get("decision") != "approved":
            raise ExpeditedEditorialError(f"{location}: Top 5 approval required")
        if str(top["event_id"]) not in approved_events:
            raise ExpeditedEditorialError(f"{location}: Top event must be approved")
        if review.get("reviewer_type") != "human":
            raise ExpeditedEditorialError(f"{location}: human reviewer required")
        if _text(review.get("reviewer_reference"), "reviewer_reference", location) != reviewer:
            raise ExpeditedEditorialError(f"{location}: reviewer mismatch")
        if _review_time(review.get("reviewed_at"), location, now) != reviewed_at:
            raise ExpeditedEditorialError(f"{location}: reviewed_at mismatch")
        top_reviews.append(
            {
                "edition_id": top["edition_id"],
                "event_id": top["event_id"],
                "position_no": top["position_no"],
                "decision": "approved",
                "selection_reason": _text(
                    review.get("selection_reason"),
                    "selection_reason",
                    location,
                    maximum=500,
                ),
                "reviewer_type": "human",
                "reviewer_reference": reviewer,
                "reviewed_at": reviewed_at,
                "official_evidence_count": top["official_evidence_count"],
                "public_eligible": True,
                "event_evidence_sha256": top["event_evidence_sha256"],
            }
        )
    return artifact, event_reviews, pair_reviews, top_reviews


def _normalize_identity(value: object) -> str:
    return " ".join(
        unicodedata.normalize("NFKC", str(value or "")).casefold().split()
    )


def _immutable_document_basis(documents: object) -> list[dict[str, object]]:
    result = []
    for raw in _list(documents, "documents"):
        item = _mapping(raw, "document")
        result.append(
            {
                key: item.get(key)
                for key in (
                    "document_id",
                    "issuer_id",
                    "country_code",
                    "source_right_id",
                    "source_class",
                    "source_key",
                    "original_language",
                    "title",
                    "original_url",
                    "content_hash",
                    "connector_id",
                    "connector_base_url",
                )
            }
        )
    return result


def _already_final(
    current: Mapping[str, object],
    candidate: Mapping[str, object],
    review: Mapping[str, object],
    marker: str,
) -> bool:
    if marker not in str(current.get("latest_revision_reason") or ""):
        return False
    if _immutable_document_basis(current.get("official_documents")) != _immutable_document_basis(
        candidate.get("official_documents")
    ):
        return False
    if review["decision"] == "rejected":
        return (
            current.get("review_status") == "rejected"
            and current.get("publication_status") == "draft"
            and current.get("identity_status") == "rejected"
        )
    payload = _mapping(review.get("api_payload"), "review.api_payload")
    actor = _mapping(payload.get("actor"), "review.actor")
    matching_actor = any(
        _mapping(raw, "current.actor").get("actor_id") == actor["actor_id"]
        and _mapping(raw, "current.actor").get("actor_role") == actor["actor_role"]
        and _mapping(raw, "current.actor").get("display_name") == actor["display_name"]
        and _mapping(raw, "current.actor").get("actor_type") == actor["actor_type"]
        and _mapping(raw, "current.actor").get("country_code") == actor["country_code"]
        and _mapping(raw, "current.actor").get("actor_review_status") == "approved"
        and _mapping(raw, "current.actor").get("relation_review_status") == "approved"
        for raw in _list(current.get("actors"), "current.actors")
    )
    return (
        current.get("review_status") == "approved"
        and current.get("publication_status") == "published"
        and current.get("identity_status") == "complete"
        and current.get("event_family") == payload["event_family"]
        and current.get("importance") == payload["importance"]
        and current.get("summary") == payload["summary"]
        and current.get("current_status") == payload["current_status"]
        and _normalize_identity(current.get("identity_action"))
        == _normalize_identity(payload["identity_action"])
        and _normalize_identity(current.get("identity_target"))
        == _normalize_identity(payload["identity_target"])
        and _normalize_identity(current.get("identity_actor_id"))
        == _normalize_identity(actor["actor_id"])
        and _optional_timestamp(
            current.get("identity_effective_at"),
            "identity_effective_at",
            "current",
        )
        == payload["identity_effective_at"]
        and _optional_timestamp(
            current.get("identity_deadline_at"),
            "identity_deadline_at",
            "current",
        )
        == payload.get("identity_deadline_at")
        and matching_actor
    )


def _current_event(
    client: EditorialClient,
    event_id: str,
    expected_revision: str,
) -> dict[str, object]:
    payload = client.event(event_id)
    meta = _mapping(payload.get("meta"), "event.meta")
    event = _validate_event(
        _mapping(payload.get("data"), "event.data").get("event"),
        "event.data.event",
    )
    if _sha40(meta.get("code_revision"), "event.meta") != expected_revision:
        raise ExpeditedEditorialError("event: code_revision mismatch")
    return event


def _human_review_document(
    *,
    revision: str,
    collected_at: str,
    artifact: Mapping[str, object],
    event_reviews: Sequence[Mapping[str, object]],
    pair_reviews: Sequence[Mapping[str, object]],
    top_reviews: Sequence[Mapping[str, object]],
    candidate_sha256: str,
) -> dict[str, object]:
    section = {
        "ground_truth_source": "human",
        "ai_generated_ground_truth": False,
        "human_attestation": True,
        "raw_counts": {
            "event_review_count": len(event_reviews),
            "same_event_pair_review_count": len(pair_reviews),
            "top5_human_reviewed_count": len(top_reviews),
            "top5_published_count": len(top_reviews),
        },
        "event_reviews": [
            {key: item[key] for key in (
                "event_id", "decision", "reviewer_type",
                "reviewer_reference", "reviewed_at",
            )}
            for item in event_reviews
        ],
        "same_event_pair_reviews": list(pair_reviews),
        "top5_reviews": [
            {key: item[key] for key in (
                "edition_id", "event_id", "decision", "reviewer_type",
                "reviewer_reference", "reviewed_at", "official_evidence_count",
                "public_eligible", "event_evidence_sha256",
            )}
            for item in top_reviews
        ],
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": HUMAN_REVIEW_KIND,
        "environment": "production",
        "evidence_source": "protected_editorial_publication",
        "is_synthetic": False,
        "code_revision": revision,
        "collected_at": collected_at,
        "evidence_as_of": collected_at,
        "candidate_artifact": dict(artifact),
        "candidate_sha256": candidate_sha256,
        **section,
        "section_sha256": canonical_sha256(section),
    }


def apply_publication(
    client: EditorialClient,
    *,
    candidate: Mapping[str, object],
    decisions: Mapping[str, object],
    revision: str,
    candidate_run_id: int,
    candidate_artifact_id: int,
    candidate_artifact_name: str,
    candidate_artifact_digest: str,
    now: datetime,
) -> tuple[dict[str, object], dict[str, object]]:
    artifact, reviews, pairs, top5 = validate_decisions(
        candidate,
        decisions,
        revision=revision,
        candidate_run_id=candidate_run_id,
        candidate_artifact_id=candidate_artifact_id,
        candidate_artifact_name=candidate_artifact_name,
        candidate_artifact_digest=candidate_artifact_digest,
        now=now,
    )
    candidate_sha = _sha256(
        candidate.get("candidate_sha256"), "candidate_sha256", "candidate"
    )
    marker = "[expedited-candidate:" + candidate_sha + "]"
    basis = _mapping(candidate.get("basis"), "candidate.basis")
    candidate_events = {
        str(_mapping(item, "candidate.event").get("event_id")): _mapping(
            item, "candidate.event"
        )
        for item in _list(basis.get("events"), "candidate.events")
    }

    # Full 20-event stale-input preflight completes before the first mutation.
    states: dict[str, tuple[str, dict[str, object]]] = {}
    for review in reviews:
        event_id = str(review["event_id"])
        current = _current_event(client, event_id, revision)
        candidate_event = candidate_events[event_id]
        if _already_final(current, candidate_event, review, marker):
            states[event_id] = ("verified_existing", current)
            continue
        if (
            current.get("review_status") != "pending"
            or current.get("publication_status") != "draft"
            or current.get("identity_status") != "needs_review"
            or current.get("updated_at") != candidate_event.get("updated_at")
            or current.get("event_evidence_sha256")
            != candidate_event.get("event_evidence_sha256")
        ):
            raise ExpeditedEditorialError(
                f"preflight: stale event or document evidence for {event_id}"
            )
        states[event_id] = ("pending", current)

    outcomes: list[dict[str, object]] = []
    mutations = 0
    for review in reviews:
        event_id = str(review["event_id"])
        state, _ = states[event_id]
        response_idempotent = state == "verified_existing"
        if state == "pending":
            response = client.review(
                event_id,
                _mapping(review.get("api_payload"), "review.api_payload"),
            )
            response_data = _mapping(response.get("data"), "review.response.data")
            if response_data.get("event_id") != event_id:
                raise ExpeditedEditorialError("review response event mismatch")
            mutations += 1
            response_idempotent = False
        current = _current_event(client, event_id, revision)
        if not _already_final(
            current,
            candidate_events[event_id],
            review,
            marker,
        ):
            raise ExpeditedEditorialError(
                f"publication: final event verification failed for {event_id}"
            )
        outcomes.append(
            {
                "event_id": event_id,
                "decision": review["decision"],
                "result": (
                    "verified_existing" if response_idempotent else "applied"
                ),
                "final_review_status": current.get("review_status"),
                "final_publication_status": current.get("publication_status"),
                "final_identity_status": current.get("identity_status"),
                "final_updated_at": current.get("updated_at"),
            }
        )

    cutoff_at = _timestamp(
        basis.get("brief_cutoff_at"), "brief_cutoff_at", "candidate.basis"
    )
    brief_payload = {
        "edition": "global",
        "cutoff_at": cutoff_at,
        "build_sha": revision,
        "empty_reason": None,
        "items": [
            {
                "event_id": item["event_id"],
                "lane": "top",
                "position_no": item["position_no"],
                "selection_reason": item["selection_reason"],
            }
            for item in top5
        ],
    }
    brief_response = client.publish_brief(brief_payload)
    brief_data = _mapping(brief_response.get("data"), "brief.response.data")
    expected_brief_id = _brief_id(cutoff_at)
    if (
        brief_data.get("brief_id") != expected_brief_id
        or brief_data.get("edition") != "global"
        or brief_data.get("published") is not True
    ):
        raise ExpeditedEditorialError("brief: publication response mismatch")
    collected_at = now.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )
    human_review = _human_review_document(
        revision=revision,
        collected_at=collected_at,
        artifact=artifact,
        event_reviews=reviews,
        pair_reviews=pairs,
        top_reviews=top5,
        candidate_sha256=candidate_sha,
    )
    decision_sha = canonical_sha256(decisions)
    semantic = {
        "candidate_artifact": artifact,
        "candidate_sha256": candidate_sha,
        "decision_sha256": decision_sha,
        "code_revision": revision,
        "event_reviews": [
            {"event_id": item["event_id"], "decision": item["decision"]}
            for item in reviews
        ],
        "same_event_pair_reviews": pairs,
        "top5": [
            {
                "event_id": item["event_id"],
                "position_no": item["position_no"],
                "selection_reason": item["selection_reason"],
            }
            for item in top5
        ],
        "brief": {
            "brief_id": expected_brief_id,
            "build_sha": revision,
            "cutoff_at": cutoff_at,
            "payload_sha256": canonical_sha256(brief_payload),
        },
    }
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "kind": PUBLICATION_KIND,
        "environment": "production",
        "evidence_source": "protected_editor_api_v2",
        "is_synthetic": False,
        "code_revision": revision,
        "collected_at": collected_at,
        "candidate_artifact": artifact,
        "candidate_sha256": candidate_sha,
        "decision_sha256": decision_sha,
        "event_review_outcomes": outcomes,
        "same_event_pair_reviews": pairs,
        "top5": top5,
        "brief": {
            **_mapping(semantic["brief"], "semantic.brief"),
            "idempotent": brief_data.get("idempotent") is True,
        },
        "mutations_applied": mutations,
        "idempotent_replay": mutations == 0 and brief_data.get("idempotent") is True,
        "semantic_receipt_sha256": canonical_sha256(semantic),
    }
    return human_review, receipt


def _artifact_identity(
    *,
    run_id: int,
    artifact_id: int,
    artifact_name: str,
    artifact_digest: str,
    location: str,
) -> dict[str, object]:
    return {
        "run_id": _positive_int(run_id, "run_id", location),
        "artifact_id": _positive_int(artifact_id, "artifact_id", location),
        "artifact_name": _text(
            artifact_name,
            "artifact_name",
            location,
            maximum=255,
        ),
        "artifact_digest": "sha256:"
        + _sha256(
            str(artifact_digest).removeprefix("sha256:"),
            "artifact_digest",
            location,
        ),
    }


def _carry_document_basis(documents: object, location: str) -> list[dict[str, object]]:
    """Return evidence identity fields that an editorial action cannot change.

    Retrieval and row-update timestamps are intentionally excluded. They may
    advance when an official connector safely observes the same source bytes
    again; every source/document identity and content-bearing field remains
    bound.
    """

    fields = (
        "document_id",
        "issuer_id",
        "country_code",
        "source_right_id",
        "source_class",
        "source_key",
        "document_type",
        "original_language",
        "title",
        "original_url",
        "content_hash",
        "filed_at",
        "published_at",
        "relation_type",
        "position_no",
        "connector_id",
        "connector_base_url",
        "coverage_mode",
        "connector_status",
    )
    result = []
    for index, raw in enumerate(_list(documents, location)):
        document = _mapping(raw, f"{location}[{index}]")
        result.append({field: document.get(field) for field in fields})
    return sorted(
        result,
        key=lambda item: (
            str(item.get("position_no")),
            str(item.get("document_id")),
        ),
    )


def _global_identity_mysql_timestamp(value: object, location: str) -> str:
    normalized = _timestamp(value, "identity timestamp", location)
    parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _global_comparison_key(
    *,
    issuer_id: object,
    event_family: object,
    action: object,
    target: object,
    actor_id: object,
    effective_at: object,
    deadline_at: object,
    location: str,
) -> str:
    """Mirror the PHP v2 global event identity contract byte-for-byte."""

    identity = {
        "issuer_id": _text(
            issuer_id,
            "issuer_id",
            location,
            maximum=96,
        ),
        "event_family": _text(
            event_family,
            "event_family",
            location,
            maximum=64,
        ),
        "action": _normalize_identity(action),
        "target": _normalize_identity(target),
        "actor_id": _normalize_identity(actor_id),
        "effective_at": _global_identity_mysql_timestamp(
            effective_at,
            location,
        ),
        "deadline_at": (
            None
            if deadline_at is None
            else _global_identity_mysql_timestamp(deadline_at, location)
        ),
    }
    for field in ("action", "target", "actor_id"):
        if not identity[field]:
            raise ExpeditedEditorialError(
                f"{location}: complete identity requires {field}"
            )
    return "global:" + canonical_sha256(identity)


def _load_legacy_human_approval_artifact() -> dict[str, object]:
    return _load_json(
        LEGACY_APPROVAL_ATTESTATION_PATH,
        "legacy human approval artifact",
    )


def _load_legacy_human_approval_correction() -> dict[str, object]:
    try:
        raw = LEGACY_APPROVAL_CORRECTION_PATH.read_bytes()
    except OSError as error:
        raise ExpeditedEditorialError(
            "legacy human approval correction: could not read immutable bytes"
        ) from error
    if hashlib.sha256(raw).hexdigest() != LEGACY_APPROVAL_CORRECTION_RAW_SHA256:
        raise ExpeditedEditorialError(
            "legacy human approval correction: raw file digest mismatch"
        )
    return _load_json(
        LEGACY_APPROVAL_CORRECTION_PATH,
        "legacy human approval correction",
    )


def _legacy_human_approval_chain_basis() -> dict[str, object]:
    return {
        "base_approval_canonical_sha256": LEGACY_APPROVAL_ATTESTATION_SHA256,
        "correction_canonical_sha256": LEGACY_APPROVAL_CORRECTION_SHA256,
        "correction_raw_sha256": LEGACY_APPROVAL_CORRECTION_RAW_SHA256,
        "event_ids_sha256": LEGACY_APPROVAL_EVENT_IDS_SHA256,
        "reviewer_reference": LEGACY_APPROVAL_REVIEWER,
        "source_candidate_sha256": LEGACY_APPROVAL_CANDIDATE_SHA256,
        "source_decision_sha256": LEGACY_APPROVAL_SOURCE_DECISION_SHA256,
    }


def _legacy_human_approval_chain_sha256() -> str:
    digest = canonical_sha256(_legacy_human_approval_chain_basis())
    if digest != LEGACY_APPROVAL_CHAIN_SHA256:
        raise ExpeditedEditorialError(
            "legacy human approval chain: pinned digest mismatch"
        )
    return digest


def _validate_legacy_human_approval_artifact(
    value: object,
    *,
    candidate: Mapping[str, object],
    candidate_artifact: Mapping[str, object],
    publication_artifact: Mapping[str, object],
    events: Sequence[Mapping[str, object]],
    event_reviews: Sequence[Mapping[str, object]],
    source_decision_sha256: object,
    publication_top5: Sequence[Mapping[str, object]],
) -> tuple[dict[str, object], str]:
    artifact = _mapping(value, "legacy human approval artifact")
    artifact_sha = canonical_sha256(artifact)
    if artifact_sha != LEGACY_APPROVAL_ATTESTATION_SHA256:
        raise ExpeditedEditorialError(
            "legacy human approval artifact: digest mismatch"
        )
    expected_fields = {
        "schema_version",
        "kind",
        "environment",
        "is_synthetic",
        "ground_truth_source",
        "ai_generated_ground_truth",
        "human_attestation",
        "reviewer_reference",
        "reviewed_at",
        "attestation_source",
        "attestation_text_sha256",
        "source_candidate_sha256",
        "source_decision_sha256",
        "source_candidate_artifact",
        "source_publication_artifact",
        "approved_transform_contract",
        "event_approvals",
        "pair_approval",
        "top5_approvals",
    }
    if (
        set(artifact) != expected_fields
        or artifact.get("schema_version") != SCHEMA_VERSION
        or artifact.get("kind")
        != "bside-global-alpha-expedited-legacy-human-approval"
        or artifact.get("environment") != "production"
        or artifact.get("is_synthetic") is not False
        or artifact.get("ground_truth_source") != "human"
        or artifact.get("ai_generated_ground_truth") is not False
        or artifact.get("human_attestation") is not True
        or artifact.get("reviewer_reference")
        != LEGACY_APPROVAL_REVIEWER
        or artifact.get("source_candidate_sha256")
        != LEGACY_APPROVAL_CANDIDATE_SHA256
        or artifact.get("source_candidate_artifact")
        != LEGACY_APPROVAL_CANDIDATE_ARTIFACT
        or artifact.get("source_publication_artifact")
        != LEGACY_APPROVAL_PUBLICATION_ARTIFACT
        or artifact.get("approved_transform_contract")
        != LEGACY_APPROVED_TRANSFORM_CONTRACT
    ):
        raise ExpeditedEditorialError(
            "legacy human approval artifact: provenance mismatch"
        )
    if artifact.get("attestation_text_sha256") != LEGACY_APPROVAL_TEXT_SHA256:
        raise ExpeditedEditorialError(
            "legacy human approval artifact: text digest mismatch"
        )
    source_decision_sha = _sha256(
        source_decision_sha256,
        "source_decision_sha256",
        "source publication",
    )
    if (
        source_decision_sha != LEGACY_APPROVAL_SOURCE_DECISION_SHA256
        or artifact.get("source_decision_sha256") != source_decision_sha
        or artifact.get("source_candidate_sha256")
        != candidate.get("candidate_sha256")
        or dict(candidate_artifact) != LEGACY_APPROVAL_CANDIDATE_ARTIFACT
        or dict(publication_artifact)
        != LEGACY_APPROVAL_PUBLICATION_ARTIFACT
    ):
        raise ExpeditedEditorialError(
            "legacy human approval artifact: source binding mismatch"
        )
    reviewed_at = _timestamp(
        artifact.get("reviewed_at"),
        "reviewed_at",
        "legacy human approval artifact",
    )
    if any(
        item.get("reviewer_reference") != LEGACY_APPROVAL_REVIEWER
        or item.get("reviewed_at") != reviewed_at
        for item in event_reviews
    ):
        raise ExpeditedEditorialError(
            "legacy human approval artifact: reviewer binding mismatch"
        )

    event_approvals = [
        _mapping(
            item,
            f"legacy human approval artifact.event_approvals[{index}]",
        )
        for index, item in enumerate(
            _list(
                artifact.get("event_approvals"),
                "legacy human approval artifact.event_approvals",
            )
        )
    ]
    expected_event_approvals = []
    for position, event in enumerate(events, start=1):
        expected_event_approvals.append(
            {
                "position_no": position,
                "event_id": event["event_id"],
                "event_family": (
                    "listing_status"
                    if position == 11
                    else event["event_family"]
                ),
                "identity_action": LEGACY_APPROVAL_ACTION_OVERRIDES.get(
                    position,
                    event["identity_action"],
                ),
                "decision": "approved",
            }
        )
    if event_approvals != expected_event_approvals:
        raise ExpeditedEditorialError(
            "legacy human approval artifact: event approval mismatch"
        )
    if artifact.get("pair_approval") != {
        "candidate_pair_count": PAIR_COUNT,
        "decision": False,
    }:
        raise ExpeditedEditorialError(
            "legacy human approval artifact: pair approval mismatch"
        )
    expected_top5 = [
        {
            "position_no": item["position_no"],
            "event_id": item["event_id"],
            "decision": "approved",
        }
        for item in publication_top5
    ]
    if artifact.get("top5_approvals") != expected_top5:
        raise ExpeditedEditorialError(
            "legacy human approval artifact: Top 5 approval mismatch"
        )
    return artifact, artifact_sha


def _validate_legacy_human_approval_correction(
    value: object,
    *,
    base_approval: Mapping[str, object],
    candidate: Mapping[str, object],
    candidate_artifact: Mapping[str, object],
    publication_artifact: Mapping[str, object],
    events: Sequence[Mapping[str, object]],
    source_decision_sha256: object,
) -> tuple[dict[str, object], str, str]:
    location = "legacy human approval correction"
    correction = _mapping(value, location)
    correction_sha = canonical_sha256(correction)
    if correction_sha != LEGACY_APPROVAL_CORRECTION_SHA256:
        raise ExpeditedEditorialError(f"{location}: digest mismatch")
    expected_fields = {
        "schema_version",
        "kind",
        "environment",
        "is_synthetic",
        "ground_truth_source",
        "ai_generated_ground_truth",
        "human_attestation",
        "reviewer_type",
        "reviewer_reference",
        "reviewed_at",
        "attestation_source",
        "attestation_text_sha256",
        "base_approval_canonical_sha256",
        "source_candidate_sha256",
        "source_decision_sha256",
        "source_candidate_artifact",
        "source_publication_artifact",
        "correction_scope",
        "preserved_decisions",
        "event_ids",
        "event_ids_sha256",
        "replaced_transform_fields",
        "corrected_transform_contract",
    }
    if (
        set(correction) != expected_fields
        or correction.get("schema_version") != SCHEMA_VERSION
        or correction.get("kind")
        != (
            "bside-global-alpha-expedited-legacy-human-approval-"
            "transform-correction"
        )
        or correction.get("environment") != "production"
        or correction.get("is_synthetic") is not False
        or correction.get("ground_truth_source") != "human"
        or correction.get("ai_generated_ground_truth") is not False
        or correction.get("human_attestation") is not True
        or correction.get("reviewer_type") != "human"
        or correction.get("reviewer_reference") != LEGACY_APPROVAL_REVIEWER
        or correction.get("attestation_source")
        != "codex_thread_human_message_2026-08-01"
        or correction.get("attestation_text_sha256")
        != LEGACY_APPROVAL_CORRECTION_TEXT_SHA256
        or correction.get("base_approval_canonical_sha256")
        != LEGACY_APPROVAL_ATTESTATION_SHA256
        or correction.get("source_candidate_sha256")
        != LEGACY_APPROVAL_CANDIDATE_SHA256
        or correction.get("source_decision_sha256")
        != LEGACY_APPROVAL_SOURCE_DECISION_SHA256
        or correction.get("source_candidate_artifact")
        != LEGACY_APPROVAL_CANDIDATE_ARTIFACT
        or correction.get("source_publication_artifact")
        != LEGACY_APPROVAL_PUBLICATION_ARTIFACT
        or correction.get("correction_scope") != "transform_contract_only"
        or correction.get("preserved_decisions")
        != LEGACY_APPROVAL_PRESERVED_DECISIONS
        or correction.get("replaced_transform_fields")
        != ["identity_target", "summary", "current_status"]
        or correction.get("corrected_transform_contract")
        != LEGACY_APPROVAL_CORRECTED_TRANSFORM_CONTRACT
    ):
        raise ExpeditedEditorialError(f"{location}: provenance mismatch")
    correction_reviewed_at = _timestamp(
        correction.get("reviewed_at"), "reviewed_at", location
    )
    base_reviewed_at = _timestamp(
        base_approval.get("reviewed_at"),
        "reviewed_at",
        "legacy human approval artifact",
    )
    if datetime.fromisoformat(
        correction_reviewed_at.replace("Z", "+00:00")
    ) <= datetime.fromisoformat(base_reviewed_at.replace("Z", "+00:00")):
        raise ExpeditedEditorialError(
            f"{location}: correction must follow base approval"
        )

    base_sha = canonical_sha256(base_approval)
    source_decision_sha = _sha256(
        source_decision_sha256,
        "source_decision_sha256",
        "source publication",
    )
    if (
        base_sha != LEGACY_APPROVAL_ATTESTATION_SHA256
        or correction.get("base_approval_canonical_sha256") != base_sha
        or candidate.get("candidate_sha256")
        != LEGACY_APPROVAL_CANDIDATE_SHA256
        or source_decision_sha != LEGACY_APPROVAL_SOURCE_DECISION_SHA256
        or dict(candidate_artifact) != LEGACY_APPROVAL_CANDIDATE_ARTIFACT
        or dict(publication_artifact)
        != LEGACY_APPROVAL_PUBLICATION_ARTIFACT
    ):
        raise ExpeditedEditorialError(f"{location}: source binding mismatch")

    base_event_approvals = [
        _mapping(item, f"base approval.event_approvals[{index}]")
        for index, item in enumerate(
            _list(base_approval.get("event_approvals"), "base event approvals")
        )
    ]
    expected_event_ids = [str(item["event_id"]) for item in base_event_approvals]
    event_ids = _list(correction.get("event_ids"), f"{location}.event_ids")
    live_event_ids = [str(item.get("event_id")) for item in events]
    if (
        event_ids != expected_event_ids
        or live_event_ids != expected_event_ids
        or len(event_ids) != EVENT_COUNT
        or len(set(event_ids)) != EVENT_COUNT
        or canonical_sha256(event_ids) != LEGACY_APPROVAL_EVENT_IDS_SHA256
        or correction.get("event_ids_sha256")
        != LEGACY_APPROVAL_EVENT_IDS_SHA256
    ):
        raise ExpeditedEditorialError(f"{location}: event binding mismatch")

    effective_contract: dict[str, object] = dict(
        LEGACY_APPROVED_TRANSFORM_CONTRACT
    )
    effective_contract.update(
        {
            "identity_target": LEGACY_APPROVAL_CORRECTED_TRANSFORM_CONTRACT[
                "identity_target"
            ],
            "summary": LEGACY_APPROVAL_CORRECTED_TRANSFORM_CONTRACT["summary"],
            "current_status": LEGACY_APPROVAL_CORRECTED_TRANSFORM_CONTRACT[
                "current_status"
            ],
        }
    )
    changed_fields = sorted(
        key
        for key, value_item in effective_contract.items()
        if LEGACY_APPROVED_TRANSFORM_CONTRACT.get(key) != value_item
    )
    if changed_fields != ["current_status", "identity_target", "summary"]:
        raise ExpeditedEditorialError(
            f"{location}: exact three-field correction required"
        )
    chain_sha = _legacy_human_approval_chain_sha256()
    return correction, correction_sha, chain_sha


def _legacy_approved_identity_target(issuer_name: str, title: str) -> str:
    return issuer_name + " — " + title


def _legacy_approved_summary(issuer_name: str, title: str) -> str:
    return issuer_name + "는 DART에 「" + title + "」을 공시했다."


def _legacy_approved_current_status(verification_status: str) -> str:
    if verification_status == "corrected":
        return "corrected_official_disclosure"
    if verification_status == "official":
        return "official_disclosure_confirmed"
    raise ExpeditedEditorialError(
        "legacy approval correction: unsupported verification status"
    )


LEGACY_CORRECTED_EVENT_ALLOWED_DELTA_FIELDS = frozenset(
    {"identity_target", "summary", "current_status", "comparison_key"}
)


def _legacy_event_comparison_key(
    event: Mapping[str, object],
    location: str,
) -> str:
    return _global_comparison_key(
        issuer_id=event.get("issuer_id"),
        event_family=event.get("event_family"),
        action=event.get("identity_action"),
        target=event.get("identity_target"),
        actor_id=event.get("identity_actor_id"),
        effective_at=event.get("identity_effective_at"),
        deadline_at=event.get("identity_deadline_at"),
        location=location,
    )


def _validate_legacy_corrected_event_delta(
    base_event: Mapping[str, object],
    corrected_event: Mapping[str, object],
    location: str,
) -> None:
    if set(base_event) != set(corrected_event):
        raise ExpeditedEditorialError(
            f"{location}: correction changed the canonical field set"
        )
    changed_fields = {
        field
        for field in base_event
        if base_event[field] != corrected_event[field]
    }
    if changed_fields != LEGACY_CORRECTED_EVENT_ALLOWED_DELTA_FIELDS:
        raise ExpeditedEditorialError(
            f"{location}: correction changed fields outside the exact scope"
        )

    for label, event in (("base", base_event), ("corrected", corrected_event)):
        expected_key = _legacy_event_comparison_key(
            event,
            f"{location}.{label}",
        )
        if event.get("comparison_key") != expected_key:
            raise ExpeditedEditorialError(
                f"{location}: {label} comparison_key is not deterministically derived"
            )

    preserved_fields = (
        "actor",
        "country",
        "official_documents",
        "official_evidence_count",
        "source_event_evidence_sha256",
    )
    if any(
        base_event.get(field) != corrected_event.get(field)
        for field in preserved_fields
    ):
        raise ExpeditedEditorialError(
            f"{location}: actor, country or official evidence changed"
        )


def _legacy_approved_canonical_basis(
    *,
    candidate: Mapping[str, object],
    candidate_artifact: Mapping[str, object],
    publication_artifact: Mapping[str, object],
    approval_attestation: Mapping[str, object],
    approval_correction: Mapping[str, object],
    source_decision_sha256: object,
    events: Sequence[Mapping[str, object]],
    event_reviews: Sequence[Mapping[str, object]],
    pair_reviews: Sequence[Mapping[str, object]],
    top_reviews: Sequence[Mapping[str, object]],
    source_outcomes: Mapping[str, Mapping[str, object]],
    publication_top5: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Reconstruct the one approved legacy judgment without re-reviewing it."""

    candidate_sha = _sha256(
        candidate.get("candidate_sha256"),
        "candidate_sha256",
        "legacy approval candidate",
    )
    if candidate_sha != LEGACY_APPROVAL_CANDIDATE_SHA256:
        raise ExpeditedEditorialError(
            "legacy approval profile: exact candidate SHA required"
        )
    if (
        dict(candidate_artifact) != LEGACY_APPROVAL_CANDIDATE_ARTIFACT
        or dict(publication_artifact)
        != LEGACY_APPROVAL_PUBLICATION_ARTIFACT
    ):
        raise ExpeditedEditorialError(
            "legacy approval profile: exact protected artifacts required"
        )
    attestation, attestation_sha = (
        _validate_legacy_human_approval_artifact(
            approval_attestation,
            candidate=candidate,
            candidate_artifact=candidate_artifact,
            publication_artifact=publication_artifact,
            events=events,
            event_reviews=event_reviews,
            source_decision_sha256=source_decision_sha256,
            publication_top5=publication_top5,
        )
    )
    correction, correction_sha, approval_chain_sha = (
        _validate_legacy_human_approval_correction(
            approval_correction,
            base_approval=attestation,
            candidate=candidate,
            candidate_artifact=candidate_artifact,
            publication_artifact=publication_artifact,
            events=events,
            source_decision_sha256=source_decision_sha256,
        )
    )
    if [str(item.get("event_id")) for item in event_reviews] != [
        str(item["event_id"]) for item in events
    ]:
        raise ExpeditedEditorialError(
            "legacy approval profile: event order changed"
        )
    if any(
        item.get("decision") != "approved"
        or item.get("reviewer_type") != "human"
        or item.get("reviewer_reference") != LEGACY_APPROVAL_REVIEWER
        for item in event_reviews
    ):
        raise ExpeditedEditorialError(
            "legacy approval profile: exact human event approval required"
        )
    if any(
        item.get("decision") is not False
        or item.get("reviewer_type") != "human"
        or item.get("reviewer_reference") != LEGACY_APPROVAL_REVIEWER
        for item in pair_reviews
    ):
        raise ExpeditedEditorialError(
            "legacy approval profile: exact separate-event decisions required"
        )
    if any(
        item.get("decision") != "approved"
        or item.get("reviewer_type") != "human"
        or item.get("reviewer_reference") != LEGACY_APPROVAL_REVIEWER
        for item in top_reviews
    ):
        raise ExpeditedEditorialError(
            "legacy approval profile: exact Top 5 approvals required"
        )

    approved_events: list[dict[str, object]] = []
    for position, candidate_event in enumerate(events, start=1):
        location = f"legacy approval E{position:02d}"
        event_id = str(candidate_event["event_id"])
        if candidate_event.get("country") != "KR":
            raise ExpeditedEditorialError(
                f"{location}: exact KR candidate required"
            )
        candidate_actors = [
            _validate_candidate_actor(raw, f"{location}.actors[{index}]")
            for index, raw in enumerate(
                _list(candidate_event.get("actors"), f"{location}.actors")
            )
        ]
        if len(candidate_actors) != 1:
            raise ExpeditedEditorialError(
                f"{location}: exactly one candidate filer required"
            )
        candidate_actor = candidate_actors[0]
        actor = {
            **candidate_actor,
            "country_code": "KR",
            "actor_review_status": "approved",
            "relation_review_status": "approved",
            "record_status": "active",
        }
        family = (
            "listing_status"
            if position == 11
            else _text(
                candidate_event.get("event_family"),
                "event_family",
                location,
                maximum=64,
            )
        )
        action = LEGACY_APPROVAL_ACTION_OVERRIDES.get(
            position,
            _text(
                candidate_event.get("identity_action"),
                "identity_action",
                location,
                maximum=255,
            ),
        )
        issuer_name = _text(
            candidate_event.get("issuer_name"),
            "issuer_name",
            location,
            maximum=255,
        )
        title = _text(
            candidate_event.get("title"),
            "title",
            location,
            maximum=700,
        )
        effective_at = _optional_timestamp(
            candidate_event.get("identity_effective_at"),
            "identity_effective_at",
            location,
        ) or _timestamp(
            candidate_event.get("occurred_at"),
            "occurred_at",
            location,
        )
        verification = _text(
            candidate_event.get("verification_status"),
            "verification_status",
            location,
            maximum=40,
        )
        stored_verification = (
            verification
            if verification in {"withdrawn", "corrected"}
            else "official"
        )
        normalized_action = _normalize_identity(action)
        source_outcome = _mapping(
            source_outcomes.get(event_id),
            f"{location}.source_outcome",
        )
        corrected_event: dict[str, object] = {
            "position_no": position,
            "event_id": event_id,
            "issuer_id": candidate_event["issuer_id"],
            "issuer_name": issuer_name,
            "country": "KR",
            "title": title,
            "original_language": candidate_event["original_language"],
            "event_family": family,
            "summary": _legacy_approved_summary(issuer_name, title),
            "importance": candidate_event["importance"],
            "current_status": _legacy_approved_current_status(verification),
            "deadline_at": None,
            "verification_status": stored_verification,
            "change_type": candidate_event["change_type"],
            "review_status": "approved",
            "publication_status": "published",
            "identity_action": normalized_action,
            "identity_target": _legacy_approved_identity_target(
                issuer_name,
                title,
            ),
            "identity_actor_id": actor["actor_id"],
            "identity_effective_at": effective_at,
            "identity_deadline_at": None,
            "identity_status": "complete",
            "occurred_at": candidate_event["occurred_at"],
            "first_observed_at": candidate_event["first_observed_at"],
            "actor": actor,
            "official_documents": _carry_document_basis(
                candidate_event.get("official_documents"),
                f"{location}.official_documents",
            ),
            "official_evidence_count": candidate_event[
                "official_evidence_count"
            ],
            "source_event_evidence_sha256": candidate_event[
                "event_evidence_sha256"
            ],
            "source_final_updated_at": _timestamp(
                source_outcome.get("final_updated_at"),
                "final_updated_at",
                f"{location}.source_outcome",
            ),
        }
        corrected_event["comparison_key"] = _legacy_event_comparison_key(
            corrected_event,
            f"{location}.corrected",
        )
        base_event = dict(corrected_event)
        base_event.update(
            {
            "summary": issuer_name + " — " + title,
            "current_status": verification,
            "identity_target": _normalize_identity(issuer_name),
            }
        )
        base_event["comparison_key"] = _legacy_event_comparison_key(
            base_event,
            f"{location}.base",
        )
        _validate_legacy_corrected_event_delta(
            base_event,
            corrected_event,
            location,
        )
        approved_events.append(corrected_event)

    pair_basis = [
        {
            "pair_id": item["pair_id"],
            "left_document_id": item["left_document_id"],
            "right_document_id": item["right_document_id"],
            "decision": False,
        }
        for item in pair_reviews
    ]
    top_review_by_event = {
        str(item["event_id"]): item for item in top_reviews
    }
    top5_basis = []
    for item in publication_top5:
        event_id = str(item["event_id"])
        human_top = top_review_by_event.get(event_id)
        if human_top is None:
            raise ExpeditedEditorialError(
                "legacy approval profile: Top 5 publication mismatch"
            )
        top5_basis.append(
            {
                "edition_id": item["edition_id"],
                "event_id": event_id,
                "position_no": item["position_no"],
                "decision": "approved",
                "selection_reason": item["selection_reason"],
                "official_evidence_count": item[
                    "official_evidence_count"
                ],
                "event_evidence_sha256": item[
                    "event_evidence_sha256"
                ],
                "public_eligible": True,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": APPROVED_CANONICAL_BASIS_KIND,
        "profile_id": (
            "legacy:"
            + LEGACY_APPROVAL_CANDIDATE_SHA256
            + ":"
            + LEGACY_APPROVAL_REVIEWER
            + ":correction:"
            + LEGACY_APPROVAL_CORRECTION_SHA256
        ),
        "source_candidate_sha256": candidate_sha,
        "reviewer_reference": LEGACY_APPROVAL_REVIEWER,
        "source_candidate_artifact": dict(candidate_artifact),
        "source_publication_artifact": dict(publication_artifact),
        "source_decision_sha256": _sha256(
            source_decision_sha256,
            "source_decision_sha256",
            "legacy approval profile",
        ),
        "human_approval_artifact": attestation,
        "human_approval_artifact_sha256": attestation_sha,
        "human_approval_correction_artifact": correction,
        "human_approval_correction_artifact_sha256": correction_sha,
        "human_approval_chain_sha256": approval_chain_sha,
        "event_decisions": [
            {"event_id": item["event_id"], "decision": "approved"}
            for item in event_reviews
        ],
        "same_event_pair_decisions": pair_basis,
        "top5_decisions": top5_basis,
        "events": approved_events,
    }


def _validate_approved_canonical_basis(
    value: object,
    digest: object,
) -> dict[str, object]:
    basis = _mapping(value, "approved canonical basis")
    approval_artifact = _mapping(
        basis.get("human_approval_artifact"),
        "approved canonical basis.human_approval_artifact",
    )
    correction_artifact = _mapping(
        basis.get("human_approval_correction_artifact"),
        "approved canonical basis.human_approval_correction_artifact",
    )
    if (
        basis.get("schema_version") != SCHEMA_VERSION
        or basis.get("kind") != APPROVED_CANONICAL_BASIS_KIND
        or basis.get("source_candidate_sha256")
        != LEGACY_APPROVAL_CANDIDATE_SHA256
        or basis.get("reviewer_reference") != LEGACY_APPROVAL_REVIEWER
        or basis.get("source_candidate_artifact")
        != LEGACY_APPROVAL_CANDIDATE_ARTIFACT
        or basis.get("source_publication_artifact")
        != LEGACY_APPROVAL_PUBLICATION_ARTIFACT
        or basis.get("source_decision_sha256")
        != LEGACY_APPROVAL_SOURCE_DECISION_SHA256
        or basis.get("human_approval_artifact_sha256")
        != LEGACY_APPROVAL_ATTESTATION_SHA256
        or canonical_sha256(approval_artifact)
        != LEGACY_APPROVAL_ATTESTATION_SHA256
        or basis.get("human_approval_correction_artifact_sha256")
        != LEGACY_APPROVAL_CORRECTION_SHA256
        or canonical_sha256(correction_artifact)
        != LEGACY_APPROVAL_CORRECTION_SHA256
        or basis.get("human_approval_chain_sha256")
        != _legacy_human_approval_chain_sha256()
        or len(_list(basis.get("events"), "approved canonical basis.events"))
        != EVENT_COUNT
        or len(
            _list(
                basis.get("same_event_pair_decisions"),
                "approved canonical basis.same_event_pair_decisions",
            )
        )
        != PAIR_COUNT
        or len(
            _list(
                basis.get("top5_decisions"),
                "approved canonical basis.top5_decisions",
            )
        )
        != TOP5_COUNT
    ):
        raise ExpeditedEditorialError(
            "approved canonical basis: profile mismatch"
        )
    claimed = _sha256(
        digest,
        "approved_canonical_basis_sha256",
        "approved canonical basis",
    )
    if claimed != canonical_sha256(basis):
        raise ExpeditedEditorialError(
            "approved canonical basis: digest mismatch"
        )
    return basis


def _validate_carry_source_candidate(
    candidate: Mapping[str, object],
) -> tuple[str, list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    source_revision = _sha40(candidate.get("code_revision"), "source candidate")
    _validate_candidate(candidate, source_revision)
    counts = _mapping(candidate.get("raw_counts"), "source candidate.raw_counts")
    if counts != {
        "event_candidate_count": EVENT_COUNT,
        "same_event_pair_candidate_count": PAIR_COUNT,
        "top5_candidate_count": TOP5_COUNT,
    }:
        raise ExpeditedEditorialError("source candidate: exact raw counts required")
    basis = _mapping(candidate.get("basis"), "source candidate.basis")
    _sha256(
        basis.get("source_snapshot_sha256"),
        "source_snapshot_sha256",
        "source candidate.basis",
    )
    cutoff_at = _timestamp(
        basis.get("brief_cutoff_at"),
        "brief_cutoff_at",
        "source candidate.basis",
    )
    events: list[dict[str, object]] = []
    for index, raw in enumerate(
        _list(basis.get("events"), "source candidate.basis.events")
    ):
        event = _validate_event(raw, f"source candidate.events[{index}]")
        if _event_basis(event) != raw:
            raise ExpeditedEditorialError(
                f"source candidate.events[{index}]: noncanonical event basis"
            )
        events.append(event)
    event_map = {str(item["event_id"]): item for item in events}
    if len(event_map) != EVENT_COUNT:
        raise ExpeditedEditorialError("source candidate: duplicate event")

    pairs = [
        dict(_mapping(raw, f"source candidate.pairs[{index}]"))
        for index, raw in enumerate(
            _list(
                basis.get("same_event_pair_candidates"),
                "source candidate.basis.same_event_pair_candidates",
            )
        )
    ]
    seen_pair_ids: set[str] = set()
    seen_document_pairs: set[tuple[str, str]] = set()
    for index, pair in enumerate(pairs):
        location = f"source candidate.pairs[{index}]"
        pair_id = _text(pair.get("pair_id"), "pair_id", location)
        stratum = _text(pair.get("stratum"), "stratum", location)
        left_event_id = _text(
            pair.get("left_event_id"), "left_event_id", location
        )
        right_event_id = _text(
            pair.get("right_event_id"), "right_event_id", location
        )
        if left_event_id not in event_map or right_event_id not in event_map:
            raise ExpeditedEditorialError(
                f"{location}: pair references an unknown event"
            )
        expected = _pair(
            stratum=stratum,
            left_event=event_map[left_event_id],
            right_event=event_map[right_event_id],
            revision=source_revision,
        )
        if pair != expected:
            raise ExpeditedEditorialError(f"{location}: pair basis mismatch")
        document_ids = sorted(
            (
                str(pair["left_document_id"]),
                str(pair["right_document_id"]),
            )
        )
        document_pair = (document_ids[0], document_ids[1])
        if pair_id in seen_pair_ids or document_pair in seen_document_pairs:
            raise ExpeditedEditorialError(f"{location}: duplicate pair")
        seen_pair_ids.add(pair_id)
        seen_document_pairs.add(document_pair)

    top5 = [
        dict(_mapping(raw, f"source candidate.top5[{index}]"))
        for index, raw in enumerate(
            _list(
                basis.get("top5_candidates"),
                "source candidate.basis.top5_candidates",
            )
        )
    ]
    edition_id = _brief_id(cutoff_at)
    expected_top5 = [
        {
            "edition_id": edition_id,
            "event_id": event["event_id"],
            "position_no": index,
            "official_evidence_count": event["official_evidence_count"],
            "event_evidence_sha256": event["event_evidence_sha256"],
            "public_eligible": True,
        }
        for index, event in enumerate(events[:TOP5_COUNT], start=1)
    ]
    if top5 != expected_top5:
        raise ExpeditedEditorialError("source candidate: Top 5 basis mismatch")
    return source_revision, events, pairs, top5


def _validate_carry_source_human_review(
    human_review: Mapping[str, object],
    *,
    candidate: Mapping[str, object],
    source_revision: str,
    events: Sequence[Mapping[str, object]],
    pairs: Sequence[Mapping[str, object]],
    top5: Sequence[Mapping[str, object]],
    candidate_artifact: Mapping[str, object],
    now: datetime,
) -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    if (
        human_review.get("schema_version") != SCHEMA_VERSION
        or human_review.get("kind") != HUMAN_REVIEW_KIND
        or human_review.get("environment") != "production"
        or human_review.get("is_synthetic") is not False
        or _sha40(human_review.get("code_revision"), "source human review")
        != source_revision
        or human_review.get("ground_truth_source") != "human"
        or human_review.get("ai_generated_ground_truth") is not False
        or human_review.get("human_attestation") is not True
        or human_review.get("evidence_source")
        != "protected_editorial_publication"
        or "carry_forward" in human_review
    ):
        raise ExpeditedEditorialError(
            "source human review: original protected publication required"
        )
    candidate_sha = _sha256(
        candidate.get("candidate_sha256"),
        "candidate_sha256",
        "source candidate",
    )
    if (
        human_review.get("candidate_sha256") != candidate_sha
        or human_review.get("candidate_artifact") != candidate_artifact
    ):
        raise ExpeditedEditorialError(
            "source human review: candidate binding mismatch"
        )
    counts = _mapping(
        human_review.get("raw_counts"),
        "source human review.raw_counts",
    )
    if counts != {
        "event_review_count": EVENT_COUNT,
        "same_event_pair_review_count": PAIR_COUNT,
        "top5_human_reviewed_count": TOP5_COUNT,
        "top5_published_count": TOP5_COUNT,
    }:
        raise ExpeditedEditorialError("source human review: exact 20/40/5 required")

    event_by_id = {str(item["event_id"]): item for item in events}
    raw_event_reviews = _list(
        human_review.get("event_reviews"),
        "source human review.event_reviews",
    )
    event_reviews: list[dict[str, object]] = []
    reviewers: set[tuple[str, str]] = set()
    for index, raw in enumerate(raw_event_reviews):
        location = f"source human review.event_reviews[{index}]"
        review = dict(_mapping(raw, location))
        event_id = _text(review.get("event_id"), "event_id", location)
        if event_id not in event_by_id:
            raise ExpeditedEditorialError(f"{location}: unknown event")
        if review.get("decision") not in {"approved", "rejected"}:
            raise ExpeditedEditorialError(f"{location}: invalid decision")
        if review.get("reviewer_type") != "human":
            raise ExpeditedEditorialError(f"{location}: human reviewer required")
        reviewer = _text(
            review.get("reviewer_reference"),
            "reviewer_reference",
            location,
            maximum=255,
        )
        reviewed_at = _review_time(review.get("reviewed_at"), location, now)
        if set(review) != {
            "event_id",
            "decision",
            "reviewer_type",
            "reviewer_reference",
            "reviewed_at",
        }:
            raise ExpeditedEditorialError(f"{location}: exact fields required")
        reviewers.add((reviewer, reviewed_at))
        event_reviews.append(review)
    if (
        len(event_reviews) != EVENT_COUNT
        or {str(item["event_id"]) for item in event_reviews} != set(event_by_id)
        or len(reviewers) != 1
    ):
        raise ExpeditedEditorialError(
            "source human review: event set or reviewer mismatch"
        )

    pair_by_id = {str(item["pair_id"]): item for item in pairs}
    pair_reviews: list[dict[str, object]] = []
    for index, raw in enumerate(
        _list(
            human_review.get("same_event_pair_reviews"),
            "source human review.same_event_pair_reviews",
        )
    ):
        location = f"source human review.same_event_pair_reviews[{index}]"
        review = dict(_mapping(raw, location))
        pair_id = _text(review.get("pair_id"), "pair_id", location)
        candidate_pair = pair_by_id.get(pair_id)
        if candidate_pair is None:
            raise ExpeditedEditorialError(f"{location}: unknown pair")
        if (
            isinstance(review.get("decision"), bool) is False
            or review.get("reviewer_type") != "human"
            or (
                review.get("left_document_id"),
                review.get("right_document_id"),
            )
            != (
                candidate_pair["left_document_id"],
                candidate_pair["right_document_id"],
            )
        ):
            raise ExpeditedEditorialError(f"{location}: pair decision mismatch")
        reviewer = _text(
            review.get("reviewer_reference"),
            "reviewer_reference",
            location,
            maximum=255,
        )
        reviewed_at = _review_time(review.get("reviewed_at"), location, now)
        if (reviewer, reviewed_at) not in reviewers or set(review) != {
            "pair_id",
            "left_document_id",
            "right_document_id",
            "decision",
            "reviewer_type",
            "reviewer_reference",
            "reviewed_at",
        }:
            raise ExpeditedEditorialError(f"{location}: reviewer or fields mismatch")
        pair_reviews.append(review)
    if (
        len(pair_reviews) != PAIR_COUNT
        or {str(item["pair_id"]) for item in pair_reviews} != set(pair_by_id)
    ):
        raise ExpeditedEditorialError("source human review: pair set mismatch")

    top_by_event = {str(item["event_id"]): item for item in top5}
    top_reviews: list[dict[str, object]] = []
    for index, raw in enumerate(
        _list(
            human_review.get("top5_reviews"),
            "source human review.top5_reviews",
        )
    ):
        location = f"source human review.top5_reviews[{index}]"
        review = dict(_mapping(raw, location))
        event_id = _text(review.get("event_id"), "event_id", location)
        candidate_top = top_by_event.get(event_id)
        if candidate_top is None:
            raise ExpeditedEditorialError(f"{location}: unknown Top 5 item")
        if (
            review.get("decision") != "approved"
            or review.get("reviewer_type") != "human"
            or review.get("edition_id") != candidate_top["edition_id"]
            or review.get("official_evidence_count")
            != candidate_top["official_evidence_count"]
            or review.get("public_eligible") is not True
            or review.get("event_evidence_sha256")
            != candidate_top["event_evidence_sha256"]
        ):
            raise ExpeditedEditorialError(f"{location}: Top 5 binding mismatch")
        reviewer = _text(
            review.get("reviewer_reference"),
            "reviewer_reference",
            location,
            maximum=255,
        )
        reviewed_at = _review_time(review.get("reviewed_at"), location, now)
        if (reviewer, reviewed_at) not in reviewers or set(review) != {
            "edition_id",
            "event_id",
            "decision",
            "reviewer_type",
            "reviewer_reference",
            "reviewed_at",
            "official_evidence_count",
            "public_eligible",
            "event_evidence_sha256",
        }:
            raise ExpeditedEditorialError(f"{location}: reviewer or fields mismatch")
        top_reviews.append(review)
    if (
        len(top_reviews) != TOP5_COUNT
        or {str(item["event_id"]) for item in top_reviews} != set(top_by_event)
    ):
        raise ExpeditedEditorialError("source human review: Top 5 set mismatch")
    approved_ids = {
        str(item["event_id"])
        for item in event_reviews
        if item["decision"] == "approved"
    }
    if any(str(item["event_id"]) not in approved_ids for item in top_reviews):
        raise ExpeditedEditorialError(
            "source human review: Top 5 event was not approved"
        )
    section = {
        field: human_review.get(field)
        for field in (
            "ground_truth_source",
            "ai_generated_ground_truth",
            "human_attestation",
            "raw_counts",
            "event_reviews",
            "same_event_pair_reviews",
            "top5_reviews",
        )
    }
    if human_review.get("section_sha256") != canonical_sha256(section):
        raise ExpeditedEditorialError(
            "source human review: section digest mismatch"
        )
    return event_reviews, pair_reviews, top_reviews


def _validate_carry_source_receipts(
    receipt: Mapping[str, object],
    replay: Mapping[str, object],
    *,
    candidate: Mapping[str, object],
    source_revision: str,
    candidate_artifact: Mapping[str, object],
    event_reviews: Sequence[Mapping[str, object]],
    pair_reviews: Sequence[Mapping[str, object]],
    top_reviews: Sequence[Mapping[str, object]],
    now: datetime,
) -> tuple[dict[str, dict[str, object]], list[dict[str, object]]]:
    candidate_sha = _sha256(
        candidate.get("candidate_sha256"),
        "candidate_sha256",
        "source candidate",
    )
    decision_sha = _sha256(
        receipt.get("decision_sha256"),
        "decision_sha256",
        "source publication",
    )
    semantic_sha = _sha256(
        receipt.get("semantic_receipt_sha256"),
        "semantic_receipt_sha256",
        "source publication",
    )
    review_by_id = {str(item["event_id"]): item for item in event_reviews}
    human_top_by_id = {str(item["event_id"]): item for item in top_reviews}
    candidate_top_by_id: dict[str, dict[str, object]] = {}
    for index, raw in enumerate(
        _list(
            _mapping(candidate.get("basis"), "source candidate.basis").get(
                "top5_candidates"
            ),
            "source candidate.basis.top5_candidates",
        )
    ):
        item = dict(_mapping(raw, f"source candidate.top5[{index}]"))
        candidate_top_by_id[str(item["event_id"])] = item
    receipt_top5 = [
        dict(_mapping(raw, f"source publication.top5[{index}]"))
        for index, raw in enumerate(
            _list(receipt.get("top5"), "source publication.top5")
        )
    ]
    if len(receipt_top5) != TOP5_COUNT:
        raise ExpeditedEditorialError("source publication: exact Top 5 required")
    for index, item in enumerate(receipt_top5):
        location = f"source publication.top5[{index}]"
        event_id = _text(item.get("event_id"), "event_id", location)
        human_top = human_top_by_id.get(event_id)
        candidate_top = candidate_top_by_id.get(event_id)
        if human_top is None or candidate_top is None:
            raise ExpeditedEditorialError(f"{location}: unknown Top 5 event")
        shared_fields = (
            "edition_id",
            "event_id",
            "decision",
            "reviewer_type",
            "reviewer_reference",
            "reviewed_at",
            "official_evidence_count",
            "public_eligible",
            "event_evidence_sha256",
        )
        if (
            any(item.get(field) != human_top.get(field) for field in shared_fields)
            or item.get("position_no") != candidate_top.get("position_no")
        ):
            raise ExpeditedEditorialError(f"{location}: human/candidate mismatch")
        _text(
            item.get("selection_reason"),
            "selection_reason",
            location,
            maximum=500,
        )
    outcome_map: dict[str, dict[str, object]] = {}
    basis = _mapping(candidate.get("basis"), "source candidate.basis")
    cutoff_at = _timestamp(
        basis.get("brief_cutoff_at"),
        "brief_cutoff_at",
        "source candidate.basis",
    )
    expected_brief_id = _brief_id(cutoff_at)

    for record, label in ((receipt, "source publication"), (replay, "source replay")):
        if (
            record.get("schema_version") != SCHEMA_VERSION
            or record.get("kind") != PUBLICATION_KIND
            or record.get("environment") != "production"
            or record.get("is_synthetic") is not False
            or record.get("evidence_source") != "protected_editor_api_v2"
            or "carry_forward" in record
            or _sha40(record.get("code_revision"), label) != source_revision
            or record.get("candidate_artifact") != candidate_artifact
            or record.get("candidate_sha256") != candidate_sha
            or record.get("decision_sha256") != decision_sha
            or record.get("semantic_receipt_sha256") != semantic_sha
            or record.get("same_event_pair_reviews") != list(pair_reviews)
            or record.get("top5") != receipt_top5
        ):
            raise ExpeditedEditorialError(f"{label}: provenance mismatch")
        collected = datetime.fromisoformat(
            _timestamp(record.get("collected_at"), "collected_at", label).replace(
                "Z", "+00:00"
            )
        )
        if collected > now + timedelta(minutes=5) or now - collected > MAX_HUMAN_REVIEW_AGE:
            raise ExpeditedEditorialError(f"{label}: stale or future receipt")
        outcomes = _list(
            record.get("event_review_outcomes"),
            f"{label}.event_review_outcomes",
        )
        current_outcomes: dict[str, dict[str, object]] = {}
        for index, raw in enumerate(outcomes):
            location = f"{label}.event_review_outcomes[{index}]"
            outcome = dict(_mapping(raw, location))
            event_id = _text(outcome.get("event_id"), "event_id", location)
            review = review_by_id.get(event_id)
            if review is None or outcome.get("decision") != review["decision"]:
                raise ExpeditedEditorialError(f"{location}: decision mismatch")
            expected_status = (
                ("approved", "published", "complete")
                if review["decision"] == "approved"
                else ("rejected", "draft", "rejected")
            )
            if (
                (
                    outcome.get("final_review_status"),
                    outcome.get("final_publication_status"),
                    outcome.get("final_identity_status"),
                )
                != expected_status
                or outcome.get("result") not in {"applied", "verified_existing"}
            ):
                raise ExpeditedEditorialError(f"{location}: final state mismatch")
            outcome["final_updated_at"] = _timestamp(
                outcome.get("final_updated_at"),
                "final_updated_at",
                location,
            )
            if event_id in current_outcomes:
                raise ExpeditedEditorialError(f"{location}: duplicate event")
            current_outcomes[event_id] = outcome
        if set(current_outcomes) != set(review_by_id):
            raise ExpeditedEditorialError(f"{label}: event outcome set mismatch")
        if label == "source publication":
            outcome_map = current_outcomes
        elif any(
            current_outcomes[event_id]["final_updated_at"]
            != outcome_map[event_id]["final_updated_at"]
            for event_id in outcome_map
        ):
            raise ExpeditedEditorialError(
                "source replay: final event timestamps changed"
            )

        brief = _mapping(record.get("brief"), f"{label}.brief")
        if (
            brief.get("brief_id") != expected_brief_id
            or brief.get("build_sha") != source_revision
            or brief.get("cutoff_at") != cutoff_at
        ):
            raise ExpeditedEditorialError(f"{label}: brief binding mismatch")
        brief_payload = {
            "edition": "global",
            "cutoff_at": cutoff_at,
            "build_sha": source_revision,
            "empty_reason": None,
            "items": [
                {
                    "event_id": item["event_id"],
                    "lane": "top",
                    "position_no": item["position_no"],
                    "selection_reason": item["selection_reason"],
                }
                for item in receipt_top5
            ],
        }
        if brief.get("payload_sha256") != canonical_sha256(brief_payload):
            raise ExpeditedEditorialError(f"{label}: brief payload digest mismatch")

    mutations = receipt.get("mutations_applied")
    if (
        isinstance(mutations, bool)
        or not isinstance(mutations, int)
        or not 0 <= mutations <= EVENT_COUNT
        or replay.get("mutations_applied") != 0
        or replay.get("idempotent_replay") is not True
        or _mapping(replay.get("brief"), "source replay.brief").get("idempotent")
        is not True
    ):
        raise ExpeditedEditorialError("source publication: replay is not idempotent")
    semantic = {
        "candidate_artifact": dict(candidate_artifact),
        "candidate_sha256": candidate_sha,
        "decision_sha256": decision_sha,
        "code_revision": source_revision,
        "event_reviews": [
            {"event_id": item["event_id"], "decision": item["decision"]}
            for item in event_reviews
        ],
        "same_event_pair_reviews": list(pair_reviews),
        "top5": [
            {
                "event_id": item["event_id"],
                "position_no": item["position_no"],
                "selection_reason": item["selection_reason"],
            }
            for item in receipt_top5
        ],
        "brief": {
            "brief_id": expected_brief_id,
            "build_sha": source_revision,
            "cutoff_at": cutoff_at,
            "payload_sha256": _mapping(
                receipt.get("brief"), "source publication.brief"
            )["payload_sha256"],
        },
    }
    if canonical_sha256(semantic) != semantic_sha:
        raise ExpeditedEditorialError(
            "source publication: semantic receipt digest mismatch"
        )
    return outcome_map, receipt_top5


def _validate_current_carry_event(
    current: Mapping[str, object],
    *,
    approved_event: Mapping[str, object],
    source_outcome: Mapping[str, object],
    candidate_marker: str,
) -> dict[str, object]:
    event_id = str(approved_event["event_id"])
    if (
        str(current.get("event_id")) != event_id
        or current.get("updated_at")
        != approved_event["source_final_updated_at"]
        or current.get("updated_at") != source_outcome["final_updated_at"]
        or candidate_marker
        not in str(current.get("latest_revision_reason") or "")
    ):
        raise ExpeditedEditorialError(
            f"carry-forward: editorial state drift for {event_id}"
        )
    source_issuer_name = _text(
        approved_event.get("issuer_name"),
        "issuer_name",
        f"approved event {event_id}",
        maximum=255,
    )
    current_issuer_name = _text(
        current.get("issuer_name"),
        "issuer_name",
        f"current event {event_id}",
        maximum=255,
    )
    exact_fields = (
        "issuer_id",
        "country",
        "title",
        "original_language",
        "event_family",
        "summary",
        "importance",
        "current_status",
        "deadline_at",
        "verification_status",
        "change_type",
        "review_status",
        "publication_status",
        "identity_action",
        "identity_target",
        "identity_actor_id",
        "identity_effective_at",
        "identity_deadline_at",
        "identity_status",
        "comparison_key",
        "occurred_at",
        "first_observed_at",
    )
    for field in exact_fields:
        if current.get(field) != approved_event.get(field):
            raise ExpeditedEditorialError(
                f"carry-forward: {field} drift for {event_id}"
            )
    current_documents = _carry_document_basis(
        current.get("official_documents"),
        f"current event {event_id}.official_documents",
    )
    if (
        approved_event.get("official_documents") != current_documents
        or current.get("official_evidence_count")
        != approved_event.get("official_evidence_count")
    ):
        raise ExpeditedEditorialError(
            f"carry-forward: official evidence drift for {event_id}"
        )
    actor_fields = (
        "actor_id",
        "display_name",
        "actor_type",
        "actor_role",
        "country_code",
        "actor_review_status",
        "relation_review_status",
        "record_status",
    )
    current_actors = [
        {
            field: _mapping(
                raw,
                f"current event {event_id}.actors[{index}]",
            ).get(field)
            for field in actor_fields
        }
        for index, raw in enumerate(
            _list(current.get("actors"), f"current event {event_id}.actors")
        )
    ]
    if current_actors != [
        {field: _mapping(approved_event["actor"], "approved actor").get(field)
         for field in actor_fields}
    ]:
        raise ExpeditedEditorialError(
            f"carry-forward: approved actor drift for {event_id}"
        )
    current_evidence_sha = _sha256(
        current.get("event_evidence_sha256"),
        "event_evidence_sha256",
        f"current event {event_id}",
    )
    stable_basis = {
        "event_id": event_id,
        "issuer_id": approved_event["issuer_id"],
        "country": approved_event["country"],
        "official_documents": approved_event["official_documents"],
        "official_evidence_count": approved_event[
            "official_evidence_count"
        ],
    }
    return {
        "event_id": event_id,
        "decision": "approved",
        "result": "verified_unchanged",
        "final_review_status": current.get("review_status"),
        "final_publication_status": current.get("publication_status"),
        "final_identity_status": current.get("identity_status"),
        "final_updated_at": current.get("updated_at"),
        "source_issuer_name": source_issuer_name,
        "current_issuer_name": current_issuer_name,
        "issuer_name_drift": current_issuer_name != source_issuer_name,
        "source_event_evidence_sha256": approved_event[
            "source_event_evidence_sha256"
        ],
        "current_event_evidence_sha256": current_evidence_sha,
        "approved_event_basis_sha256": canonical_sha256(approved_event),
        "immutable_evidence_basis_sha256": canonical_sha256(stable_basis),
    }


def _current_carry_event_with_snapshot(
    client: EditorialClient,
    event_id: str,
    expected_revision: str,
) -> tuple[dict[str, object], str]:
    payload = client.event(event_id)
    meta = _mapping(payload.get("meta"), "event.meta")
    event = _validate_event(
        _mapping(payload.get("data"), "event.data").get("event"),
        "event.data.event",
    )
    if _sha40(meta.get("code_revision"), "event.meta") != expected_revision:
        raise ExpeditedEditorialError("event: code_revision mismatch")
    snapshot_sha = _sha256(
        meta.get("snapshot_sha256"),
        "snapshot_sha256",
        f"event {event_id}.meta",
    )
    return event, snapshot_sha


def _reconstruct_legacy_carry_forward_basis(
    *,
    candidate: Mapping[str, object],
    source_human_review: Mapping[str, object],
    source_receipt: Mapping[str, object],
    source_replay_receipt: Mapping[str, object],
    candidate_artifact: Mapping[str, object],
    publication_artifact: Mapping[str, object],
    now: datetime,
) -> dict[str, object]:
    """Reconstruct the one immutable human-approved source chain."""

    source_revision, events, pairs, top5 = _validate_carry_source_candidate(
        candidate
    )
    expected_candidate_artifact = _mapping(
        candidate_artifact, "candidate artifact"
    )
    expected_publication_artifact = _mapping(
        publication_artifact, "publication artifact"
    )
    if set(expected_candidate_artifact) != set(CARRY_FORWARD_ARTIFACT_FIELDS):
        raise ExpeditedEditorialError("candidate artifact: exact fields required")
    if set(expected_publication_artifact) != set(CARRY_FORWARD_ARTIFACT_FIELDS):
        raise ExpeditedEditorialError(
            "publication artifact: exact fields required"
        )
    event_reviews, pair_reviews, top_reviews = (
        _validate_carry_source_human_review(
            source_human_review,
            candidate=candidate,
            source_revision=source_revision,
            events=events,
            pairs=pairs,
            top5=top5,
            candidate_artifact=expected_candidate_artifact,
            now=now,
        )
    )
    source_outcomes, publication_top5 = _validate_carry_source_receipts(
        source_receipt,
        source_replay_receipt,
        candidate=candidate,
        source_revision=source_revision,
        candidate_artifact=expected_candidate_artifact,
        event_reviews=event_reviews,
        pair_reviews=pair_reviews,
        top_reviews=top_reviews,
        now=now,
    )
    approval_attestation = _load_legacy_human_approval_artifact()
    approval_correction = _load_legacy_human_approval_correction()
    approved_canonical_basis = _legacy_approved_canonical_basis(
        candidate=candidate,
        candidate_artifact=expected_candidate_artifact,
        publication_artifact=expected_publication_artifact,
        approval_attestation=approval_attestation,
        approval_correction=approval_correction,
        source_decision_sha256=source_receipt.get("decision_sha256"),
        events=events,
        event_reviews=event_reviews,
        pair_reviews=pair_reviews,
        top_reviews=top_reviews,
        source_outcomes=source_outcomes,
        publication_top5=publication_top5,
    )
    approved_canonical_basis_sha = canonical_sha256(
        approved_canonical_basis
    )
    _validate_approved_canonical_basis(
        approved_canonical_basis,
        approved_canonical_basis_sha,
    )
    approved_events = [
        _mapping(item, f"approved canonical basis.events[{index}]")
        for index, item in enumerate(
            _list(
                approved_canonical_basis.get("events"),
                "approved canonical basis.events",
            )
        )
    ]
    return {
        "source_revision": source_revision,
        "events": events,
        "event_reviews": event_reviews,
        "pair_reviews": pair_reviews,
        "top_reviews": top_reviews,
        "source_outcomes": source_outcomes,
        "publication_top5": publication_top5,
        "candidate_artifact": dict(expected_candidate_artifact),
        "publication_artifact": dict(expected_publication_artifact),
        "approved_canonical_basis": approved_canonical_basis,
        "approved_canonical_basis_sha256": approved_canonical_basis_sha,
        "approved_by_id": {
            str(item["event_id"]): item for item in approved_events
        },
        "candidate_sha256": _sha256(
            candidate.get("candidate_sha256"),
            "candidate_sha256",
            "source candidate",
        ),
    }


def _display_target_repair_reason(
    candidate_sha256: str,
    human_approval_chain_sha256: str,
) -> str:
    return (
        f"[expedited-candidate:{candidate_sha256}] "
        f"[human-approval:{human_approval_chain_sha256}] "
        f"[display-target-repair:{LEGACY_APPROVAL_CORRECTION_SHA256}] "
        "Apply the exact human-approved display target without changing "
        "canonical identity, evidence, or timestamps."
    )


def _validate_display_target_repair_current(
    current: Mapping[str, object],
    *,
    approved_event: Mapping[str, object],
    source_outcome: Mapping[str, object],
    candidate_marker: str,
    require_exact_target: bool,
) -> dict[str, object]:
    event_id = str(approved_event["event_id"])
    exact_target = _text(
        approved_event.get("identity_target"),
        "identity_target",
        f"approved display target {event_id}",
        maximum=700,
    )
    normalized_target = _normalize_identity(exact_target)
    if exact_target == normalized_target:
        raise ExpeditedEditorialError(
            f"display-target repair: {event_id} has no approved raw-form delta"
        )
    current_target = _text(
        current.get("identity_target"),
        "identity_target",
        f"current display target {event_id}",
        maximum=700,
    )
    if _normalize_identity(current_target) != normalized_target:
        raise ExpeditedEditorialError(
            f"display-target repair: normalized target drift for {event_id}"
        )
    if current_target not in {exact_target, normalized_target}:
        raise ExpeditedEditorialError(
            f"display-target repair: unexpected raw target for {event_id}"
        )
    if require_exact_target and current_target != exact_target:
        raise ExpeditedEditorialError(
            f"display-target repair: exact target was not persisted for {event_id}"
        )

    # Reuse the complete carry-forward fence after substituting only the one
    # explicitly permitted raw-form field. This binds every other canonical
    # field, official document, actor, source timestamp and comparison key to
    # the immutable human-approved basis.
    allowed_basis = dict(approved_event)
    allowed_basis["identity_target"] = current_target
    outcome = _validate_current_carry_event(
        current,
        approved_event=allowed_basis,
        source_outcome=source_outcome,
        candidate_marker=candidate_marker,
    )
    actors = [
        {
            field: _mapping(raw, f"current event {event_id}.actor").get(field)
            for field in (
                "actor_id",
                "display_name",
                "actor_type",
                "actor_role",
                "country_code",
                "actor_review_status",
                "relation_review_status",
                "record_status",
            )
        }
        for raw in _list(current.get("actors"), f"current event {event_id}.actors")
    ]
    state_basis = {
        "event_id": event_id,
        "identity_target": current_target,
        "updated_at": current.get("updated_at"),
        "event_evidence_sha256": outcome["current_event_evidence_sha256"],
        "comparison_key": current.get("comparison_key"),
        "official_documents": _carry_document_basis(
            current.get("official_documents"),
            f"current event {event_id}.official_documents",
        ),
        "actors": actors,
    }
    return {
        "event_id": event_id,
        "exact_identity_target": exact_target,
        "normalized_identity_target": normalized_target,
        "current_identity_target": current_target,
        "updated_at": current.get("updated_at"),
        "event_evidence_sha256": outcome["current_event_evidence_sha256"],
        "comparison_key": current.get("comparison_key"),
        "official_documents_sha256": canonical_sha256(
            state_basis["official_documents"]
        ),
        "actors_sha256": canonical_sha256(actors),
        "state_sha256": canonical_sha256(state_basis),
    }


def repair_legacy_display_targets(
    client: EditorialClient,
    *,
    candidate: Mapping[str, object],
    source_human_review: Mapping[str, object],
    source_receipt: Mapping[str, object],
    source_replay_receipt: Mapping[str, object],
    revision: str,
    candidate_artifact: Mapping[str, object],
    publication_artifact: Mapping[str, object],
    now: datetime,
) -> dict[str, object]:
    """Repair only six human-approved display targets, fail-closed."""

    revision = _sha40(revision, "display-target repair revision")
    health = client.health()
    if (
        health.get("service") != "bside-global-market-terminal"
        or _sha40(health.get("code_revision"), "health") != revision
        or health.get("schema_version") != 12
    ):
        raise ExpeditedEditorialError(
            "display-target repair: current health mismatch"
        )
    chain = _reconstruct_legacy_carry_forward_basis(
        candidate=candidate,
        source_human_review=source_human_review,
        source_receipt=source_receipt,
        source_replay_receipt=source_replay_receipt,
        candidate_artifact=candidate_artifact,
        publication_artifact=publication_artifact,
        now=now,
    )
    approved_basis = _mapping(
        chain["approved_canonical_basis"],
        "approved canonical basis",
    )
    approved_by_id = _mapping(chain["approved_by_id"], "approved event map")
    source_outcomes = _mapping(chain["source_outcomes"], "source outcomes")
    candidate_sha = str(chain["candidate_sha256"])
    human_chain_sha = _sha256(
        approved_basis.get("human_approval_chain_sha256"),
        "human_approval_chain_sha256",
        "approved canonical basis",
    )
    expected_ids = list(LEGACY_DISPLAY_TARGET_REPAIR_EVENT_IDS)
    derived_ids: list[str] = []
    for raw_approved_event in _list(
        approved_basis.get("events"), "approved events"
    ):
        approved_event = _mapping(raw_approved_event, "approved event")
        approved_target = _text(
            approved_event.get("identity_target"),
            "identity_target",
            "approved event",
            maximum=700,
        )
        if approved_target != _normalize_identity(approved_target):
            derived_ids.append(str(approved_event["event_id"]))
    if derived_ids != expected_ids:
        raise ExpeditedEditorialError(
            "display-target repair: exact six-event basis changed"
        )
    candidate_marker = f"[expedited-candidate:{candidate_sha}]"
    human_marker = f"[human-approval:{human_chain_sha}]"
    correction_marker = (
        f"[display-target-repair:{LEGACY_APPROVAL_CORRECTION_SHA256}]"
    )
    reason = _display_target_repair_reason(candidate_sha, human_chain_sha)

    preflight: dict[str, tuple[dict[str, object], str]] = {}
    for event_id in expected_ids:
        current, snapshot_sha = _current_carry_event_with_snapshot(
            client,
            event_id,
            revision,
        )
        state = _validate_display_target_repair_current(
            current,
            approved_event=_mapping(
                approved_by_id[event_id],
                f"approved canonical event {event_id}",
            ),
            source_outcome=_mapping(
                source_outcomes[event_id],
                f"source outcome {event_id}",
            ),
            candidate_marker=candidate_marker,
            require_exact_target=False,
        )
        if state["current_identity_target"] == state["exact_identity_target"]:
            latest_reason = str(current.get("latest_revision_reason") or "")
            if human_marker not in latest_reason or correction_marker not in latest_reason:
                raise ExpeditedEditorialError(
                    f"display-target repair: unproven existing raw target for {event_id}"
                )
        preflight[event_id] = (state, snapshot_sha)

    applied_ids: list[str] = []
    response_flags: dict[str, dict[str, object]] = {}
    for event_id in expected_ids:
        before, _ = preflight[event_id]
        if before["current_identity_target"] == before["exact_identity_target"]:
            continue
        approved = _mapping(
            approved_by_id[event_id],
            f"approved canonical event {event_id}",
        )
        actor = _mapping(approved.get("actor"), f"approved actor {event_id}")
        payload = {
            "decision": "approve",
            "expected_updated_at": before["updated_at"],
            "expected_evidence_sha256": before["event_evidence_sha256"],
            "reason": reason,
            "event_family": approved["event_family"],
            "identity_action": approved["identity_action"],
            "identity_target": approved["identity_target"],
            "identity_effective_at": approved["identity_effective_at"],
            "identity_deadline_at": approved["identity_deadline_at"],
            "importance": approved["importance"],
            "summary": approved["summary"],
            "current_status": approved["current_status"],
            "actor": {field: actor.get(field) for field in SAFE_ACTOR_FIELDS},
        }
        response = _mapping(
            client.review(event_id, payload).get("data"),
            f"display-target repair response {event_id}",
        )
        if (
            response.get("event_id") != event_id
            or response.get("decision") != "approved"
            or response.get("published") is not True
            or response.get("display_target_repaired") is not True
            or response.get("updated_at_preserved") is not True
        ):
            raise ExpeditedEditorialError(
                f"display-target repair: response mismatch for {event_id}"
            )
        applied_ids.append(event_id)
        response_flags[event_id] = {
            "display_target_repaired": True,
            "updated_at_preserved": True,
        }

    event_results: list[dict[str, object]] = []
    verified_ids: list[str] = []
    for event_id in expected_ids:
        before, before_snapshot = preflight[event_id]
        current, after_snapshot = _current_carry_event_with_snapshot(
            client,
            event_id,
            revision,
        )
        after = _validate_display_target_repair_current(
            current,
            approved_event=_mapping(
                approved_by_id[event_id],
                f"approved canonical event {event_id}",
            ),
            source_outcome=_mapping(
                source_outcomes[event_id],
                f"source outcome {event_id}",
            ),
            candidate_marker=candidate_marker,
            require_exact_target=True,
        )
        latest_reason = str(current.get("latest_revision_reason") or "")
        if human_marker not in latest_reason or correction_marker not in latest_reason:
            raise ExpeditedEditorialError(
                f"display-target repair: approval markers missing for {event_id}"
            )
        for field in (
            "updated_at",
            "event_evidence_sha256",
            "comparison_key",
            "official_documents_sha256",
            "actors_sha256",
        ):
            if before[field] != after[field]:
                raise ExpeditedEditorialError(
                    f"display-target repair: {field} changed for {event_id}"
                )
        applied = event_id in applied_ids
        event_results.append(
            {
                "event_id": event_id,
                "expected_identity_target": after["exact_identity_target"],
                "normalized_identity_target": after[
                    "normalized_identity_target"
                ],
                "before_identity_target": before["current_identity_target"],
                "after_identity_target": after["current_identity_target"],
                "applied": applied,
                "verified": True,
                "updated_at": after["updated_at"],
                "event_evidence_sha256": after["event_evidence_sha256"],
                "comparison_key": after["comparison_key"],
                "official_documents_sha256": after[
                    "official_documents_sha256"
                ],
                "actors_sha256": after["actors_sha256"],
                "before_state_sha256": before["state_sha256"],
                "after_state_sha256": after["state_sha256"],
                "before_snapshot_sha256": before_snapshot,
                "after_snapshot_sha256": after_snapshot,
                "response_flags": response_flags.get(event_id),
            }
        )
        verified_ids.append(event_id)

    collected_at = (
        now.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "kind": DISPLAY_TARGET_REPAIR_KIND,
        "environment": "production",
        "is_synthetic": False,
        "code_revision": revision,
        "collected_at": collected_at,
        "source_code_revision": chain["source_revision"],
        "candidate_artifact": chain["candidate_artifact"],
        "source_publication_artifact": chain["publication_artifact"],
        "candidate_sha256": candidate_sha,
        "source_decision_sha256": _sha256(
            source_receipt.get("decision_sha256"),
            "decision_sha256",
            "source publication",
        ),
        "approved_canonical_basis_sha256": chain[
            "approved_canonical_basis_sha256"
        ],
        "human_approval_chain_sha256": human_chain_sha,
        "human_approval_attestation_sha256": LEGACY_APPROVAL_ATTESTATION_SHA256,
        "human_approval_correction_sha256": LEGACY_APPROVAL_CORRECTION_SHA256,
        "human_approval_correction_text_sha256": (
            LEGACY_APPROVAL_CORRECTION_TEXT_SHA256
        ),
        "reviewer_type": "human",
        "reviewer_reference": LEGACY_APPROVAL_REVIEWER,
        "ai_generated_ground_truth": False,
        "discovery_artifact": dict(
            LEGACY_DISPLAY_TARGET_REPAIR_DISCOVERY_ARTIFACT
        ),
        "expected_event_ids": expected_ids,
        "applied_event_ids": applied_ids,
        "verified_event_ids": verified_ids,
        "mutations_applied": len(applied_ids),
        "idempotent_replay": len(applied_ids) == 0,
        "event_results": event_results,
    }
    return {
        **receipt,
        "receipt_sha256": canonical_sha256(receipt),
    }


def _validate_display_target_repair_receipt(
    value: object,
    *,
    revision: str,
) -> dict[str, object]:
    receipt = dict(_mapping(value, "display-target repair receipt"))
    expected_fields = {
        "schema_version",
        "kind",
        "environment",
        "is_synthetic",
        "code_revision",
        "collected_at",
        "source_code_revision",
        "candidate_artifact",
        "source_publication_artifact",
        "candidate_sha256",
        "source_decision_sha256",
        "approved_canonical_basis_sha256",
        "human_approval_chain_sha256",
        "human_approval_attestation_sha256",
        "human_approval_correction_sha256",
        "human_approval_correction_text_sha256",
        "reviewer_type",
        "reviewer_reference",
        "ai_generated_ground_truth",
        "discovery_artifact",
        "expected_event_ids",
        "applied_event_ids",
        "verified_event_ids",
        "mutations_applied",
        "idempotent_replay",
        "event_results",
        "receipt_sha256",
    }
    if (
        set(receipt) != expected_fields
        or receipt.get("schema_version") != SCHEMA_VERSION
        or receipt.get("kind") != DISPLAY_TARGET_REPAIR_KIND
        or receipt.get("environment") != "production"
        or receipt.get("is_synthetic") is not False
        or _sha40(receipt.get("code_revision"), "display-target repair")
        != revision
        or receipt.get("candidate_artifact")
        != LEGACY_APPROVAL_CANDIDATE_ARTIFACT
        or receipt.get("source_publication_artifact")
        != LEGACY_APPROVAL_PUBLICATION_ARTIFACT
        or receipt.get("candidate_sha256")
        != LEGACY_APPROVAL_CANDIDATE_SHA256
        or receipt.get("source_decision_sha256")
        != LEGACY_APPROVAL_SOURCE_DECISION_SHA256
        or receipt.get("human_approval_chain_sha256")
        != LEGACY_APPROVAL_CHAIN_SHA256
        or receipt.get("human_approval_attestation_sha256")
        != LEGACY_APPROVAL_ATTESTATION_SHA256
        or receipt.get("human_approval_correction_sha256")
        != LEGACY_APPROVAL_CORRECTION_SHA256
        or receipt.get("human_approval_correction_text_sha256")
        != LEGACY_APPROVAL_CORRECTION_TEXT_SHA256
        or receipt.get("reviewer_type") != "human"
        or receipt.get("reviewer_reference") != LEGACY_APPROVAL_REVIEWER
        or receipt.get("ai_generated_ground_truth") is not False
        or receipt.get("discovery_artifact")
        != LEGACY_DISPLAY_TARGET_REPAIR_DISCOVERY_ARTIFACT
    ):
        raise ExpeditedEditorialError(
            "display-target repair receipt: provenance mismatch"
        )
    _sha40(receipt.get("source_code_revision"), "display-target repair source")
    _timestamp(receipt.get("collected_at"), "collected_at", "display-target repair")
    _sha256(
        receipt.get("approved_canonical_basis_sha256"),
        "approved_canonical_basis_sha256",
        "display-target repair",
    )
    supplied_sha = _sha256(
        receipt.get("receipt_sha256"),
        "receipt_sha256",
        "display-target repair",
    )
    if canonical_sha256(
        {key: item for key, item in receipt.items() if key != "receipt_sha256"}
    ) != supplied_sha:
        raise ExpeditedEditorialError(
            "display-target repair receipt: digest mismatch"
        )
    expected_ids = list(LEGACY_DISPLAY_TARGET_REPAIR_EVENT_IDS)
    applied_ids = _list(
        receipt.get("applied_event_ids"),
        "display-target repair.applied_event_ids",
    )
    verified_ids = _list(
        receipt.get("verified_event_ids"),
        "display-target repair.verified_event_ids",
    )
    mutations = receipt.get("mutations_applied")
    if (
        receipt.get("expected_event_ids") != expected_ids
        or verified_ids != expected_ids
        or len(applied_ids) != len(set(str(item) for item in applied_ids))
        or any(item not in expected_ids for item in applied_ids)
        or isinstance(mutations, bool)
        or not isinstance(mutations, int)
        or mutations != len(applied_ids)
        or not 0 <= mutations <= len(expected_ids)
        or receipt.get("idempotent_replay") is not (mutations == 0)
    ):
        raise ExpeditedEditorialError(
            "display-target repair receipt: event count mismatch"
        )
    results = [
        _mapping(item, f"display-target repair.event_results[{index}]")
        for index, item in enumerate(
            _list(receipt.get("event_results"), "display-target repair results")
        )
    ]
    result_fields = {
        "event_id",
        "expected_identity_target",
        "normalized_identity_target",
        "before_identity_target",
        "after_identity_target",
        "applied",
        "verified",
        "updated_at",
        "event_evidence_sha256",
        "comparison_key",
        "official_documents_sha256",
        "actors_sha256",
        "before_state_sha256",
        "after_state_sha256",
        "before_snapshot_sha256",
        "after_snapshot_sha256",
        "response_flags",
    }
    if len(results) != len(expected_ids):
        raise ExpeditedEditorialError(
            "display-target repair receipt: result count mismatch"
        )
    for event_id, item in zip(expected_ids, results, strict=True):
        applied = event_id in applied_ids
        expected_target = _text(
            item.get("expected_identity_target"),
            "expected_identity_target",
            f"display-target repair result {event_id}",
            maximum=700,
        )
        normalized_target = _text(
            item.get("normalized_identity_target"),
            "normalized_identity_target",
            f"display-target repair result {event_id}",
            maximum=700,
        )
        if (
            set(item) != result_fields
            or item.get("event_id") != event_id
            or item.get("verified") is not True
            or item.get("applied") is not applied
            or expected_target == normalized_target
            or _normalize_identity(expected_target) != normalized_target
            or item.get("after_identity_target") != expected_target
            or item.get("before_identity_target")
            not in {expected_target, normalized_target}
            or (not applied and item.get("before_identity_target") != expected_target)
        ):
            raise ExpeditedEditorialError(
                f"display-target repair receipt: result mismatch for {event_id}"
            )
        for field in (
            "event_evidence_sha256",
            "official_documents_sha256",
            "actors_sha256",
            "before_state_sha256",
            "after_state_sha256",
            "before_snapshot_sha256",
            "after_snapshot_sha256",
        ):
            _sha256(item.get(field), field, f"display-target repair {event_id}")
        comparison_key = item.get("comparison_key")
        if not isinstance(comparison_key, str) or re.fullmatch(
            r"global:[0-9a-f]{64}", comparison_key
        ) is None:
            raise ExpeditedEditorialError(
                "display-target repair receipt: invalid comparison key for "
                + event_id
            )
        _timestamp(item.get("updated_at"), "updated_at", f"display-target repair {event_id}")
        flags = item.get("response_flags")
        if applied:
            if flags != {
                "display_target_repaired": True,
                "updated_at_preserved": True,
            }:
                raise ExpeditedEditorialError(
                    f"display-target repair receipt: response flags for {event_id}"
                )
        elif flags is not None:
            raise ExpeditedEditorialError(
                f"display-target repair receipt: unexpected response for {event_id}"
            )
    return receipt


def validate_display_target_repair_receipts(
    receipt: object,
    replay: object,
    *,
    revision: str,
    approved_canonical_basis_sha256: object,
) -> tuple[dict[str, object], dict[str, object]]:
    revision = _sha40(revision, "display-target repair revision")
    first = _validate_display_target_repair_receipt(receipt, revision=revision)
    second = _validate_display_target_repair_receipt(replay, revision=revision)
    approved_sha = _sha256(
        approved_canonical_basis_sha256,
        "approved_canonical_basis_sha256",
        "carry-forward intent",
    )
    if (
        first["approved_canonical_basis_sha256"] != approved_sha
        or second["approved_canonical_basis_sha256"] != approved_sha
        or second["mutations_applied"] != 0
        or second["applied_event_ids"] != []
        or second["idempotent_replay"] is not True
    ):
        raise ExpeditedEditorialError(
            "display-target repair receipts: replay/provenance mismatch"
        )
    first_results = _list(first["event_results"], "first repair results")
    second_results = _list(second["event_results"], "second repair results")
    for first_raw, second_raw in zip(first_results, second_results, strict=True):
        first_item = _mapping(first_raw, "first repair result")
        second_item = _mapping(second_raw, "second repair result")
        if (
            first_item["event_id"] != second_item["event_id"]
            or second_item["before_identity_target"]
            != first_item["after_identity_target"]
            or second_item["after_identity_target"]
            != first_item["after_identity_target"]
            or second_item["before_state_sha256"]
            != first_item["after_state_sha256"]
            or second_item["after_state_sha256"]
            != first_item["after_state_sha256"]
            or second_item["updated_at"] != first_item["updated_at"]
            or second_item["event_evidence_sha256"]
            != first_item["event_evidence_sha256"]
            or second_item["comparison_key"] != first_item["comparison_key"]
        ):
            raise ExpeditedEditorialError(
                "display-target repair receipts: idempotent state mismatch"
            )
    return first, second


def prepare_carry_forward_publication(
    client: EditorialClient,
    *,
    candidate: Mapping[str, object],
    source_human_review: Mapping[str, object],
    source_receipt: Mapping[str, object],
    source_replay_receipt: Mapping[str, object],
    revision: str,
    candidate_artifact: Mapping[str, object],
    publication_artifact: Mapping[str, object],
    now: datetime,
) -> dict[str, object]:
    """Freeze a reviewed current-SHA publication intent without publishing.

    This never calls the event review endpoint. It proves the source
    publication, verifies all current event/evidence rows are unchanged, and
    freezes the complete human and current-event basis before any brief write.
    """

    revision = _sha40(revision, "carry-forward revision")
    health = client.health()
    if (
        health.get("service") != "bside-global-market-terminal"
        or _sha40(health.get("code_revision"), "health") != revision
        or health.get("schema_version") != 12
    ):
        raise ExpeditedEditorialError("carry-forward: current health mismatch")
    source_revision, events, pairs, top5 = _validate_carry_source_candidate(
        candidate
    )
    expected_candidate_artifact = _mapping(
        candidate_artifact, "candidate artifact"
    )
    expected_publication_artifact = _mapping(
        publication_artifact, "publication artifact"
    )
    if set(expected_candidate_artifact) != set(CARRY_FORWARD_ARTIFACT_FIELDS):
        raise ExpeditedEditorialError("candidate artifact: exact fields required")
    if set(expected_publication_artifact) != set(CARRY_FORWARD_ARTIFACT_FIELDS):
        raise ExpeditedEditorialError("publication artifact: exact fields required")
    event_reviews, pair_reviews, top_reviews = (
        _validate_carry_source_human_review(
            source_human_review,
            candidate=candidate,
            source_revision=source_revision,
            events=events,
            pairs=pairs,
            top5=top5,
            candidate_artifact=expected_candidate_artifact,
            now=now,
        )
    )
    source_outcomes, publication_top5 = _validate_carry_source_receipts(
        source_receipt,
        source_replay_receipt,
        candidate=candidate,
        source_revision=source_revision,
        candidate_artifact=expected_candidate_artifact,
        event_reviews=event_reviews,
        pair_reviews=pair_reviews,
        top_reviews=top_reviews,
        now=now,
    )
    approval_attestation = _load_legacy_human_approval_artifact()
    approval_correction = _load_legacy_human_approval_correction()
    approved_canonical_basis = _legacy_approved_canonical_basis(
        candidate=candidate,
        candidate_artifact=expected_candidate_artifact,
        publication_artifact=expected_publication_artifact,
        approval_attestation=approval_attestation,
        approval_correction=approval_correction,
        source_decision_sha256=source_receipt.get("decision_sha256"),
        events=events,
        event_reviews=event_reviews,
        pair_reviews=pair_reviews,
        top_reviews=top_reviews,
        source_outcomes=source_outcomes,
        publication_top5=publication_top5,
    )
    approved_canonical_basis_sha = canonical_sha256(
        approved_canonical_basis
    )
    _validate_approved_canonical_basis(
        approved_canonical_basis,
        approved_canonical_basis_sha,
    )
    candidate_sha = _sha256(
        candidate.get("candidate_sha256"),
        "candidate_sha256",
        "source candidate",
    )
    marker = "[expedited-candidate:" + candidate_sha + "]"
    approved_events = [
        _mapping(item, f"approved canonical basis.events[{index}]")
        for index, item in enumerate(
            _list(
                approved_canonical_basis.get("events"),
                "approved canonical basis.events",
            )
        )
    ]
    approved_by_id = {
        str(item["event_id"]): item for item in approved_events
    }

    def verify_current_basis() -> list[dict[str, object]]:
        outcomes = []
        for candidate_event in events:
            event_id = str(candidate_event["event_id"])
            current, snapshot_sha = _current_carry_event_with_snapshot(
                client,
                event_id,
                revision,
            )
            outcome = _validate_current_carry_event(
                current,
                approved_event=_mapping(
                    approved_by_id[event_id],
                    f"approved canonical event {event_id}",
                ),
                source_outcome=source_outcomes[event_id],
                candidate_marker=marker,
            )
            outcome["current_snapshot_sha256"] = snapshot_sha
            outcomes.append(outcome)
        return outcomes

    first_verified_outcomes = verify_current_basis()
    verified_outcomes = verify_current_basis()
    current_basis_fence_sha = canonical_sha256(verified_outcomes)
    if canonical_sha256(first_verified_outcomes) != current_basis_fence_sha:
        raise ExpeditedEditorialError(
            "carry-forward: current event basis changed during pre-publication fence"
        )
    immutable_basis_sha = canonical_sha256(
        [
            {
                "event_id": item["event_id"],
                "final_updated_at": item["final_updated_at"],
                "source_issuer_name": item["source_issuer_name"],
                "current_issuer_name": item["current_issuer_name"],
                "issuer_name_drift": item["issuer_name_drift"],
                "source_event_evidence_sha256": item[
                    "source_event_evidence_sha256"
                ],
                "current_event_evidence_sha256": item[
                    "current_event_evidence_sha256"
                ],
                "current_snapshot_sha256": item[
                    "current_snapshot_sha256"
                ],
                "immutable_evidence_basis_sha256": item[
                    "immutable_evidence_basis_sha256"
                ],
            }
            for item in verified_outcomes
        ]
    )
    collected_at = (
        now.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
    source_cutoff_at = _timestamp(
        source_receipt.get("collected_at"),
        "collected_at",
        "source publication",
    )
    # A brief ID is edition|cutoff. Reusing the reviewed source cutoff with a
    # new build SHA would deterministically conflict with the already-frozen
    # source brief. Freeze one distinct current-SHA cutoff in this intent.
    cutoff_at = collected_at
    top_event_ids = {
        str(item["event_id"]) for item in publication_top5
    }
    top_snapshots = {
        str(item["event_id"]): item["current_snapshot_sha256"]
        for item in verified_outcomes
        if str(item["event_id"]) in top_event_ids
    }
    snapshot_basis = [
        {
            "event_id": event_id,
            "snapshot_sha256": top_snapshots[event_id],
        }
        for event_id in sorted(top_snapshots)
    ]
    brief_payload = {
        "edition": "global",
        "cutoff_at": cutoff_at,
        "build_sha": revision,
        "empty_reason": None,
        "items": [
            {
                "event_id": item["event_id"],
                "lane": "top",
                "position_no": item["position_no"],
                "selection_reason": item["selection_reason"],
                "expected_snapshot_sha256": top_snapshots[
                    str(item["event_id"])
                ],
            }
            for item in publication_top5
        ],
        "expected_event_basis_sha256": canonical_sha256(snapshot_basis),
    }
    expected_brief_id = _brief_id(cutoff_at)
    original_section = {
        field: source_human_review.get(field)
        for field in (
            "ground_truth_source",
            "ai_generated_ground_truth",
            "human_attestation",
            "raw_counts",
            "event_reviews",
            "same_event_pair_reviews",
            "top5_reviews",
        )
    }
    source_semantic = _sha256(
        source_receipt.get("semantic_receipt_sha256"),
        "semantic_receipt_sha256",
        "source publication",
    )
    carry_provenance = {
        "kind": CARRY_FORWARD_KIND,
        "source_code_revision": source_revision,
        "source_candidate_artifact": dict(expected_candidate_artifact),
        "source_publication_artifact": dict(expected_publication_artifact),
        "source_publication_semantic_receipt_sha256": source_semantic,
        "source_human_review_section_sha256": source_human_review[
            "section_sha256"
        ],
        "source_brief_cutoff_at": source_cutoff_at,
        "approved_canonical_basis": approved_canonical_basis,
        "approved_canonical_basis_sha256": approved_canonical_basis_sha,
        "human_approval_chain_sha256": approved_canonical_basis[
            "human_approval_chain_sha256"
        ],
        "current_immutable_editorial_basis_sha256": immutable_basis_sha,
        "current_basis_fence_sha256": current_basis_fence_sha,
        "event_mutations_applied": 0,
    }
    decision_sha = _sha256(
        source_receipt.get("decision_sha256"),
        "decision_sha256",
        "source publication",
    )
    prepared_at = collected_at
    fresh_until = (
        datetime.fromisoformat(prepared_at.replace("Z", "+00:00"))
        + CARRY_FORWARD_INTENT_MAX_FRESH_AGE
    ).isoformat().replace("+00:00", "Z")
    intent_payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": CARRY_FORWARD_INTENT_KIND,
        "environment": "production",
        "is_synthetic": False,
        "code_revision": revision,
        "prepared_at": prepared_at,
        "fresh_until": fresh_until,
        "candidate_artifact": dict(expected_candidate_artifact),
        "source_publication_artifact": dict(expected_publication_artifact),
        "candidate_sha256": candidate_sha,
        "decision_sha256": decision_sha,
        "source_human_review_section": original_section,
        "source_human_review_section_sha256": canonical_sha256(
            original_section
        ),
        "carry_forward": carry_provenance,
        "event_reviews": [
            {"event_id": item["event_id"], "decision": item["decision"]}
            for item in event_reviews
        ],
        "same_event_pair_reviews": list(pair_reviews),
        "top5": list(publication_top5),
        "verified_outcomes": verified_outcomes,
        "brief_payload": brief_payload,
        "expected_brief_id": expected_brief_id,
    }
    return {
        **intent_payload,
        "intent_sha256": canonical_sha256(intent_payload),
    }


def _validate_carry_forward_intent(
    value: object,
    *,
    revision: str,
) -> dict[str, object]:
    intent = _mapping(value, "carry-forward intent")
    expected_fields = {
        "schema_version",
        "kind",
        "environment",
        "is_synthetic",
        "code_revision",
        "prepared_at",
        "fresh_until",
        "candidate_artifact",
        "source_publication_artifact",
        "candidate_sha256",
        "decision_sha256",
        "source_human_review_section",
        "source_human_review_section_sha256",
        "carry_forward",
        "event_reviews",
        "same_event_pair_reviews",
        "top5",
        "verified_outcomes",
        "brief_payload",
        "expected_brief_id",
        "intent_sha256",
    }
    if (
        set(intent) != expected_fields
        or intent.get("schema_version") != SCHEMA_VERSION
        or intent.get("kind") != CARRY_FORWARD_INTENT_KIND
        or intent.get("environment") != "production"
        or intent.get("is_synthetic") is not False
        or _sha40(intent.get("code_revision"), "carry-forward intent")
        != revision
    ):
        raise ExpeditedEditorialError("carry-forward intent: contract mismatch")
    supplied_digest = _sha256(
        intent.get("intent_sha256"),
        "intent_sha256",
        "carry-forward intent",
    )
    digest_basis = {
        key: item for key, item in intent.items() if key != "intent_sha256"
    }
    if canonical_sha256(digest_basis) != supplied_digest:
        raise ExpeditedEditorialError("carry-forward intent: digest mismatch")
    prepared_at = _timestamp(
        intent.get("prepared_at"),
        "prepared_at",
        "carry-forward intent",
    )
    fresh_until = _timestamp(
        intent.get("fresh_until"),
        "fresh_until",
        "carry-forward intent",
    )
    expected_fresh_until = (
        datetime.fromisoformat(prepared_at.replace("Z", "+00:00"))
        + CARRY_FORWARD_INTENT_MAX_FRESH_AGE
    ).isoformat().replace("+00:00", "Z")
    if fresh_until != expected_fresh_until:
        raise ExpeditedEditorialError(
            "carry-forward intent: freshness contract mismatch"
        )
    candidate_artifact = _mapping(
        intent.get("candidate_artifact"),
        "carry-forward intent.candidate_artifact",
    )
    publication_artifact = _mapping(
        intent.get("source_publication_artifact"),
        "carry-forward intent.source_publication_artifact",
    )
    if (
        candidate_artifact != LEGACY_APPROVAL_CANDIDATE_ARTIFACT
        or publication_artifact != LEGACY_APPROVAL_PUBLICATION_ARTIFACT
        or intent.get("candidate_sha256")
        != LEGACY_APPROVAL_CANDIDATE_SHA256
        or intent.get("decision_sha256")
        != LEGACY_APPROVAL_SOURCE_DECISION_SHA256
    ):
        raise ExpeditedEditorialError(
            "carry-forward intent: approved source binding mismatch"
        )
    original_section = _mapping(
        intent.get("source_human_review_section"),
        "carry-forward intent.source_human_review_section",
    )
    if (
        canonical_sha256(original_section)
        != intent.get("source_human_review_section_sha256")
        or original_section.get("ground_truth_source") != "human"
        or original_section.get("ai_generated_ground_truth") is not False
        or original_section.get("human_attestation") is not True
    ):
        raise ExpeditedEditorialError(
            "carry-forward intent: human review binding mismatch"
        )
    carry = _mapping(
        intent.get("carry_forward"),
        "carry-forward intent.carry_forward",
    )
    approved_basis = _validate_approved_canonical_basis(
        carry.get("approved_canonical_basis"),
        carry.get("approved_canonical_basis_sha256"),
    )
    if (
        carry.get("kind") != CARRY_FORWARD_KIND
        or carry.get("event_mutations_applied") != 0
        or carry.get("source_candidate_artifact") != candidate_artifact
        or carry.get("source_publication_artifact")
        != publication_artifact
        or carry.get("human_approval_chain_sha256")
        != _legacy_human_approval_chain_sha256()
        or approved_basis.get("human_approval_chain_sha256")
        != carry.get("human_approval_chain_sha256")
        or len(_list(approved_basis.get("events"), "approved events"))
        != EVENT_COUNT
    ):
        raise ExpeditedEditorialError(
            "carry-forward intent: provenance mismatch"
        )
    event_reviews = [
        _mapping(item, f"carry-forward intent.event_reviews[{index}]")
        for index, item in enumerate(
            _list(intent.get("event_reviews"), "intent.event_reviews")
        )
    ]
    pair_reviews = _list(
        intent.get("same_event_pair_reviews"),
        "intent.same_event_pair_reviews",
    )
    top5 = [
        _mapping(item, f"carry-forward intent.top5[{index}]")
        for index, item in enumerate(
            _list(intent.get("top5"), "intent.top5")
        )
    ]
    outcomes = [
        _mapping(item, f"carry-forward intent.verified_outcomes[{index}]")
        for index, item in enumerate(
            _list(intent.get("verified_outcomes"), "intent.verified_outcomes")
        )
    ]
    outcome_audit_valid = True
    for index, item in enumerate(outcomes):
        location = f"carry-forward intent.verified_outcomes[{index}]"
        if set(item) != CARRY_FORWARD_OUTCOME_FIELDS:
            outcome_audit_valid = False
            break
        source_name = _text(
            item.get("source_issuer_name"),
            "source_issuer_name",
            location,
            maximum=255,
        )
        current_name = _text(
            item.get("current_issuer_name"),
            "current_issuer_name",
            location,
            maximum=255,
        )
        name_drift = item.get("issuer_name_drift")
        if (
            not isinstance(name_drift, bool)
            or name_drift != (source_name != current_name)
        ):
            outcome_audit_valid = False
            break
    if (
        len(event_reviews) != EVENT_COUNT
        or len(pair_reviews) != PAIR_COUNT
        or len(top5) != TOP5_COUNT
        or len(outcomes) != EVENT_COUNT
        or not outcome_audit_valid
        or any(item.get("decision") != "approved" for item in event_reviews)
        or any(
            item.get("result") != "verified_unchanged"
            or not SHA256.fullmatch(
                str(item.get("current_snapshot_sha256") or "")
            )
            for item in outcomes
        )
    ):
        raise ExpeditedEditorialError(
            "carry-forward intent: frozen review basis mismatch"
        )
    brief_payload = _mapping(
        intent.get("brief_payload"),
        "carry-forward intent.brief_payload",
    )
    if set(brief_payload) != {
        "edition",
        "cutoff_at",
        "build_sha",
        "empty_reason",
        "items",
        "expected_event_basis_sha256",
    }:
        raise ExpeditedEditorialError(
            "carry-forward intent: brief payload fields mismatch"
        )
    brief_items = [
        _mapping(item, f"carry-forward intent.brief_payload.items[{index}]")
        for index, item in enumerate(
            _list(brief_payload.get("items"), "intent.brief_payload.items")
        )
    ]
    outcome_snapshots = {
        str(item["event_id"]): item["current_snapshot_sha256"]
        for item in outcomes
        if str(item["event_id"])
        in {str(top["event_id"]) for top in top5}
    }
    expected_top_items = [
        {
            "event_id": item["event_id"],
            "lane": "top",
            "position_no": item["position_no"],
            "selection_reason": item["selection_reason"],
            "expected_snapshot_sha256": outcome_snapshots[
                str(item["event_id"])
            ],
        }
        for item in top5
    ]
    expected_snapshot_basis = [
        {
            "event_id": event_id,
            "snapshot_sha256": outcome_snapshots[event_id],
        }
        for event_id in sorted(outcome_snapshots)
    ]
    if (
        brief_payload.get("edition") != "global"
        or brief_payload.get("cutoff_at") != prepared_at
        or brief_payload.get("build_sha") != revision
        or brief_payload.get("empty_reason") is not None
        or brief_items != expected_top_items
        or brief_payload.get("expected_event_basis_sha256")
        != canonical_sha256(expected_snapshot_basis)
        or intent.get("expected_brief_id") != _brief_id(prepared_at)
    ):
        raise ExpeditedEditorialError(
            "carry-forward intent: frozen brief mismatch"
        )
    return intent


def _validate_carry_intent_artifact(
    value: object,
    *,
    revision: str,
) -> dict[str, object]:
    artifact = _mapping(value, "carry-forward intent artifact")
    if set(artifact) != set(CARRY_FORWARD_ARTIFACT_FIELDS):
        raise ExpeditedEditorialError(
            "carry-forward intent artifact: exact fields required"
        )
    run_id = artifact.get("run_id")
    artifact_id = artifact.get("artifact_id")
    name = _text(
        artifact.get("artifact_name"),
        "artifact_name",
        "carry-forward intent artifact",
        maximum=255,
    )
    digest = "sha256:" + _sha256(
        str(artifact.get("artifact_digest") or "").removeprefix("sha256:"),
        "artifact_digest",
        "carry-forward intent artifact",
    )
    expected_prefix = (
        "global-alpha-expedited-editorial-carry-intent-" + revision + "-"
    )
    if (
        isinstance(run_id, bool)
        or not isinstance(run_id, int)
        or run_id < 1
        or isinstance(artifact_id, bool)
        or not isinstance(artifact_id, int)
        or artifact_id < 1
        or not name.startswith(expected_prefix)
    ):
        raise ExpeditedEditorialError(
            "carry-forward intent artifact: identity mismatch"
        )
    return {
        "run_id": run_id,
        "artifact_id": artifact_id,
        "artifact_name": name,
        "artifact_digest": digest,
    }


def publish_carry_forward_intent(
    client: EditorialClient,
    *,
    intent: Mapping[str, object],
    revision: str,
    intent_artifact: Mapping[str, object],
    now: datetime,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    """Publish or safely recover one pre-uploaded immutable carry intent."""

    revision = _sha40(revision, "carry-forward revision")
    health = client.health()
    if (
        health.get("service") != "bside-global-market-terminal"
        or _sha40(health.get("code_revision"), "health") != revision
        or health.get("schema_version") != 12
    ):
        raise ExpeditedEditorialError("carry-forward: current health mismatch")
    frozen = _validate_carry_forward_intent(intent, revision=revision)
    artifact = _validate_carry_intent_artifact(
        intent_artifact,
        revision=revision,
    )
    prepared_at = datetime.fromisoformat(
        str(frozen["prepared_at"]).replace("Z", "+00:00")
    ).astimezone(timezone.utc)
    fresh_until = datetime.fromisoformat(
        str(frozen["fresh_until"]).replace("Z", "+00:00")
    ).astimezone(timezone.utc)
    current = now.astimezone(timezone.utc)
    if current < prepared_at - timedelta(minutes=5):
        raise ExpeditedEditorialError(
            "carry-forward intent: prepared_at is in the future"
        )
    recovery_only = current > fresh_until
    brief_payload = dict(
        _mapping(frozen.get("brief_payload"), "carry-forward intent brief")
    )
    if recovery_only:
        brief_payload["require_existing"] = True
    first_brief = _mapping(
        client.publish_brief(brief_payload).get("data"),
        "carry-forward brief response",
    )
    replay_brief = _mapping(
        client.publish_brief(brief_payload).get("data"),
        "carry-forward brief replay response",
    )
    expected_brief_id = str(frozen["expected_brief_id"])
    for response, label in (
        (first_brief, "carry-forward brief"),
        (replay_brief, "carry-forward brief replay"),
    ):
        if (
            response.get("brief_id") != expected_brief_id
            or response.get("edition") != "global"
            or response.get("published") is not True
        ):
            raise ExpeditedEditorialError(f"{label}: publication mismatch")
    recovered_existing_brief = first_brief.get("idempotent") is True
    if (
        first_brief.get("idempotent") is not False
        and first_brief.get("idempotent") is not True
    ):
        raise ExpeditedEditorialError(
            "carry-forward brief omitted idempotency state"
        )
    if recovery_only and not recovered_existing_brief:
        raise ExpeditedEditorialError(
            "carry-forward stale intent created a new brief"
        )
    if replay_brief.get("idempotent") is not True:
        raise ExpeditedEditorialError(
            "carry-forward brief replay was not idempotent"
        )

    collected_at = (
        current.replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
    original_section = _mapping(
        frozen.get("source_human_review_section"),
        "carry-forward intent human section",
    )
    carry_provenance = {
        **_mapping(
            frozen.get("carry_forward"),
            "carry-forward intent provenance",
        ),
        "prepared_intent_artifact": artifact,
        "prepared_intent_sha256": frozen["intent_sha256"],
    }
    human_review = {
        "schema_version": SCHEMA_VERSION,
        "kind": HUMAN_REVIEW_KIND,
        "environment": "production",
        "evidence_source": "protected_editorial_carry_forward",
        "is_synthetic": False,
        "code_revision": revision,
        "collected_at": collected_at,
        "evidence_as_of": collected_at,
        "candidate_artifact": frozen["candidate_artifact"],
        "candidate_sha256": frozen["candidate_sha256"],
        **original_section,
        "section_sha256": canonical_sha256(original_section),
        "carry_forward": carry_provenance,
    }
    top5 = [
        _mapping(item, f"carry-forward intent.top5[{index}]")
        for index, item in enumerate(
            _list(frozen.get("top5"), "carry-forward intent.top5")
        )
    ]
    semantic_brief_payload = dict(
        _mapping(
            frozen.get("brief_payload"),
            "carry-forward intent.brief_payload",
        )
    )
    semantic = {
        "candidate_artifact": frozen["candidate_artifact"],
        "candidate_sha256": frozen["candidate_sha256"],
        "decision_sha256": frozen["decision_sha256"],
        "code_revision": revision,
        "prepared_intent_artifact": artifact,
        "prepared_intent_sha256": frozen["intent_sha256"],
        "carry_forward": carry_provenance,
        "event_review_outcomes": frozen["verified_outcomes"],
        "event_reviews": frozen["event_reviews"],
        "same_event_pair_reviews": frozen["same_event_pair_reviews"],
        "top5": [
            {
                "event_id": item["event_id"],
                "position_no": item["position_no"],
                "selection_reason": item["selection_reason"],
            }
            for item in top5
        ],
        "brief": {
            "brief_id": expected_brief_id,
            "build_sha": revision,
            "cutoff_at": frozen["prepared_at"],
            "payload_sha256": canonical_sha256(semantic_brief_payload),
        },
    }
    semantic_sha = canonical_sha256(semantic)

    def receipt(brief: Mapping[str, object]) -> dict[str, object]:
        idempotent = brief.get("idempotent") is True
        return {
            "schema_version": SCHEMA_VERSION,
            "kind": PUBLICATION_KIND,
            "environment": "production",
            "evidence_source": "protected_editorial_carry_forward",
            "is_synthetic": False,
            "code_revision": revision,
            "collected_at": collected_at,
            "candidate_artifact": frozen["candidate_artifact"],
            "candidate_sha256": frozen["candidate_sha256"],
            "decision_sha256": frozen["decision_sha256"],
            "prepared_intent_artifact": artifact,
            "prepared_intent_sha256": frozen["intent_sha256"],
            "publication_mode": (
                "existing_only_recovery" if recovery_only else "fresh"
            ),
            "event_review_outcomes": frozen["verified_outcomes"],
            "same_event_pair_reviews": frozen[
                "same_event_pair_reviews"
            ],
            "top5": top5,
            "brief": {
                **_mapping(semantic["brief"], "carry-forward semantic brief"),
                "idempotent": idempotent,
            },
            "mutations_applied": 0,
            "event_mutations_applied": 0,
            "idempotent_replay": idempotent,
            "recovered_existing_brief": recovered_existing_brief,
            "semantic_receipt_sha256": semantic_sha,
            "carry_forward": carry_provenance,
        }

    return human_review, receipt(first_brief), receipt(replay_brief)


def _environment_client() -> tuple[EditorialClient, str]:
    api_base = (
        os.environ.get("BSIDE_API_BASE_URL")
        or os.environ.get("GOVERNANCE_API_BASE_URL")
        or ""
    )
    editor_token = os.environ.get("BSIDE_EDITOR_TOKEN") or ""
    revision = _sha40(os.environ.get("GITHUB_SHA"), "environment")
    return EditorialClient(api_base, editor_token), revision


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export and publish protected expedited editorial reviews"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    export_parser = sub.add_parser("export")
    export_parser.add_argument("--output-dir", type=Path, required=True)
    apply_parser = sub.add_parser("apply")
    apply_parser.add_argument("--candidate", type=Path, required=True)
    apply_parser.add_argument("--decisions", type=Path, required=True)
    apply_parser.add_argument("--candidate-run-id", type=int, required=True)
    apply_parser.add_argument("--candidate-artifact-id", type=int, required=True)
    apply_parser.add_argument("--candidate-artifact-name", required=True)
    apply_parser.add_argument("--candidate-artifact-digest", required=True)
    apply_parser.add_argument("--output-dir", type=Path, required=True)
    def add_carry_source_arguments(
        command_parser: argparse.ArgumentParser,
    ) -> None:
        command_parser.add_argument("--candidate", type=Path, required=True)
        command_parser.add_argument(
            "--source-human-review",
            type=Path,
            required=True,
        )
        command_parser.add_argument(
            "--source-publication-receipt",
            type=Path,
            required=True,
        )
        command_parser.add_argument(
            "--source-publication-replay-receipt",
            type=Path,
            required=True,
        )
        command_parser.add_argument(
            "--candidate-run-id",
            type=int,
            required=True,
        )
        command_parser.add_argument(
            "--candidate-artifact-id",
            type=int,
            required=True,
        )
        command_parser.add_argument(
            "--candidate-artifact-name",
            required=True,
        )
        command_parser.add_argument(
            "--candidate-artifact-digest",
            required=True,
        )
        command_parser.add_argument(
            "--publication-run-id",
            type=int,
            required=True,
        )
        command_parser.add_argument(
            "--publication-artifact-id",
            type=int,
            required=True,
        )
        command_parser.add_argument(
            "--publication-artifact-name",
            required=True,
        )
        command_parser.add_argument(
            "--publication-artifact-digest",
            required=True,
        )
        command_parser.add_argument(
            "--output-dir",
            type=Path,
            required=True,
        )

    repair_parser = sub.add_parser("carry-forward-repair-targets")
    add_carry_source_arguments(repair_parser)
    prepare_parser = sub.add_parser("carry-forward-prepare")
    add_carry_source_arguments(prepare_parser)
    prepare_parser.add_argument(
        "--display-target-repair-receipt",
        type=Path,
        required=True,
    )
    prepare_parser.add_argument(
        "--display-target-repair-replay-receipt",
        type=Path,
        required=True,
    )
    publish_parser = sub.add_parser("carry-forward-publish")
    publish_parser.add_argument("--intent", type=Path, required=True)
    publish_parser.add_argument("--intent-run-id", type=int, required=True)
    publish_parser.add_argument(
        "--intent-artifact-id",
        type=int,
        required=True,
    )
    publish_parser.add_argument("--intent-artifact-name", required=True)
    publish_parser.add_argument("--intent-artifact-digest", required=True)
    publish_parser.add_argument(
        "--display-target-repair-receipt",
        type=Path,
        required=True,
    )
    publish_parser.add_argument(
        "--display-target-repair-replay-receipt",
        type=Path,
        required=True,
    )
    publish_parser.add_argument("--output-dir", type=Path, required=True)
    decode_parser = sub.add_parser("decode-decisions")
    decode_parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "decode-decisions":
        decode_human_decisions_secret(
            os.environ.get("HUMAN_DECISIONS_GZIP_B64") or "",
            args.output,
        )
        print("bounded human-decision input validated")
        return 0
    client, revision = _environment_client()
    try:
        if args.command == "export":
            candidate, template, review_pack = export_candidates(
                client,
                expected_revision=revision,
            )
            _write_json(args.output_dir / "editorial-candidates.json", candidate)
            _write_json(
                args.output_dir / "human-decisions-template.json",
                template,
            )
            (args.output_dir / "review-pack.md").write_text(
                review_pack,
                encoding="utf-8",
                newline="\n",
            )
            print(
                "expedited editorial candidates ready: "
                f"events={EVENT_COUNT} pairs={PAIR_COUNT} top5={TOP5_COUNT} "
                f"candidate_sha256={candidate['candidate_sha256']}"
            )
            return 0
        if args.command in {
            "carry-forward-repair-targets",
            "carry-forward-prepare",
        }:
            candidate = _load_json(args.candidate, "source candidate")
            candidate_artifact = _artifact_identity(
                run_id=args.candidate_run_id,
                artifact_id=args.candidate_artifact_id,
                artifact_name=args.candidate_artifact_name,
                artifact_digest=args.candidate_artifact_digest,
                location="source candidate artifact",
            )
            publication_artifact = _artifact_identity(
                run_id=args.publication_run_id,
                artifact_id=args.publication_artifact_id,
                artifact_name=args.publication_artifact_name,
                artifact_digest=args.publication_artifact_digest,
                location="source publication artifact",
            )
            source_human_review = _load_json(
                args.source_human_review,
                "source human review",
            )
            source_receipt = _load_json(
                args.source_publication_receipt,
                "source publication receipt",
            )
            source_replay_receipt = _load_json(
                args.source_publication_replay_receipt,
                "source publication replay receipt",
            )
            if args.command == "carry-forward-repair-targets":
                repair_receipt = repair_legacy_display_targets(
                    client,
                    candidate=candidate,
                    source_human_review=source_human_review,
                    source_receipt=source_receipt,
                    source_replay_receipt=source_replay_receipt,
                    revision=revision,
                    candidate_artifact=candidate_artifact,
                    publication_artifact=publication_artifact,
                    now=datetime.now(timezone.utc),
                )
                _write_json(
                    args.output_dir / "display-target-repair-receipt.json",
                    repair_receipt,
                )
                print(
                    "expedited editorial display-target repair verified: "
                    f"expected={len(LEGACY_DISPLAY_TARGET_REPAIR_EVENT_IDS)} "
                    f"mutations={repair_receipt['mutations_applied']} "
                    f"idempotent_replay={repair_receipt['idempotent_replay']}"
                )
                return 0
            intent = prepare_carry_forward_publication(
                client,
                candidate=candidate,
                source_human_review=source_human_review,
                source_receipt=source_receipt,
                source_replay_receipt=source_replay_receipt,
                revision=revision,
                candidate_artifact=candidate_artifact,
                publication_artifact=publication_artifact,
                now=datetime.now(timezone.utc),
            )
            repair_receipt, repair_replay_receipt = (
                validate_display_target_repair_receipts(
                    _load_json(
                        args.display_target_repair_receipt,
                        "display-target repair receipt",
                    ),
                    _load_json(
                        args.display_target_repair_replay_receipt,
                        "display-target repair replay receipt",
                    ),
                    revision=revision,
                    approved_canonical_basis_sha256=_mapping(
                        intent.get("carry_forward"),
                        "carry-forward intent provenance",
                    ).get("approved_canonical_basis_sha256"),
                )
            )
            _write_json(
                args.output_dir / "carry-forward-intent.json",
                intent,
            )
            _write_json(
                args.output_dir / "display-target-repair-receipt.json",
                repair_receipt,
            )
            _write_json(
                args.output_dir / "display-target-repair-replay-receipt.json",
                repair_replay_receipt,
            )
            print(
                "expedited editorial carry-forward intent verified: "
                "event_mutations=0 brief_mutations=0 "
                f"intent_sha256={intent['intent_sha256']}"
            )
            return 0
        if args.command == "carry-forward-publish":
            intent_artifact = _artifact_identity(
                run_id=args.intent_run_id,
                artifact_id=args.intent_artifact_id,
                artifact_name=args.intent_artifact_name,
                artifact_digest=args.intent_artifact_digest,
                location="carry-forward intent artifact",
            )
            frozen_intent = _load_json(
                args.intent,
                "carry-forward intent",
            )
            repair_receipt, repair_replay_receipt = (
                validate_display_target_repair_receipts(
                    _load_json(
                        args.display_target_repair_receipt,
                        "display-target repair receipt",
                    ),
                    _load_json(
                        args.display_target_repair_replay_receipt,
                        "display-target repair replay receipt",
                    ),
                    revision=revision,
                    approved_canonical_basis_sha256=_mapping(
                        frozen_intent.get("carry_forward"),
                        "carry-forward intent provenance",
                    ).get("approved_canonical_basis_sha256"),
                )
            )
            human_review, receipt, replay = publish_carry_forward_intent(
                client,
                intent=frozen_intent,
                revision=revision,
                intent_artifact=intent_artifact,
                now=datetime.now(timezone.utc),
            )
            _write_json(args.output_dir / "human-review.json", human_review)
            _write_json(
                args.output_dir / "publication-receipt.json",
                receipt,
            )
            _write_json(
                args.output_dir / "publication-replay-receipt.json",
                replay,
            )
            _write_json(
                args.output_dir / "display-target-repair-receipt.json",
                repair_receipt,
            )
            _write_json(
                args.output_dir / "display-target-repair-replay-receipt.json",
                repair_replay_receipt,
            )
            print(
                "expedited editorial carry-forward verified: "
                "event_mutations=0 "
                f"idempotent_replay={replay['idempotent_replay']} "
                f"semantic_receipt_sha256={receipt['semantic_receipt_sha256']}"
            )
            return 0
        candidate = _validate_candidate(
            _load_json(args.candidate, "candidate"),
            revision,
        )
        decisions = _load_json(args.decisions, "decisions")
        human_review, receipt = apply_publication(
            client,
            candidate=candidate,
            decisions=decisions,
            revision=revision,
            candidate_run_id=args.candidate_run_id,
            candidate_artifact_id=args.candidate_artifact_id,
            candidate_artifact_name=args.candidate_artifact_name,
            candidate_artifact_digest=args.candidate_artifact_digest,
            now=datetime.now(timezone.utc),
        )
        _write_json(args.output_dir / "human-review.json", human_review)
        _write_json(args.output_dir / "publication-receipt.json", receipt)
        print(
            "expedited editorial publication verified: "
            f"mutations={receipt['mutations_applied']} "
            f"idempotent_replay={receipt['idempotent_replay']} "
            f"semantic_receipt_sha256={receipt['semantic_receipt_sha256']}"
        )
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
