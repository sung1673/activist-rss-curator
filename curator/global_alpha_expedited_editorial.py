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
EVENT_COUNT = 20
PAIR_COUNT = 40
TOP5_COUNT = 5
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
