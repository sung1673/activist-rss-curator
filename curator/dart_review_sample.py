from __future__ import annotations

import argparse
import csv
import hashlib
import hmac
import io
import json
import os
import re
import sys
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping, Sequence
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import httpx


CORPUS_CONTRACT_VERSION = "dart-review-corpus-v1"
SAMPLE_CONTRACT_VERSION = "dart-review-sample-v1"
SAMPLE_SCHEMA_VERSION = 1
DEFAULT_SAMPLE_SIZE = 100
MAX_SAMPLE_SIZE = 1_000
MAX_CORPUS_SIZE = 100_000
MAX_RANGE_DAYS = 31
REVIEW_SAMPLE_DAYS = 30

REVISION_STATUSES = (
    "current",
    "original_superseded",
    "correction_linked",
    "correction_unlinked",
    "withdrawal_linked",
    "withdrawal_unlinked",
)

CORPUS_ITEM_FIELDS = (
    "document_id",
    "event_id",
    "company_id",
    "company_name",
    "event_type",
    "revision_status",
    "external_id",
    "title",
    "original_language",
    "original_url",
    "published_at",
    "source_right_id",
    "correction_of_document_id",
    "version_no",
    "has_later_correction",
    "has_successor",
    "is_correction",
    "is_cancelled",
    "event_verification_status",
    "document_verification_status",
    "document_publication_status",
    "identity_status",
    "review_status",
    "importance",
)

_RESPONSE_FIELDS = {
    "ok",
    "api_version",
    "contract_version",
    "range",
    "backend_binding_id",
    "population_count",
    "corpus_sha256",
    "items",
    "next_cursor",
}
_ENTITY_ID_RE = re.compile(r"^[A-Za-z0-9_.:\-]{1,96}$")
_COMPANY_ID_RE = re.compile(r"^[0-9]{8}$")
_RECEIPT_RE = re.compile(r"^[0-9]{14}$")
_LANGUAGE_RE = re.compile(r"^[a-z]{2,3}(?:-[A-Z]{2})?$")
_TOKEN_RE = re.compile(r"^[a-z][a-z0-9_.:\-]{0,63}$")
_CURSOR_RE = re.compile(r"^[A-Za-z0-9_-]{1,512}$")
_SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
_REVISION_RE = re.compile(r"^[a-f0-9]{7,40}$")
_IDEMPOTENT_GET_TRANSPORT_ATTEMPTS = 3
_DNS_HOST_RE = re.compile(
    r"(?=.{1,253}\Z)"
    r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?"
)


class DartReviewSampleError(RuntimeError):
    """A review corpus or its backfill provenance failed closed."""


class DartReviewApiError(DartReviewSampleError):
    """The authenticated review-corpus API did not satisfy its contract."""


@dataclass(frozen=True)
class CorpusSnapshot:
    from_date: date
    to_date: date
    backend_binding_id: str
    population_count: int
    corpus_sha256: str
    items: tuple[dict[str, object], ...]


@dataclass(frozen=True)
class BackfillEvidence:
    code_revision: str
    job_fingerprint: str
    checkpoint_version: int
    checkpoint_sha256: str
    report_sha256: str
    completed_window_count: int
    expected_dart_document_count: int


def _parse_date(value: str, *, field: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise DartReviewSampleError(f"{field} must be an ISO date") from exc


def validate_date_range(from_date: date, to_date: date) -> None:
    days = (to_date - from_date).days
    if days < 1 or days > MAX_RANGE_DAYS:
        raise DartReviewSampleError(
            f"review corpus range must contain between 1 and {MAX_RANGE_DAYS} completed days"
        )


def validate_sample_date_range(
    from_date: date,
    to_date: date,
    *,
    now: datetime | None = None,
) -> None:
    validate_date_range(from_date, to_date)
    if (to_date - from_date).days != REVIEW_SAMPLE_DAYS:
        raise DartReviewSampleError(
            f"review sample range must contain exactly {REVIEW_SAMPLE_DAYS} completed days"
        )
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise DartReviewSampleError("review sample clock must include a timezone")
    completed_kst_end_exclusive = current.astimezone(ZoneInfo("Asia/Seoul")).date()
    if to_date > completed_kst_end_exclusive:
        raise DartReviewSampleError(
            "review sample range must contain completed KST dates only"
        )


def _code_revision(value: str) -> str:
    revision = value.strip().casefold()
    if _REVISION_RE.fullmatch(revision) is None:
        raise DartReviewSampleError(
            "code_revision must be 7-40 lowercase hexadecimal characters"
        )
    return revision


def _validated_api_base_url(raw: str) -> str:
    value = raw.strip()
    if not value:
        return ""
    if (
        any(ord(character) <= 32 or ord(character) == 127 for character in value)
        or "\\" in value
        or "%" in value
    ):
        raise DartReviewApiError(
            "review corpus API base URL contains an unsafe URL character"
        )
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise DartReviewApiError("review corpus API base URL is invalid") from exc
    hostname = (parsed.hostname or "").casefold()
    if (
        parsed.scheme.casefold() != "https"
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or _DNS_HOST_RE.fullmatch(hostname) is None
        or port not in (None, 443)
    ):
        raise DartReviewApiError(
            "review corpus API base URL must use credential-free canonical HTTPS"
        )
    path = parsed.path.rstrip("/")
    segments = path.split("/")
    if (
        not path.startswith("/")
        or not path.endswith("/api/v1")
        or "//" in path
        or any(segment in {".", ".."} for segment in segments)
    ):
        raise DartReviewApiError(
            "review corpus API base URL must end with /api/v1"
        )
    return urlunsplit(("https", hostname, path, "", ""))


def _required_text(
    row: Mapping[str, object],
    field: str,
    *,
    maximum: int,
) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise DartReviewApiError(f"corpus item has invalid {field}")
    return value


def _required_token(row: Mapping[str, object], field: str) -> str:
    value = _required_text(row, field, maximum=64)
    if _TOKEN_RE.fullmatch(value) is None:
        raise DartReviewApiError(f"corpus item has invalid {field}")
    return value


def _required_bool(row: Mapping[str, object], field: str) -> bool:
    value = row.get(field)
    if not isinstance(value, bool):
        raise DartReviewApiError(f"corpus item has invalid {field}")
    return value


def _required_int(row: Mapping[str, object], field: str) -> int:
    value = row.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise DartReviewApiError(f"corpus item has invalid {field}")
    return value


def _published_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DartReviewApiError("corpus item has invalid published_at") from exc
    if parsed.tzinfo is None:
        raise DartReviewApiError("corpus item has invalid published_at")
    return parsed.astimezone(timezone.utc)


def _validate_revision_status(row: Mapping[str, object]) -> None:
    status = str(row["revision_status"])
    correction_of = row["correction_of_document_id"]
    linked = correction_of is not None
    version_no = _required_int(row, "version_no")
    is_correction = bool(row["is_correction"])
    is_cancelled = bool(row["is_cancelled"])
    document_verification = str(row["document_verification_status"])
    withdrawn = is_cancelled or document_verification == "withdrawn"
    superseded = bool(row["has_later_correction"]) or bool(row["has_successor"])

    if (linked and version_no < 2) or (not linked and version_no != 1):
        raise DartReviewApiError("corpus item has inconsistent document lineage")
    expected = ""
    if withdrawn:
        expected = "withdrawal_linked" if linked else "withdrawal_unlinked"
    elif is_correction:
        expected = "correction_linked" if linked else "correction_unlinked"
    elif linked:
        raise DartReviewApiError("corpus item has inconsistent document lineage")
    elif superseded:
        expected = "original_superseded"
    else:
        expected = "current"
    if status != expected:
        raise DartReviewApiError("corpus item has inconsistent revision_status")


def normalize_corpus_item(
    value: object,
    *,
    from_date: date,
    to_date: date,
) -> dict[str, object]:
    if not isinstance(value, dict):
        raise DartReviewApiError("corpus item must be an object")
    if set(value) != set(CORPUS_ITEM_FIELDS):
        raise DartReviewApiError("corpus item fields do not match the contract")

    document_id = _required_text(value, "document_id", maximum=96)
    event_id = _required_text(value, "event_id", maximum=96)
    company_id = _required_text(value, "company_id", maximum=8)
    external_id = _required_text(value, "external_id", maximum=14)
    if (
        _ENTITY_ID_RE.fullmatch(document_id) is None
        or _ENTITY_ID_RE.fullmatch(event_id) is None
        or _COMPANY_ID_RE.fullmatch(company_id) is None
        or _RECEIPT_RE.fullmatch(external_id) is None
        or document_id != f"dart:{external_id}"
    ):
        raise DartReviewApiError("corpus item has invalid stable identifiers")

    company_name = _required_text(value, "company_name", maximum=255)
    event_type = _required_token(value, "event_type")
    revision_status = _required_token(value, "revision_status")
    if revision_status not in REVISION_STATUSES:
        raise DartReviewApiError("corpus item has invalid revision_status")
    title = _required_text(value, "title", maximum=700)
    original_language = _required_text(value, "original_language", maximum=16)
    if _LANGUAGE_RE.fullmatch(original_language) is None:
        raise DartReviewApiError("corpus item has invalid original_language")

    original_url = _required_text(value, "original_url", maximum=65_535)
    parsed_url = urlsplit(original_url)
    if (
        parsed_url.scheme != "https"
        or not parsed_url.netloc
        or parsed_url.username is not None
        or parsed_url.password is not None
        or parsed_url.fragment
    ):
        raise DartReviewApiError("corpus item has invalid original_url")

    published_at = _required_text(value, "published_at", maximum=40)
    published = _published_datetime(published_at)
    try:
        datetime.strptime(external_id[:8], "%Y%m%d")
    except ValueError as exc:
        raise DartReviewApiError("corpus item has invalid receipt date") from exc
    if not (from_date <= published.date() < to_date):
        raise DartReviewApiError(
            "corpus item falls outside the requested publication-date range"
        )

    source_right_id = _required_text(value, "source_right_id", maximum=64)
    if source_right_id != "official:dart":
        raise DartReviewApiError("corpus item has invalid source_right_id")
    correction_value = value.get("correction_of_document_id")
    if correction_value is not None and (
        not isinstance(correction_value, str)
        or _ENTITY_ID_RE.fullmatch(correction_value) is None
        or correction_value == document_id
        or not correction_value.startswith("dart:")
    ):
        raise DartReviewApiError("corpus item has invalid correction_of_document_id")

    version_no = _required_int(value, "version_no")
    if version_no < 1:
        raise DartReviewApiError("corpus item has invalid version_no")
    has_later_correction = _required_bool(value, "has_later_correction")
    has_successor = _required_bool(value, "has_successor")
    is_correction = _required_bool(value, "is_correction")
    is_cancelled = _required_bool(value, "is_cancelled")
    event_verification_status = _required_token(value, "event_verification_status")
    document_verification_status = _required_token(
        value, "document_verification_status"
    )
    document_publication_status = _required_token(
        value, "document_publication_status"
    )
    identity_status = _required_token(value, "identity_status")
    review_status = _required_token(value, "review_status")
    importance = _required_token(value, "importance")

    normalized: dict[str, object] = {
        "document_id": document_id,
        "event_id": event_id,
        "company_id": company_id,
        "company_name": company_name,
        "event_type": event_type,
        "revision_status": revision_status,
        "external_id": external_id,
        "title": title,
        "original_language": original_language,
        "original_url": original_url,
        "published_at": published_at,
        "source_right_id": source_right_id,
        "correction_of_document_id": correction_value,
        "version_no": version_no,
        "has_later_correction": has_later_correction,
        "has_successor": has_successor,
        "is_correction": is_correction,
        "is_cancelled": is_cancelled,
        "event_verification_status": event_verification_status,
        "document_verification_status": document_verification_status,
        "document_publication_status": document_publication_status,
        "identity_status": identity_status,
        "review_status": review_status,
        "importance": importance,
    }
    _validate_revision_status(normalized)
    return normalized


def _canonical_json(value: Mapping[str, object]) -> str:
    return json.dumps(
        dict(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _corpus_sort_key(row: Mapping[str, object]) -> tuple[str, str, str]:
    return (
        str(row["published_at"]),
        str(row["document_id"]),
        str(row["event_id"]),
    )


def corpus_sha256(rows: Iterable[Mapping[str, object]]) -> str:
    context = hashlib.sha256()
    for row in sorted(rows, key=_corpus_sort_key):
        context.update(_canonical_json(row).encode("utf-8"))
        context.update(b"\n")
    return context.hexdigest()


class DartReviewCorpusClient:
    def __init__(
        self,
        *,
        base_url: str | None = None,
        token: str | None = None,
        backend_binding_id: str | None = None,
        timeout: float = 90.0,
        max_population: int = MAX_CORPUS_SIZE,
        transport: httpx.BaseTransport | None = None,
        client_factory: Callable[..., httpx.Client] = httpx.Client,
    ) -> None:
        configured_url = (
            base_url
            if base_url is not None
            else (
                os.environ.get("BSIDE_API_BASE_URL", "").strip()
                or os.environ.get("GOVERNANCE_API_BASE_URL", "").strip()
            )
        )
        configured_token = (
            token
            if token is not None
            else os.environ.get("BSIDE_OPS_TOKEN", "").strip()
        )
        configured_binding_id = (
            backend_binding_id
            if backend_binding_id is not None
            else os.environ.get("BSIDE_BACKEND_BINDING_ID", "").strip()
        )
        self.base_url = _validated_api_base_url(configured_url).rstrip("/")
        self.token = configured_token.strip()
        self.backend_binding_id = configured_binding_id.strip()
        self.timeout = timeout
        self.max_population = max_population
        self.transport = transport
        self.client_factory = client_factory
        if (
            not self.base_url
            or len(self.token) < 32
            or _SHA256_RE.fullmatch(self.backend_binding_id) is None
        ):
            raise DartReviewApiError(
                "review corpus API requires a configured base URL, an ops token "
                "of at least 32 characters, and a lowercase SHA-256 backend binding"
            )
        if timeout <= 0 or max_population < 1:
            raise ValueError("invalid review corpus client limits")

    @property
    def endpoint(self) -> str:
        return f"{self.base_url}/ops/dart-review-corpus"

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
            "Cache-Control": "no-cache",
            "Connection": "close",
        }

    def _get_with_transport_retry(
        self,
        client: httpx.Client,
        *,
        params: dict[str, str],
    ) -> httpx.Response:
        """Retry only an idempotent corpus GET after a transport failure."""

        last_error: httpx.TransportError | None = None
        for attempt in range(_IDEMPOTENT_GET_TRANSPORT_ATTEMPTS):
            try:
                if attempt == 0:
                    return client.get(
                        self.endpoint,
                        params=params,
                        headers=self._headers(),
                    )
                with self.client_factory(
                    timeout=self.timeout,
                    transport=self.transport,
                ) as retry_client:
                    return retry_client.get(
                        self.endpoint,
                        params=params,
                        headers=self._headers(),
                    )
            except httpx.TransportError as exc:
                last_error = exc
        if last_error is None:  # pragma: no cover - defensive invariant
            raise RuntimeError("review corpus GET retry exhausted without an error")
        raise last_error

    @staticmethod
    def _response_object(response: httpx.Response) -> dict[str, object]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise DartReviewApiError(
                f"review corpus API returned invalid JSON (HTTP {response.status_code})"
            ) from exc
        if not isinstance(payload, dict):
            raise DartReviewApiError("review corpus API response must be an object")
        if response.status_code != 200:
            raise DartReviewApiError(
                f"review corpus API rejected the request (HTTP {response.status_code})"
            )
        if set(payload) != _RESPONSE_FIELDS:
            raise DartReviewApiError(
                "review corpus API response fields do not match the contract"
            )
        return payload

    def fetch(
        self,
        *,
        from_date: date,
        to_date: date,
        page_size: int = 100,
    ) -> CorpusSnapshot:
        validate_date_range(from_date, to_date)
        if page_size < 1 or page_size > 100:
            raise ValueError("review corpus page_size must be between 1 and 100")

        expected_population: int | None = None
        expected_sha256: str | None = None
        records: list[dict[str, object]] = []
        document_ids: set[str] = set()
        cursor: str | None = None
        seen_cursors: set[str] = set()

        try:
            with self.client_factory(
                timeout=self.timeout,
                transport=self.transport,
            ) as client:
                while True:
                    params = {
                        "from": from_date.isoformat(),
                        "to": to_date.isoformat(),
                        "limit": str(page_size),
                    }
                    if cursor is not None:
                        params["cursor"] = cursor
                    response = self._get_with_transport_retry(
                        client,
                        params=params,
                    )
                    payload = self._response_object(response)
                    if payload.get("ok") is not True:
                        raise DartReviewApiError(
                            f"review corpus API rejected the request (HTTP {response.status_code})"
                        )
                    if payload.get("api_version") != "v1":
                        raise DartReviewApiError(
                            "review corpus API api_version mismatch"
                        )
                    if payload.get("contract_version") != CORPUS_CONTRACT_VERSION:
                        raise DartReviewApiError(
                            "review corpus API contract_version mismatch"
                        )
                    remote_binding_id = payload.get("backend_binding_id")
                    if (
                        not isinstance(remote_binding_id, str)
                        or _SHA256_RE.fullmatch(remote_binding_id) is None
                        or not hmac.compare_digest(
                            remote_binding_id, self.backend_binding_id
                        )
                    ):
                        raise DartReviewApiError(
                            "review corpus API backend binding acknowledgment does not match"
                        )
                    response_range = payload.get("range")
                    if response_range != {
                        "from": from_date.isoformat(),
                        "to": to_date.isoformat(),
                    }:
                        raise DartReviewApiError("review corpus API range mismatch")

                    population = payload.get("population_count")
                    if (
                        isinstance(population, bool)
                        or not isinstance(population, int)
                        or population < 0
                        or population > self.max_population
                    ):
                        raise DartReviewApiError(
                            "review corpus API population_count is invalid"
                        )
                    remote_sha = payload.get("corpus_sha256")
                    if (
                        not isinstance(remote_sha, str)
                        or _SHA256_RE.fullmatch(remote_sha) is None
                    ):
                        raise DartReviewApiError(
                            "review corpus API corpus_sha256 is invalid"
                        )
                    if expected_population is None:
                        expected_population = population
                        expected_sha256 = remote_sha
                    elif (
                        population != expected_population
                        or remote_sha != expected_sha256
                    ):
                        raise DartReviewApiError(
                            "review corpus changed while pagination was in progress"
                        )

                    items = payload.get("items")
                    if not isinstance(items, list) or len(items) > page_size:
                        raise DartReviewApiError(
                            "review corpus API returned an invalid item page"
                        )
                    for item in items:
                        normalized = normalize_corpus_item(
                            item,
                            from_date=from_date,
                            to_date=to_date,
                        )
                        document_id = str(normalized["document_id"])
                        if document_id in document_ids:
                            raise DartReviewApiError(
                                "review corpus API returned a duplicate document"
                            )
                        document_ids.add(document_id)
                        records.append(normalized)
                        if len(records) > self.max_population:
                            raise DartReviewApiError(
                                "review corpus exceeds the configured population limit"
                            )

                    next_value = payload.get("next_cursor")
                    if next_value is None:
                        break
                    if (
                        not isinstance(next_value, str)
                        or _CURSOR_RE.fullmatch(next_value) is None
                        or next_value == cursor
                        or next_value in seen_cursors
                        or not items
                    ):
                        raise DartReviewApiError(
                            "review corpus API cursor did not advance"
                        )
                    seen_cursors.add(next_value)
                    cursor = next_value
        except DartReviewApiError:
            raise
        except httpx.HTTPError as exc:
            raise DartReviewApiError(
                f"review corpus API request failed ({type(exc).__name__})"
            ) from None

        if expected_population is None or expected_sha256 is None:
            raise DartReviewApiError("review corpus API returned no terminal page")
        if len(records) != expected_population:
            raise DartReviewApiError(
                "review corpus item count does not match population_count"
            )
        local_sha = corpus_sha256(records)
        if not hmac.compare_digest(local_sha, expected_sha256):
            raise DartReviewApiError(
                "review corpus digest does not match the returned items"
            )
        return CorpusSnapshot(
            from_date=from_date,
            to_date=to_date,
            backend_binding_id=self.backend_binding_id,
            population_count=expected_population,
            corpus_sha256=expected_sha256,
            items=tuple(sorted(records, key=_corpus_sort_key)),
        )


def _stable_rank(seed: int, namespace: str, *parts: object) -> str:
    material = "\x1f".join([str(seed), namespace, *(str(part) for part in parts)])
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _company_round_robin_queue(
    rows: Sequence[Mapping[str, object]],
    *,
    seed: int,
    event_type: str,
    revision_status: str,
) -> deque[dict[str, object]]:
    by_company: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        by_company[str(row["company_id"])].append(dict(row))
    companies = sorted(
        by_company,
        key=lambda company_id: (
            _stable_rank(
                seed,
                "company",
                event_type,
                revision_status,
                company_id,
            ),
            company_id,
        ),
    )
    buckets: dict[str, deque[dict[str, object]]] = {}
    for company_id in companies:
        ordered = sorted(
            by_company[company_id],
            key=lambda row: (
                _stable_rank(
                    seed,
                    "row",
                    event_type,
                    revision_status,
                    company_id,
                    row["document_id"],
                    row["event_id"],
                ),
                str(row["document_id"]),
                str(row["event_id"]),
            ),
        )
        buckets[company_id] = deque(ordered)

    result: deque[dict[str, object]] = deque()
    active = deque(companies)
    while active:
        company_id = active.popleft()
        bucket = buckets[company_id]
        result.append(bucket.popleft())
        if bucket:
            active.append(company_id)
    return result


def select_stratified_sample(
    rows: Sequence[Mapping[str, object]],
    *,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    seed: int = 20260724,
) -> list[dict[str, object]]:
    if sample_size < 1 or sample_size > MAX_SAMPLE_SIZE:
        raise DartReviewSampleError(
            f"sample_size must be between 1 and {MAX_SAMPLE_SIZE}"
        )
    if len(rows) < sample_size:
        raise DartReviewSampleError(
            f"insufficient corpus for exact sample: required={sample_size}, actual={len(rows)}"
        )

    seen_documents: set[str] = set()
    strata: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for source_row in rows:
        row = dict(source_row)
        missing = [
            field
            for field in ("document_id", "event_id", "company_id", "event_type", "revision_status")
            if not isinstance(row.get(field), str) or not str(row.get(field))
        ]
        if missing:
            raise DartReviewSampleError(
                "stratified sample input is missing required corpus fields"
            )
        document_id = str(row["document_id"])
        if document_id in seen_documents:
            raise DartReviewSampleError(
                "stratified sample input contains duplicate documents"
            )
        seen_documents.add(document_id)
        status = str(row["revision_status"])
        if status not in REVISION_STATUSES:
            raise DartReviewSampleError(
                "stratified sample input has an invalid revision_status"
            )
        strata[(str(row["event_type"]), status)].append(row)

    ordered_strata = sorted(
        strata,
        key=lambda key: (
            _stable_rank(seed, "stratum", key[0], key[1]),
            key,
        ),
    )
    queues = {
        key: _company_round_robin_queue(
            strata[key],
            seed=seed,
            event_type=key[0],
            revision_status=key[1],
        )
        for key in ordered_strata
    }
    active = deque(ordered_strata)
    selected: list[dict[str, object]] = []
    while active and len(selected) < sample_size:
        key = active.popleft()
        queue = queues[key]
        selected.append(queue.popleft())
        if queue:
            active.append(key)
    if len(selected) != sample_size:
        raise DartReviewSampleError("stratified sampler did not produce the exact size")
    return selected


def _json_file(path: Path, *, label: str) -> tuple[dict[str, object], bytes]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DartReviewSampleError(f"{label} is not a readable UTF-8 JSON object") from exc
    if not isinstance(value, dict):
        raise DartReviewSampleError(f"{label} must contain a JSON object")
    return value, raw


def _int_field(row: Mapping[str, object], field: str) -> int:
    value = row.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise DartReviewSampleError(f"backfill evidence has invalid {field}")
    return value


def _backfill_summary_succeeded(summary: Mapping[str, object]) -> bool:
    required = (
        "official_failed",
        "official_skipped",
        "official_remote_ack_mismatches",
        "official_remote_run_persisted",
        "official_remote_raw_count",
        "official_remote_ack_count",
        "official_remote_failed",
        "official_remote_skipped",
        "official_remote_synced",
        "official_dart_requests",
        "official_dart_fetched",
        "official_dart_accepted",
        "official_dart_errors",
        "official_dart_quota_exhausted",
    )
    try:
        values = {field: _int_field(summary, field) for field in required}
    except DartReviewSampleError:
        return False
    return bool(
        all(value >= 0 for value in values.values())
        and values["official_failed"] == 0
        and values["official_skipped"] == 0
        and values["official_remote_ack_mismatches"] == 0
        and values["official_remote_run_persisted"] == 1
        and values["official_remote_raw_count"]
        == values["official_remote_ack_count"]
        == values["official_dart_accepted"]
        and values["official_remote_failed"] == 0
        and values["official_remote_skipped"] == 0
        and values["official_remote_synced"] >= 1
        and values["official_dart_requests"] >= 1
        and values["official_dart_fetched"]
        >= values["official_dart_accepted"]
        and values["official_dart_errors"] == 0
        and values["official_dart_quota_exhausted"] == 0
    )


def _expected_window_keys(from_date: date, to_date: date) -> list[str]:
    keys: list[str] = []
    cursor = from_date
    while cursor < to_date:
        next_date = cursor + timedelta(days=1)
        keys.append(f"{cursor.isoformat()}:{next_date.isoformat()}")
        cursor = next_date
    return keys


def validate_backfill_evidence(
    *,
    report_path: Path,
    checkpoint_path: Path,
    from_date: date,
    to_date: date,
    population_count: int,
    code_revision: str,
) -> BackfillEvidence:
    validate_sample_date_range(from_date, to_date)
    revision = _code_revision(code_revision)
    report, report_bytes = _json_file(report_path, label="backfill report")
    checkpoint, checkpoint_bytes = _json_file(
        checkpoint_path,
        label="backfill checkpoint",
    )
    if (
        report.get("schema_version") != 1
        or report.get("status") != "succeeded"
        or report.get("dry_run") is not False
        or report.get("range_start") != from_date.isoformat()
        or report.get("range_end_exclusive") != to_date.isoformat()
        or report.get("checkpoint_source") != "mysql_remote"
        or report.get("code_revision") != revision
        or _int_field(report, "windows_total") != (to_date - from_date).days
        or _int_field(report, "windows_remaining") != 0
    ):
        raise DartReviewSampleError(
            "backfill report is not a complete applied range"
        )
    checkpoint_version = _int_field(report, "checkpoint_version")
    if checkpoint_version < 1:
        raise DartReviewSampleError("backfill report has invalid checkpoint_version")

    if checkpoint.get("schema_version") != 1:
        raise DartReviewSampleError("backfill checkpoint schema_version mismatch")
    job = checkpoint.get("job")
    completed = checkpoint.get("completed_windows")
    failed = checkpoint.get("failed_windows")
    if not isinstance(job, dict) or not isinstance(completed, dict) or not isinstance(failed, dict):
        raise DartReviewSampleError("backfill checkpoint structure is invalid")
    if failed:
        raise DartReviewSampleError("backfill checkpoint contains failed windows")

    fingerprint = job.get("fingerprint")
    if not isinstance(fingerprint, str) or _SHA256_RE.fullmatch(fingerprint) is None:
        raise DartReviewSampleError("backfill checkpoint fingerprint is invalid")
    contract = {key: value for key, value in job.items() if key != "fingerprint"}
    contract_sha = hashlib.sha256(
        json.dumps(
            contract,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    sources = contract.get("sources")
    if (
        contract_sha != fingerprint
        or report.get("job_fingerprint") != fingerprint
        or contract.get("code_revision") != revision
        or contract.get("range_start") != from_date.isoformat()
        or contract.get("range_end_exclusive") != to_date.isoformat()
        or contract.get("chunk_days") != 1
        or not isinstance(sources, list)
        or sources != ["dart"]
    ):
        raise DartReviewSampleError(
            "backfill checkpoint job does not match the review corpus"
        )

    expected_keys = _expected_window_keys(from_date, to_date)
    if set(completed) != set(expected_keys):
        raise DartReviewSampleError(
            "backfill checkpoint does not contain every requested one-day window"
        )
    expected_population = 0
    for key in expected_keys:
        result = completed[key]
        if not isinstance(result, dict):
            raise DartReviewSampleError("backfill checkpoint window is invalid")
        start, end = key.split(":", 1)
        idempotency_digest = hashlib.sha256(
            f"{fingerprint}|{key}".encode("utf-8")
        ).hexdigest()[:32]
        expected_idempotency_key = (
            f"official-backfill-v1:{idempotency_digest}"
        )
        summary = result.get("summary")
        if (
            result.get("status") != "succeeded"
            or result.get("code_revision") != revision
            or result.get("window_start") != start
            or result.get("window_end_exclusive") != end
            or not isinstance(result.get("attempt"), int)
            or isinstance(result.get("attempt"), bool)
            or int(result["attempt"]) < 1
            or not isinstance(result.get("idempotency_key"), str)
            or result.get("idempotency_key") != expected_idempotency_key
            or not isinstance(summary, dict)
            or not _backfill_summary_succeeded(summary)
        ):
            raise DartReviewSampleError(
                "backfill checkpoint contains an unacknowledged window"
            )
        expected_population += _int_field(summary, "official_dart_accepted")
    if expected_population != population_count:
        raise DartReviewSampleError(
            "backfill DART acknowledgement total does not match the review corpus population"
        )

    checkpoint_sha = hashlib.sha256(checkpoint_bytes).hexdigest()
    return BackfillEvidence(
        code_revision=revision,
        job_fingerprint=fingerprint,
        checkpoint_version=checkpoint_version,
        checkpoint_sha256=checkpoint_sha,
        report_sha256=hashlib.sha256(report_bytes).hexdigest(),
        completed_window_count=len(expected_keys),
        expected_dart_document_count=expected_population,
    )


def _sample_id(row: Mapping[str, object]) -> str:
    digest = hashlib.sha256(
        (
            str(row["document_id"])
            + "\x1f"
            + str(row["event_id"])
        ).encode("utf-8")
    ).hexdigest()[:48]
    return f"dart-review:{digest}"


def build_sample_records(
    selected: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for index, row in enumerate(selected, start=1):
        records.append(
            {
                "schema_version": SAMPLE_SCHEMA_VERSION,
                "sample_id": _sample_id(row),
                "sample_position": index,
                **dict(row),
            }
        )
    return records


def _distribution(rows: Sequence[Mapping[str, object]]) -> dict[str, object]:
    event_types = Counter(str(row["event_type"]) for row in rows)
    revision_statuses = Counter(str(row["revision_status"]) for row in rows)
    companies = Counter(str(row["company_id"]) for row in rows)
    strata = Counter(
        (str(row["event_type"]), str(row["revision_status"])) for row in rows
    )
    return {
        "event_types": dict(sorted(event_types.items())),
        "revision_statuses": dict(sorted(revision_statuses.items())),
        "companies": dict(sorted(companies.items())),
        "company_count": len(companies),
        "strata": [
            {
                "event_type": event_type,
                "revision_status": revision_status,
                "count": count,
            }
            for (event_type, revision_status), count in sorted(strata.items())
        ],
        "stratum_count": len(strata),
    }


def _jsonl_bytes(records: Sequence[Mapping[str, object]]) -> bytes:
    return (
        "".join(
            json.dumps(
                dict(row),
                ensure_ascii=False,
                separators=(",", ":"),
            )
            + "\n"
            for row in records
        )
    ).encode("utf-8")


def _csv_value(value: object) -> object:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        candidate = value.lstrip()
        if value[:1] in {"\t", "\r", "\n"} or candidate[:1] in {
            "=",
            "+",
            "-",
            "@",
        }:
            return "'" + value
    return value


def _csv_bytes(records: Sequence[Mapping[str, object]]) -> bytes:
    buffer = io.StringIO(newline="")
    fields = (
        "schema_version",
        "sample_id",
        "sample_position",
        *CORPUS_ITEM_FIELDS,
        "review_outcome",
        "review_note",
    )
    writer = csv.DictWriter(
        buffer,
        fieldnames=fields,
        lineterminator="\n",
        extrasaction="raise",
    )
    writer.writeheader()
    for record in records:
        writer.writerow(
            {
                **{field: _csv_value(record.get(field)) for field in fields},
                "review_outcome": "",
                "review_note": "",
            }
        )
    return buffer.getvalue().encode("utf-8-sig")


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_bytes(content)
    temporary.replace(path)


def write_review_bundle(
    *,
    snapshot: CorpusSnapshot,
    evidence: BackfillEvidence,
    sample_size: int,
    seed: int,
    code_revision: str,
    jsonl_output: Path,
    csv_output: Path,
    manifest_output: Path,
    generated_at: datetime | None = None,
) -> dict[str, object]:
    revision = _code_revision(code_revision)
    validate_sample_date_range(snapshot.from_date, snapshot.to_date)
    if evidence.completed_window_count != REVIEW_SAMPLE_DAYS:
        raise DartReviewSampleError(
            f"backfill evidence must contain exactly {REVIEW_SAMPLE_DAYS} completed windows"
        )
    if evidence.code_revision != revision:
        raise DartReviewSampleError(
            "backfill evidence code_revision does not match exporter code_revision"
        )
    resolved_outputs = {
        jsonl_output.resolve(),
        csv_output.resolve(),
        manifest_output.resolve(),
    }
    if len(resolved_outputs) != 3:
        raise DartReviewSampleError("review bundle output paths must be distinct")

    selected = select_stratified_sample(
        snapshot.items,
        sample_size=sample_size,
        seed=seed,
    )
    records = build_sample_records(selected)
    jsonl_content = _jsonl_bytes(records)
    csv_content = _csv_bytes(records)
    now = generated_at or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise DartReviewSampleError("generated_at must include a timezone")
    generated = (
        now.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
    manifest: dict[str, object] = {
        "schema_version": SAMPLE_SCHEMA_VERSION,
        "contract_version": SAMPLE_CONTRACT_VERSION,
        "release_eligible": False,
        "reason": "operational sample requires independent human review",
        "generated_at": generated,
        "code_revision": revision,
        "backend_binding_id": snapshot.backend_binding_id,
        "range": {
            "from": snapshot.from_date.isoformat(),
            "to": snapshot.to_date.isoformat(),
            "semantics": "half_open",
        },
        "seed": seed,
        "algorithm": {
            "name": "event-type-revision-status-company-round-robin",
            "version": 1,
            "outer_strata": ["event_type", "revision_status"],
            "inner_rotation": "company_id",
            "tie_breaker": "sha256(seed,namespace,stable_ids)",
        },
        "population_count": snapshot.population_count,
        "sample_size_requested": sample_size,
        "sample_count": len(records),
        "corpus_sha256": snapshot.corpus_sha256,
        "sample_records_sha256": corpus_sha256(records),
        "backfill_evidence": {
            "job_fingerprint": evidence.job_fingerprint,
            "checkpoint_version": evidence.checkpoint_version,
            "checkpoint_sha256": evidence.checkpoint_sha256,
            "report_sha256": evidence.report_sha256,
            "completed_window_count": evidence.completed_window_count,
            "expected_dart_document_count": evidence.expected_dart_document_count,
        },
        "population_distribution": _distribution(snapshot.items),
        "sample_distribution": _distribution(records),
        "files": {
            "jsonl": {
                "sha256": hashlib.sha256(jsonl_content).hexdigest(),
                "bytes": len(jsonl_content),
            },
            "csv": {
                "sha256": hashlib.sha256(csv_content).hexdigest(),
                "bytes": len(csv_content),
                "encoding": "utf-8-sig",
            },
        },
    }
    manifest_content = (
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    _atomic_write(jsonl_output, jsonl_content)
    _atomic_write(csv_output, csv_content)
    _atomic_write(manifest_output, manifest_content)
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Export and deterministically stratify an exact human-review sample "
            "from the authoritative DART governance corpus."
        )
    )
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
    )
    parser.add_argument("--seed", type=int, default=20260724)
    parser.add_argument(
        "--api-base-url",
        default=(
            os.environ.get("BSIDE_API_BASE_URL", "").strip()
            or os.environ.get("GOVERNANCE_API_BASE_URL", "").strip()
        ),
    )
    parser.add_argument(
        "--ops-token",
        default=os.environ.get("BSIDE_OPS_TOKEN", "").strip(),
    )
    parser.add_argument(
        "--backend-binding-id",
        default=os.environ.get("BSIDE_BACKEND_BINDING_ID", "").strip(),
    )
    parser.add_argument("--backfill-report", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--jsonl-output", type=Path, required=True)
    parser.add_argument("--csv-output", type=Path, required=True)
    parser.add_argument("--manifest-output", type=Path, required=True)
    parser.add_argument(
        "--code-revision",
        default=(
            os.environ.get("GITHUB_SHA", "").strip()
            or os.environ.get("CURATOR_CODE_REVISION", "").strip()
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        from_date = _parse_date(args.from_date, field="from_date")
        to_date = _parse_date(args.to_date, field="to_date")
        validate_sample_date_range(from_date, to_date)
        code_revision = _code_revision(args.code_revision)
        client = DartReviewCorpusClient(
            base_url=args.api_base_url,
            token=args.ops_token,
            backend_binding_id=args.backend_binding_id,
        )
        snapshot = client.fetch(from_date=from_date, to_date=to_date)
        evidence = validate_backfill_evidence(
            report_path=args.backfill_report,
            checkpoint_path=args.checkpoint,
            from_date=from_date,
            to_date=to_date,
            population_count=snapshot.population_count,
            code_revision=code_revision,
        )
        manifest = write_review_bundle(
            snapshot=snapshot,
            evidence=evidence,
            sample_size=args.sample_size,
            seed=args.seed,
            code_revision=code_revision,
            jsonl_output=args.jsonl_output,
            csv_output=args.csv_output,
            manifest_output=args.manifest_output,
        )
    except (DartReviewSampleError, OSError, ValueError) as exc:
        print(
            json.dumps(
                {"status": "failed", "error": str(exc)},
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(
        json.dumps(
            {
                "status": "generated",
                "sample_count": manifest["sample_count"],
                "population_count": manifest["population_count"],
                "corpus_sha256": manifest["corpus_sha256"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
