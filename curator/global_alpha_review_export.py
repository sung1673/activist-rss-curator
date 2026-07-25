from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Mapping, Sequence
from urllib.parse import quote, urlsplit, urlunsplit

import httpx


SCHEMA_VERSION = 1
EXPORT_KIND = "bside-global-alpha-review-candidate-export"
EVIDENCE_SOURCE = "production_preview_api_v2"
DEFAULT_EVENT_COUNT = 60
DEFAULT_PAIR_COUNT = 120
DEFAULT_MAX_EVENTS = 500
MAX_RESPONSE_BYTES = 250_000
MAX_EXPORT_BYTES = 1_000_000
OFFICIAL_SOURCE_CLASSES = frozenset(
    {
        "official_disclosure",
        "official_register",
        "company_statement",
        "official_issuer",
    }
)
TELEGRAM_HOSTS = frozenset({"t.me", "telegram.me", "telegram.org"})
EVENT_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.:\-]{1,96}$")
SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
FORBIDDEN_REVIEW_KEYS = frozenset(
    {
        "decision",
        "ground_truth",
        "ground_truth_source",
        "human_attestation",
        "label",
        "reviewed_at",
        "reviewer_reference",
        "reviewer_type",
    }
)


class ReviewExportError(ValueError):
    """Raised when a review export cannot be tied to safe production data."""


@dataclass(frozen=True)
class ReviewExportConfig:
    api_base_url: str
    preview_token: str
    expected_revision: str
    event_count: int = DEFAULT_EVENT_COUNT
    pair_count: int = DEFAULT_PAIR_COUNT
    max_events: int = DEFAULT_MAX_EVENTS


@dataclass(frozen=True)
class OfficialDocument:
    document_id: str
    title: str
    original_url: str


@dataclass(frozen=True)
class ReviewEvent:
    event_id: str
    issuer_id: str
    issuer_name: str
    country: str
    event_family: str
    importance: str
    verification_status: str
    title: str
    documents: tuple[OfficialDocument, ...]


def _text(value: object, field: str, location: str) -> str:
    result = value.strip() if isinstance(value, str) else ""
    if not result:
        raise ReviewExportError(f"{location}: {field} must be a non-empty string")
    if any(ord(character) < 32 for character in result):
        raise ReviewExportError(f"{location}: {field} contains control characters")
    return result


def _mapping(value: object, location: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ReviewExportError(f"{location}: expected an object")
    return dict(value)


def _list(value: object, location: str) -> list[object]:
    if not isinstance(value, list):
        raise ReviewExportError(f"{location}: expected an array")
    return value


def _revision(value: object, location: str) -> str:
    result = _text(value, "code_revision", location).casefold()
    if SHA_PATTERN.fullmatch(result) is None:
        raise ReviewExportError(
            f"{location}: code_revision must be a full 40-character Git SHA"
        )
    return result


def _iso_time(value: object, field: str, location: str) -> datetime:
    raw = _text(value, field, location)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReviewExportError(f"{location}: {field} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ReviewExportError(f"{location}: {field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def normalize_api_base(value: str) -> str:
    raw = value.strip().rstrip("/")
    if not raw:
        raise ReviewExportError("invalid_api_base_url")
    parsed = urlsplit(raw)
    if (
        parsed.scheme.casefold() != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ReviewExportError("invalid_api_base_url")
    path = parsed.path.rstrip("/")
    if path.endswith("/api/v1"):
        path = path[: -len("/api/v1")] + "/api/v2"
    elif not path.endswith("/api/v2"):
        path += "/api/v2"
    return urlunsplit(("https", parsed.netloc, path, "", ""))


def config_from_environment(
    values: Mapping[str, str] | None = None,
) -> ReviewExportConfig:
    source = dict(os.environ if values is None else values)
    base = (
        source.get("BSIDE_API_BASE_URL")
        or source.get("GOVERNANCE_API_BASE_URL")
        or ""
    )
    token = str(source.get("GOVERNANCE_PREVIEW_TOKEN") or "").strip()
    if not token:
        raise ReviewExportError("missing_preview_token")
    revision = _revision(source.get("GITHUB_SHA"), "environment")
    return ReviewExportConfig(
        api_base_url=normalize_api_base(base),
        preview_token=token,
        expected_revision=revision,
    )


def _strict_json(response: httpx.Response, location: str) -> dict[str, object]:
    if 300 <= response.status_code < 400:
        raise ReviewExportError(f"{location}: redirects are forbidden")
    if response.status_code != 200:
        raise ReviewExportError(
            f"{location}: unexpected HTTP status {response.status_code}"
        )
    content_length = response.headers.get("content-length", "").strip()
    if content_length.isdigit() and int(content_length) > MAX_RESPONSE_BYTES:
        raise ReviewExportError(f"{location}: response exceeds the API budget")
    if len(response.content) > MAX_RESPONSE_BYTES:
        raise ReviewExportError(f"{location}: response exceeds the API budget")
    try:
        text = response.content.decode("utf-8")
        value = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReviewExportError(f"{location}: invalid JSON response") from exc
    payload = _mapping(value, location)
    if payload.get("ok") is not True or payload.get("api_version") != "v2":
        raise ReviewExportError(f"{location}: successful API v2 response required")
    return payload


def _get_json(
    client: httpx.Client,
    config: ReviewExportConfig,
    path: str,
    *,
    params: Mapping[str, str | int] | None = None,
    public: bool = True,
) -> dict[str, object]:
    headers = {"Accept": "application/json"}
    if public:
        headers["Authorization"] = f"Bearer {config.preview_token}"
    try:
        response = client.get(
            config.api_base_url + path,
            params=params,
            headers=headers,
        )
    except httpx.HTTPError as exc:
        raise ReviewExportError(f"{path}: API request failed") from exc
    return _strict_json(response, path)


def _health_revision(
    client: httpx.Client,
    config: ReviewExportConfig,
) -> datetime:
    payload = _get_json(client, config, "/health", public=False)
    if payload.get("service") != "bside-global-market-terminal":
        raise ReviewExportError("/health: unexpected service")
    if _revision(payload.get("code_revision"), "/health") != config.expected_revision:
        raise ReviewExportError("/health: code_revision mismatch")
    return _iso_time(payload.get("time"), "time", "/health")


def _event_rows(
    client: httpx.Client,
    config: ReviewExportConfig,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    seen: set[str] = set()
    offset = 0
    while len(rows) < config.max_events:
        payload = _get_json(
            client,
            config,
            "/events",
            params={"limit": 100, "offset": offset},
        )
        data = _mapping(payload.get("data"), "/events.data")
        items = _list(data.get("items"), "/events.data.items")
        meta = _mapping(payload.get("meta"), "/events.meta")
        returned = meta.get("returned")
        has_more = meta.get("has_more")
        next_offset = meta.get("next_offset")
        if (
            isinstance(returned, bool)
            or not isinstance(returned, int)
            or returned != len(items)
            or not isinstance(has_more, bool)
        ):
            raise ReviewExportError("/events: invalid pagination contract")
        for index, raw_item in enumerate(items):
            item = _mapping(raw_item, f"/events.items[{index}]")
            event_id = _text(
                item.get("event_id"),
                "event_id",
                f"/events.items[{index}]",
            )
            if EVENT_ID_PATTERN.fullmatch(event_id) is None:
                raise ReviewExportError("/events: invalid event_id")
            if event_id in seen:
                raise ReviewExportError("/events: duplicate event_id across pages")
            official_count = item.get("official_evidence_count")
            if (
                isinstance(official_count, bool)
                or not isinstance(official_count, int)
                or official_count < 1
            ):
                raise ReviewExportError(
                    f"/events.items[{index}]: official evidence is required"
                )
            seen.add(event_id)
            rows.append(item)
            if len(rows) >= config.max_events:
                break
        if len(rows) >= config.max_events or not has_more:
            break
        if (
            not items
            or isinstance(next_offset, bool)
            or not isinstance(next_offset, int)
            or next_offset <= offset
        ):
            raise ReviewExportError("/events: invalid continuation")
        offset = next_offset
    return rows


def _official_url(value: object, location: str) -> str:
    url = _text(value, "original_url", location)
    parsed = urlsplit(url)
    host = (parsed.hostname or "").casefold().rstrip(".")
    if (
        parsed.scheme.casefold() != "https"
        or not host
        or parsed.username is not None
        or parsed.password is not None
        or any(host == blocked or host.endswith("." + blocked) for blocked in TELEGRAM_HOSTS)
    ):
        raise ReviewExportError(f"{location}: official HTTPS URL required")
    return url


def _same_public_identity(
    summary: Mapping[str, object],
    detail: Mapping[str, object],
    field: str,
    location: str,
) -> str:
    summary_value = _text(summary.get(field), field, location)
    detail_value = _text(detail.get(field), field, location)
    if summary_value != detail_value:
        raise ReviewExportError(f"{location}: event changed during export")
    return detail_value


def _review_event(
    client: httpx.Client,
    config: ReviewExportConfig,
    summary: Mapping[str, object],
) -> ReviewEvent:
    event_id = _text(summary.get("event_id"), "event_id", "event-summary")
    payload = _get_json(
        client,
        config,
        "/events/" + quote(event_id, safe=""),
    )
    data = _mapping(payload.get("data"), f"/events/{event_id}.data")
    detail = _mapping(data.get("event"), f"/events/{event_id}.data.event")
    if _text(detail.get("event_id"), "event_id", "event-detail") != event_id:
        raise ReviewExportError(f"/events/{event_id}: event_id mismatch")
    issuer_id = _same_public_identity(
        summary,
        detail,
        "issuer_id",
        f"/events/{event_id}",
    )
    issuer_name = _same_public_identity(
        summary,
        detail,
        "issuer_name",
        f"/events/{event_id}",
    )
    country = _same_public_identity(
        summary,
        detail,
        "country",
        f"/events/{event_id}",
    )
    event_family = _same_public_identity(
        summary,
        detail,
        "event_family",
        f"/events/{event_id}",
    )
    importance = _same_public_identity(
        summary,
        detail,
        "importance",
        f"/events/{event_id}",
    )
    verification_status = _same_public_identity(
        summary,
        detail,
        "verification_status",
        f"/events/{event_id}",
    )
    title = _same_public_identity(
        summary,
        detail,
        "title",
        f"/events/{event_id}",
    )
    documents: list[OfficialDocument] = []
    seen_documents: set[str] = set()
    for index, raw_document in enumerate(
        _list(data.get("documents"), f"/events/{event_id}.data.documents")
    ):
        location = f"/events/{event_id}.data.documents[{index}]"
        document = _mapping(raw_document, location)
        source_class = _text(document.get("source_class"), "source_class", location)
        raw_url = str(document.get("original_url") or "")
        if "telegram" in source_class.casefold():
            raise ReviewExportError(f"{location}: Telegram source exposure")
        parsed_host = (urlsplit(raw_url).hostname or "").casefold().rstrip(".")
        if any(
            parsed_host == blocked or parsed_host.endswith("." + blocked)
            for blocked in TELEGRAM_HOSTS
        ):
            raise ReviewExportError(f"{location}: Telegram URL exposure")
        if source_class not in OFFICIAL_SOURCE_CLASSES:
            continue
        document_id = _text(document.get("document_id"), "document_id", location)
        if document_id in seen_documents:
            raise ReviewExportError(f"{location}: duplicate document_id")
        seen_documents.add(document_id)
        documents.append(
            OfficialDocument(
                document_id=document_id,
                title=_text(document.get("title"), "title", location),
                original_url=_official_url(document.get("original_url"), location),
            )
        )
    if not documents:
        raise ReviewExportError(f"/events/{event_id}: official document required")
    documents.sort(key=lambda item: (item.document_id, item.original_url))
    return ReviewEvent(
        event_id=event_id,
        issuer_id=issuer_id,
        issuer_name=issuer_name,
        country=country,
        event_family=event_family,
        importance=importance,
        verification_status=verification_status,
        title=title,
        documents=tuple(documents),
    )


def _round_robin_events(events: Iterable[ReviewEvent]) -> list[ReviewEvent]:
    groups: dict[tuple[str, str, str], list[ReviewEvent]] = defaultdict(list)
    for event in events:
        groups[(event.country, event.event_family, event.importance)].append(event)
    queues = [
        sorted(group, key=lambda event: event.event_id)
        for _, group in sorted(groups.items())
    ]
    result: list[ReviewEvent] = []
    while queues:
        remaining: list[list[ReviewEvent]] = []
        for queue in queues:
            if queue:
                result.append(queue.pop(0))
            if queue:
                remaining.append(queue)
        queues = remaining
    return result


def _summary_order(
    rows: Sequence[dict[str, object]],
    top_event_ids: Sequence[str],
) -> list[dict[str, object]]:
    by_id = {
        _text(item.get("event_id"), "event_id", "event-summary"): item
        for item in rows
    }
    missing = [event_id for event_id in top_event_ids if event_id not in by_id]
    if missing:
        raise ReviewExportError(
            "/briefs/latest: Top 5 event missing from paginated /events"
        )
    groups: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    for item in rows:
        groups[
            (
                _text(item.get("country"), "country", "event-summary"),
                _text(item.get("event_family"), "event_family", "event-summary"),
                _text(item.get("importance"), "importance", "event-summary"),
            )
        ].append(item)
    queues = [
        sorted(
            group,
            key=lambda item: _text(
                item.get("event_id"),
                "event_id",
                "event-summary",
            ),
        )
        for _, group in sorted(groups.items())
    ]
    diversified: list[dict[str, object]] = []
    while queues:
        remaining: list[list[dict[str, object]]] = []
        for queue in queues:
            if queue:
                diversified.append(queue.pop(0))
            if queue:
                remaining.append(queue)
        queues = remaining
    top_set = set(top_event_ids)
    return [by_id[event_id] for event_id in top_event_ids] + [
        item
        for item in diversified
        if str(item.get("event_id") or "") not in top_set
    ]


def _pair_identity(
    stratum: str,
    left: OfficialDocument,
    right: OfficialDocument,
) -> tuple[tuple[str, str], str]:
    first, second = sorted((left.document_id, right.document_id))
    document_pair = (first, second)
    digest = hashlib.sha256(
        (stratum + "\x1f" + "\x1f".join(document_pair)).encode("utf-8")
    ).hexdigest()
    return document_pair, "pair:" + digest[:40]


def _pair_candidate(
    stratum: str,
    left: OfficialDocument,
    right: OfficialDocument,
    used_pairs: set[tuple[str, str]],
) -> dict[str, object] | None:
    document_pair, pair_id = _pair_identity(stratum, left, right)
    if left.document_id == right.document_id or document_pair in used_pairs:
        return None
    used_pairs.add(document_pair)
    if right.document_id < left.document_id:
        left, right = right, left
    return {
        "pair_id": pair_id,
        "left_document_id": left.document_id,
        "right_document_id": right.document_id,
        "left_title": left.title,
        "right_title": right.title,
        "left_url": left.original_url,
        "right_url": right.original_url,
        "stratum": stratum,
    }


def _take_pair_generators(
    generators: Sequence[Iterator[tuple[OfficialDocument, OfficialDocument]]],
    *,
    stratum: str,
    count: int,
    used_pairs: set[tuple[str, str]],
) -> list[dict[str, object]]:
    active = list(generators)
    result: list[dict[str, object]] = []
    while active and len(result) < count:
        remaining: list[Iterator[tuple[OfficialDocument, OfficialDocument]]] = []
        for generator in active:
            if len(result) >= count:
                remaining.append(generator)
                continue
            try:
                left, right = next(generator)
            except StopIteration:
                continue
            candidate = _pair_candidate(stratum, left, right, used_pairs)
            if candidate is not None:
                result.append(candidate)
            remaining.append(generator)
        active = remaining
    if len(result) != count:
        raise ReviewExportError(
            f"pair-candidates: insufficient {stratum} candidates "
            f"(required={count}, available={len(result)})"
        )
    return result


def _predicted_same_pairs(
    events: Sequence[ReviewEvent],
    *,
    count: int,
    used_pairs: set[tuple[str, str]],
) -> list[dict[str, object]]:
    generators = [
        iter(itertools.combinations(event.documents, 2))
        for event in sorted(events, key=lambda item: item.event_id)
        if len(event.documents) >= 2
    ]
    return _take_pair_generators(
        generators,
        stratum="predicted_same",
        count=count,
        used_pairs=used_pairs,
    )


def _hard_negative_pairs(
    events: Sequence[ReviewEvent],
    *,
    count: int,
    used_pairs: set[tuple[str, str]],
) -> list[dict[str, object]]:
    groups: dict[tuple[str, str], list[ReviewEvent]] = defaultdict(list)
    for event in events:
        groups[(event.issuer_id, event.event_family)].append(event)

    def document_pairs(
        group: Sequence[ReviewEvent],
    ) -> Iterator[tuple[OfficialDocument, OfficialDocument]]:
        for left, right in itertools.combinations(
            sorted(group, key=lambda item: item.event_id),
            2,
        ):
            yield left.documents[0], right.documents[0]

    generators = [
        document_pairs(group)
        for _, group in sorted(groups.items())
        if len(group) >= 2
    ]
    return _take_pair_generators(
        generators,
        stratum="hard_negative",
        count=count,
        used_pairs=used_pairs,
    )


def _easy_negative_pairs(
    events: Sequence[ReviewEvent],
    *,
    count: int,
    used_pairs: set[tuple[str, str]],
    revision: str,
) -> list[dict[str, object]]:
    ordered = sorted(
        events,
        key=lambda event: hashlib.sha256(
            (revision + "\x1f" + event.event_id).encode("utf-8")
        ).hexdigest(),
    )

    def document_pairs() -> Iterator[tuple[OfficialDocument, OfficialDocument]]:
        for left, right in itertools.combinations(ordered, 2):
            if (
                left.issuer_id == right.issuer_id
                or left.event_family == right.event_family
            ):
                continue
            yield left.documents[0], right.documents[0]

    return _take_pair_generators(
        [document_pairs()],
        stratum="easy_negative",
        count=count,
        used_pairs=used_pairs,
    )


def _pair_quotas(total: int) -> tuple[int, int, int]:
    base, remainder = divmod(total, 3)
    quotas = [base, base, base]
    for index in range(remainder):
        quotas[index] += 1
    return quotas[0], quotas[1], quotas[2]


def _build_pairs(
    events: Sequence[ReviewEvent],
    *,
    count: int,
    revision: str,
) -> list[dict[str, object]]:
    predicted_count, hard_count, easy_count = _pair_quotas(count)
    used_pairs: set[tuple[str, str]] = set()
    pairs = [
        *_predicted_same_pairs(
            events,
            count=predicted_count,
            used_pairs=used_pairs,
        ),
        *_hard_negative_pairs(
            events,
            count=hard_count,
            used_pairs=used_pairs,
        ),
        *_easy_negative_pairs(
            events,
            count=easy_count,
            used_pairs=used_pairs,
            revision=revision,
        ),
    ]
    if len(pairs) != count or len(used_pairs) != count:
        raise ReviewExportError("pair-candidates: duplicate or incomplete selection")
    return sorted(pairs, key=lambda item: (str(item["stratum"]), str(item["pair_id"])))


def _latest_brief(
    client: httpx.Client,
    config: ReviewExportConfig,
) -> tuple[str, list[dict[str, object]]]:
    payload = _get_json(
        client,
        config,
        "/briefs/latest",
        params={"edition": "global"},
    )
    data = _mapping(payload.get("data"), "/briefs/latest.data")
    brief_id = _text(data.get("brief_id"), "brief_id", "/briefs/latest.data")
    if data.get("edition") != "global":
        raise ReviewExportError("/briefs/latest: global edition required")
    if _revision(data.get("build_sha"), "/briefs/latest.data") != config.expected_revision:
        raise ReviewExportError("/briefs/latest: build_sha mismatch")
    if data.get("stale") is not False or data.get("empty_reason") is not None:
        raise ReviewExportError("/briefs/latest: current approved Top 5 required")
    raw_top = _list(data.get("top"), "/briefs/latest.data.top")
    if len(raw_top) != 5:
        raise ReviewExportError("/briefs/latest: exactly five Top items required")
    top: list[dict[str, object]] = []
    seen: set[str] = set()
    for index, raw_item in enumerate(raw_top, start=1):
        item = _mapping(raw_item, f"/briefs/latest.data.top[{index - 1}]")
        event_id = _text(
            item.get("event_id"),
            "event_id",
            f"/briefs/latest.data.top[{index - 1}]",
        )
        if EVENT_ID_PATTERN.fullmatch(event_id) is None or event_id in seen:
            raise ReviewExportError("/briefs/latest: invalid or duplicate Top event")
        seen.add(event_id)
        top.append(
            {
                "edition_id": brief_id,
                "event_id": event_id,
                "position_no": index,
                "title": _text(
                    item.get("title"),
                    "title",
                    f"/briefs/latest.data.top[{index - 1}]",
                ),
            }
        )
    return brief_id, top


def _event_candidate(event: ReviewEvent) -> dict[str, object]:
    return {
        "event_id": event.event_id,
        "title": event.title,
        "issuer_name": event.issuer_name,
        "country": event.country,
        "event_family": event.event_family,
        "importance": event.importance,
        "verification_status": event.verification_status,
        "official_document_ids": [
            document.document_id for document in event.documents
        ],
        "official_urls": sorted(
            {document.original_url for document in event.documents}
        ),
    }


def _assert_no_human_labels(value: object, location: str = "export") -> None:
    if isinstance(value, dict):
        forbidden = set(value) & FORBIDDEN_REVIEW_KEYS
        if forbidden:
            raise ReviewExportError(
                f"{location}: human label fields are forbidden: {sorted(forbidden)}"
            )
        for key, item in value.items():
            _assert_no_human_labels(item, f"{location}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_human_labels(item, f"{location}[{index}]")


def export_review_candidates(
    config: ReviewExportConfig,
    *,
    client: httpx.Client | None = None,
) -> dict[str, object]:
    revision = _revision(config.expected_revision, "configuration")
    preview_token = _text(
        config.preview_token,
        "preview_token",
        "configuration",
    )
    if config.event_count < DEFAULT_EVENT_COUNT:
        raise ReviewExportError("configuration: at least 60 event candidates required")
    if config.pair_count < DEFAULT_PAIR_COUNT:
        raise ReviewExportError("configuration: at least 120 pair candidates required")
    if config.max_events < config.event_count or config.max_events > 10_000:
        raise ReviewExportError("configuration: invalid max_events")
    owns_client = client is None
    http = client or httpx.Client(
        timeout=httpx.Timeout(30.0),
        follow_redirects=False,
    )
    try:
        _health_revision(http, config)
        brief_id, top = _latest_brief(http, config)
        rows = _event_rows(http, config)
        top_ids = [str(item["event_id"]) for item in top]
        ordered_summaries = _summary_order(rows, top_ids)
        review_events: list[ReviewEvent] = []
        for summary in ordered_summaries:
            review_events.append(_review_event(http, config, summary))
        if len(review_events) < config.event_count:
            raise ReviewExportError(
                "event-candidates: insufficient official-evidence events"
            )
        selected_events = _round_robin_events(review_events)[: config.event_count]
        pairs = _build_pairs(
            review_events,
            count=config.pair_count,
            revision=revision,
        )
        event_by_id = {event.event_id: event for event in review_events}
        top_candidates: list[dict[str, object]] = []
        for item in top:
            event_id = str(item["event_id"])
            event = event_by_id.get(event_id)
            if event is None:
                raise ReviewExportError("/briefs/latest: Top event detail missing")
            top_candidates.append(
                {
                    **item,
                    "official_url": event.documents[0].original_url,
                }
            )
        final_brief_id, final_top = _latest_brief(http, config)
        if final_brief_id != brief_id or final_top != top:
            raise ReviewExportError(
                "/briefs/latest: Top 5 changed during export"
            )
        final_health = _health_revision(http, config)
        export = {
            "schema_version": SCHEMA_VERSION,
            "kind": EXPORT_KIND,
            "environment": "production",
            "evidence_source": EVIDENCE_SOURCE,
            "is_synthetic": False,
            "code_revision": revision,
            "collected_at": final_health.isoformat(),
            "event_candidates": [
                _event_candidate(event) for event in selected_events
            ],
            "same_event_pair_candidates": pairs,
            "top5_candidates": top_candidates,
        }
        _assert_no_human_labels(export)
        encoded = json.dumps(
            export,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        if preview_token.encode("utf-8") in encoded:
            raise ReviewExportError("export: preview token exposure")
        if len(encoded) > MAX_EXPORT_BYTES:
            raise ReviewExportError("export: review candidate output is too large")
        return export
    finally:
        if owns_client:
            http.close()


def write_export(path: Path, value: Mapping[str, object]) -> None:
    if path.exists():
        raise ReviewExportError(f"output already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export same-SHA Production Alpha human review candidates",
    )
    parser.add_argument(
        "--api-base-url",
        default=os.environ.get("BSIDE_API_BASE_URL")
        or os.environ.get("GOVERNANCE_API_BASE_URL"),
    )
    parser.add_argument(
        "--preview-token",
        default=os.environ.get("GOVERNANCE_PREVIEW_TOKEN"),
    )
    parser.add_argument(
        "--expected-revision",
        default=os.environ.get("GITHUB_SHA"),
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--event-count", type=int, default=DEFAULT_EVENT_COUNT)
    parser.add_argument("--pair-count", type=int, default=DEFAULT_PAIR_COUNT)
    parser.add_argument("--max-events", type=int, default=DEFAULT_MAX_EVENTS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        config = ReviewExportConfig(
            api_base_url=normalize_api_base(str(args.api_base_url or "")),
            preview_token=_text(
                args.preview_token,
                "preview_token",
                "configuration",
            ),
            expected_revision=_revision(
                args.expected_revision,
                "configuration",
            ),
            event_count=args.event_count,
            pair_count=args.pair_count,
            max_events=args.max_events,
        )
        export = export_review_candidates(config)
        write_export(args.output, export)
    except ReviewExportError as exc:
        print(f"Global Alpha review export failed: {exc}")
        return 1
    digest = hashlib.sha256(
        json.dumps(
            export,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    print(
        "Global Alpha review candidates exported "
        f"events={len(_list(export.get('event_candidates'), 'export.event_candidates'))} "
        f"pairs={len(_list(export.get('same_event_pair_candidates'), 'export.same_event_pair_candidates'))} "
        f"top5={len(_list(export.get('top5_candidates'), 'export.top5_candidates'))} "
        f"sha256={digest}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
