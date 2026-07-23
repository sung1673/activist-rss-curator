from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from zoneinfo import ZoneInfo


COMPARISON_KEY_RE = re.compile(r"^eventcmp:v1:[0-9a-f]{64}$")
REVISION_RE = re.compile(r"^[0-9a-f]{40}$")
ENGINES = {"legacy", "candidate"}
EVIDENCE_KINDS = {"canonical_url", "document_id", "official_receipt"}
DAY_DELTA_SCOPE = "kst_observation_day_delta_v1"
UNBOUNDED_SCOPE = "producer_supplied_unbounded_v1"
RECORD_SCOPES = {DAY_DELTA_SCOPE, UNBOUNDED_SCOPE}
LEGACY_RECORD_ID_FIELDS = ("guid", "cluster_guid", "story_id", "record_id", "cluster_key")
NESTED_EVIDENCE_FIELDS = ("articles", "documents", "observations", "evidence", "links")
TRACKING_QUERY_FIELDS = {
    "fbclid",
    "gclid",
    "utm_campaign",
    "utm_content",
    "utm_medium",
    "utm_source",
    "utm_term",
}
KST = ZoneInfo("Asia/Seoul")


class ShadowEngineError(ValueError):
    """Raised when an engine output cannot become immutable shadow evidence."""


def canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def comparison_keys_sha256(keys: Sequence[str]) -> str:
    """Match PHP's sha256(json_encode(sorted comparison key list))."""

    return hashlib.sha256(
        json.dumps(list(keys), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _safe_identifier(value: object, location: str) -> str:
    text = str(value or "").strip()
    if not text or len(text) > 191 or re.search(r"[\x00-\x1f\x7f]", text):
        raise ShadowEngineError(f"{location} is missing or invalid")
    return text.casefold()


def _safe_url(value: object) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = urlsplit(raw)
        port = parsed.port
    except (TypeError, ValueError):
        return None
    if (
        parsed.scheme.casefold() not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
    ):
        return None
    scheme = parsed.scheme.casefold()
    hostname = str(parsed.hostname).casefold()
    if ":" in hostname and not hostname.startswith("["):
        hostname = f"[{hostname}]"
    if port is not None and not (
        (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    ):
        netloc = f"{hostname}:{port}"
    else:
        netloc = hostname
    path = parsed.path.rstrip("/") if parsed.path != "/" else ""
    query = urlencode(
        sorted(
            (key, item)
            for key, item in parse_qsl(parsed.query, keep_blank_values=True)
            if key.casefold() not in TRACKING_QUERY_FIELDS
        ),
        doseq=True,
    )
    normalized = urlunsplit((scheme, netloc, path, query, ""))
    if len(normalized) > 2048:
        return None
    return normalized


def _receipt_from_document_id(document_id: str) -> tuple[str, str] | None:
    source, separator, receipt = document_id.partition(":")
    source = source.casefold()
    receipt = receipt.strip().casefold()
    if separator and source in {"dart", "kind"} and receipt:
        return source, receipt
    return None


def _receipts_from_url(url: str) -> set[tuple[str, str]]:
    parsed = urlsplit(url)
    host = (parsed.hostname or "").casefold()
    params = {key.casefold(): value.strip().casefold() for key, value in parse_qsl(parsed.query)}
    receipts: set[tuple[str, str]] = set()
    dart_receipt = params.get("rcpno", "")
    if host in {"dart.fss.or.kr", "opendart.fss.or.kr"} and re.fullmatch(
        r"\d{14}", dart_receipt
    ):
        receipts.add(("dart", dart_receipt))
    kind_receipt = params.get("acptno", "")
    if host == "kind.krx.co.kr" and re.fullmatch(
        r"[a-z0-9][a-z0-9_-]{0,180}", kind_receipt
    ):
        receipts.add(("kind", kind_receipt))
    return receipts


def stable_source_evidence(record: Mapping[str, object]) -> list[dict[str, str]]:
    """Return exact source identifiers without using title or theme similarity.

    Evidence is deliberately narrow: an explicit document identifier, an
    official DART/KIND receipt, or a canonical HTTP(S) URL.  Known nested
    evidence containers are traversed, but arbitrary text is never inspected.
    """

    evidence: set[tuple[str, str]] = set()

    def add_document_id(value: object, location: str) -> None:
        document_id = _safe_identifier(value, location)
        evidence.add(("document_id", document_id))
        receipt = _receipt_from_document_id(document_id)
        if receipt is not None:
            evidence.add(("official_receipt", f"{receipt[0]}:{receipt[1]}"))

    def visit(row: Mapping[str, object], location: str) -> None:
        if row.get("document_id") is not None and row.get("document_id") != "":
            add_document_id(row.get("document_id"), f"{location}.document_id")
        document_ids = row.get("document_ids")
        if document_ids is not None and document_ids != "":
            if (
                not isinstance(document_ids, Sequence)
                or isinstance(document_ids, (str, bytes, bytearray))
            ):
                raise ShadowEngineError(f"{location}.document_ids must be an array")
            for index, document_id in enumerate(document_ids):
                add_document_id(document_id, f"{location}.document_ids[{index}]")

        source = str(row.get("source") or row.get("source_key") or "").strip().casefold()
        receipt_fields = (
            ("rcept_no", "dart"),
            ("acptno", "kind"),
            ("receipt_no", source),
            ("external_id", source),
        )
        for field, receipt_source in receipt_fields:
            raw_receipt = str(row.get(field) or "").strip().casefold()
            if not raw_receipt:
                continue
            if receipt_source not in {"dart", "kind"}:
                prefixed_source, separator, prefixed_receipt = raw_receipt.partition(":")
                if separator and prefixed_source in {"dart", "kind"}:
                    receipt_source = prefixed_source
                    raw_receipt = prefixed_receipt
            if receipt_source not in {"dart", "kind"}:
                continue
            if len(raw_receipt) > 181 or re.fullmatch(r"[a-z0-9][a-z0-9_-]*", raw_receipt) is None:
                raise ShadowEngineError(f"{location}.{field} is invalid")
            evidence.add(("official_receipt", f"{receipt_source}:{raw_receipt}"))
            evidence.add(("document_id", f"{receipt_source}:{raw_receipt}"))

        for field in ("canonical_url", "original_url", "url", "link", "representative_url"):
            normalized = _safe_url(row.get(field))
            if normalized is None:
                continue
            evidence.add(("canonical_url", normalized))
            for receipt_source, receipt in _receipts_from_url(normalized):
                evidence.add(("official_receipt", f"{receipt_source}:{receipt}"))
                evidence.add(("document_id", f"{receipt_source}:{receipt}"))

        for field in NESTED_EVIDENCE_FIELDS:
            children = row.get(field)
            if children is None or children == "":
                continue
            if not isinstance(children, Sequence) or isinstance(
                children, (str, bytes, bytearray)
            ):
                raise ShadowEngineError(f"{location}.{field} must be an array")
            for index, child in enumerate(children):
                if not isinstance(child, Mapping):
                    raise ShadowEngineError(f"{location}.{field}[{index}] must be an object")
                visit(child, f"{location}.{field}[{index}]")

    visit(record, "record")
    return [{"kind": kind, "value": value} for kind, value in sorted(evidence)]


def _legacy_record_id(record: Mapping[str, object], index: int) -> str:
    for field in LEGACY_RECORD_ID_FIELDS:
        if record.get(field) is not None and record.get(field) != "":
            return _safe_identifier(record.get(field), f"records[{index}].{field}")
    raise ShadowEngineError(f"records[{index}] has no stable legacy record identifier")


def _validate_source_evidence(value: object, location: str) -> list[tuple[str, str]]:
    if not isinstance(value, list) or len(value) > 1000:
        raise ShadowEngineError(f"{location} must be an array of at most 1000 entries")
    result: list[tuple[str, str]] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict) or set(item) != {"kind", "value"}:
            raise ShadowEngineError(f"{location}[{index}] is invalid")
        kind = str(item.get("kind") or "").strip().casefold()
        evidence_value = str(item.get("value") or "").strip()
        if kind not in EVIDENCE_KINDS or not evidence_value or len(evidence_value) > 2048:
            raise ShadowEngineError(f"{location}[{index}] is invalid")
        if kind != "canonical_url":
            evidence_value = evidence_value.casefold()
        if item.get("kind") != kind or item.get("value") != evidence_value:
            raise ShadowEngineError(f"{location}[{index}] is not canonical")
        if kind == "canonical_url" and _safe_url(evidence_value) != evidence_value:
            raise ShadowEngineError(f"{location}[{index}] URL is not canonical")
        result.append((kind, evidence_value))
    if result != sorted(set(result)):
        raise ShadowEngineError(f"{location} must be sorted and unique")
    return result


def _full_revision(value: object) -> str:
    revision = str(value or "").strip().casefold()
    if not REVISION_RE.fullmatch(revision):
        raise ShadowEngineError("code_revision must be a full 40-character Git SHA")
    return revision


def _date(value: object, location: str = "observation_date") -> date:
    try:
        parsed = date.fromisoformat(str(value or "").strip())
    except ValueError as exc:
        raise ShadowEngineError(f"{location} must be YYYY-MM-DD") from exc
    if parsed.isoformat() != str(value or "").strip():
        raise ShadowEngineError(f"{location} must be YYYY-MM-DD")
    return parsed


def _utc_timestamp(value: object, location: str) -> str:
    text = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ShadowEngineError(f"{location} must be an offset-aware timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ShadowEngineError(f"{location} must be an offset-aware timestamp")
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def _record_on_day(record: Mapping[str, object], observation_date: date) -> bool:
    for field in ("occurred_at", "published_at", "last_article_at", "created_at"):
        raw = str(record.get(field) or "").strip()
        if not raw:
            continue
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
            try:
                return date.fromisoformat(raw) == observation_date
            except ValueError:
                return False
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            try:
                return date.fromisoformat(raw[:10]) == observation_date
            except ValueError:
                return False
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return False
        return parsed.astimezone(KST).date() == observation_date
    # An output row without a trustworthy observation time cannot be assigned
    # to a KST day. Excluding it is safer than inventing midnight semantics.
    return False


def canonical_keys_from_records(
    records: Iterable[Mapping[str, object]],
    *,
    observation_date: date,
    filter_to_day: bool,
) -> tuple[list[str], int, int]:
    """Extract only explicit canonical keys; never infer one from titles/themes.

    Rows without a ``comparison_key`` are counted as non-canonical engine output.
    A row that claims a complete identity but omits its key is corrupt and stops
    the run. Any present malformed key also stops the run.
    """

    keys: set[str] = set()
    input_count = 0
    noncanonical_count = 0
    for index, record in enumerate(records):
        input_count += 1
        if filter_to_day and not _record_on_day(record, observation_date):
            continue
        raw_key = record.get("comparison_key")
        key = str(raw_key or "").strip().casefold()
        identity_status = str(record.get("identity_status") or "").strip().casefold()
        if not key:
            if identity_status == "complete":
                raise ShadowEngineError(
                    f"records[{index}] has complete identity without comparison_key"
                )
            noncanonical_count += 1
            continue
        if not COMPARISON_KEY_RE.fullmatch(key):
            raise ShadowEngineError(f"records[{index}] has invalid comparison_key")
        keys.add(key)
    return sorted(keys), input_count, noncanonical_count


def _snapshot_records(
    records: Iterable[Mapping[str, object]],
    *,
    engine: str,
    observation_date: date,
    filter_to_day: bool,
) -> tuple[list[dict[str, object]], list[dict[str, object]], int, int]:
    events_by_key: dict[str, set[tuple[str, str]]] = {}
    legacy_by_id: dict[str, dict[str, object]] = {}
    input_count = 0
    noncanonical_count = 0
    for index, record in enumerate(records):
        input_count += 1
        if filter_to_day and not _record_on_day(record, observation_date):
            continue
        key = str(record.get("comparison_key") or "").strip().casefold()
        identity_status = str(record.get("identity_status") or "").strip().casefold()
        if not key:
            if identity_status == "complete":
                raise ShadowEngineError(
                    f"records[{index}] has complete identity without comparison_key"
                )
            noncanonical_count += 1
        elif not COMPARISON_KEY_RE.fullmatch(key):
            raise ShadowEngineError(f"records[{index}] has invalid comparison_key")

        source_evidence = stable_source_evidence(record)
        evidence_set = {
            (str(item["kind"]), str(item["value"])) for item in source_evidence
        }
        if engine == "candidate":
            if key:
                if not evidence_set:
                    raise ShadowEngineError(
                        f"records[{index}] canonical candidate event has no stable source evidence"
                    )
                events_by_key.setdefault(key, set()).update(evidence_set)
            continue

        record_id = _legacy_record_id(record, index)
        legacy_record: dict[str, object] = {
            "legacy_record_id": record_id,
            "comparison_key": key or None,
            "source_evidence": source_evidence,
        }
        previous = legacy_by_id.get(record_id)
        if previous is not None and previous != legacy_record:
            raise ShadowEngineError(
                f"legacy record {record_id!r} has conflicting content in one snapshot"
            )
        legacy_by_id[record_id] = legacy_record
        if key:
            events_by_key.setdefault(key, set()).update(evidence_set)

    events: list[dict[str, object]] = []
    for key in sorted(events_by_key):
        event_evidence: list[dict[str, str]] = [
            {"kind": kind, "value": value}
            for kind, value in sorted(events_by_key[key])
        ]
        events.append(
            {"comparison_key": key, "source_evidence": event_evidence}
        )
    legacy_records = [legacy_by_id[record_id] for record_id in sorted(legacy_by_id)]
    return events, legacy_records, input_count, noncanonical_count


def build_engine_snapshot(
    *,
    engine: str,
    observation_date: date,
    code_revision: str,
    status: str,
    records: Iterable[Mapping[str, object]],
    source_run_id: str,
    producer_run_id: str | None = None,
    generated_at: datetime | None = None,
    filter_to_day: bool = True,
) -> dict[str, object]:
    engine = engine.strip().casefold()
    if engine not in ENGINES:
        raise ShadowEngineError("engine must be legacy or candidate")
    if status not in {"succeeded", "failed"}:
        raise ShadowEngineError("status must be succeeded or failed")
    revision = _full_revision(code_revision)
    run_id = str(source_run_id or "").strip()
    if not run_id or len(run_id) > 191 or not re.fullmatch(r"[A-Za-z0-9_.:@/-]+", run_id):
        raise ShadowEngineError("source_run_id is missing or invalid")
    github_run_id = str(os.environ.get("GITHUB_RUN_ID") or "").strip()
    github_run_attempt = str(os.environ.get("GITHUB_RUN_ATTEMPT") or "1").strip()
    inferred_producer_run_id = (
        f"github:{github_run_id}:{github_run_attempt}" if github_run_id else run_id
    )
    producer_id = str(producer_run_id or inferred_producer_run_id).strip()
    if (
        not producer_id
        or len(producer_id) > 191
        or not re.fullmatch(r"[A-Za-z0-9_.:@/-]+", producer_id)
    ):
        raise ShadowEngineError("producer_run_id is missing or invalid")
    generated = generated_at or datetime.now(timezone.utc)
    if generated.tzinfo is None or generated.utcoffset() is None:
        raise ShadowEngineError("generated_at must be timezone-aware")
    generated_text = generated.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )
    events, legacy_records, input_count, noncanonical_count = _snapshot_records(
        records,
        engine=engine,
        observation_date=observation_date,
        filter_to_day=filter_to_day,
    )
    keys = [str(event["comparison_key"]) for event in events]
    payload: dict[str, object] = {
        "schema_version": 3,
        "engine": engine,
        "observation_date": observation_date.isoformat(),
        "record_scope": DAY_DELTA_SCOPE if filter_to_day else UNBOUNDED_SCOPE,
        "code_revision": revision,
        "status": status,
        "source_run_id": run_id,
        "producer_run_id": producer_id,
        "generated_at": generated_text,
        "input_record_count": input_count,
        "noncanonical_record_count": noncanonical_count,
        "event_count": len(keys),
        "events_sha256": comparison_keys_sha256(keys),
        "events": events,
        "legacy_record_count": len(legacy_records),
        "legacy_records_sha256": hashlib.sha256(
            canonical_json_bytes(legacy_records)
        ).hexdigest(),
        "legacy_records": legacy_records,
    }
    payload["snapshot_sha256"] = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
    return payload


def validate_engine_snapshot(
    payload: object,
    *,
    expected_engine: str | None = None,
    expected_date: date | None = None,
    expected_revision: str | None = None,
    require_succeeded: bool = True,
) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ShadowEngineError("engine snapshot must be a JSON object")
    allowed = {
        "schema_version",
        "engine",
        "observation_date",
        "record_scope",
        "code_revision",
        "status",
        "source_run_id",
        "producer_run_id",
        "generated_at",
        "input_record_count",
        "noncanonical_record_count",
        "event_count",
        "events_sha256",
        "events",
        "legacy_record_count",
        "legacy_records_sha256",
        "legacy_records",
        "snapshot_sha256",
    }
    unexpected = sorted(set(payload) - allowed)
    if unexpected:
        raise ShadowEngineError(f"engine snapshot has unexpected fields: {unexpected}")
    if payload.get("schema_version") != 3:
        raise ShadowEngineError("engine snapshot schema_version must be 3")
    engine = str(payload.get("engine") or "").strip().casefold()
    if engine not in ENGINES or (expected_engine is not None and engine != expected_engine):
        raise ShadowEngineError("engine snapshot engine mismatch")
    observed = _date(payload.get("observation_date"))
    if expected_date is not None and observed != expected_date:
        raise ShadowEngineError("engine snapshot observation_date mismatch")
    record_scope = str(payload.get("record_scope") or "").strip()
    if record_scope not in RECORD_SCOPES:
        raise ShadowEngineError("engine snapshot record_scope is invalid")
    revision = _full_revision(payload.get("code_revision"))
    if expected_revision is not None and revision != _full_revision(expected_revision):
        raise ShadowEngineError("engine snapshot code_revision mismatch")
    status = str(payload.get("status") or "")
    if status not in {"succeeded", "failed"}:
        raise ShadowEngineError("engine snapshot status is invalid")
    if require_succeeded and status != "succeeded":
        raise ShadowEngineError(f"{engine} engine did not succeed")
    _utc_timestamp(payload.get("generated_at"), "generated_at")
    run_id = str(payload.get("source_run_id") or "").strip()
    if not run_id or len(run_id) > 191:
        raise ShadowEngineError("engine snapshot source_run_id is invalid")
    producer_run_id = str(payload.get("producer_run_id") or "").strip()
    if not producer_run_id or len(producer_run_id) > 191:
        raise ShadowEngineError("engine snapshot producer_run_id is invalid")
    for field in (
        "input_record_count",
        "noncanonical_record_count",
        "event_count",
        "legacy_record_count",
    ):
        value = payload.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ShadowEngineError(f"engine snapshot {field} is invalid")
    events = payload.get("events")
    if not isinstance(events, list) or len(events) > 10_000:
        raise ShadowEngineError("engine snapshot events must be an array of at most 10000")
    keys: list[str] = []
    for index, event in enumerate(events):
        if not isinstance(event, dict) or set(event) != {
            "comparison_key",
            "source_evidence",
        }:
            raise ShadowEngineError(f"events[{index}] is invalid")
        key = str(event.get("comparison_key") or "").strip().casefold()
        if not COMPARISON_KEY_RE.fullmatch(key):
            raise ShadowEngineError(f"events[{index}] has invalid comparison_key")
        evidence = _validate_source_evidence(
            event.get("source_evidence"), f"events[{index}].source_evidence"
        )
        if engine == "candidate" and not evidence:
            raise ShadowEngineError(f"events[{index}] has no stable source evidence")
        keys.append(key)
    if keys != sorted(set(keys)):
        raise ShadowEngineError("engine snapshot comparison keys must be sorted and unique")
    if payload.get("event_count") != len(keys):
        raise ShadowEngineError("engine snapshot event_count mismatch")
    if str(payload.get("events_sha256") or "") != comparison_keys_sha256(keys):
        raise ShadowEngineError("engine snapshot events_sha256 mismatch")
    legacy_records = payload.get("legacy_records")
    if not isinstance(legacy_records, list) or len(legacy_records) > 10_000:
        raise ShadowEngineError(
            "engine snapshot legacy_records must be an array of at most 10000"
        )
    legacy_ids: list[str] = []
    legacy_keys: list[str] = []
    for index, record in enumerate(legacy_records):
        if not isinstance(record, dict) or set(record) != {
            "legacy_record_id",
            "comparison_key",
            "source_evidence",
        }:
            raise ShadowEngineError(f"legacy_records[{index}] is invalid")
        record_id = _safe_identifier(
            record.get("legacy_record_id"), f"legacy_records[{index}].legacy_record_id"
        )
        if record_id != record.get("legacy_record_id"):
            raise ShadowEngineError(
                f"legacy_records[{index}].legacy_record_id is not canonical"
            )
        legacy_ids.append(record_id)
        raw_key = record.get("comparison_key")
        if raw_key is not None:
            legacy_key = str(raw_key).strip().casefold()
            if not COMPARISON_KEY_RE.fullmatch(legacy_key) or legacy_key != raw_key:
                raise ShadowEngineError(
                    f"legacy_records[{index}] has invalid comparison_key"
                )
            legacy_keys.append(legacy_key)
        _validate_source_evidence(
            record.get("source_evidence"),
            f"legacy_records[{index}].source_evidence",
        )
    if legacy_ids != sorted(set(legacy_ids)):
        raise ShadowEngineError("legacy snapshot record identifiers must be sorted and unique")
    if payload.get("legacy_record_count") != len(legacy_records):
        raise ShadowEngineError("engine snapshot legacy_record_count mismatch")
    claimed_legacy_hash = str(payload.get("legacy_records_sha256") or "").casefold()
    actual_legacy_hash = hashlib.sha256(canonical_json_bytes(legacy_records)).hexdigest()
    if not re.fullmatch(r"[0-9a-f]{64}", claimed_legacy_hash) or not hmac.compare_digest(
        claimed_legacy_hash, actual_legacy_hash
    ):
        raise ShadowEngineError("engine snapshot legacy_records_sha256 mismatch")
    if engine == "candidate" and legacy_records:
        raise ShadowEngineError("candidate snapshot must not contain legacy records")
    if engine == "legacy" and keys != sorted(set(legacy_keys)):
        raise ShadowEngineError("legacy explicit events must match legacy record keys")
    claimed_snapshot_hash = str(payload.get("snapshot_sha256") or "").casefold()
    without_hash = dict(payload)
    without_hash.pop("snapshot_sha256", None)
    actual_snapshot_hash = hashlib.sha256(canonical_json_bytes(without_hash)).hexdigest()
    if not re.fullmatch(r"[0-9a-f]{64}", claimed_snapshot_hash) or not hmac.compare_digest(
        claimed_snapshot_hash, actual_snapshot_hash
    ):
        raise ShadowEngineError("engine snapshot snapshot_sha256 mismatch")
    return dict(payload)


def write_engine_snapshot(path: Path, payload: Mapping[str, object]) -> None:
    validate_engine_snapshot(payload, require_succeeded=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def write_candidate_snapshot_from_events(
    events: Iterable[Mapping[str, object]],
    *,
    observation_date: date,
    status: str,
    output_path: str | Path,
    code_revision: str | None = None,
    source_run_id: str | None = None,
    generated_at: datetime | None = None,
) -> dict[str, object]:
    payload = build_engine_snapshot(
        engine="candidate",
        observation_date=observation_date,
        code_revision=code_revision or os.environ.get("GITHUB_SHA") or "",
        status=status,
        records=events,
        source_run_id=source_run_id
        or f"github:{os.environ.get('GITHUB_RUN_ID', '')}:{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}",
        generated_at=generated_at,
        filter_to_day=True,
    )
    write_engine_snapshot(Path(output_path), payload)
    return payload


def _state_records(path: Path) -> list[Mapping[str, object]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ShadowEngineError("legacy state is missing or invalid JSON") from exc
    if not isinstance(payload, dict):
        raise ShadowEngineError("legacy state must be a JSON object")
    rows = payload.get("published_clusters")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ShadowEngineError("legacy state published_clusters is invalid")
    return rows


def _default_observation_date() -> date:
    return datetime.now(timezone.utc).astimezone(KST).date()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create a signed-content snapshot from an actual shadow engine output."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    state_parser = subparsers.add_parser(
        "snapshot-legacy-state",
        help="Snapshot explicit canonical events in the completed legacy curator state.",
    )
    state_parser.add_argument("--state", type=Path, required=True)
    state_parser.add_argument("--output", type=Path, required=True)
    state_parser.add_argument("--observation-date", type=date.fromisoformat, default=None)
    state_parser.add_argument("--expected-revision", default=os.environ.get("GITHUB_SHA", ""))
    state_parser.add_argument(
        "--source-run-id",
        default=f"github:{os.environ.get('GITHUB_RUN_ID', '')}:{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}",
    )
    args = parser.parse_args()
    try:
        if args.command == "snapshot-legacy-state":
            observed = args.observation_date or _default_observation_date()
            payload = build_engine_snapshot(
                engine="legacy",
                observation_date=observed,
                code_revision=args.expected_revision,
                status="succeeded",
                records=_state_records(args.state),
                source_run_id=args.source_run_id,
                filter_to_day=True,
            )
            write_engine_snapshot(args.output, payload)
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    except ShadowEngineError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
