from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from .governance import CampaignStage
from .remote_api import post_remote_action, remote_api_configured
from .telegram_sources import load_env_files


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = 1
ENTITY_NAMES = (
    "actors",
    "event_actors",
    "campaigns",
    "claim_evidence",
    "proposal_votes",
    "commitment_outcomes",
    "timeline_entries",
)
ID_PATTERN = re.compile(r"^[A-Za-z0-9_.:\-]{1,96}$")
LANGUAGE_PATTERN = re.compile(r"^[a-z]{2,3}(?:-[A-Z]{2})?$")
CODE_PATTERN = re.compile(r"^[A-Za-z0-9_.:\-]{1,40}$")

ALLOWED_FIELDS: dict[str, frozenset[str]] = {
    "actors": frozenset(
        {
            "actor_id",
            "actor_type",
            "display_name",
            "display_name_en",
            "company_id",
            "country_code",
            "aliases",
            "homepage_url",
            "record_status",
            "review_status",
        }
    ),
    "event_actors": frozenset({"event_id", "actor_id", "actor_role", "review_status"}),
    "campaigns": frozenset(
        {
            "campaign_id",
            "company_id",
            "lead_actor_id",
            "title",
            "original_language",
            "demand_text",
            "stage",
            "outcome",
            "started_at",
            "ended_at",
            "review_status",
            "publication_status",
            "evidence_document_ids",
        }
    ),
    "claim_evidence": frozenset(
        {
            "claim_id",
            "event_id",
            "campaign_id",
            "actor_id",
            "document_id",
            "claim_type",
            "claim_text",
            "original_language",
            "evidence_locator",
            "editorial_status",
        }
    ),
    "proposal_votes": frozenset(
        {
            "proposal_vote_id",
            "event_id",
            "campaign_id",
            "company_id",
            "proposer_actor_id",
            "agenda_no",
            "agenda_title",
            "original_language",
            "meeting_at",
            "recommendation",
            "recommendation_source",
            "result",
            "votes_for",
            "votes_against",
            "votes_abstain",
            "evidence_document_id",
            "review_status",
            "publication_status",
        }
    ),
    "commitment_outcomes": frozenset(
        {
            "commitment_id",
            "event_id",
            "campaign_id",
            "company_id",
            "commitment_text",
            "original_language",
            "target_at",
            "actual_action",
            "status",
            "target_metrics",
            "actual_metrics",
            "evidence_document_id",
            "review_status",
            "publication_status",
        }
    ),
    "timeline_entries": frozenset(
        {
            "timeline_entry_id",
            "event_id",
            "campaign_id",
            "document_id",
            "occurred_at",
            "entry_type",
            "title",
            "description",
            "original_language",
            "review_status",
            "publication_status",
        }
    ),
}


class EditorialValidationError(ValueError):
    pass


def _location(entity: str, index: int, field: str = "") -> str:
    base = f"{entity}[{index}]"
    return f"{base}.{field}" if field else base


def _require_text(
    record: dict[str, object],
    field: str,
    location: str,
    *,
    max_length: int | None = None,
) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value.strip():
        raise EditorialValidationError(f"{location}.{field} must be a non-empty string")
    if max_length is not None and len(value) > max_length:
        raise EditorialValidationError(f"{location}.{field} must be at most {max_length} characters")
    return value


def _optional_text(
    record: dict[str, object],
    field: str,
    location: str,
    *,
    max_length: int | None = None,
) -> str | None:
    value = record.get(field)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise EditorialValidationError(f"{location}.{field} must be a non-empty string when present")
    if max_length is not None and len(value) > max_length:
        raise EditorialValidationError(f"{location}.{field} must be at most {max_length} characters")
    return value


def _id(record: dict[str, object], field: str, location: str, *, max_length: int = 96) -> str:
    value = _require_text(record, field, location)
    if value != value.strip() or len(value) > max_length or not ID_PATTERN.fullmatch(value):
        raise EditorialValidationError(f"{location}.{field} is not a valid stable ID")
    return value


def _optional_id(record: dict[str, object], field: str, location: str, *, max_length: int = 96) -> str | None:
    if record.get(field) is None:
        return None
    return _id(record, field, location, max_length=max_length)


def _company_id(record: dict[str, object], location: str) -> str:
    value = _require_text(record, "company_id", location)
    if not re.fullmatch(r"\d{8}", value):
        raise EditorialValidationError(f"{location}.company_id must be an 8-digit DART corp_code")
    return value


def _language(record: dict[str, object], location: str) -> str:
    value = _require_text(record, "original_language", location)
    if not LANGUAGE_PATTERN.fullmatch(value):
        raise EditorialValidationError(f"{location}.original_language must be an explicit language tag")
    return value


def _timestamp(record: dict[str, object], field: str, location: str, *, required: bool = True) -> datetime | None:
    value = record.get(field)
    if value is None and not required:
        return None
    if not isinstance(value, str) or not value.strip():
        raise EditorialValidationError(f"{location}.{field} must be a timezone-aware ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EditorialValidationError(
            f"{location}.{field} must be a timezone-aware ISO-8601 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise EditorialValidationError(f"{location}.{field} must include a timezone offset")
    return parsed


def _code(record: dict[str, object], field: str, location: str) -> str:
    value = _require_text(record, field, location)
    if value != value.strip() or not CODE_PATTERN.fullmatch(value):
        raise EditorialValidationError(f"{location}.{field} contains an invalid code")
    return value


def _fail_closed_field(record: dict[str, object], field: str, required_value: str, location: str) -> None:
    value = record.get(field, required_value)
    if value != required_value:
        raise EditorialValidationError(f"{location}.{field} must be {required_value!r} for editorial ingest")
    record[field] = required_value


def _document_ids(record: dict[str, object], field: str, location: str) -> list[str]:
    values = record.get(field)
    if not isinstance(values, list) or not values:
        raise EditorialValidationError(f"{location}.{field} must contain at least one evidence document ID")
    normalized: list[str] = []
    for index, value in enumerate(values):
        if not isinstance(value, str) or value != value.strip() or not ID_PATTERN.fullmatch(value):
            raise EditorialValidationError(f"{location}.{field}[{index}] is not a valid document ID")
        normalized.append(value)
    if len(set(normalized)) != len(normalized):
        raise EditorialValidationError(f"{location}.{field} contains duplicate document IDs")
    return normalized


def _at_least_one_parent(record: dict[str, object], location: str) -> None:
    event_id = _optional_id(record, "event_id", location)
    campaign_id = _optional_id(record, "campaign_id", location)
    if event_id is None and campaign_id is None:
        raise EditorialValidationError(f"{location} requires event_id or campaign_id")


def _vote_percentage(record: dict[str, object], field: str, location: str) -> None:
    value = record.get(field)
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        raise EditorialValidationError(f"{location}.{field} must be a finite percentage")
    if not 0 <= float(value) <= 100:
        raise EditorialValidationError(f"{location}.{field} must be between 0 and 100")


def _validate_actor(record: dict[str, object], location: str) -> None:
    _id(record, "actor_id", location, max_length=64)
    actor_type = _code(record, "actor_type", location)
    if actor_type not in {
        "company",
        "activist_shareholder",
        "institution",
        "shareholder_coalition",
        "regulator",
        "advisor",
    }:
        raise EditorialValidationError(f"{location}.actor_type is not supported")
    _require_text(record, "display_name", location, max_length=255)
    _optional_text(record, "display_name_en", location, max_length=255)
    if record.get("company_id") is not None:
        _company_id(record, location)
    country = _optional_text(record, "country_code", location)
    if country is not None and not re.fullmatch(r"[A-Z]{2}", country):
        raise EditorialValidationError(f"{location}.country_code must be ISO 3166-1 alpha-2 uppercase")
    aliases = record.get("aliases", [])
    if not isinstance(aliases, list) or len(aliases) > 20 or any(
        not isinstance(alias, str) or not alias.strip() or len(alias) > 255 for alias in aliases
    ):
        raise EditorialValidationError(
            f"{location}.aliases must contain at most 20 non-empty strings"
        )
    if len(set(aliases)) != len(aliases):
        raise EditorialValidationError(f"{location}.aliases contains duplicates")
    _optional_text(record, "homepage_url", location)
    _fail_closed_field(record, "record_status", "inactive", location)
    _fail_closed_field(record, "review_status", "pending", location)


def _validate_event_actor(record: dict[str, object], location: str) -> None:
    _id(record, "event_id", location)
    _id(record, "actor_id", location, max_length=64)
    _code(record, "actor_role", location)
    _fail_closed_field(record, "review_status", "pending", location)


def _validate_campaign(record: dict[str, object], location: str) -> None:
    _id(record, "campaign_id", location)
    _company_id(record, location)
    _id(record, "lead_actor_id", location, max_length=64)
    _require_text(record, "title", location, max_length=700)
    _language(record, location)
    _require_text(record, "demand_text", location, max_length=1_000_000)
    stage = _require_text(record, "stage", location)
    if stage not in {item.value for item in CampaignStage}:
        raise EditorialValidationError(f"{location}.stage is not a supported campaign stage")
    outcome = _optional_text(record, "outcome", location, max_length=40)
    if outcome is not None and outcome not in {"settled", "withdrawn", "passed", "failed"}:
        raise EditorialValidationError(f"{location}.outcome is not supported")
    started_at = _timestamp(record, "started_at", location)
    ended_at = _timestamp(record, "ended_at", location, required=False)
    if started_at is not None and ended_at is not None and ended_at < started_at:
        raise EditorialValidationError(f"{location}.ended_at must not precede started_at")
    _document_ids(record, "evidence_document_ids", location)
    _fail_closed_field(record, "review_status", "pending", location)
    _fail_closed_field(record, "publication_status", "draft", location)


def _validate_claim(record: dict[str, object], location: str) -> None:
    _id(record, "claim_id", location)
    _id(record, "event_id", location)
    _optional_id(record, "campaign_id", location)
    _optional_id(record, "actor_id", location, max_length=64)
    _id(record, "document_id", location)
    claim_type = _code(record, "claim_type", location)
    if claim_type not in {
        "actor_claim",
        "company_response",
        "official_fact",
        "media_report",
        "editorial_interpretation",
    }:
        raise EditorialValidationError(f"{location}.claim_type is not supported")
    _require_text(record, "claim_text", location, max_length=1_000_000)
    _language(record, location)
    _optional_text(record, "evidence_locator", location, max_length=500)
    _fail_closed_field(record, "editorial_status", "pending", location)


def _validate_vote(record: dict[str, object], location: str) -> None:
    _id(record, "proposal_vote_id", location)
    _at_least_one_parent(record, location)
    _company_id(record, location)
    _optional_id(record, "proposer_actor_id", location, max_length=64)
    _optional_text(record, "agenda_no", location, max_length=40)
    _require_text(record, "agenda_title", location, max_length=700)
    _language(record, location)
    _timestamp(record, "meeting_at", location)
    _optional_text(record, "recommendation", location, max_length=40)
    _optional_text(record, "recommendation_source", location, max_length=255)
    result = _optional_text(record, "result", location, max_length=24)
    if result is not None and result not in {"pending", "passed", "failed", "withdrawn"}:
        raise EditorialValidationError(f"{location}.result is not supported")
    _id(record, "evidence_document_id", location)
    for field in ("votes_for", "votes_against", "votes_abstain"):
        _vote_percentage(record, field, location)
    _fail_closed_field(record, "review_status", "pending", location)
    _fail_closed_field(record, "publication_status", "draft", location)


def _validate_commitment(record: dict[str, object], location: str) -> None:
    _id(record, "commitment_id", location)
    _at_least_one_parent(record, location)
    _company_id(record, location)
    _require_text(record, "commitment_text", location, max_length=1_000_000)
    _language(record, location)
    _timestamp(record, "target_at", location, required=False)
    _optional_text(record, "actual_action", location, max_length=1_000_000)
    status = _optional_text(record, "status", location, max_length=32)
    if status is not None and status not in {
        "planned",
        "announced",
        "in_progress",
        "met",
        "partially_met",
        "missed",
        "cancelled",
    }:
        raise EditorialValidationError(f"{location}.status is not supported")
    _id(record, "evidence_document_id", location)
    for field in ("target_metrics", "actual_metrics"):
        value = record.get(field)
        if value is not None and not isinstance(value, dict):
            raise EditorialValidationError(f"{location}.{field} must be an object when present")
    _fail_closed_field(record, "review_status", "pending", location)
    _fail_closed_field(record, "publication_status", "draft", location)


def _validate_timeline(record: dict[str, object], location: str) -> None:
    _id(record, "timeline_entry_id", location)
    _at_least_one_parent(record, location)
    _id(record, "document_id", location)
    _timestamp(record, "occurred_at", location)
    _code(record, "entry_type", location)
    _require_text(record, "title", location, max_length=700)
    _optional_text(record, "description", location, max_length=1_000_000)
    _language(record, location)
    _fail_closed_field(record, "review_status", "pending", location)
    _fail_closed_field(record, "publication_status", "draft", location)


VALIDATORS = {
    "actors": _validate_actor,
    "event_actors": _validate_event_actor,
    "campaigns": _validate_campaign,
    "claim_evidence": _validate_claim,
    "proposal_votes": _validate_vote,
    "commitment_outcomes": _validate_commitment,
    "timeline_entries": _validate_timeline,
}


def validate_bundle(value: object) -> dict[str, list[dict[str, object]]]:
    if not isinstance(value, dict):
        raise EditorialValidationError("bundle must be a JSON object")
    unknown_entities = set(value) - set(ENTITY_NAMES) - {"schema_version"}
    if unknown_entities:
        raise EditorialValidationError(f"bundle contains unsupported entities: {', '.join(sorted(unknown_entities))}")
    if value.get("schema_version", SCHEMA_VERSION) != SCHEMA_VERSION:
        raise EditorialValidationError(f"schema_version must be {SCHEMA_VERSION}")

    normalized: dict[str, list[dict[str, object]]] = {name: [] for name in ENTITY_NAMES}
    total = 0
    for entity in ENTITY_NAMES:
        rows = value.get(entity, [])
        if not isinstance(rows, list):
            raise EditorialValidationError(f"{entity} must be an array")
        seen_ids: set[tuple[object, ...]] = set()
        for index, raw_record in enumerate(rows):
            location = _location(entity, index)
            if not isinstance(raw_record, dict):
                raise EditorialValidationError(f"{location} must be an object")
            unknown_fields = set(raw_record) - ALLOWED_FIELDS[entity]
            if unknown_fields:
                raise EditorialValidationError(
                    f"{location} contains unsupported fields: {', '.join(sorted(unknown_fields))}"
                )
            # Copy the mapping only so fail-closed metadata defaults can be
            # applied. Every user-provided text value remains byte-for-byte the
            # same Unicode string; no translation, trimming, or normalization occurs.
            record = dict(raw_record)
            VALIDATORS[entity](record, location)
            identity: tuple[object, ...]
            if entity == "event_actors":
                identity = (record["event_id"], record["actor_id"], record["actor_role"])
            else:
                primary_field = {
                    "actors": "actor_id",
                    "campaigns": "campaign_id",
                    "claim_evidence": "claim_id",
                    "proposal_votes": "proposal_vote_id",
                    "commitment_outcomes": "commitment_id",
                    "timeline_entries": "timeline_entry_id",
                }[entity]
                identity = (record[primary_field],)
            if identity in seen_ids:
                raise EditorialValidationError(f"{location} duplicates an earlier entity identity")
            seen_ids.add(identity)
            normalized[entity].append(record)
            total += 1
    if total == 0:
        raise EditorialValidationError("bundle must contain at least one entity")
    return normalized


def load_bundle(path: Path) -> dict[str, list[dict[str, object]]]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except UnicodeDecodeError as exc:
        raise EditorialValidationError("bundle must be UTF-8 JSON") from exc
    except json.JSONDecodeError as exc:
        raise EditorialValidationError(f"bundle is not valid JSON: {exc.msg}") from exc
    return validate_bundle(raw)


def canonical_bundle_bytes(bundle: dict[str, list[dict[str, object]]]) -> bytes:
    canonical = {"schema_version": SCHEMA_VERSION, **{name: bundle[name] for name in ENTITY_NAMES}}
    return json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def bundle_sha256(bundle: dict[str, list[dict[str, object]]]) -> str:
    return hashlib.sha256(canonical_bundle_bytes(bundle)).hexdigest()


def entity_counts(bundle: dict[str, list[dict[str, object]]]) -> dict[str, int]:
    return {name: len(bundle[name]) for name in ENTITY_NAMES}


def _record_chunks(records: list[dict[str, object]], size: int) -> Iterable[list[dict[str, object]]]:
    for index in range(0, len(records), size):
        yield records[index : index + size]


def build_remote_chunks(
    bundle: dict[str, list[dict[str, object]]],
    *,
    chunk_size: int,
) -> list[dict[str, object]]:
    if not 1 <= chunk_size <= 500:
        raise EditorialValidationError("chunk_size must be between 1 and 500")
    digest = bundle_sha256(bundle)
    entity_chunks = [
        (entity, records)
        for entity in ENTITY_NAMES
        for records in _record_chunks(bundle[entity], chunk_size)
    ]
    total = len(entity_chunks)
    payloads: list[dict[str, object]] = []
    for index, (entity, records) in enumerate(entity_chunks, start=1):
        payload: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "bundle_sha256": digest,
            "chunk_id": f"editorial:{digest[:32]}:{index:04d}",
            "chunk_index": index,
            "chunk_count": total,
        }
        payload.update({name: records if name == entity else [] for name in ENTITY_NAMES})
        payloads.append(payload)
    return payloads


def _response_count(response: dict[str, Any], entity: str) -> int:
    accepted = response.get("accepted")
    if not isinstance(accepted, dict):
        return -1
    try:
        return int(accepted.get(entity, -1))
    except (TypeError, ValueError):
        return -1


def _response_rejected(response: dict[str, Any]) -> int:
    value = response.get("rejected", 0)
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


def ingest_bundle(
    bundle: dict[str, list[dict[str, object]]],
    *,
    dry_run: bool,
    chunk_size: int = 100,
) -> dict[str, object]:
    payloads = build_remote_chunks(bundle, chunk_size=chunk_size)
    counts = entity_counts(bundle)
    digest = bundle_sha256(bundle)
    report: dict[str, object] = {
        "ok": True,
        "mode": "dry-run" if dry_run else "live",
        "schema_version": SCHEMA_VERSION,
        "bundle_sha256": digest,
        "counts": counts,
        "total_count": sum(counts.values()),
        "chunk_count": len(payloads),
    }
    if dry_run:
        report["accepted"] = {name: 0 for name in ENTITY_NAMES}
        return report
    if not remote_api_configured():
        raise RuntimeError("ACTIVIST_API_URL and ACTIVIST_API_SECRET are required for live editorial ingest")

    accepted = {name: 0 for name in ENTITY_NAMES}
    for payload in payloads:
        entity = next(name for name in ENTITY_NAMES if payload[name])
        expected = len(payload[entity])  # type: ignore[arg-type]
        response = post_remote_action("upsert_editorial_snapshot", payload, timeout=45.0)
        rejected = _response_rejected(response)
        accepted_count = _response_count(response, entity)
        if not response.get("ok") or rejected != 0 or accepted_count != expected:
            raise RuntimeError(
                f"editorial chunk {payload['chunk_index']}/{payload['chunk_count']} rejected: "
                f"entity={entity} expected={expected} accepted={accepted_count} rejected={rejected}"
            )
        accepted[entity] += accepted_count
    if accepted != counts:
        raise RuntimeError("editorial ingest accepted counts do not match the validated bundle")
    report["accepted"] = accepted
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate and ingest a governed editorial entity JSON bundle.")
    parser.add_argument("--bundle", type=Path, required=True, help="UTF-8 JSON bundle path")
    parser.add_argument("--root", type=Path, default=PROJECT_ROOT, help="project root containing environment files")
    parser.add_argument("--chunk-size", type=int, default=100, help="records per HMAC request (1-500)")
    parser.add_argument("--dry-run", action="store_true", help="validate and hash without remote mutation")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    project_root = args.root.resolve()
    load_env_files(project_root)
    try:
        bundle = load_bundle(args.bundle.resolve())
        report = ingest_bundle(bundle, dry_run=bool(args.dry_run), chunk_size=int(args.chunk_size))
    except (EditorialValidationError, OSError, RuntimeError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
