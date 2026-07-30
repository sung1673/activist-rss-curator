from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Mapping, Sequence


SCHEMA_VERSION = 1
BUNDLE_KIND = "official-dart-frozen-replay-bundle"
WINDOW_KIND = "official-dart-frozen-replay-window"
ARTIFACT_BINDING_KIND = "official-dart-frozen-replay-artifact-binding"
PROBE_KIND = "official-dart-fresh-drift-probe"
PROBE_RELEASE_GATE_POLICY = "stable-public-payload-source-count-diagnostic-v1"
MAX_BUNDLE_BYTES = 25_000_000
MAX_WINDOW_BYTES = 1_000_000
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CODE_REVISION_RE = re.compile(r"^[0-9a-f]{40}$")
_FORBIDDEN_KEY_RE = re.compile(
    r"^(?:authorization|cookie|cookies|headers?|request_headers|response_headers|"
    r"request_body|response_body|raw|raw_body|raw_response|html|api_key|apikey|"
    r"dart_api_key|opendart_api_keys|access_token|secret)$",
    re.IGNORECASE,
)
_FORBIDDEN_URL_RE = re.compile(
    r"(?:[?&](?:crtfc_key|api_key|apikey|access_token|token|secret)=)",
    re.IGNORECASE,
)
_ID_FIELDS = {
    "companies": "company_id",
    "documents": "document_id",
    "events": "event_id",
}
_VOLATILE_FIELDS = {
    "documents": frozenset({"retrieved_at"}),
}


class FrozenReplayBundleError(RuntimeError):
    """A frozen replay artifact is incomplete, unsafe, or not exactly bound."""


@dataclass(frozen=True)
class FrozenBundle:
    root: Path
    manifest: dict[str, object]
    artifact_binding: dict[str, object]


def artifact_binding_checkpoint_sha256(path: Path) -> str:
    binding, _ = _strict_object(path.resolve(), max_bytes=MAX_WINDOW_BYTES)
    value = str(binding.get("checkpoint_payload_sha256") or "")
    if _SHA256_RE.fullmatch(value) is None:
        raise FrozenReplayBundleError(
            "artifact binding checkpoint digest is missing"
        )
    return value


def build_artifact_binding(
    *,
    bundle_root: Path,
    replay_state_path: Path,
    output_path: Path,
    artifact_id: int,
    artifact_name: str,
    artifact_digest: str,
    artifact_created_at: str,
    producer_run_id: int,
    producer_run_attempt: int,
    producer_run_started_at: str,
    consumer_repository: str,
    consumer_workflow: str,
    consumer_run_id: int,
    consumer_run_attempt: int,
    consumer_code_revision: str,
    expected_range_start: str,
    expected_range_end_exclusive: str,
    freshness_hours: int = 72,
    now: datetime | None = None,
) -> dict[str, object]:
    """Validate an uploaded apply artifact and create its replay binding."""

    root = bundle_root.resolve()
    manifest, manifest_raw = _strict_object(
        root / "manifest.json", max_bytes=MAX_WINDOW_BYTES
    )
    replay_state, _ = _strict_object(
        replay_state_path.resolve(), max_bytes=2_000_000
    )
    producer = manifest.get("producer")
    job = manifest.get("job")
    checkpoint = manifest.get("checkpoint")
    windows = manifest.get("windows")
    state_checkpoint = replay_state.get("checkpoint")
    try:
        expected_start = date.fromisoformat(expected_range_start)
        expected_end = date.fromisoformat(expected_range_end_exclusive)
    except ValueError as exc:
        raise FrozenReplayBundleError("expected replay range is invalid") from exc
    if (expected_end - expected_start).days != 30:
        raise FrozenReplayBundleError("expected replay range must be exactly 30 days")
    expected_artifact_name = (
        f"official-dart-frozen-replay-apply-{producer_run_id}-"
        f"{producer_run_attempt}"
    )
    if (
        manifest.get("schema_version") != SCHEMA_VERSION
        or manifest.get("kind") != BUNDLE_KIND
        or manifest.get("bundle_status") != "complete"
        or manifest.get("code_revision") != consumer_code_revision
        or not isinstance(producer, Mapping)
        or producer.get("repository") != consumer_repository
        or producer.get("workflow") != "official-backfill.yml"
        or consumer_workflow != "official-backfill.yml"
        or producer.get("run_id") != producer_run_id
        or producer.get("run_attempt") != producer_run_attempt
        or not isinstance(job, Mapping)
        or not isinstance(checkpoint, Mapping)
        or not isinstance(windows, list)
        or len(windows) != 30
        or not isinstance(state_checkpoint, Mapping)
        or state_checkpoint.get("job_fingerprint") != job.get("fingerprint")
        or state_checkpoint.get("checkpoint_payload_sha256")
        != checkpoint.get("payload_sha256")
        or state_checkpoint.get("completed_window_count") != 30
        or state_checkpoint.get("failed_window_count") != 0
    ):
        raise FrozenReplayBundleError(
            "artifact manifest is not bound to production apply state"
        )
    if (
        artifact_id <= 0
        or producer_run_id <= 0
        or producer_run_attempt <= 0
        or consumer_run_id <= 0
        or consumer_run_attempt <= 0
        or _CODE_REVISION_RE.fullmatch(consumer_code_revision) is None
        or artifact_name != expected_artifact_name
        or job.get("range_start") != expected_range_start
        or job.get("range_end_exclusive") != expected_range_end_exclusive
    ):
        raise FrozenReplayBundleError("artifact or workflow identity is invalid")
    digest = artifact_digest.strip().casefold()
    if _SHA256_RE.fullmatch(digest):
        digest = f"sha256:{digest}"
    if re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
        raise FrozenReplayBundleError("artifact digest is invalid")

    def parsed_timestamp(value: str, label: str) -> datetime:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FrozenReplayBundleError(f"{label} is invalid") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise FrozenReplayBundleError(f"{label} lacks a timezone")
        return parsed.astimezone(timezone.utc)

    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise FrozenReplayBundleError("binding clock lacks a timezone")
    current = current.astimezone(timezone.utc)
    artifact_created = parsed_timestamp(
        artifact_created_at, "artifact created_at"
    )
    producer_started = parsed_timestamp(
        producer_run_started_at, "producer run_started_at"
    )
    freshness = timedelta(hours=freshness_hours)
    if (
        freshness_hours < 1
        or current - artifact_created > freshness
        or current - producer_started > freshness
        or artifact_created > current + timedelta(minutes=5)
        or producer_started > current + timedelta(minutes=5)
    ):
        raise FrozenReplayBundleError("apply artifact is outside its freshness window")
    binding: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "kind": ARTIFACT_BINDING_KIND,
        "consumer": {
            "repository": consumer_repository,
            "workflow": consumer_workflow,
            "run_id": consumer_run_id,
            "run_attempt": consumer_run_attempt,
            "code_revision": consumer_code_revision,
        },
        "producer": {
            "run_id": producer_run_id,
            "run_attempt": producer_run_attempt,
            "run_started_at": producer_run_started_at,
        },
        "artifact": {
            "id": artifact_id,
            "name": artifact_name,
            "digest": digest,
            "created_at": artifact_created_at,
        },
        "job_fingerprint": job.get("fingerprint"),
        "range_start": job.get("range_start"),
        "range_end_exclusive": job.get("range_end_exclusive"),
        "checkpoint_payload_sha256": checkpoint.get("payload_sha256"),
        "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
        "leaf_sha256": [
            row.get("sha256") for row in windows if isinstance(row, Mapping)
        ],
        "freshness_hours": freshness_hours,
        "artifact_sanitization": {"status": "verified"},
    }
    binding["binding_sha256"] = canonical_sha256(binding)
    _assert_public_safe(binding, location="artifact_binding")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    temporary.write_bytes(canonical_json_bytes(binding) + b"\n")
    temporary.replace(output_path)
    return binding


def canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _strict_object(path: Path, *, max_bytes: int) -> tuple[dict[str, object], bytes]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise FrozenReplayBundleError(f"cannot read {path.name}") from exc
    if not raw or len(raw) > max_bytes:
        raise FrozenReplayBundleError(f"{path.name} is empty or oversized")
    try:
        def reject_constant(value: str) -> object:
            raise ValueError(f"non-finite JSON number {value}")

        def reject_duplicates(
            pairs: list[tuple[str, object]],
        ) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, child in pairs:
                if key in result:
                    raise ValueError(f"duplicate JSON key {key}")
                result[key] = child
            return result

        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise FrozenReplayBundleError(
            f"{path.name} is not strict UTF-8 JSON"
        ) from exc
    if not isinstance(value, dict):
        raise FrozenReplayBundleError(f"{path.name} must contain an object")
    _assert_public_safe(value, location=path.name)
    return value, raw


def _assert_public_safe(
    value: object,
    *,
    location: str,
    depth: int = 0,
) -> None:
    if depth > 32:
        raise FrozenReplayBundleError(f"JSON nesting is excessive at {location}")
    if isinstance(value, Mapping):
        if len(value) > 10_000:
            raise FrozenReplayBundleError(f"JSON object is excessive at {location}")
        for key, child in value.items():
            field = str(key)
            if _FORBIDDEN_KEY_RE.fullmatch(field):
                raise FrozenReplayBundleError(
                    f"forbidden sensitive field at {location}.{field}"
                )
            _assert_public_safe(
                child,
                location=f"{location}.{field}",
                depth=depth + 1,
            )
        return
    if isinstance(value, list):
        if len(value) > 100_000:
            raise FrozenReplayBundleError(f"JSON array is excessive at {location}")
        for index, child in enumerate(value):
            _assert_public_safe(
                child,
                location=f"{location}[{index}]",
                depth=depth + 1,
            )
        return
    if isinstance(value, str):
        if len(value.encode("utf-8")) > MAX_WINDOW_BYTES:
            raise FrozenReplayBundleError(f"JSON string is excessive at {location}")
        if (
            "authorization: bearer " in value.casefold()
            or _FORBIDDEN_URL_RE.search(value)
        ):
            raise FrozenReplayBundleError(
                f"credential-bearing text at {location}"
            )


def _assert_runtime_credentials_absent(raw: bytes) -> None:
    candidates: set[str] = set()
    for name in ("OPENDART_API_KEYS", "DART_API_KEY"):
        configured = os.environ.get(name, "")
        if configured:
            candidates.add(configured.strip())
            candidates.update(
                value.strip()
                for value in re.split(r"[\s,;]+", configured)
                if value.strip()
            )
    for candidate in candidates:
        if len(candidate) >= 8 and candidate.encode("utf-8") in raw:
            raise FrozenReplayBundleError(
                "frozen bundle contains an OpenDART credential"
            )


def _records(payload: Mapping[str, object], key: str) -> list[dict[str, object]]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise FrozenReplayBundleError(f"payload.{key} must be a list")
    rows: list[dict[str, object]] = []
    for index, row in enumerate(value):
        if not isinstance(row, dict):
            raise FrozenReplayBundleError(f"payload.{key}[{index}] must be an object")
        rows.append(dict(row))
    return rows


def _event_document_ids(event: Mapping[str, object]) -> set[str]:
    ids = event.get("document_ids")
    return (
        {str(value) for value in ids if str(value)}
        if isinstance(ids, list)
        else set()
    )


def public_dart_payload(payload: Mapping[str, object]) -> dict[str, object]:
    """Return only normalized public rows needed for an exact DART replay."""

    documents = [
        row
        for row in _records(payload, "documents")
        if str(row.get("source_right_id") or "").strip().casefold()
        == "official:dart"
    ]
    document_ids = {
        str(row.get("document_id") or "") for row in documents
        if str(row.get("document_id") or "")
    }
    events = [
        row
        for row in _records(payload, "events")
        if _event_document_ids(row) & document_ids
    ]
    company_ids = {
        str(row.get("company_id") or "")
        for row in [*documents, *events]
        if str(row.get("company_id") or "")
    }
    companies = [
        row
        for row in _records(payload, "companies")
        if str(row.get("company_id") or "") in company_ids
    ]
    for key, rows in (
        ("companies", companies),
        ("documents", documents),
        ("events", events),
    ):
        identity = _ID_FIELDS[key]
        if any(not str(row.get(identity) or "") for row in rows):
            raise FrozenReplayBundleError(f"payload.{key} has a missing stable ID")
        rows.sort(key=lambda row: str(row[identity]))
    snapshot: dict[str, object] = {
        "companies": companies,
        "documents": documents,
        "events": events,
        # SourceRight records are managed out of band. A replay may reference
        # the approved right but must never recreate or mutate the grant.
        "source_rights": [],
    }
    _assert_public_safe(snapshot, location="payload")
    return snapshot


def _semantic_row(kind: str, row: Mapping[str, object]) -> dict[str, object]:
    volatile = _VOLATILE_FIELDS.get(kind, frozenset())
    return {key: value for key, value in row.items() if key not in volatile}


def stable_payload_sha256(payload: Mapping[str, object]) -> str:
    """Match official_ingest's non-weakenable DART document/event digest."""

    documents = [
        _semantic_row("documents", row)
        for row in _records(payload, "documents")
        if str(row.get("source_right_id") or "").strip().casefold()
        == "official:dart"
    ]
    document_ids = {
        str(row.get("document_id") or "") for row in documents
        if str(row.get("document_id") or "")
    }
    events = [
        dict(row)
        for row in _records(payload, "events")
        if _event_document_ids(row) & document_ids
    ]
    documents.sort(key=lambda row: str(row.get("document_id") or ""))
    events.sort(key=lambda row: str(row.get("event_id") or ""))
    return canonical_sha256(
        {
            "contract_version": 1,
            "documents": documents,
            "events": events,
        }
    )


def leaf_hashes(payload: Mapping[str, object]) -> dict[str, dict[str, str]]:
    hashes: dict[str, dict[str, str]] = {}
    for kind in ("companies", "documents", "events"):
        identity = _ID_FIELDS[kind]
        hashes[kind] = {
            str(row[identity]): canonical_sha256(_semantic_row(kind, row))
            for row in _records(payload, kind)
        }
    return hashes


def public_payload_semantic_sha256(payload: Mapping[str, object]) -> str:
    snapshot = public_dart_payload(payload)
    return canonical_sha256(
        {
            kind: [
                _semantic_row(kind, row)
                for row in _records(snapshot, kind)
            ]
            for kind in ("companies", "documents", "events")
        }
    )


def source_semantic_counts(
    source_outcome: Mapping[str, object],
) -> dict[str, int]:
    mapping = {
        "fetched_count": "fetched",
        "accepted_count": "accepted",
        "rejected_count": "rejected_non_governance",
        "duplicate_count": "duplicate_count",
        "discarded_count": "discarded_valid_count",
        "error_count": "error_count",
    }
    counts: dict[str, int] = {}
    for target, source in mapping.items():
        value = source_outcome.get(source)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise FrozenReplayBundleError(
                f"DART source outcome {source} must be a non-negative integer"
            )
        counts[target] = value
    if counts["accepted_count"] > counts["fetched_count"]:
        raise FrozenReplayBundleError("DART accepted count exceeds fetched count")
    return counts


def _nonnegative_int(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FrozenReplayBundleError(f"{label} must be a non-negative integer")
    return value


def build_window_leaf(
    *,
    code_revision: str,
    job_fingerprint: str,
    window_start: str,
    window_end_exclusive: str,
    idempotency_key: str,
    payload: Mapping[str, object],
    run: Mapping[str, object],
) -> dict[str, object]:
    if _CODE_REVISION_RE.fullmatch(code_revision) is None:
        raise FrozenReplayBundleError("window code revision must be exact")
    snapshot = public_dart_payload(payload)
    stable_hash = stable_payload_sha256(snapshot)
    run_stable_hash = str(run.get("stable_payload_sha256") or "")
    source_outcomes = run.get("source_outcomes")
    dart_outcome = (
        source_outcomes.get("dart")
        if isinstance(source_outcomes, Mapping)
        else None
    )
    if (
        run.get("stable_payload_contract_version") != 1
        or not isinstance(dart_outcome, Mapping)
        or not hmac.compare_digest(run_stable_hash, stable_hash)
        or str(run.get("idempotency_key") or "") != idempotency_key
        or str(run.get("code_revision") or "") != code_revision
        or str(run.get("source_key") or "") != "dart"
        or str(run.get("ingest_mode") or "") != "apply"
    ):
        raise FrozenReplayBundleError("apply run is not bound to the frozen payload")
    counts = source_semantic_counts(dart_outcome)
    run_contract: dict[str, object] = {
        "run_id": str(run.get("run_id") or ""),
        "fetched_count": _nonnegative_int(
            run.get("fetched_count"), label="run fetched_count"
        ),
        "resolved_count": _nonnegative_int(
            run.get("resolved_count"), label="run resolved_count"
        ),
        "accepted_count": _nonnegative_int(
            run.get("accepted_count"), label="run accepted_count"
        ),
        "error_count": _nonnegative_int(
            run.get("error_count"), label="run error_count"
        ),
    }
    if (
        not run_contract["run_id"]
        or run_contract["error_count"] != 0
    ):
        raise FrozenReplayBundleError("apply run counts are invalid")
    leaf: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "kind": WINDOW_KIND,
        "source": "dart",
        "mode": "apply",
        "code_revision": code_revision,
        "job_fingerprint": job_fingerprint,
        "window_start": window_start,
        "window_end_exclusive": window_end_exclusive,
        "idempotency_key": idempotency_key,
        "stable_payload_contract_version": 1,
        "stable_payload_sha256": stable_hash,
        "public_payload_sha256": public_payload_semantic_sha256(snapshot),
        "record_counts": {
            key: len(_records(snapshot, key))
            for key in ("companies", "documents", "events")
        },
        "source_semantic_counts": counts,
        "run_contract": run_contract,
        "entity_leaf_sha256": leaf_hashes(snapshot),
        "volatile_fields_excluded_from_semantic_hash": {
            "documents": sorted(_VOLATILE_FIELDS["documents"]),
        },
        "payload": snapshot,
        "artifact_sanitization": {
            "status": "verified",
            "data_classification": "official_public_metadata",
            "contains_provider_response_body": False,
            "contains_full_document_body": False,
            "contains_credentials": False,
        },
    }
    _assert_public_safe(leaf, location="window")
    _assert_runtime_credentials_absent(canonical_json_bytes(leaf))
    return leaf


def write_window_leaf(
    root: Path,
    *,
    index: int,
    leaf: Mapping[str, object],
) -> dict[str, object]:
    root = root.resolve()
    windows = root / "windows"
    windows.mkdir(parents=True, exist_ok=True)
    start = str(leaf.get("window_start") or "")
    relative = PurePosixPath("windows", f"{index:02d}-{start}.json")
    destination = root / Path(*relative.parts)
    raw = canonical_json_bytes(dict(leaf)) + b"\n"
    if not raw or len(raw) > MAX_WINDOW_BYTES:
        raise FrozenReplayBundleError("frozen DART window leaf exceeds 1 MB")
    digest = hashlib.sha256(raw).hexdigest()
    try:
        with destination.open("xb") as output:
            output.write(raw)
            output.flush()
            os.fsync(output.fileno())
    except FileExistsError:
        existing = destination.read_bytes()
        if not hmac.compare_digest(hashlib.sha256(existing).hexdigest(), digest):
            raise FrozenReplayBundleError(
                f"refusing to overwrite changed frozen leaf {relative.as_posix()}"
            )
    run_metadata = leaf.get("run_contract")
    if not isinstance(run_metadata, Mapping):
        raise FrozenReplayBundleError("frozen leaf run contract is missing")
    return {
        "index": index,
        "window_start": start,
        "window_end_exclusive": str(leaf.get("window_end_exclusive") or ""),
        "path": relative.as_posix(),
        "sha256": digest,
        "byte_size": len(raw),
        "stable_payload_sha256": str(
            leaf.get("stable_payload_sha256") or ""
        ),
        "public_payload_sha256": str(leaf.get("public_payload_sha256") or ""),
        "run_id": str(run_metadata.get("run_id") or ""),
        "idempotency_key": str(leaf.get("idempotency_key") or ""),
        "source_semantic_counts_sha256": canonical_sha256(
            leaf.get("source_semantic_counts")
        ),
    }


def finalize_apply_bundle(
    root: Path,
    *,
    code_revision: str,
    job_fingerprint: str,
    range_start: str,
    range_end_exclusive: str,
    checkpoint_payload_sha256: str,
    checkpoint_version: int,
    window_metadata: list[Mapping[str, object]],
) -> dict[str, object]:
    if (
        _CODE_REVISION_RE.fullmatch(code_revision) is None
        or _SHA256_RE.fullmatch(checkpoint_payload_sha256) is None
        or len(window_metadata) != 30
        or checkpoint_version < 1
    ):
        raise FrozenReplayBundleError("apply bundle final binding is incomplete")
    normalized_windows = [dict(row) for row in window_metadata]
    try:
        cursor = date.fromisoformat(range_start)
        final_boundary = date.fromisoformat(range_end_exclusive)
    except ValueError as exc:
        raise FrozenReplayBundleError("apply bundle range is invalid") from exc
    for index, row in enumerate(normalized_windows):
        next_cursor = cursor + timedelta(days=1)
        if (
            row.get("index") != index
            or row.get("window_start") != cursor.isoformat()
            or row.get("window_end_exclusive") != next_cursor.isoformat()
        ):
            raise FrozenReplayBundleError(
                "apply bundle windows are not consecutive"
            )
        cursor = next_cursor
    if cursor != final_boundary:
        raise FrozenReplayBundleError("apply bundle range is incomplete")
    producer_run_id = os.environ.get("GITHUB_RUN_ID", "").strip()
    producer_run_attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "").strip()
    if not producer_run_id.isdecimal() or not producer_run_attempt.isdecimal():
        raise FrozenReplayBundleError("GitHub producer run identity is missing")
    manifest: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "kind": BUNDLE_KIND,
        "source": "dart",
        "mode": "apply",
        "bundle_status": "complete",
        "code_revision": code_revision,
        "producer": {
            "repository": os.environ.get("GITHUB_REPOSITORY", ""),
            "workflow": "official-backfill.yml",
            "run_id": int(producer_run_id),
            "run_attempt": int(producer_run_attempt),
        },
        "job": {
            "fingerprint": job_fingerprint,
            "range_start": range_start,
            "range_end_exclusive": range_end_exclusive,
            "chunk_days": 1,
            "source": "dart",
            "sync_company_master": False,
            "window_count": 30,
        },
        "checkpoint": {
            "payload_sha256": checkpoint_payload_sha256,
            "version": checkpoint_version,
        },
        "windows": normalized_windows,
        "artifact_sanitization": {
            "status": "verified",
            "data_classification": "official_public_metadata",
            "contains_provider_response_body": False,
            "contains_full_document_body": False,
            "contains_credentials": False,
        },
    }
    manifest["leaf_sequence_sha256"] = canonical_sha256(
        [row.get("sha256") for row in normalized_windows]
    )
    _assert_public_safe(manifest, location="manifest")
    root = root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    raw = canonical_json_bytes(manifest) + b"\n"
    destination = root / "manifest.json"
    temporary = root / "manifest.json.tmp"
    temporary.write_bytes(raw)
    temporary.replace(destination)
    return manifest


def write_partial_apply_bundle(
    root: Path,
    *,
    code_revision: str,
    job_fingerprint: str,
    range_start: str,
    range_end_exclusive: str,
    checkpoint_payload_sha256: str,
    checkpoint_version: int,
    completed_window_metadata: list[Mapping[str, object]],
) -> dict[str, object]:
    """Persist resumable leaf provenance after each checkpoint CAS.

    A partial manifest is never release evidence. It exists only so an
    ``always()`` checkpoint artifact can restore already-ACKed leaf bytes
    without re-fetching or silently manufacturing them.
    """

    if (
        _CODE_REVISION_RE.fullmatch(code_revision) is None
        or _SHA256_RE.fullmatch(checkpoint_payload_sha256) is None
        or checkpoint_version < 1
        or len(completed_window_metadata) > 30
    ):
        raise FrozenReplayBundleError("partial frozen apply binding is invalid")
    producer_run_id = os.environ.get("GITHUB_RUN_ID", "").strip()
    producer_run_attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "").strip()
    if not producer_run_id.isdecimal() or not producer_run_attempt.isdecimal():
        raise FrozenReplayBundleError("GitHub producer run identity is missing")
    manifest: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "kind": BUNDLE_KIND,
        "source": "dart",
        "mode": "apply",
        "bundle_status": "partial",
        "code_revision": code_revision,
        "producer": {
            "repository": os.environ.get("GITHUB_REPOSITORY", ""),
            "workflow": "official-backfill.yml",
            "run_id": int(producer_run_id),
            "run_attempt": int(producer_run_attempt),
        },
        "job": {
            "fingerprint": job_fingerprint,
            "range_start": range_start,
            "range_end_exclusive": range_end_exclusive,
            "chunk_days": 1,
            "source": "dart",
            "sync_company_master": False,
            "window_count": 30,
        },
        "checkpoint": {
            "payload_sha256": checkpoint_payload_sha256,
            "version": checkpoint_version,
        },
        "windows": [dict(row) for row in completed_window_metadata],
        "artifact_sanitization": {
            "status": "verified",
            "data_classification": "official_public_metadata",
            "contains_provider_response_body": False,
            "contains_full_document_body": False,
            "contains_credentials": False,
        },
    }
    leaf_sequence = [
        row.get("sha256") for row in completed_window_metadata
    ]
    manifest["leaf_sequence_sha256"] = canonical_sha256(leaf_sequence)
    _assert_public_safe(manifest, location="partial_manifest")
    root = root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    temporary = root / "manifest.json.tmp"
    temporary.write_bytes(canonical_json_bytes(manifest) + b"\n")
    temporary.replace(root / "manifest.json")
    return manifest


def _validate_binding(
    binding: Mapping[str, object],
    *,
    manifest: Mapping[str, object],
    manifest_raw: bytes,
) -> None:
    supplied_hash = str(binding.get("binding_sha256") or "")
    unsigned = {
        key: value for key, value in binding.items() if key != "binding_sha256"
    }
    artifact = binding.get("artifact")
    binding_producer = binding.get("producer")
    manifest_producer = manifest.get("producer")
    job = manifest.get("job")
    checkpoint = manifest.get("checkpoint")
    windows = manifest.get("windows")
    expected_leaf_hashes = (
        [row.get("sha256") for row in windows if isinstance(row, Mapping)]
        if isinstance(windows, list)
        else []
    )
    if (
        binding.get("schema_version") != SCHEMA_VERSION
        or binding.get("kind") != ARTIFACT_BINDING_KIND
        or not isinstance(artifact, Mapping)
        or not isinstance(binding_producer, Mapping)
        or not isinstance(manifest_producer, Mapping)
        or binding_producer.get("run_id")
        != manifest_producer.get("run_id")
        or binding_producer.get("run_attempt")
        != manifest_producer.get("run_attempt")
        or str(artifact.get("name") or "")
        != (
            "official-dart-frozen-replay-apply-"
            f"{binding_producer.get('run_id')}-"
            f"{binding_producer.get('run_attempt')}"
        )
        or re.fullmatch(
            r"sha256:[0-9a-f]{64}",
            str(artifact.get("digest") or ""),
        )
        is None
        or not isinstance(job, Mapping)
        or not isinstance(checkpoint, Mapping)
        or binding.get("job_fingerprint") != job.get("fingerprint")
        or binding.get("range_start") != job.get("range_start")
        or binding.get("range_end_exclusive") != job.get("range_end_exclusive")
        or binding.get("checkpoint_payload_sha256")
        != checkpoint.get("payload_sha256")
        or binding.get("leaf_sha256") != expected_leaf_hashes
        or binding.get("manifest_sha256")
        != hashlib.sha256(manifest_raw).hexdigest()
        or not hmac.compare_digest(supplied_hash, canonical_sha256(unsigned))
    ):
        raise FrozenReplayBundleError("frozen artifact binding is invalid")
    consumer = binding.get("consumer")
    if (
        not isinstance(consumer, Mapping)
        or consumer.get("code_revision") != manifest.get("code_revision")
    ):
        raise FrozenReplayBundleError("frozen artifact consumer revision mismatch")


def load_bundle(
    root: Path,
    artifact_binding_path: Path,
    *,
    expected_code_revision: str,
    expected_job_fingerprint: str,
    expected_checkpoint_sha256: str,
) -> FrozenBundle:
    root = root.resolve()
    if not root.is_dir() or (root / "manifest.json").is_symlink():
        raise FrozenReplayBundleError("frozen bundle root is missing or unsafe")
    manifest, manifest_raw = _strict_object(
        root / "manifest.json",
        max_bytes=MAX_WINDOW_BYTES,
    )
    binding, _ = _strict_object(
        artifact_binding_path.resolve(),
        max_bytes=MAX_WINDOW_BYTES,
    )
    job = manifest.get("job")
    checkpoint = manifest.get("checkpoint")
    windows = manifest.get("windows")
    if (
        manifest.get("schema_version") != SCHEMA_VERSION
        or manifest.get("kind") != BUNDLE_KIND
        or manifest.get("source") != "dart"
        or manifest.get("mode") != "apply"
        or manifest.get("bundle_status") != "complete"
        or manifest.get("code_revision") != expected_code_revision
        or not isinstance(job, Mapping)
        or job.get("fingerprint") != expected_job_fingerprint
        or not isinstance(checkpoint, Mapping)
        or checkpoint.get("payload_sha256") != expected_checkpoint_sha256
        or not isinstance(windows, list)
        or len(windows) != 30
    ):
        raise FrozenReplayBundleError("frozen bundle manifest binding is invalid")
    expected_paths = {"manifest.json"}
    try:
        cursor = date.fromisoformat(str(job.get("range_start") or ""))
        range_end = date.fromisoformat(
            str(job.get("range_end_exclusive") or "")
        )
    except ValueError as exc:
        raise FrozenReplayBundleError("frozen bundle range is invalid") from exc
    if (range_end - cursor).days != 30:
        raise FrozenReplayBundleError("frozen bundle is not an exact 30-day range")
    for index, metadata in enumerate(windows):
        next_cursor = cursor + timedelta(days=1)
        if (
            not isinstance(metadata, Mapping)
            or metadata.get("index") != index
            or metadata.get("window_start") != cursor.isoformat()
            or metadata.get("window_end_exclusive")
            != next_cursor.isoformat()
            or re.fullmatch(
                r"official-backfill-v1:[0-9a-f]{32}",
                str(metadata.get("idempotency_key") or ""),
            )
            is None
            or _SHA256_RE.fullmatch(
                str(metadata.get("stable_payload_sha256") or "")
            )
            is None
        ):
            raise FrozenReplayBundleError("frozen window sequence is invalid")
        relative = PurePosixPath(str(metadata.get("path") or ""))
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or relative.parts[:1] != ("windows",)
            or len(relative.parts) != 2
        ):
            raise FrozenReplayBundleError("frozen window path is unsafe")
        path = (root / Path(*relative.parts)).resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise FrozenReplayBundleError("frozen window escaped bundle root") from exc
        if path.is_symlink() or not path.is_file():
            raise FrozenReplayBundleError("frozen window is missing or unsafe")
        raw = path.read_bytes()
        if (
            len(raw) != metadata.get("byte_size")
            or hashlib.sha256(raw).hexdigest() != metadata.get("sha256")
        ):
            raise FrozenReplayBundleError("frozen window file digest mismatch")
        expected_paths.add(relative.as_posix())
        cursor = next_cursor
    if cursor != range_end:
        raise FrozenReplayBundleError("frozen window range is incomplete")
    actual_paths = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() or path.is_symlink()
    }
    if actual_paths != expected_paths:
        raise FrozenReplayBundleError("frozen bundle has extra or missing files")
    if sum((root / path).stat().st_size for path in expected_paths) > MAX_BUNDLE_BYTES:
        raise FrozenReplayBundleError("frozen bundle exceeds 25 MB")
    _validate_binding(binding, manifest=manifest, manifest_raw=manifest_raw)
    return FrozenBundle(root=root, manifest=manifest, artifact_binding=binding)


def load_window(bundle: FrozenBundle, *, index: int) -> dict[str, object]:
    windows = bundle.manifest["windows"]
    assert isinstance(windows, list)
    metadata = windows[index]
    assert isinstance(metadata, Mapping)
    relative = PurePosixPath(str(metadata["path"]))
    leaf, raw = _strict_object(
        bundle.root / Path(*relative.parts),
        max_bytes=MAX_WINDOW_BYTES,
    )
    payload = leaf.get("payload")
    manifest_job = bundle.manifest.get("job")
    if (
        leaf.get("schema_version") != SCHEMA_VERSION
        or leaf.get("kind") != WINDOW_KIND
        or leaf.get("code_revision") != bundle.manifest.get("code_revision")
        or not isinstance(manifest_job, Mapping)
        or leaf.get("job_fingerprint") != manifest_job.get("fingerprint")
        or not isinstance(payload, Mapping)
        or stable_payload_sha256(payload)
        != leaf.get("stable_payload_sha256")
        or public_payload_semantic_sha256(payload)
        != leaf.get("public_payload_sha256")
        or hashlib.sha256(raw).hexdigest() != metadata.get("sha256")
        or leaf.get("window_start") != metadata.get("window_start")
        or leaf.get("window_end_exclusive")
        != metadata.get("window_end_exclusive")
        or leaf.get("idempotency_key") != metadata.get("idempotency_key")
        or leaf.get("stable_payload_sha256")
        != metadata.get("stable_payload_sha256")
    ):
        raise FrozenReplayBundleError("frozen window content binding is invalid")
    return leaf


def compare_fresh_payload(
    expected_leaf: Mapping[str, object],
    actual_payload: Mapping[str, object],
    *,
    actual_source_counts: Mapping[str, int],
) -> dict[str, object]:
    expected_payload = expected_leaf.get("payload")
    expected_counts = expected_leaf.get("source_semantic_counts")
    if not isinstance(expected_payload, Mapping) or not isinstance(
        expected_counts, Mapping
    ):
        raise FrozenReplayBundleError("frozen window lacks semantic comparison data")
    actual = public_dart_payload(actual_payload)
    changes: list[dict[str, object]] = []
    for kind in ("companies", "documents", "events"):
        identity = _ID_FIELDS[kind]
        expected_rows = {
            str(row[identity]): _semantic_row(kind, row)
            for row in _records(expected_payload, kind)
        }
        actual_rows = {
            str(row[identity]): _semantic_row(kind, row)
            for row in _records(actual, kind)
        }
        for record_id in sorted(set(expected_rows) | set(actual_rows)):
            expected = expected_rows.get(record_id)
            observed = actual_rows.get(record_id)
            if expected == observed:
                continue
            fields = (
                ["$record"]
                if expected is None or observed is None
                else sorted(
                    key
                    for key in set(expected) | set(observed)
                    if expected.get(key) != observed.get(key)
                )
            )
            changes.append(
                {
                    "entity": kind[:-1],
                    "id": record_id,
                    "field_names": fields,
                    "expected_leaf_sha256": (
                        canonical_sha256(expected) if expected is not None else None
                    ),
                    "actual_leaf_sha256": (
                        canonical_sha256(observed) if observed is not None else None
                    ),
                }
            )
    normalized_actual_counts = {
        key: int(actual_source_counts.get(key, -1))
        for key in expected_counts
    }
    if dict(expected_counts) != normalized_actual_counts:
        changes.append(
            {
                "entity": "source_counts",
                "id": "dart",
                "field_names": sorted(
                    key
                    for key in set(expected_counts) | set(normalized_actual_counts)
                    if expected_counts.get(key) != normalized_actual_counts.get(key)
                ),
                "expected_leaf_sha256": canonical_sha256(dict(expected_counts)),
                "actual_leaf_sha256": canonical_sha256(normalized_actual_counts),
            }
        )
    expected_stable_payload_sha256 = str(
        expected_leaf.get("stable_payload_sha256") or ""
    )
    actual_stable_payload_sha256 = stable_payload_sha256(actual)
    diagnostic_change_count = sum(
        1 for change in changes if change.get("entity") == "source_counts"
    )
    blocking_change_count = len(changes) - diagnostic_change_count
    release_gate_matched = (
        expected_stable_payload_sha256 == actual_stable_payload_sha256
        and blocking_change_count == 0
    )
    return {
        "matched": not changes,
        "release_gate_policy": PROBE_RELEASE_GATE_POLICY,
        "release_gate_matched": release_gate_matched,
        "diagnostic_change_count": diagnostic_change_count,
        "blocking_change_count": blocking_change_count,
        "expected_stable_payload_sha256": expected_stable_payload_sha256,
        "actual_stable_payload_sha256": actual_stable_payload_sha256,
        "changed_entity_count": len(changes),
        "changes": changes,
    }


def _probe_release_gate_summary(
    windows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    diagnostic_only_window_count = 0
    blocking_drift_window_count = 0
    failed_window_count = 0
    for raw_window in windows:
        if raw_window.get("status") == "probe_failed":
            failed_window_count += 1
            continue
        changes = raw_window.get("changes")
        expected_digest = str(
            raw_window.get("expected_stable_payload_sha256") or ""
        )
        actual_digest = str(
            raw_window.get("actual_stable_payload_sha256") or ""
        )
        if not isinstance(changes, list):
            blocking_drift_window_count += 1
            continue
        entities = [
            change.get("entity")
            for change in changes
            if isinstance(change, Mapping)
        ]
        stable_payload_matched = (
            _SHA256_RE.fullmatch(expected_digest) is not None
            and hmac.compare_digest(expected_digest, actual_digest)
        )
        source_counts_only = bool(entities) and all(
            entity == "source_counts" for entity in entities
        )
        if stable_payload_matched and source_counts_only:
            diagnostic_only_window_count += 1
        elif not stable_payload_matched or any(
            entity != "source_counts" for entity in entities
        ):
            blocking_drift_window_count += 1
    return {
        "release_gate_policy": PROBE_RELEASE_GATE_POLICY,
        "release_gate_matched": (
            len(windows) == 30
            and failed_window_count == 0
            and blocking_drift_window_count == 0
        ),
        "diagnostic_only_window_count": diagnostic_only_window_count,
        "blocking_drift_window_count": blocking_drift_window_count,
    }


def write_probe_report(
    path: Path,
    *,
    code_revision: str,
    job_fingerprint: str,
    range_start: str,
    range_end_exclusive: str,
    status: str,
    windows: Sequence[Mapping[str, object]],
    error_code: str | None = None,
    quota_ledger_write_attempted: bool = True,
) -> dict[str, object]:
    if status not in {"matched", "drift_detected", "probe_failed"}:
        raise FrozenReplayBundleError("invalid drift probe status")
    release_gate = _probe_release_gate_summary(windows)
    release_gate["release_gate_matched"] = (
        release_gate["release_gate_matched"] is True
        and status in {"matched", "drift_detected"}
        and error_code is None
        and quota_ledger_write_attempted is True
    )
    report: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "kind": PROBE_KIND,
        "source": "dart",
        "code_revision": code_revision,
        "range_start": range_start,
        "range_end_exclusive": range_end_exclusive,
        "job_fingerprint": job_fingerprint,
        "read_only": True,
        "governance_write_attempted": False,
        "checkpoint_write_attempted": False,
        # The live source probe consumes the same durable quota permits as any
        # other physical OpenDART request. That ledger write is acknowledged
        # explicitly rather than hidden behind a generic read-only claim.
        "quota_ledger_write_attempted": quota_ledger_write_attempted,
        "status": status,
        **release_gate,
        "window_count": len(windows),
        "windows": [dict(row) for row in windows],
        "error_code": error_code,
        "artifact_sanitization": {
            "status": "verified",
            "contains_provider_response_body": False,
            "contains_credentials": False,
        },
    }
    _assert_public_safe(report, location="probe")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(canonical_json_bytes(report) + b"\n")
    temporary.replace(path)
    return report


def _validator_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate private frozen DART replay evidence."
    )
    parser.add_argument(
        "command",
        choices=(
            "build-binding",
            "validate-apply",
            "validate-replay",
            "validate-probe",
            "validate-resume",
        ),
    )
    parser.add_argument("--path", type=Path, required=True)
    parser.add_argument("--artifact-binding", type=Path)
    parser.add_argument("--expected-code-revision")
    parser.add_argument("--expected-from-date")
    parser.add_argument("--expected-to-date")
    parser.add_argument("--expected-job-fingerprint")
    parser.add_argument("--expected-checkpoint-sha256")
    parser.add_argument("--github-output", type=Path)
    parser.add_argument("--replay-state", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--artifact-id", type=int)
    parser.add_argument("--artifact-name")
    parser.add_argument("--artifact-digest")
    parser.add_argument("--artifact-created-at")
    parser.add_argument("--producer-run-id", type=int)
    parser.add_argument("--producer-run-attempt", type=int)
    parser.add_argument("--producer-run-started-at")
    parser.add_argument("--consumer-repository")
    parser.add_argument("--consumer-workflow", default="official-backfill.yml")
    parser.add_argument("--consumer-run-id", type=int)
    parser.add_argument("--consumer-run-attempt", type=int)
    parser.add_argument("--freshness-hours", type=int, default=72)
    return parser


def _validator_expected(value: object, expected: str | None, label: str) -> None:
    if expected is not None and value != expected:
        raise FrozenReplayBundleError(f"{label} does not match the expected value")


def _write_validator_outputs(
    path: Path | None,
    values: Mapping[str, object],
) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as output:
        for key, value in values.items():
            text = str(value).casefold() if isinstance(value, bool) else str(value)
            if "\n" in text or "\r" in text:
                raise FrozenReplayBundleError("validator output contains a newline")
            output.write(f"{key}={text}\n")


def _validate_apply_tree(args: argparse.Namespace) -> dict[str, object]:
    root = args.path.resolve()
    manifest_path = root / "manifest.json" if root.is_dir() else root
    manifest, manifest_raw = _strict_object(
        manifest_path,
        max_bytes=MAX_WINDOW_BYTES,
    )
    job = manifest.get("job")
    checkpoint = manifest.get("checkpoint")
    windows = manifest.get("windows")
    if (
        manifest.get("schema_version") != SCHEMA_VERSION
        or manifest.get("kind") != BUNDLE_KIND
        or manifest.get("source") != "dart"
        or manifest.get("mode") != "apply"
        or manifest.get("bundle_status") != "complete"
        or not isinstance(job, Mapping)
        or not isinstance(checkpoint, Mapping)
        or not isinstance(windows, list)
        or len(windows) != 30
    ):
        raise FrozenReplayBundleError("frozen apply manifest contract is invalid")
    _validator_expected(
        manifest.get("code_revision"),
        args.expected_code_revision,
        "code revision",
    )
    _validator_expected(
        job.get("range_start"),
        args.expected_from_date,
        "range start",
    )
    _validator_expected(
        job.get("range_end_exclusive"),
        args.expected_to_date,
        "range end",
    )
    _validator_expected(
        job.get("fingerprint"),
        args.expected_job_fingerprint,
        "job fingerprint",
    )
    _validator_expected(
        checkpoint.get("payload_sha256"),
        args.expected_checkpoint_sha256,
        "checkpoint digest",
    )
    if args.artifact_binding is not None:
        if not all(
            (
                args.expected_code_revision,
                args.expected_job_fingerprint,
                args.expected_checkpoint_sha256,
            )
        ):
            raise FrozenReplayBundleError(
                "artifact validation requires expected revision, job, and checkpoint"
            )
        load_bundle(
            root,
            args.artifact_binding,
            expected_code_revision=args.expected_code_revision,
            expected_job_fingerprint=args.expected_job_fingerprint,
            expected_checkpoint_sha256=args.expected_checkpoint_sha256,
        )
    else:
        # Producer-side validation still verifies every leaf. The GitHub
        # artifact identity does not exist until after upload.
        expected_paths = {"manifest.json"}
        try:
            cursor = date.fromisoformat(str(job.get("range_start") or ""))
            range_end = date.fromisoformat(
                str(job.get("range_end_exclusive") or "")
            )
        except ValueError as exc:
            raise FrozenReplayBundleError(
                "frozen apply range is invalid"
            ) from exc
        if (range_end - cursor).days != 30:
            raise FrozenReplayBundleError(
                "frozen apply range is not exactly 30 days"
            )
        for index, metadata in enumerate(windows):
            next_cursor = cursor + timedelta(days=1)
            if (
                not isinstance(metadata, Mapping)
                or metadata.get("index") != index
                or metadata.get("window_start") != cursor.isoformat()
                or metadata.get("window_end_exclusive")
                != next_cursor.isoformat()
            ):
                raise FrozenReplayBundleError("frozen apply window order is invalid")
            relative = PurePosixPath(str(metadata.get("path") or ""))
            if (
                relative.is_absolute()
                or ".." in relative.parts
                or relative.parts[:1] != ("windows",)
                or len(relative.parts) != 2
            ):
                raise FrozenReplayBundleError("frozen apply window path is unsafe")
            leaf_path = root / Path(*relative.parts)
            leaf, raw = _strict_object(leaf_path, max_bytes=MAX_WINDOW_BYTES)
            if (
                leaf.get("kind") != WINDOW_KIND
                or leaf.get("job_fingerprint") != job.get("fingerprint")
                or leaf.get("code_revision") != manifest.get("code_revision")
                or len(raw) != metadata.get("byte_size")
                or hashlib.sha256(raw).hexdigest() != metadata.get("sha256")
            ):
                raise FrozenReplayBundleError("frozen apply leaf binding is invalid")
            expected_paths.add(relative.as_posix())
            cursor = next_cursor
        if cursor != range_end:
            raise FrozenReplayBundleError("frozen apply window range is incomplete")
        actual_paths = {
            path.relative_to(root).as_posix()
            for path in root.rglob("*")
            if path.is_file() or path.is_symlink()
        }
        if actual_paths != expected_paths:
            raise FrozenReplayBundleError(
                "frozen apply bundle has an extra or missing file"
            )
    return {
        "evidence_safe": True,
        "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
        "window_count": len(windows),
    }


def validate_probe_contract(
    probe: Mapping[str, object],
    *,
    expected_code_revision: str | None = None,
    expected_range_start: str | None = None,
    expected_range_end_exclusive: str | None = None,
    expected_job_fingerprint: str | None = None,
) -> dict[str, object]:
    """Validate and rederive the fresh 30-day DART drift conclusion."""

    status = probe.get("status")
    sanitization = probe.get("artifact_sanitization")
    windows = probe.get("windows")
    diagnostic_only_window_count = probe.get("diagnostic_only_window_count")
    blocking_drift_window_count = probe.get("blocking_drift_window_count")
    if (
        probe.get("schema_version") != SCHEMA_VERSION
        or probe.get("kind") != PROBE_KIND
        or probe.get("source") != "dart"
        or probe.get("release_gate_policy") != PROBE_RELEASE_GATE_POLICY
        or type(probe.get("release_gate_matched")) is not bool
        or isinstance(diagnostic_only_window_count, bool)
        or not isinstance(diagnostic_only_window_count, int)
        or diagnostic_only_window_count < 0
        or isinstance(blocking_drift_window_count, bool)
        or not isinstance(blocking_drift_window_count, int)
        or blocking_drift_window_count < 0
        or _CODE_REVISION_RE.fullmatch(
            str(probe.get("code_revision") or "")
        )
        is None
        or _SHA256_RE.fullmatch(
            str(probe.get("job_fingerprint") or "")
        )
        is None
        or probe.get("read_only") is not True
        or probe.get("governance_write_attempted") is not False
        or probe.get("checkpoint_write_attempted") is not False
        or type(probe.get("quota_ledger_write_attempted")) is not bool
        or status not in {"matched", "drift_detected", "probe_failed"}
        or not isinstance(windows, list)
        or isinstance(probe.get("window_count"), bool)
        or not isinstance(probe.get("window_count"), int)
        or probe.get("window_count") != len(windows)
        or not isinstance(sanitization, Mapping)
        or sanitization.get("status") != "verified"
        or sanitization.get("contains_provider_response_body") is not False
        or sanitization.get("contains_credentials") is not False
    ):
        raise FrozenReplayBundleError("DART drift probe contract is invalid")
    _validator_expected(
        probe.get("code_revision"),
        expected_code_revision,
        "code revision",
    )
    _validator_expected(
        probe.get("range_start"),
        expected_range_start,
        "range start",
    )
    _validator_expected(
        probe.get("range_end_exclusive"),
        expected_range_end_exclusive,
        "range end",
    )
    _validator_expected(
        probe.get("job_fingerprint"),
        expected_job_fingerprint,
        "job fingerprint",
    )
    try:
        cursor = date.fromisoformat(str(probe.get("range_start") or ""))
        range_end = date.fromisoformat(
            str(probe.get("range_end_exclusive") or "")
        )
    except ValueError as exc:
        raise FrozenReplayBundleError(
            "DART drift probe range is invalid"
        ) from exc
    if (
        probe.get("range_start") != cursor.isoformat()
        or probe.get("range_end_exclusive") != range_end.isoformat()
        or (range_end - cursor).days != 30
    ):
        raise FrozenReplayBundleError(
            "DART drift probe range must cover exactly 30 days"
        )
    matched_count = 0
    drift_count = 0
    failure_count = 0
    derived_diagnostic_only_window_count = 0
    derived_blocking_drift_window_count = 0
    failure_code: str | None = None
    for index, raw_window in enumerate(windows):
        if not isinstance(raw_window, Mapping):
            raise FrozenReplayBundleError(
                "DART drift probe window is invalid"
            )
        next_cursor = cursor + timedelta(days=1)
        window_index = raw_window.get("index")
        if (
            isinstance(window_index, bool)
            or not isinstance(window_index, int)
            or window_index != index
            or raw_window.get("window_start") != cursor.isoformat()
            or raw_window.get("window_end_exclusive")
            != next_cursor.isoformat()
            or type(raw_window.get("matched")) is not bool
        ):
            raise FrozenReplayBundleError(
                "DART drift probe window boundary is invalid"
            )
        if raw_window.get("status") == "probe_failed":
            error_code = raw_window.get("error_code")
            if (
                raw_window.get("matched") is not False
                or index != len(windows) - 1
                or not isinstance(error_code, str)
                or re.fullmatch(r"[a-z0-9_]{1,64}", error_code) is None
            ):
                raise FrozenReplayBundleError(
                    "DART drift probe failure row is invalid"
                )
            failure_count += 1
            failure_code = error_code
            cursor = next_cursor
            continue
        expected_digest = str(
            raw_window.get("expected_stable_payload_sha256") or ""
        )
        actual_digest = str(
            raw_window.get("actual_stable_payload_sha256") or ""
        )
        changed_count = raw_window.get("changed_entity_count")
        changes = raw_window.get("changes")
        execution = raw_window.get("probe_execution")
        if (
            _SHA256_RE.fullmatch(expected_digest) is None
            or _SHA256_RE.fullmatch(actual_digest) is None
            or isinstance(changed_count, bool)
            or not isinstance(changed_count, int)
            or changed_count < 0
            or not isinstance(changes, list)
            or len(changes) != changed_count
            or not all(isinstance(change, Mapping) for change in changes)
            or not isinstance(execution, Mapping)
        ):
            raise FrozenReplayBundleError(
                "DART drift probe comparison row is invalid"
            )
        diagnostic_change_count = sum(
            1
            for change in changes
            if change.get("entity") == "source_counts"
        )
        blocking_change_count = len(changes) - diagnostic_change_count
        stable_payload_matched = hmac.compare_digest(
            expected_digest,
            actual_digest,
        )
        derived_release_gate_matched = (
            stable_payload_matched and blocking_change_count == 0
        )
        if (
            raw_window.get("release_gate_policy")
            != PROBE_RELEASE_GATE_POLICY
            or raw_window.get("release_gate_matched")
            is not derived_release_gate_matched
            or raw_window.get("diagnostic_change_count")
            != diagnostic_change_count
            or raw_window.get("blocking_change_count")
            != blocking_change_count
        ):
            raise FrozenReplayBundleError(
                "DART drift probe release-gate classification is not derived "
                "from raw changes and stable payload hashes"
            )
        source_requests = execution.get("source_requests")
        source_pages = execution.get("source_pages")
        source_rows = execution.get("source_rows_fetched")
        if (
            isinstance(source_requests, bool)
            or not isinstance(source_requests, int)
            or source_requests < 1
            or isinstance(source_pages, bool)
            or not isinstance(source_pages, int)
            or source_pages < 1
            or isinstance(source_rows, bool)
            or not isinstance(source_rows, int)
            or source_rows < 0
        ):
            raise FrozenReplayBundleError(
                "DART drift probe execution receipt is invalid"
            )
        if raw_window.get("matched") is True:
            if (
                changed_count != 0
                or changes
                or not stable_payload_matched
            ):
                raise FrozenReplayBundleError(
                    "DART drift probe matched row contains drift"
                )
            matched_count += 1
        else:
            if changed_count < 1 or not changes:
                raise FrozenReplayBundleError(
                    "DART drift probe drift row has no changed entity"
                )
            drift_count += 1
            if derived_release_gate_matched:
                derived_diagnostic_only_window_count += 1
            else:
                derived_blocking_drift_window_count += 1
        cursor = next_cursor
    top_error = probe.get("error_code")
    fully_matched = (
        status == "matched"
        and len(windows) == 30
        and cursor == range_end
        and matched_count == 30
        and drift_count == 0
        and failure_count == 0
        and top_error is None
        and probe.get("quota_ledger_write_attempted") is True
    )
    fully_drifted = (
        status == "drift_detected"
        and len(windows) == 30
        and cursor == range_end
        and matched_count + drift_count == 30
        and drift_count >= 1
        and failure_count == 0
        and top_error is None
    )
    structurally_failed = (
        status == "probe_failed"
        and 1 <= len(windows) <= 30
        and failure_count == 1
        and failure_code == top_error
    )
    if not (fully_matched or fully_drifted or structurally_failed):
        raise FrozenReplayBundleError(
            "DART drift probe status is not derived from its window evidence"
        )
    release_gate_matched = (
        status in {"matched", "drift_detected"}
        and len(windows) == 30
        and cursor == range_end
        and matched_count + drift_count == 30
        and failure_count == 0
        and top_error is None
        and probe.get("quota_ledger_write_attempted") is True
        and derived_blocking_drift_window_count == 0
    )
    if (
        probe.get("release_gate_matched") is not release_gate_matched
        or diagnostic_only_window_count
        != derived_diagnostic_only_window_count
        or blocking_drift_window_count
        != derived_blocking_drift_window_count
    ):
        raise FrozenReplayBundleError(
            "DART drift probe release-gate summary is not derived from raw "
            "window evidence"
        )
    return {
        "status": status,
        "release_gate_policy": PROBE_RELEASE_GATE_POLICY,
        "release_gate_matched": release_gate_matched,
        "window_count": len(windows),
        "matched_window_count": matched_count,
        "drift_window_count": drift_count,
        "failed_window_count": failure_count,
        "diagnostic_only_window_count": derived_diagnostic_only_window_count,
        "blocking_drift_window_count": derived_blocking_drift_window_count,
        "fully_matched": fully_matched,
    }


def _validate_probe(args: argparse.Namespace) -> dict[str, object]:
    probe, raw = _strict_object(args.path, max_bytes=2_000_000)
    expected = (
        args.expected_code_revision,
        args.expected_from_date,
        args.expected_to_date,
        args.expected_job_fingerprint,
    )
    if any(value is None for value in expected):
        raise FrozenReplayBundleError(
            "release DART drift probe validation requires exact expected bindings"
        )
    validation = validate_probe_contract(
        probe,
        expected_code_revision=args.expected_code_revision,
        expected_range_start=args.expected_from_date,
        expected_range_end_exclusive=args.expected_to_date,
        expected_job_fingerprint=args.expected_job_fingerprint,
    )
    if validation.get("release_gate_matched") is not True:
        raise FrozenReplayBundleError(
            "release DART drift probe has blocking public payload drift"
        )
    return {
        "evidence_safe": True,
        "sha256": hashlib.sha256(raw).hexdigest(),
        **validation,
    }


def _validate_replay_report(args: argparse.Namespace) -> dict[str, object]:
    report, raw = _strict_object(args.path, max_bytes=5_000_000)
    before = report.get("checkpoint_before")
    after = report.get("checkpoint_after")
    if (
        report.get("status") != "succeeded"
        or report.get("mode") != "replay"
        or report.get("idempotent") is not True
        or report.get("replay_verified") is not True
        or report.get("windows_total") != 30
        or report.get("windows_succeeded") != 30
        or report.get("windows_failed") != 0
        or before != after
    ):
        raise FrozenReplayBundleError("frozen replay report contract is invalid")
    _validator_expected(
        report.get("code_revision"),
        args.expected_code_revision,
        "code revision",
    )
    _validator_expected(
        report.get("range_start"),
        args.expected_from_date,
        "range start",
    )
    _validator_expected(
        report.get("range_end_exclusive"),
        args.expected_to_date,
        "range end",
    )
    _validator_expected(
        report.get("job_fingerprint"),
        args.expected_job_fingerprint,
        "job fingerprint",
    )
    _validator_expected(
        report.get("checkpoint_payload_sha256"),
        args.expected_checkpoint_sha256,
        "checkpoint digest",
    )
    return {
        "evidence_safe": True,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "window_count": 30,
    }


def _validate_resume_tree(args: argparse.Namespace) -> dict[str, object]:
    def link_like(path: Path) -> bool:
        junction_check = getattr(path, "is_junction", None)
        return path.is_symlink() or bool(
            callable(junction_check) and junction_check()
        )

    requested_root = args.path.absolute()
    if link_like(requested_root):
        raise FrozenReplayBundleError(
            "partial frozen bundle root cannot be a link"
        )
    root = requested_root.resolve()
    if not root.is_dir():
        raise FrozenReplayBundleError("partial frozen bundle directory is missing")
    manifest_path = root / "manifest.json"
    windows_root = root / "windows"
    root_entries = list(root.iterdir())
    if (
        {entry.name for entry in root_entries} != {"manifest.json", "windows"}
        or any(link_like(entry) for entry in root_entries)
        or not manifest_path.is_file()
        or not windows_root.is_dir()
    ):
        raise FrozenReplayBundleError(
            "partial frozen bundle inventory contains an extra, missing, "
            "or linked entry"
        )
    manifest, _ = _strict_object(
        manifest_path, max_bytes=MAX_WINDOW_BYTES
    )
    manifest_job = manifest.get("job")
    manifest_checkpoint = manifest.get("checkpoint")
    completed_metadata = manifest.get("windows")
    expected_range_start = getattr(args, "expected_from_date", None)
    expected_range_end = getattr(args, "expected_to_date", None)
    expected_checkpoint = getattr(
        args,
        "expected_checkpoint_sha256",
        None,
    )
    if (
        manifest.get("schema_version") != SCHEMA_VERSION
        or manifest.get("kind") != BUNDLE_KIND
        or manifest.get("source") != "dart"
        or manifest.get("mode") != "apply"
        or manifest.get("bundle_status") not in {"partial", "complete"}
        or manifest.get("code_revision") != args.expected_code_revision
        or not isinstance(manifest_job, Mapping)
        or not isinstance(manifest_checkpoint, Mapping)
        or (
            args.expected_job_fingerprint is not None
            and manifest_job.get("fingerprint")
            != args.expected_job_fingerprint
        )
        or (
            expected_range_start is not None
            and manifest_job.get("range_start") != expected_range_start
        )
        or (
            expected_range_end is not None
            and manifest_job.get("range_end_exclusive")
            != expected_range_end
        )
        or (
            expected_checkpoint is not None
            and manifest_checkpoint.get("payload_sha256")
            != expected_checkpoint
        )
        or _SHA256_RE.fullmatch(
            str(manifest_checkpoint.get("payload_sha256") or "")
        )
        is None
        or isinstance(manifest_checkpoint.get("version"), bool)
        or not isinstance(manifest_checkpoint.get("version"), int)
        or int(manifest_checkpoint["version"]) < 1
        or not isinstance(completed_metadata, list)
        or len(completed_metadata) > 30
        or (
            manifest.get("bundle_status") == "complete"
            and len(completed_metadata) != 30
        )
    ):
        raise FrozenReplayBundleError("partial frozen manifest binding is invalid")
    window_entries = list(windows_root.iterdir())
    if any(
        link_like(entry)
        or not entry.is_file()
        or entry.suffix != ".json"
        for entry in window_entries
    ):
        raise FrozenReplayBundleError(
            "partial frozen bundle windows inventory is invalid"
        )
    paths = sorted(window_entries)
    if not paths or len(paths) > 30:
        raise FrozenReplayBundleError("partial frozen bundle leaf count is invalid")
    total_bytes = manifest_path.stat().st_size + sum(
        path.stat().st_size for path in paths
    )
    if total_bytes > MAX_BUNDLE_BYTES:
        raise FrozenReplayBundleError("partial frozen bundle exceeds size limit")
    identities: set[tuple[str, str]] = set()
    leaf_indexes: set[int] = set()
    paths_by_relative: dict[
        str,
        tuple[dict[str, object], bytes, int],
    ] = {}
    for path in paths:
        leaf, raw = _strict_object(path, max_bytes=MAX_WINDOW_BYTES)
        identity = (
            str(leaf.get("window_start") or ""),
            str(leaf.get("window_end_exclusive") or ""),
        )
        filename_match = re.fullmatch(
            r"([0-2][0-9])-(\d{4}-\d{2}-\d{2})\.json",
            path.name,
        )
        leaf_index = (
            int(filename_match.group(1))
            if filename_match is not None
            else -1
        )
        payload = leaf.get("payload")
        if (
            filename_match is None
            or leaf_index in leaf_indexes
            or filename_match.group(2) != identity[0]
            or leaf.get("kind") != WINDOW_KIND
            or identity in identities
            or leaf.get("code_revision") != args.expected_code_revision
            or (
                args.expected_job_fingerprint is not None
                and leaf.get("job_fingerprint")
                != args.expected_job_fingerprint
            )
            or not isinstance(payload, Mapping)
            or stable_payload_sha256(payload)
            != leaf.get("stable_payload_sha256")
            or public_payload_semantic_sha256(payload)
            != leaf.get("public_payload_sha256")
        ):
            raise FrozenReplayBundleError("partial frozen leaf binding is invalid")
        leaf_indexes.add(leaf_index)
        identities.add(identity)
        paths_by_relative[path.relative_to(root).as_posix()] = (
            leaf,
            raw,
            leaf_index,
        )
    completed_indexes: set[int] = set()
    for metadata in completed_metadata:
        if not isinstance(metadata, Mapping):
            raise FrozenReplayBundleError(
                "partial completed leaf metadata is invalid"
            )
        relative = str(metadata.get("path") or "")
        bound = paths_by_relative.get(relative)
        if bound is None:
            raise FrozenReplayBundleError(
                "partial completed leaf digest binding is invalid"
            )
        metadata_index = metadata.get("index")
        bound_leaf, bound_raw, bound_index = bound
        bound_run = bound_leaf.get("run_contract")
        if (
            isinstance(metadata_index, bool)
            or not isinstance(metadata_index, int)
            or not 0 <= metadata_index < 30
            or metadata_index in completed_indexes
            or metadata_index != bound_index
            or len(bound_raw) != metadata.get("byte_size")
            or hashlib.sha256(bound_raw).hexdigest() != metadata.get("sha256")
            or bound_leaf.get("window_start")
            != metadata.get("window_start")
            or bound_leaf.get("window_end_exclusive")
            != metadata.get("window_end_exclusive")
            or bound_leaf.get("stable_payload_sha256")
            != metadata.get("stable_payload_sha256")
            or bound_leaf.get("public_payload_sha256")
            != metadata.get("public_payload_sha256")
            or bound_leaf.get("idempotency_key")
            != metadata.get("idempotency_key")
            or not isinstance(bound_run, Mapping)
            or bound_run.get("run_id") != metadata.get("run_id")
            or canonical_sha256(bound_leaf.get("source_semantic_counts"))
            != metadata.get("source_semantic_counts_sha256")
        ):
            raise FrozenReplayBundleError(
                "partial completed leaf digest binding is invalid"
            )
        completed_indexes.add(metadata_index)
    if manifest.get("leaf_sequence_sha256") != canonical_sha256(
        [
            metadata.get("sha256")
            for metadata in completed_metadata
            if isinstance(metadata, Mapping)
        ]
    ):
        raise FrozenReplayBundleError(
            "partial completed leaf sequence binding is invalid"
        )
    return {
        "evidence_safe": True,
        "window_count": len(paths),
        "completed_window_count": len(completed_metadata),
    }


def main(argv: list[str] | None = None) -> None:
    args = _validator_parser().parse_args(argv)
    try:
        if args.command == "build-binding":
            required = {
                "replay_state": args.replay_state,
                "output": args.output,
                "artifact_id": args.artifact_id,
                "artifact_name": args.artifact_name,
                "artifact_digest": args.artifact_digest,
                "artifact_created_at": args.artifact_created_at,
                "producer_run_id": args.producer_run_id,
                "producer_run_attempt": args.producer_run_attempt,
                "producer_run_started_at": args.producer_run_started_at,
                "consumer_repository": args.consumer_repository,
                "consumer_run_id": args.consumer_run_id,
                "consumer_run_attempt": args.consumer_run_attempt,
                "expected_code_revision": args.expected_code_revision,
                "expected_from_date": args.expected_from_date,
                "expected_to_date": args.expected_to_date,
            }
            missing = sorted(
                key for key, value in required.items() if value in (None, "")
            )
            if missing:
                raise FrozenReplayBundleError(
                    "build-binding is missing: " + ", ".join(missing)
                )
            binding = build_artifact_binding(
                bundle_root=args.path,
                replay_state_path=args.replay_state,
                output_path=args.output,
                artifact_id=args.artifact_id,
                artifact_name=args.artifact_name,
                artifact_digest=args.artifact_digest,
                artifact_created_at=args.artifact_created_at,
                producer_run_id=args.producer_run_id,
                producer_run_attempt=args.producer_run_attempt,
                producer_run_started_at=args.producer_run_started_at,
                consumer_repository=args.consumer_repository,
                consumer_workflow=args.consumer_workflow,
                consumer_run_id=args.consumer_run_id,
                consumer_run_attempt=args.consumer_run_attempt,
                consumer_code_revision=args.expected_code_revision,
                expected_range_start=args.expected_from_date,
                expected_range_end_exclusive=args.expected_to_date,
                freshness_hours=args.freshness_hours,
            )
            outputs = {
                "evidence_safe": True,
                "binding_sha256": binding["binding_sha256"],
            }
        elif args.command == "validate-apply":
            outputs = _validate_apply_tree(args)
        elif args.command == "validate-replay":
            outputs = _validate_replay_report(args)
        elif args.command == "validate-probe":
            outputs = _validate_probe(args)
        else:
            outputs = _validate_resume_tree(args)
        _write_validator_outputs(args.github_output, outputs)
    except (FrozenReplayBundleError, OSError, ValueError) as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}), file=sys.stderr)
        raise SystemExit(2) from exc
    print(
        json.dumps(
            {"status": "verified", "command": args.command, **outputs},
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
