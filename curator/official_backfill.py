from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Mapping, Protocol
from zoneinfo import ZoneInfo

from .backfill_checkpoint_api import (
    RemoteCheckpointClient,
    RemoteCheckpointError,
    RemoteCheckpointSnapshot,
    RemoteCheckpointWrite,
    canonical_checkpoint,
    checkpoint_api_configured,
    checkpoint_payload_hash,
)
from .dart_quota import (
    durable_dart_quota_client,
    durable_dart_quota_configured,
    durable_dart_quota_required,
)
from .dart_frozen_replay_bundle import (
    artifact_binding_checkpoint_sha256,
    FrozenBundle,
    FrozenReplayBundleError,
    build_window_leaf,
    compare_fresh_payload,
    finalize_apply_bundle,
    load_bundle,
    load_window,
    source_semantic_counts,
    write_partial_apply_bundle,
    write_probe_report,
    write_window_leaf,
)
from .config import load_config
from .official_ingest import (
    replay_frozen_dart_window,
    run as run_official_ingest,
)
from .official_source_rights import (
    DartOfficialSourceRightClient,
    OfficialSourceRightError,
    source_right_api_configured,
)
from .official_sources import (
    DartInvocationQuota,
    DartRequestBudget,
    DartRequestQuota,
)
from .opendart_credentials import (
    OpenDartCredentialConfigurationError,
    load_opendart_credentials,
)
from .remote_api import remote_api_configured


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MIN_BACKFILL_DATE = date(2021, 1, 1)
DEFAULT_CHECKPOINT_PATH = Path("data/backfill_official_checkpoint.json")
CHECKPOINT_SCHEMA_VERSION = 1
_CODE_REVISION_RE = re.compile(r"^[a-f0-9]{7,40}$")


class BackfillConfigurationError(ValueError):
    pass


class CheckpointError(RuntimeError):
    pass


def _completed_kst_end_exclusive(now: datetime | None = None) -> date:
    """Return the exclusive boundary that contains completed KST dates only."""

    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise BackfillConfigurationError("backfill clock must be timezone-aware")
    return current.astimezone(ZoneInfo("Asia/Seoul")).date()


@dataclass(frozen=True)
class DateWindow:
    """A half-open backfill window: ``start <= date < end_exclusive``."""

    start: date
    end_exclusive: date

    @property
    def key(self) -> str:
        return f"{self.start.isoformat()}:{self.end_exclusive.isoformat()}"

    @property
    def source_end_inclusive(self) -> date:
        return self.end_exclusive - timedelta(days=1)


@dataclass(frozen=True)
class BackfillOptions:
    start: date
    end_exclusive: date
    checkpoint_path: Path
    chunk_days: int = 1
    sources: tuple[str, ...] = ("dart",)
    page_count: int = 100
    max_pages: int = 100
    max_chunks: int = 0
    # Per invocation; the durable credential-pool ledger owns the separate
    # 40,000-request KST-day ceiling.
    request_budget: int = 10_000
    dry_run: bool = False
    replay: bool = False
    restart: bool = False
    continue_on_error: bool = False
    sync_company_master: bool = False
    write_frozen_bundle_dir: Path | None = None
    frozen_bundle_dir: Path | None = None
    frozen_artifact_binding: Path | None = None
    drift_probe_output: Path | None = None
    drift_probe_only: bool = False

    def validate(self) -> None:
        if self.start < MIN_BACKFILL_DATE:
            raise BackfillConfigurationError(
                f"official governance backfill starts at {MIN_BACKFILL_DATE.isoformat()} or later"
            )
        if self.end_exclusive <= self.start:
            raise BackfillConfigurationError("end_exclusive must be after start")
        completed_kst_end_exclusive = _completed_kst_end_exclusive()
        if self.end_exclusive > completed_kst_end_exclusive:
            raise BackfillConfigurationError(
                "end_exclusive cannot be later than the current KST date as an "
                "exclusive boundary; current or future KST dates must never be "
                "checkpointed as complete"
            )
        if self.chunk_days < 1:
            raise BackfillConfigurationError("chunk_days must be at least 1")
        if not 1 <= self.page_count <= 100:
            raise BackfillConfigurationError("page_count must be between 1 and 100")
        if self.max_pages < 1:
            raise BackfillConfigurationError("max_pages must be at least 1")
        if self.max_chunks < 0:
            raise BackfillConfigurationError("max_chunks cannot be negative")
        if self.request_budget < 1 or self.request_budget > 10_000:
            raise BackfillConfigurationError("request_budget must be between 1 and 10000")
        if not self.sources or set(self.sources) - {"dart", "kind"}:
            raise BackfillConfigurationError("sources must contain dart and/or kind")
        if self.sync_company_master and "dart" not in self.sources:
            raise BackfillConfigurationError("sync_company_master requires the dart source")
        if self.replay:
            if self.dry_run:
                raise BackfillConfigurationError("replay cannot be combined with dry_run")
            if self.restart:
                raise BackfillConfigurationError("replay cannot replace its apply checkpoint")
            if self.sources != ("dart",):
                raise BackfillConfigurationError("replay currently requires source=dart")
            if self.chunk_days != 1:
                raise BackfillConfigurationError("replay requires one-day windows")
            if (self.end_exclusive - self.start).days != 30:
                raise BackfillConfigurationError(
                    "replay requires the exact completed 30-day apply range"
                )
            if self.max_chunks not in {0, 30}:
                raise BackfillConfigurationError(
                    "replay must reprocess all 30 completed windows"
                )
            if self.sync_company_master:
                raise BackfillConfigurationError(
                    "replay cannot mutate the DART company master"
                )
        if self.write_frozen_bundle_dir is not None:
            if (
                self.dry_run
                or self.replay
                or self.drift_probe_only
                or self.sources != ("dart",)
                or self.chunk_days != 1
                or (self.end_exclusive - self.start).days != 30
                or self.max_chunks not in {0, 30}
                or self.sync_company_master
            ):
                raise BackfillConfigurationError(
                    "frozen apply bundle requires an exact 30-day, DART-only "
                    "apply with one-day windows and no company-master sync"
                )
        frozen_replay_requested = self.frozen_bundle_dir is not None
        if frozen_replay_requested != (self.frozen_artifact_binding is not None):
            raise BackfillConfigurationError(
                "frozen bundle and artifact binding must be supplied together"
            )
        if self.replay and not frozen_replay_requested:
            # Preserve the legacy replay interface for local compatibility;
            # protected workflows require the frozen arguments.
            pass
        if self.drift_probe_only:
            if (
                self.replay
                or self.dry_run
                or not frozen_replay_requested
                or self.drift_probe_output is None
                or self.sources != ("dart",)
                or self.chunk_days != 1
                or (self.end_exclusive - self.start).days != 30
                or self.max_chunks not in {0, 30}
                or self.sync_company_master
            ):
                raise BackfillConfigurationError(
                    "drift probe requires one exact 30-day frozen DART bundle"
                )
        elif self.drift_probe_output is not None:
            raise BackfillConfigurationError(
                "--drift-probe-output requires --drift-probe-only"
            )


IngestRunner = Callable[..., dict[str, object]]


class CheckpointStore(Protocol):
    def get(self, fingerprint: str) -> RemoteCheckpointSnapshot: ...

    def put(
        self,
        fingerprint: str,
        *,
        expected_version: int,
        checkpoint: dict[str, object],
    ) -> RemoteCheckpointWrite: ...


def build_date_windows(start: date, end_exclusive: date, chunk_days: int) -> list[DateWindow]:
    if chunk_days < 1:
        raise BackfillConfigurationError("chunk_days must be at least 1")
    windows: list[DateWindow] = []
    cursor = start
    while cursor < end_exclusive:
        next_cursor = min(cursor + timedelta(days=chunk_days), end_exclusive)
        windows.append(DateWindow(cursor, next_cursor))
        cursor = next_cursor
    return windows


def load_env_files(
    project_root: Path,
    names: tuple[str, ...] = (".env", ".env.local", ".env.api"),
) -> list[Path]:
    loaded: list[Path] = []
    for name in names:
        path = project_root / name
        if not path.exists():
            continue
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
        loaded.append(path)
    return loaded


def job_contract(
    options: BackfillOptions,
    *,
    code_revision: str | None = None,
) -> dict[str, object]:
    contract: dict[str, object] = {
        "range_start": options.start.isoformat(),
        "range_end_exclusive": options.end_exclusive.isoformat(),
        "chunk_days": options.chunk_days,
        "sources": sorted(options.sources),
        "page_count": options.page_count,
        "max_pages": options.max_pages,
        "sync_company_master": options.sync_company_master,
    }
    if code_revision is not None:
        contract["code_revision"] = code_revision
    return contract


def job_fingerprint(job: Mapping[str, object]) -> str:
    serialized = json.dumps(dict(job), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def window_idempotency_key(fingerprint: str, window: DateWindow) -> str:
    digest = hashlib.sha256(f"{fingerprint}|{window.key}".encode("utf-8")).hexdigest()[:32]
    return f"official-backfill-v1:{digest}"


def _timestamp(now_provider: Callable[[], datetime]) -> str:
    value = now_provider()
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _optional_code_revision() -> str | None:
    revision = (
        os.environ.get("GITHUB_SHA", "")
        or os.environ.get("CURATOR_CODE_REVISION", "")
    ).strip().casefold()
    if not revision:
        return None
    if _CODE_REVISION_RE.fullmatch(revision) is None:
        raise BackfillConfigurationError(
            "GITHUB_SHA or CURATOR_CODE_REVISION must contain "
            "7-40 lowercase hexadecimal characters"
        )
    return revision


def _code_revision() -> str:
    revision = _optional_code_revision()
    if revision is None:
        raise BackfillConfigurationError(
            "applied backfill requires GITHUB_SHA or CURATOR_CODE_REVISION "
            "(7-40 lowercase hexadecimal characters)"
        )
    return revision


def new_checkpoint(
    job: dict[str, object],
    fingerprint: str,
    *,
    now_provider: Callable[[], datetime],
) -> dict[str, object]:
    created_at = _timestamp(now_provider)
    return {
        "schema_version": CHECKPOINT_SCHEMA_VERSION,
        "job": {**job, "fingerprint": fingerprint},
        "created_at": created_at,
        "updated_at": created_at,
        "company_master_synced": False,
        "dart_quota_blocked_until": None,
        "completed_windows": {},
        "failed_windows": {},
    }


def validate_checkpoint(
    value: object,
    *,
    label: str,
) -> dict[str, object]:
    if not isinstance(value, dict):
        raise CheckpointError(f"checkpoint {label} must contain a JSON object")
    if value.get("schema_version") != CHECKPOINT_SCHEMA_VERSION:
        raise CheckpointError(
            f"checkpoint {label} schema_version must be {CHECKPOINT_SCHEMA_VERSION}; use --restart"
        )
    if not isinstance(value.get("job"), dict):
        raise CheckpointError(f"checkpoint {label} is missing job metadata")
    if not isinstance(value.get("company_master_synced"), bool):
        raise CheckpointError(f"checkpoint {label} company_master_synced must be boolean")
    blocked_until = value.get("dart_quota_blocked_until")
    if blocked_until is not None:
        try:
            date.fromisoformat(str(blocked_until))
        except ValueError as exc:
            raise CheckpointError(
                f"checkpoint {label} dart_quota_blocked_until must be an ISO date or null"
            ) from exc
    if not isinstance(value.get("completed_windows"), dict) or not isinstance(value.get("failed_windows"), dict):
        raise CheckpointError(f"checkpoint {label} window maps are invalid")
    completed = value["completed_windows"]
    failed = value["failed_windows"]
    assert isinstance(completed, dict) and isinstance(failed, dict)
    overlap = set(completed) & set(failed)
    if overlap:
        raise CheckpointError(
            f"checkpoint {label} contains windows in both completed and failed maps"
        )
    for map_name, records, expected_status in (
        ("completed_windows", completed, "succeeded"),
        ("failed_windows", failed, "failed"),
    ):
        for window_key, result in records.items():
            if not isinstance(result, dict):
                raise CheckpointError(
                    f"checkpoint {label} {map_name}.{window_key} must be an object"
                )
            reconstructed_key = (
                f"{result.get('window_start')}:{result.get('window_end_exclusive')}"
            )
            try:
                attempt = int(result.get("attempt") or 0)
            except (TypeError, ValueError) as exc:
                raise CheckpointError(
                    f"checkpoint {label} {map_name}.{window_key} has invalid attempt"
                ) from exc
            if (
                reconstructed_key != window_key
                or result.get("status") != expected_status
                or attempt < 1
                or _CODE_REVISION_RE.fullmatch(
                    str(result.get("code_revision") or "")
                )
                is None
                or not str(result.get("idempotency_key") or "").startswith(
                    "official-backfill-v1:"
                )
            ):
                raise CheckpointError(
                    f"checkpoint {label} {map_name}.{window_key} is inconsistent"
                )
            summary = result.get("summary")
            if expected_status == "succeeded" and (
                not isinstance(summary, dict)
                or not _summary_succeeded(summary, dry_run=False)
            ):
                raise CheckpointError(
                    f"checkpoint {label} {map_name}.{window_key} lacks an exact remote ACK"
                )
    return value


def load_checkpoint(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CheckpointError(f"cannot read checkpoint {path}: {exc}") from exc
    return validate_checkpoint(value, label=str(path))


def save_checkpoint(path: Path, checkpoint: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(checkpoint, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def validate_runtime(options: BackfillOptions) -> None:
    missing: list[str] = []
    frozen_database_replay = bool(
        options.replay and options.frozen_bundle_dir is not None
    )
    if "dart" in options.sources and not frozen_database_replay:
        try:
            dart_credentials = load_opendart_credentials()
        except OpenDartCredentialConfigurationError as exc:
            raise BackfillConfigurationError(
                "OpenDART credential configuration is invalid"
            ) from exc
        if not dart_credentials:
            missing.append("OPENDART_API_KEYS or DART_API_KEY")
    if "kind" in options.sources and not os.environ.get("KIND_DISCLOSURE_ENDPOINT", "").strip():
        missing.append("KIND_DISCLOSURE_ENDPOINT")
    if "kind" in options.sources:
        try:
            rights_api_configured = source_right_api_configured()
        except OfficialSourceRightError as exc:
            raise BackfillConfigurationError(str(exc)) from exc
        if not rights_api_configured:
            missing.append(
                "BSIDE_API_BASE_URL (or GOVERNANCE_API_BASE_URL/ACTIVIST_API_URL)"
                "/BSIDE_OPS_TOKEN for KIND SourceRight preflight"
            )
    governance_write_or_replay = not options.dry_run and not options.drift_probe_only
    if governance_write_or_replay and not remote_api_configured():
        missing.append("ACTIVIST_API_URL/ACTIVIST_API_SECRET")
    if governance_write_or_replay and not checkpoint_api_configured():
        missing.append("BSIDE_API_BASE_URL/BSIDE_OPS_TOKEN")
    backend_binding_id = os.environ.get("BSIDE_BACKEND_BINDING_ID", "").strip()
    if governance_write_or_replay and (
        len(backend_binding_id) != 64
        or any(character not in "0123456789abcdef" for character in backend_binding_id)
    ):
        missing.append("BSIDE_BACKEND_BINDING_ID")
    if governance_write_or_replay or options.drift_probe_only:
        try:
            _code_revision()
        except BackfillConfigurationError:
            missing.append("GITHUB_SHA/CURATOR_CODE_REVISION")
    if missing:
        raise BackfillConfigurationError("missing required runtime configuration: " + ", ".join(missing))
    if governance_write_or_replay and "dart" in options.sources:
        try:
            DartOfficialSourceRightClient().preflight(_code_revision())
        except OfficialSourceRightError as exc:
            raise BackfillConfigurationError(
                f"OpenDART protected SourceRight preflight failed: {exc}"
            ) from exc


def _summary_int(summary: Mapping[str, object], key: str) -> int:
    value = summary.get(key)
    try:
        return int(str(value)) if value not in (None, "") else 0
    except (TypeError, ValueError):
        return 0


def _summary_succeeded(summary: Mapping[str, object], *, dry_run: bool) -> bool:
    if (
        _summary_int(summary, "official_failed")
        or _summary_int(summary, "official_skipped")
        or _summary_int(summary, "official_remote_ack_mismatches")
    ):
        return False
    if dry_run:
        return True
    if _summary_int(summary, "official_remote_run_persisted") != 1:
        return False
    if "official_remote_raw_count" not in summary or "official_remote_ack_count" not in summary:
        return False
    if _summary_int(summary, "official_remote_raw_count") != _summary_int(
        summary, "official_remote_ack_count"
    ):
        return False
    return not (
        _summary_int(summary, "official_remote_failed")
        or _summary_int(summary, "official_remote_skipped")
        or _summary_int(summary, "official_remote_synced") < 1
    )


def _summary_totals(results: list[dict[str, object]]) -> dict[str, int]:
    keys = (
        "official_fetched",
        "official_documents",
        "official_events",
        "official_companies",
        "official_source_rights",
        "official_remote_synced",
        "official_remote_failed",
        "official_remote_ack_mismatches",
        "official_dart_requests",
        "official_dart_fetched",
        "official_dart_accepted",
        "official_dart_rejected",
        "official_dart_duplicates",
        "official_dart_discarded",
        "official_dart_pages",
        "official_dart_errors",
        "official_dart_quota_exhausted",
        "official_kind_fetched",
        "official_kind_accepted",
        "official_kind_rejected",
        "official_kind_duplicates",
        "official_kind_discarded",
        "official_kind_pages",
        "official_kind_errors",
    )
    totals = {key: 0 for key in keys}
    for result in results:
        summary = result.get("summary")
        if not isinstance(summary, dict):
            continue
        for key in keys:
            totals[key] += _summary_int(summary, key)
    return totals


def _canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _summary_replay_counts(
    summary: Mapping[str, object],
    *,
    location: str,
) -> dict[str, int]:
    """Return the complete numeric official-ingest contract for one window."""

    counts: dict[str, int] = {}
    for key, value in summary.items():
        if not str(key).startswith("official_"):
            continue
        if isinstance(value, bool):
            counts[str(key)] = int(value)
        elif isinstance(value, int):
            counts[str(key)] = value
    required = {
        "official_dart_fetched",
        "official_dart_accepted",
        "official_dart_rejected",
        "official_dart_requests",
        "official_dart_errors",
        "official_dart_quota_exhausted",
        "official_remote_raw_count",
        "official_remote_ack_count",
        "official_remote_ack_mismatches",
        "official_remote_run_persisted",
        "official_remote_synced",
        "official_remote_failed",
        "official_failed",
    }
    missing = sorted(required - counts.keys())
    if missing:
        raise CheckpointError(
            f"{location} is missing replay count fields: {', '.join(missing)}"
        )
    raw = counts["official_dart_fetched"]
    accepted = counts["official_dart_accepted"]
    remote_raw = counts["official_remote_raw_count"]
    acknowledged = counts["official_remote_ack_count"]
    frozen_replay = counts.get("official_frozen_replay", 0) == 1
    if (
        min(counts.values()) < 0
        or raw < accepted
        or accepted != remote_raw
        or remote_raw != acknowledged
        or counts["official_remote_ack_mismatches"] != 0
        or counts["official_remote_run_persisted"] != 1
        or counts["official_remote_synced"] < 1
        or counts["official_remote_failed"] != 0
        or counts["official_failed"] != 0
        or counts["official_dart_errors"] != 0
        or counts["official_dart_quota_exhausted"] != 0
        or (
            counts["official_dart_requests"] != 0
            if frozen_replay
            else counts["official_dart_requests"] < 1
        )
    ):
        raise CheckpointError(f"{location} does not contain an exact remote ACK")
    return counts


def _summary_replay_semantic_counts(
    summary: Mapping[str, object],
    *,
    location: str,
) -> dict[str, int]:
    """Compare source semantics without fabricating replay network activity."""

    keys = (
        "official_fetched",
        "official_documents",
        "official_events",
        "official_companies",
        "official_dart_fetched",
        "official_dart_accepted",
        "official_dart_rejected",
        "official_dart_duplicates",
        "official_dart_discarded",
        "official_dart_errors",
        "official_dart_quota_exhausted",
        "official_remote_raw_count",
        "official_remote_ack_count",
        "official_remote_ack_mismatches",
        "official_remote_run_persisted",
        "official_remote_failed",
        "official_failed",
    )
    counts = {key: _summary_int(summary, key) for key in keys}
    if (
        min(counts.values()) < 0
        or counts["official_dart_accepted"]
        != counts["official_remote_raw_count"]
        or counts["official_remote_raw_count"]
        != counts["official_remote_ack_count"]
        or counts["official_remote_ack_mismatches"] != 0
        or counts["official_remote_run_persisted"] != 1
        or counts["official_remote_failed"] != 0
        or counts["official_failed"] != 0
        or counts["official_dart_errors"] != 0
        or counts["official_dart_quota_exhausted"] != 0
    ):
        raise CheckpointError(f"{location} lacks an exact semantic ACK")
    return counts


def _window_receipt(
    *,
    fingerprint: str,
    checkpoint_hash: str,
    window_key: str,
    result: Mapping[str, object],
    replay: bool,
) -> dict[str, object]:
    summary = result.get("summary")
    if not isinstance(summary, Mapping):
        raise CheckpointError(f"window {window_key} is missing its ingest summary")
    counts = _summary_replay_counts(summary, location=f"window {window_key}")
    start = str(result.get("window_start") or "")
    end = str(result.get("window_end_exclusive") or "")
    idempotency_key = str(result.get("idempotency_key") or "")
    if (
        window_key != f"{start}:{end}"
        or not idempotency_key.startswith("official-backfill-v1:")
        or str(result.get("status") or "") != "succeeded"
    ):
        raise CheckpointError(f"window {window_key} is not a completed apply receipt")
    raw = counts["official_dart_fetched"]
    accepted = counts["official_dart_accepted"]
    acknowledged = counts["official_remote_ack_count"]
    payload_contract = {
        "job_fingerprint": fingerprint,
        "window_key": window_key,
        "idempotency_key": idempotency_key,
        "summary_counts": counts,
    }
    stable_payload_digest = str(result.get("stable_payload_sha256") or "")
    payload_digest = (
        stable_payload_digest
        if re.fullmatch(r"[a-f0-9]{64}", stable_payload_digest)
        else _canonical_sha256(payload_contract)
    )
    ingest_digest = _canonical_sha256(
        {
            "job_fingerprint": fingerprint,
            "window_key": window_key,
            "idempotency_key": idempotency_key,
            "payload_sha256": payload_digest,
        }
    )
    receipt_contract: dict[str, object] = {
        "window_start": start,
        "window_end_exclusive": end,
        "raw_count": raw,
        "filtered_out_count": raw - accepted,
        "accepted_count": accepted,
        "acknowledged_count": acknowledged,
        "status": "complete",
        "code_revision": str(result.get("code_revision") or ""),
        "payload_sha256": payload_digest,
        "idempotency_key": idempotency_key,
        "ingest_id": f"official-dart:{ingest_digest}",
        "idempotent": replay,
        "replay_verified": replay,
    }
    if replay:
        replay_attempted_at = str(result.get("replay_attempted_at") or "")
        try:
            parsed_replay_attempt = datetime.fromisoformat(
                replay_attempted_at.replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise CheckpointError(
                f"window {window_key} has no valid replay_attempted_at"
            ) from exc
        if (
            parsed_replay_attempt.tzinfo is None
            or parsed_replay_attempt.utcoffset() is None
        ):
            raise CheckpointError(
                f"window {window_key} replay_attempted_at lacks a timezone"
            )
        receipt_contract["replay_attempted_at"] = (
            parsed_replay_attempt.astimezone(timezone.utc).isoformat()
        )
    receipt_contract["receipt_sha256"] = _canonical_sha256(
        {
            "checkpoint_payload_sha256": checkpoint_hash,
            **receipt_contract,
        }
    )
    return receipt_contract


def _checkpoint_receipt_contract(
    checkpoint: Mapping[str, object],
    *,
    fingerprint: str,
    checkpoint_hash: str,
    replay: bool,
    replay_results: Mapping[str, Mapping[str, object]] | None = None,
    frozen_bundle_manifest_sha256: str | None = None,
    frozen_artifact_binding_sha256: str | None = None,
    source_network_accessed: bool | None = None,
) -> dict[str, object]:
    completed = checkpoint.get("completed_windows")
    if not isinstance(completed, Mapping):
        raise CheckpointError("checkpoint completed_windows is missing")
    windows: list[dict[str, object]] = []
    for window_key in sorted(str(key) for key in completed):
        apply_result = completed.get(window_key)
        if not isinstance(apply_result, Mapping):
            raise CheckpointError(f"checkpoint window {window_key} is invalid")
        source_result = apply_result
        if replay:
            if replay_results is None or window_key not in replay_results:
                raise CheckpointError(f"replay result for {window_key} is missing")
            source_result = replay_results[window_key]
        windows.append(
            _window_receipt(
                fingerprint=fingerprint,
                checkpoint_hash=checkpoint_hash,
                window_key=window_key,
                result=source_result,
                replay=replay,
            )
        )
    contract: dict[str, object] = {
        "schema_version": 1,
        "source": "dart",
        "mode": "replay" if replay else "apply",
        "job_fingerprint": fingerprint,
        "checkpoint_payload_sha256": checkpoint_hash,
        "window_count": len(windows),
        "windows": windows,
        "contract_sha256": _canonical_sha256(windows),
    }
    if frozen_bundle_manifest_sha256 is not None:
        if re.fullmatch(r"[a-f0-9]{64}", frozen_bundle_manifest_sha256) is None:
            raise CheckpointError("frozen bundle manifest digest is invalid")
        contract["frozen_bundle_manifest_sha256"] = (
            frozen_bundle_manifest_sha256
        )
    if frozen_artifact_binding_sha256 is not None:
        if (
            not replay
            or re.fullmatch(r"[a-f0-9]{64}", frozen_artifact_binding_sha256)
            is None
        ):
            raise CheckpointError("frozen artifact binding digest is invalid")
        contract["frozen_artifact_binding_sha256"] = (
            frozen_artifact_binding_sha256
        )
    if source_network_accessed is not None:
        if not replay or source_network_accessed is not False:
            raise CheckpointError(
                "only a network-free frozen replay may set source access evidence"
            )
        contract["source_network_accessed"] = False
    return contract


def _run_backfill(
    project_root: Path,
    options: BackfillOptions,
    *,
    ingest_runner: IngestRunner = run_official_ingest,
    now_provider: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    checkpoint_store: CheckpointStore | None = None,
    owned_resources: list[object],
) -> dict[str, object]:
    """Run pending chunks against the authoritative remote checkpoint.

    Apply mode reads progress only from MySQL through the ops checkpoint API.
    The local JSON is overwritten after each acknowledged remote update solely
    as an evidence/recovery artifact and is never used to decide what to run.
    A dry run performs no checkpoint reads or writes, local or remote.
    """

    options.validate()
    code_revision = None if options.dry_run else _code_revision()
    # Keep dry-run job identity and per-window idempotency stable for local
    # reproduction. Only the emitted report is bound to the immutable workflow
    # revision when one is available.
    report_code_revision = (
        _optional_code_revision() if options.dry_run else code_revision
    )
    job = job_contract(options, code_revision=code_revision)
    fingerprint = job_fingerprint(job)
    remote_version = 0
    remote_payload_hash: str | None = None
    checkpoint_before: dict[str, object] | None = None
    checkpoint_before_version: int | None = None
    checkpoint_before_hash: str | None = None
    frozen_bundle: FrozenBundle | None = None
    store: CheckpointStore | None = None
    if options.dry_run:
        checkpoint = new_checkpoint(job, fingerprint, now_provider=now_provider)
    else:
        store = checkpoint_store or RemoteCheckpointClient()
        snapshot = store.get(fingerprint)
        remote_version = snapshot.version
        remote_payload_hash = snapshot.payload_hash
        checkpoint = (
            new_checkpoint(job, fingerprint, now_provider=now_provider)
            if options.restart or snapshot.checkpoint is None
            else validate_checkpoint(snapshot.checkpoint, label="remote MySQL checkpoint")
        )
        checkpoint_job = checkpoint.get("job")
        checkpoint_fingerprint = (
            str(checkpoint_job.get("fingerprint") or "") if isinstance(checkpoint_job, dict) else ""
        )
        checkpoint_contract = (
            {key: value for key, value in checkpoint_job.items() if key != "fingerprint"}
            if isinstance(checkpoint_job, dict)
            else {}
        )
        if (
            checkpoint_fingerprint != fingerprint
            or checkpoint_contract != job
            or job_fingerprint(checkpoint_contract) != fingerprint
        ):
            raise CheckpointError(
                "remote checkpoint job fingerprint does not match the requested range/options; use --restart"
            )
        if options.replay:
            if snapshot.checkpoint is None or snapshot.version < 1:
                raise CheckpointError(
                    "replay requires the existing completed apply checkpoint"
                )
            canonical_before = canonical_checkpoint(checkpoint)
            calculated_before_hash = checkpoint_payload_hash(canonical_before)
            if (
                not isinstance(snapshot.payload_hash, str)
                or len(snapshot.payload_hash) != 64
                or calculated_before_hash != snapshot.payload_hash
            ):
                raise CheckpointError(
                    "replay apply checkpoint payload hash is missing or inconsistent"
                )
            checkpoint_before = copy.deepcopy(canonical_before)
            checkpoint_before_version = snapshot.version
            checkpoint_before_hash = snapshot.payload_hash
        if options.restart or snapshot.checkpoint is None:
            write = store.put(
                fingerprint,
                expected_version=remote_version,
                checkpoint=checkpoint,
            )
            remote_version = write.version
            remote_payload_hash = write.payload_hash
        save_checkpoint(options.checkpoint_path, checkpoint)

        blocked_until_text = checkpoint.get("dart_quota_blocked_until")
        if (
            "dart" in options.sources
            and blocked_until_text
            and not (options.replay and options.frozen_bundle_dir is not None)
        ):
            blocked_until = date.fromisoformat(str(blocked_until_text))
            current_time = now_provider()
            if current_time.tzinfo is None:
                current_time = current_time.replace(tzinfo=timezone.utc)
            today_kst = current_time.astimezone(ZoneInfo("Asia/Seoul")).date()
            if today_kst < blocked_until:
                raise BackfillConfigurationError(
                    "OpenDART status 020 already exhausted this KST quota day; "
                    f"resume from the same MySQL checkpoint on or after {blocked_until.isoformat()}"
                )
        if options.replay and options.frozen_bundle_dir is not None:
            assert options.frozen_artifact_binding is not None
            if not isinstance(snapshot.payload_hash, str):
                raise CheckpointError(
                    "frozen replay requires an exact apply checkpoint hash"
                )
            frozen_bundle = load_bundle(
                options.frozen_bundle_dir,
                options.frozen_artifact_binding,
                expected_code_revision=str(code_revision),
                expected_job_fingerprint=fingerprint,
                expected_checkpoint_sha256=snapshot.payload_hash,
            )

    completed_windows = checkpoint["completed_windows"]
    failed_windows = checkpoint["failed_windows"]
    assert isinstance(completed_windows, dict) and isinstance(failed_windows, dict)

    all_windows = build_date_windows(options.start, options.end_exclusive, options.chunk_days)
    expected_window_keys = {window.key for window in all_windows}
    unknown_window_keys = (set(completed_windows) | set(failed_windows)) - expected_window_keys
    if unknown_window_keys:
        raise CheckpointError(
            "checkpoint contains windows outside the requested job: "
            + ", ".join(sorted(unknown_window_keys))
        )
    if options.replay and (
        set(completed_windows) != expected_window_keys
        or failed_windows
        or len(completed_windows) != 30
    ):
        raise CheckpointError(
            "replay requires exactly 30 completed apply windows and no failed windows"
        )
    if options.dry_run:
        pending = all_windows
    elif options.replay:
        pending = all_windows
    else:
        pending = [window for window in all_windows if window.key not in completed_windows]
    pending_before_limit = len(pending)
    selected = pending[: options.max_chunks] if options.max_chunks else pending
    results: list[dict[str, object]] = []
    invocation_failures = 0
    dart_request_budget: DartRequestQuota | None = None
    if "dart" in options.sources and frozen_bundle is None:
        if durable_dart_quota_required() or durable_dart_quota_configured():
            dart_request_budget = DartInvocationQuota(
                durable_dart_quota_client(
                    phase=os.environ.get(
                        "CURATOR_DART_QUOTA_PHASE",
                        "official-backfill",
                    )
                ),
                limit=options.request_budget,
                close_delegate=True,
            )
            owned_resources.append(dart_request_budget)
        else:
            dart_request_budget = DartRequestBudget(options.request_budget)

    window_indexes = {window.key: index for index, window in enumerate(all_windows)}
    frozen_leaf_metadata: dict[str, dict[str, object]] = {}
    for window in selected:
        previous_failure = failed_windows.get(window.key)
        previous_attempts = (
            int(previous_failure.get("attempt") or 0) if isinstance(previous_failure, dict) else 0
        )
        attempt_time = now_provider()
        if attempt_time.tzinfo is None:
            attempt_time = attempt_time.replace(tzinfo=timezone.utc)
        else:
            attempt_time = attempt_time.astimezone(timezone.utc)
        master_sync_needed = options.sync_company_master and not bool(checkpoint.get("company_master_synced"))
        overrides: dict[str, object] = {
            "dart_enabled": "dart" in options.sources,
            "kind_enabled": "kind" in options.sources,
            "page_count": options.page_count,
            "max_pages": options.max_pages,
            "sync_company_master": master_sync_needed,
        }
        if dart_request_budget is not None:
            overrides["dart_request_budget"] = dart_request_budget
        result: dict[str, object] = {
            "window_start": window.start.isoformat(),
            "window_end_exclusive": window.end_exclusive.isoformat(),
            "source_end_inclusive": window.source_end_inclusive.isoformat(),
            "idempotency_key": window_idempotency_key(fingerprint, window),
            "attempt": previous_attempts + 1,
            "started_at": attempt_time.isoformat(),
        }
        if code_revision is not None:
            result["code_revision"] = code_revision
        if options.replay:
            # Keep the real replay clock in the immutable receipt. Replay
            # identity comes from the stable window idempotency key and payload
            # digest; an apply timestamp must never be fabricated here.
            result["replay_attempted_at"] = attempt_time.isoformat()
        try:
            if frozen_bundle is not None:
                frozen_leaf = load_window(
                    frozen_bundle,
                    index=window_indexes[window.key],
                )
                if (
                    frozen_leaf.get("window_start") != window.start.isoformat()
                    or frozen_leaf.get("window_end_exclusive")
                    != window.end_exclusive.isoformat()
                    or frozen_leaf.get("idempotency_key")
                    != result["idempotency_key"]
                ):
                    raise FrozenReplayBundleError(
                        f"frozen replay window {window.key} identity mismatch"
                    )
                summary = replay_frozen_dart_window(
                    frozen_leaf,
                    now=attempt_time,
                )
                result["stable_payload_sha256"] = frozen_leaf[
                    "stable_payload_sha256"
                ]
                result["frozen_leaf_sha256"] = frozen_bundle.manifest[
                    "windows"
                ][window_indexes[window.key]]["sha256"]  # type: ignore[index]
            else:
                captured_leaf: dict[str, object] | None = None
                captured_leaf_count = 0

                def capture_payload(
                    payload: dict[str, object],
                    run: dict[str, object],
                ) -> None:
                    nonlocal captured_leaf, captured_leaf_count
                    if options.write_frozen_bundle_dir is None:
                        return
                    captured_leaf_count += 1
                    if captured_leaf_count != 1:
                        raise FrozenReplayBundleError(
                            "DART frozen apply window captured more than one payload"
                        )
                    captured_leaf = build_window_leaf(
                        code_revision=str(code_revision),
                        job_fingerprint=fingerprint,
                        window_start=window.start.isoformat(),
                        window_end_exclusive=window.end_exclusive.isoformat(),
                        idempotency_key=str(result["idempotency_key"]),
                        payload=payload,
                        run=run,
                    )

                summary = ingest_runner(
                    project_root,
                    # Retrieval metadata must describe the real attempt time.
                    now=attempt_time,
                    start=window.start,
                    end=window.source_end_inclusive,
                    settings_overrides=overrides,
                    dry_run=options.dry_run,
                    idempotency_key=result["idempotency_key"],
                    replay=options.replay,
                    **(
                        {"payload_capture": capture_payload}
                        if options.write_frozen_bundle_dir is not None
                        else {}
                    ),
                )
                result["summary"] = summary
                remote_summary_succeeded = _summary_succeeded(
                    summary,
                    dry_run=options.dry_run,
                )
                if (
                    remote_summary_succeeded
                    and options.write_frozen_bundle_dir is not None
                    and (captured_leaf_count != 1 or captured_leaf is None)
                ):
                    raise FrozenReplayBundleError(
                        "successful DART frozen apply window must capture exactly one payload"
                    )
                if captured_leaf is not None and remote_summary_succeeded:
                    assert options.write_frozen_bundle_dir is not None
                    frozen_leaf_metadata[window.key] = write_window_leaf(
                        options.write_frozen_bundle_dir,
                        index=window_indexes[window.key],
                        leaf=captured_leaf,
                    )
                    result["stable_payload_sha256"] = captured_leaf[
                        "stable_payload_sha256"
                    ]
                    result["frozen_bundle_leaf"] = frozen_leaf_metadata[
                        window.key
                    ]
            result["summary"] = summary
            succeeded = _summary_succeeded(summary, dry_run=options.dry_run)
            if succeeded and options.replay:
                apply_result = completed_windows.get(window.key)
                apply_summary = (
                    apply_result.get("summary")
                    if isinstance(apply_result, Mapping)
                    else None
                )
                if not isinstance(apply_summary, Mapping):
                    raise CheckpointError(
                        f"apply checkpoint window {window.key} is missing its summary"
                    )
                apply_counts = _summary_replay_semantic_counts(
                    apply_summary,
                    location=f"apply window {window.key}",
                )
                replay_counts = _summary_replay_semantic_counts(
                    summary,
                    location=f"replay window {window.key}",
                )
                result["apply_summary_counts_sha256"] = _canonical_sha256(
                    apply_counts
                )
                result["replay_summary_counts_sha256"] = _canonical_sha256(
                    replay_counts
                )
                result["apply_semantic_counts_sha256"] = result[
                    "apply_summary_counts_sha256"
                ]
                result["replay_semantic_counts_sha256"] = result[
                    "replay_summary_counts_sha256"
                ]
                if apply_counts != replay_counts:
                    succeeded = False
                    result["error"] = (
                        "replay summary counts do not exactly match the apply "
                        f"checkpoint for {window.key}"
                    )
                else:
                    result["idempotent"] = True
                    result["replay_verified"] = True
            if not succeeded:
                result.setdefault(
                    "error",
                    "official ingest or required remote sync did not succeed",
                )
        except Exception as exc:  # the checkpoint must record connector/API failures
            succeeded = False
            result["error"] = f"{type(exc).__name__}: {exc}"

        result["status"] = "dry-run-succeeded" if succeeded and options.dry_run else (
            "succeeded" if succeeded else "failed"
        )
        result["finished_at"] = _timestamp(now_provider)
        results.append(result)

        if options.dry_run:
            if not succeeded:
                invocation_failures += 1
                if not options.continue_on_error:
                    break
            continue
        if options.replay:
            if not succeeded:
                invocation_failures += 1
                if not options.continue_on_error:
                    break
            continue

        candidate = copy.deepcopy(checkpoint)
        candidate_completed = candidate["completed_windows"]
        candidate_failed = candidate["failed_windows"]
        assert isinstance(candidate_completed, dict) and isinstance(candidate_failed, dict)
        candidate["updated_at"] = result["finished_at"]
        quota_exhausted = False
        if succeeded:
            candidate_completed[window.key] = result
            candidate_failed.pop(window.key, None)
            candidate["dart_quota_blocked_until"] = None
            if master_sync_needed:
                candidate["company_master_synced"] = True
        else:
            invocation_failures += 1
            candidate_failed[window.key] = result
            failed_summary = result.get("summary")
            quota_exhausted = isinstance(failed_summary, dict) and _summary_int(
                failed_summary, "official_dart_quota_exhausted"
            ) > 0
            if quota_exhausted:
                failed_at = now_provider()
                if failed_at.tzinfo is None:
                    failed_at = failed_at.replace(tzinfo=timezone.utc)
                candidate["dart_quota_blocked_until"] = (
                    failed_at.astimezone(ZoneInfo("Asia/Seoul")).date() + timedelta(days=1)
                ).isoformat()
        assert store is not None
        write = store.put(
            fingerprint,
            expected_version=remote_version,
            checkpoint=candidate,
        )
        remote_version = write.version
        remote_payload_hash = write.payload_hash
        checkpoint = candidate
        completed_windows = candidate_completed
        failed_windows = candidate_failed
        save_checkpoint(options.checkpoint_path, checkpoint)
        if options.write_frozen_bundle_dir is not None:
            partial_metadata: list[Mapping[str, object]] = []
            for applied_window in all_windows:
                completed_result = candidate_completed.get(applied_window.key)
                metadata = (
                    completed_result.get("frozen_bundle_leaf")
                    if isinstance(completed_result, Mapping)
                    else None
                )
                if isinstance(metadata, Mapping):
                    partial_metadata.append(metadata)
            write_partial_apply_bundle(
                options.write_frozen_bundle_dir,
                code_revision=str(code_revision),
                job_fingerprint=fingerprint,
                range_start=options.start.isoformat(),
                range_end_exclusive=options.end_exclusive.isoformat(),
                checkpoint_payload_sha256=remote_payload_hash,
                checkpoint_version=remote_version,
                completed_window_metadata=partial_metadata,
            )
        if not succeeded and (quota_exhausted or not options.continue_on_error):
            break
    checkpoint_after_version: int | None = None
    checkpoint_after_hash: str | None = None
    if options.replay:
        assert store is not None
        assert checkpoint_before is not None
        assert checkpoint_before_version is not None
        assert checkpoint_before_hash is not None
        replay_snapshot = store.get(fingerprint)
        if replay_snapshot.checkpoint is None:
            raise CheckpointError("replay apply checkpoint disappeared during the run")
        canonical_after = canonical_checkpoint(
            validate_checkpoint(
                replay_snapshot.checkpoint,
                label="post-replay remote MySQL checkpoint",
            )
        )
        calculated_after_hash = checkpoint_payload_hash(canonical_after)
        checkpoint_after_version = replay_snapshot.version
        checkpoint_after_hash = replay_snapshot.payload_hash
        if (
            checkpoint_after_hash is None
            or calculated_after_hash != checkpoint_after_hash
            or checkpoint_after_version != checkpoint_before_version
            or checkpoint_after_hash != checkpoint_before_hash
            or canonical_after != checkpoint_before
        ):
            raise CheckpointError(
                "replay mutated the authoritative apply checkpoint version or payload"
            )
        remote_version = checkpoint_after_version
        remote_payload_hash = checkpoint_after_hash

    checkpoint_hash = (
        checkpoint_payload_hash(canonical_checkpoint(checkpoint))
        if options.dry_run
        else remote_payload_hash
    )
    if frozen_bundle is not None:
        assert options.frozen_artifact_binding is not None
        assert isinstance(checkpoint_hash, str)
        post_replay_bundle = load_bundle(
            frozen_bundle.root,
            options.frozen_artifact_binding,
            expected_code_revision=str(code_revision),
            expected_job_fingerprint=fingerprint,
            expected_checkpoint_sha256=checkpoint_hash,
        )
        if post_replay_bundle.manifest != frozen_bundle.manifest:
            raise CheckpointError("frozen bundle changed during read-only replay")
    frozen_manifest: dict[str, object] | None = None
    frozen_manifest_sha256: str | None = None
    if options.write_frozen_bundle_dir is not None:
        frozen_output_dir = options.write_frozen_bundle_dir
        if (
            invocation_failures
            or not isinstance(checkpoint_hash, str)
            or len(completed_windows) != len(all_windows)
        ):
            raise CheckpointError(
                "frozen apply bundle cannot be finalized before all windows "
                "have exact remote ACKs"
            )
        manifest_windows: list[Mapping[str, object]] = []
        for window in all_windows:
            stored_result = completed_windows.get(window.key)
            metadata = (
                stored_result.get("frozen_bundle_leaf")
                if isinstance(stored_result, Mapping)
                else None
            )
            if not isinstance(metadata, Mapping):
                raise CheckpointError(
                    f"completed window {window.key} lacks its frozen leaf binding; "
                    "restore the partial frozen bundle before resuming"
                )
            manifest_windows.append(metadata)
        frozen_manifest = finalize_apply_bundle(
            options.write_frozen_bundle_dir,
            code_revision=str(code_revision),
            job_fingerprint=fingerprint,
            range_start=options.start.isoformat(),
            range_end_exclusive=options.end_exclusive.isoformat(),
            checkpoint_payload_sha256=checkpoint_hash,
            checkpoint_version=remote_version,
            window_metadata=manifest_windows,
        )
        frozen_manifest_sha256 = hashlib.sha256(
            (frozen_output_dir / "manifest.json").read_bytes()
        ).hexdigest()
    replay_manifest_sha256: str | None = None
    replay_binding_sha256: str | None = None
    if frozen_bundle is not None:
        assert options.frozen_artifact_binding is not None
        replay_manifest_sha256 = hashlib.sha256(
            (frozen_bundle.root / "manifest.json").read_bytes()
        ).hexdigest()
        replay_binding_sha256 = hashlib.sha256(
            options.frozen_artifact_binding.read_bytes()
        ).hexdigest()
    receipt_manifest_sha256 = (
        replay_manifest_sha256
        if options.replay
        else frozen_manifest_sha256
    )
    receipt_contract: dict[str, object] | None = None
    apply_receipt_contract: dict[str, object] | None = None
    if (
        not options.dry_run
        and options.sources == ("dart",)
        and isinstance(checkpoint_hash, str)
    ):
        apply_receipt_contract = _checkpoint_receipt_contract(
            checkpoint,
            fingerprint=fingerprint,
            checkpoint_hash=checkpoint_hash,
            replay=False,
            frozen_bundle_manifest_sha256=receipt_manifest_sha256,
        )
        if options.replay:
            replay_results = {
                f"{result.get('window_start')}:{result.get('window_end_exclusive')}": result
                for result in results
                if result.get("window_start") and result.get("window_end_exclusive")
            }
            if not invocation_failures and len(replay_results) == len(all_windows):
                receipt_contract = _checkpoint_receipt_contract(
                    checkpoint,
                    fingerprint=fingerprint,
                    checkpoint_hash=checkpoint_hash,
                    replay=True,
                    replay_results=replay_results,
                    frozen_bundle_manifest_sha256=replay_manifest_sha256,
                    frozen_artifact_binding_sha256=replay_binding_sha256,
                    source_network_accessed=(
                        False if frozen_bundle is not None else None
                    ),
                )
        else:
            receipt_contract = apply_receipt_contract
    replay_succeeded = sum(
        1 for row in results if row.get("status") == "succeeded"
    )
    remaining = (
        0
        if options.dry_run
        else (
            len(all_windows) - replay_succeeded
            if options.replay
            else len(
                [
                    window
                    for window in all_windows
                    if window.key not in completed_windows
                ]
            )
        )
    )
    return {
        "schema_version": 1,
        "status": "failed" if invocation_failures else "succeeded",
        "mode": "replay" if options.replay else (
            "dry-run" if options.dry_run else "apply"
        ),
        "dry_run": options.dry_run,
        "idempotent": bool(options.replay and not invocation_failures),
        "replay_verified": bool(options.replay and not invocation_failures),
        "code_revision": report_code_revision,
        "job_fingerprint": fingerprint,
        "range_start": options.start.isoformat(),
        "range_end_exclusive": options.end_exclusive.isoformat(),
        "windows_total": len(all_windows),
        "windows_already_completed": (
            0
            if options.dry_run
            else (
                len(completed_windows)
                if options.replay
                else len(all_windows) - pending_before_limit
            )
        ),
        "windows_pending_before_limit": pending_before_limit,
        "windows_selected": len(selected),
        "windows_attempted": len(results),
        "windows_succeeded": sum(1 for row in results if str(row.get("status")).endswith("succeeded")),
        "windows_failed": invocation_failures,
        "windows_remaining": remaining,
        "checkpoint_path": None if options.dry_run else str(options.checkpoint_path),
        "checkpoint_source": None if options.dry_run else "mysql_remote",
        "checkpoint_version": None if options.dry_run else remote_version,
        "checkpoint_payload_sha256": None if options.dry_run else checkpoint_hash,
        "checkpoint_before": (
            {
                "version": checkpoint_before_version,
                "payload_sha256": checkpoint_before_hash,
            }
            if options.replay
            else None
        ),
        "checkpoint_after": (
            {
                "version": checkpoint_after_version,
                "payload_sha256": checkpoint_after_hash,
            }
            if options.replay
            else None
        ),
        "apply_receipt_contract": apply_receipt_contract,
        "receipt_contract": receipt_contract,
        "frozen_replay_bundle": (
            {
                "status": "verified",
                "manifest_sha256": frozen_manifest_sha256,
                "window_count": 30,
            }
            if frozen_manifest is not None
            else (
                {
                    "status": "consumed",
                    "manifest_sha256": hashlib.sha256(
                        (frozen_bundle.root / "manifest.json").read_bytes()
                    ).hexdigest(),
                    "window_count": 30,
                }
                if frozen_bundle is not None
                else None
            )
        ),
        "totals": _summary_totals(results),
        "window_results": results,
    }


def run_backfill(
    project_root: Path,
    options: BackfillOptions,
    *,
    ingest_runner: IngestRunner = run_official_ingest,
    now_provider: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    checkpoint_store: CheckpointStore | None = None,
) -> dict[str, object]:
    """Run a backfill and close only durable quota resources created here."""

    owned_resources: list[object] = []
    try:
        return _run_backfill(
            project_root,
            options,
            ingest_runner=ingest_runner,
            now_provider=now_provider,
            checkpoint_store=checkpoint_store,
            owned_resources=owned_resources,
        )
    finally:
        for resource in reversed(owned_resources):
            close = getattr(resource, "close", None)
            if callable(close):
                close()


def run_drift_probe(
    project_root: Path,
    options: BackfillOptions,
    *,
    ingest_runner: IngestRunner = run_official_ingest,
    now_provider: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> dict[str, object]:
    """Compare a fresh DART observation with the immutable apply payload.

    Governance and checkpoint APIs are never called. Physical source requests
    still consume the durable quota ledger and are reported as such.
    """

    options.validate()
    if not options.drift_probe_only:
        raise BackfillConfigurationError("drift probe mode was not requested")
    assert options.frozen_bundle_dir is not None
    assert options.frozen_artifact_binding is not None
    assert options.drift_probe_output is not None
    revision = _code_revision()
    job = job_contract(options, code_revision=revision)
    fingerprint = job_fingerprint(job)
    checkpoint_hash = artifact_binding_checkpoint_sha256(
        options.frozen_artifact_binding
    )
    bundle = load_bundle(
        options.frozen_bundle_dir,
        options.frozen_artifact_binding,
        expected_code_revision=revision,
        expected_job_fingerprint=fingerprint,
        expected_checkpoint_sha256=checkpoint_hash,
    )
    windows = build_date_windows(
        options.start,
        options.end_exclusive,
        options.chunk_days,
    )
    probe_windows: list[dict[str, object]] = []
    overall_status = "matched"
    error_code: str | None = None
    request_budget: DartRequestQuota
    durable_quota = durable_dart_quota_required() or durable_dart_quota_configured()
    if durable_quota:
        request_budget = DartInvocationQuota(
            durable_dart_quota_client(phase="official-backfill-drift-probe"),
            limit=options.request_budget,
            close_delegate=True,
        )
    else:
        request_budget = DartRequestBudget(options.request_budget)
    try:
        for index, window in enumerate(windows):
            expected = load_window(bundle, index=index)
            captured_payload: dict[str, object] | None = None
            captured_run: dict[str, object] | None = None

            def capture(
                payload: dict[str, object],
                run: dict[str, object],
            ) -> None:
                nonlocal captured_payload, captured_run
                captured_payload = copy.deepcopy(payload)
                captured_run = copy.deepcopy(run)

            try:
                summary = ingest_runner(
                    project_root,
                    now=now_provider(),
                    start=window.start,
                    end=window.source_end_inclusive,
                    settings_overrides={
                        "dart_enabled": True,
                        "kind_enabled": False,
                        "page_count": options.page_count,
                        "max_pages": options.max_pages,
                        "sync_company_master": False,
                        "dart_request_budget": request_budget,
                    },
                    dry_run=True,
                    idempotency_key=window_idempotency_key(
                        fingerprint, window
                    ),
                    replay=False,
                    payload_capture=capture,
                )
                if (
                    _summary_int(summary, "official_failed")
                    or captured_payload is None
                    or captured_run is None
                ):
                    raise FrozenReplayBundleError("source_probe_failed")
                outcomes = captured_run.get("source_outcomes")
                dart_outcome = (
                    outcomes.get("dart")
                    if isinstance(outcomes, Mapping)
                    else None
                )
                if not isinstance(dart_outcome, Mapping):
                    raise FrozenReplayBundleError(
                        "source_probe_outcome_missing"
                    )
                comparison = compare_fresh_payload(
                    expected,
                    captured_payload,
                    actual_source_counts=source_semantic_counts(dart_outcome),
                )
                probe_windows.append(
                    {
                        "index": index,
                        "window_start": window.start.isoformat(),
                        "window_end_exclusive": window.end_exclusive.isoformat(),
                        **comparison,
                        "probe_execution": {
                            "source_requests": _summary_int(
                                summary, "official_dart_requests"
                            ),
                            "source_pages": _summary_int(
                                summary, "official_dart_pages"
                            ),
                            "source_rows_fetched": _summary_int(
                                summary, "official_dart_fetched"
                            ),
                        },
                    }
                )
                if comparison["matched"] is not True:
                    overall_status = "drift_detected"
            except Exception as exc:  # noqa: BLE001 - emit sanitized probe evidence.
                overall_status = "probe_failed"
                error_code = re.sub(
                    r"[^a-z0-9_]",
                    "_",
                    type(exc).__name__.casefold(),
                )[:64]
                probe_windows.append(
                    {
                        "index": index,
                        "window_start": window.start.isoformat(),
                        "window_end_exclusive": window.end_exclusive.isoformat(),
                        "matched": False,
                        "status": "probe_failed",
                        "error_code": error_code,
                    }
                )
                break
    finally:
        close = getattr(request_budget, "close", None)
        if callable(close):
            close()
    return write_probe_report(
        options.drift_probe_output,
        code_revision=revision,
        job_fingerprint=fingerprint,
        range_start=options.start.isoformat(),
        range_end_exclusive=options.end_exclusive.isoformat(),
        status=overall_status,
        windows=probe_windows,
        error_code=error_code,
        quota_ledger_write_attempted=durable_quota,
    )


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from exc


def _sources(value: str) -> tuple[str, ...]:
    return ("dart", "kind") if value == "both" else (value,)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Restartable, idempotent DART/KIND governance-disclosure backfill (2021+)."
    )
    parser.add_argument("--root", type=Path, default=PROJECT_ROOT, help="project root containing config.yaml")
    parser.add_argument("--from-date", type=_parse_date, help="inclusive start date (default: config backfill_start)")
    parser.add_argument(
        "--to-date",
        type=_parse_date,
        help="exclusive end date (default: current KST date, so only completed dates are included)",
    )
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=1,
        help="date-window size (operational backfills use the safe one-day default)",
    )
    parser.add_argument("--source", choices=("dart", "kind", "both"), default="dart")
    parser.add_argument("--page-count", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=100, help="maximum connector pages per source and chunk")
    parser.add_argument("--max-chunks", type=int, default=0, help="process at most N pending chunks (0 = unlimited)")
    parser.add_argument(
        "--request-budget",
        type=int,
        default=10_000,
        help="maximum physical OpenDART requests for this invocation (max 10000)",
    )
    parser.add_argument("--checkpoint", type=Path, default=DEFAULT_CHECKPOINT_PATH)
    parser.add_argument("--dry-run", action="store_true", help="fetch/normalize but do not sync or mutate checkpoint")
    parser.add_argument(
        "--replay",
        action="store_true",
        help=(
            "re-fetch and remotely upsert every window from one exact completed "
            "30-day DART apply checkpoint without mutating that checkpoint"
        ),
    )
    parser.add_argument("--restart", action="store_true", help="replace the checkpoint for this requested job")
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--sync-company-master", action="store_true")
    parser.add_argument(
        "--write-frozen-bundle-dir",
        type=Path,
        help="private output directory for an exact DART apply replay bundle",
    )
    parser.add_argument(
        "--frozen-bundle-dir",
        type=Path,
        help="downloaded immutable DART apply replay bundle",
    )
    parser.add_argument(
        "--frozen-artifact-binding",
        type=Path,
        help="GitHub artifact identity/digest binding for the frozen bundle",
    )
    parser.add_argument(
        "--drift-probe-output",
        type=Path,
        help="sanitized output path for the separate live source drift probe",
    )
    parser.add_argument(
        "--drift-probe-only",
        action="store_true",
        help="read live DART only; never write governance data or checkpoints",
    )
    return parser


def options_from_args(args: argparse.Namespace) -> tuple[Path, BackfillOptions]:
    project_root = args.root.resolve()
    config = load_config(project_root / "config.yaml")
    settings = config.get("official_ingest", {})
    configured_start = str(settings.get("backfill_start") or MIN_BACKFILL_DATE.isoformat()) if isinstance(settings, dict) else MIN_BACKFILL_DATE.isoformat()
    start = args.from_date or _parse_date(configured_start)
    end_exclusive = args.to_date or _completed_kst_end_exclusive()
    checkpoint = args.checkpoint if args.checkpoint.is_absolute() else project_root / args.checkpoint
    def resolved_optional(path: Path | None) -> Path | None:
        if path is None:
            return None
        return path.resolve() if path.is_absolute() else (project_root / path).resolve()

    return project_root, BackfillOptions(
        start=start,
        end_exclusive=end_exclusive,
        checkpoint_path=checkpoint,
        chunk_days=args.chunk_days,
        sources=_sources(args.source),
        page_count=args.page_count,
        max_pages=args.max_pages,
        max_chunks=args.max_chunks,
        request_budget=args.request_budget,
        dry_run=args.dry_run,
        replay=args.replay,
        restart=args.restart,
        continue_on_error=args.continue_on_error,
        sync_company_master=args.sync_company_master,
        write_frozen_bundle_dir=resolved_optional(args.write_frozen_bundle_dir),
        frozen_bundle_dir=resolved_optional(args.frozen_bundle_dir),
        frozen_artifact_binding=resolved_optional(args.frozen_artifact_binding),
        drift_probe_output=resolved_optional(args.drift_probe_output),
        drift_probe_only=args.drift_probe_only,
    )


def _execution_report_succeeded(
    report: Mapping[str, object],
    *,
    drift_probe_only: bool,
) -> bool:
    if drift_probe_only:
        return report.get("release_gate_matched") is True
    return report.get("status") == "succeeded"


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        project_root, options = options_from_args(args)
        load_env_files(project_root)
        options.validate()
        validate_runtime(options)
        report = (
            run_drift_probe(project_root, options)
            if options.drift_probe_only
            else run_backfill(project_root, options)
        )
    except (
        BackfillConfigurationError,
        CheckpointError,
        FrozenReplayBundleError,
        RemoteCheckpointError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(2) from exc
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    if not _execution_report_succeeded(
        report,
        drift_probe_only=options.drift_probe_only,
    ):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
