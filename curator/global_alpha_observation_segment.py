from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


SEGMENT_SCHEMA_VERSION = 1
SEGMENT_KIND = "bside-global-alpha-observation-segment"
SEGMENT_COUNTS = (58, 58, 58, 57, 57)
SEGMENT_COUNT = len(SEGMENT_COUNTS)
TOTAL_OBSERVATIONS = sum(SEGMENT_COUNTS)
OBSERVATION_INTERVAL_SECONDS = 300
MAX_SLOT_LATENESS_SECONDS = 120
FIRST_OBSERVATION_TOLERANCE_SECONDS = 300
SUCCESSOR_DISPATCH_LEAD_SECONDS = 900
SHA_PATTERN = re.compile(r"^[a-f0-9]{40}$")
POSITIVE_INTEGER_PATTERN = re.compile(r"^[1-9][0-9]*$")
REPOSITORY_PATTERN = re.compile(
    r"^[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}$"
)
ARTIFACT_DIGEST_PATTERN = re.compile(r"^sha256:[a-f0-9]{64}$")


class ObservationSegmentError(ValueError):
    """Raised when a chained observation segment cannot be trusted."""


@dataclass(frozen=True)
class SegmentIdentity:
    chain_id: str
    segment_index: int
    run_id: str
    run_attempt: int
    code_revision: str
    predecessor_run_id: str | None
    predecessor_artifact_digest: str | None


@dataclass(frozen=True)
class CandidateWindow:
    started_at: datetime
    ends_at: datetime
    cadence_anchor: datetime


def _utc_datetime(value: object, *, code: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ObservationSegmentError(code) from exc
    if parsed.tzinfo is None:
        raise ObservationSegmentError(code)
    return parsed.astimezone(timezone.utc)


def _utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00",
        "Z",
    )


def segment_slot_bounds(segment_index: int) -> tuple[int, int]:
    if segment_index < 1 or segment_index > SEGMENT_COUNT:
        raise ObservationSegmentError("segment_index_invalid")
    first = sum(SEGMENT_COUNTS[: segment_index - 1])
    return first, first + SEGMENT_COUNTS[segment_index - 1] - 1


def normalize_identity(
    *,
    chain_id: str,
    segment_index: int,
    run_id: str,
    run_attempt: int,
    code_revision: str,
    predecessor_run_id: str,
    predecessor_artifact_digest: str,
) -> SegmentIdentity:
    normalized_chain = chain_id.strip()
    normalized_run = run_id.strip()
    revision = code_revision.strip().casefold()
    predecessor = predecessor_run_id.strip() or None
    predecessor_digest = predecessor_artifact_digest.strip().casefold() or None
    if (
        POSITIVE_INTEGER_PATTERN.fullmatch(normalized_chain) is None
        or POSITIVE_INTEGER_PATTERN.fullmatch(normalized_run) is None
        or run_attempt != 1
        or SHA_PATTERN.fullmatch(revision) is None
    ):
        raise ObservationSegmentError("segment_identity_invalid")
    if segment_index == 1:
        if (
            normalized_chain != normalized_run
            or predecessor is not None
            or predecessor_digest is not None
        ):
            raise ObservationSegmentError("segment_seed_identity_invalid")
    else:
        if (
            predecessor is None
            or POSITIVE_INTEGER_PATTERN.fullmatch(predecessor) is None
            or predecessor == normalized_run
            or predecessor_digest is None
            or ARTIFACT_DIGEST_PATTERN.fullmatch(predecessor_digest) is None
        ):
            raise ObservationSegmentError("segment_predecessor_identity_invalid")
    segment_slot_bounds(segment_index)
    return SegmentIdentity(
        chain_id=normalized_chain,
        segment_index=segment_index,
        run_id=normalized_run,
        run_attempt=run_attempt,
        code_revision=revision,
        predecessor_run_id=predecessor,
        predecessor_artifact_digest=predecessor_digest,
    )


def candidate_from_first_observation(
    observation: Mapping[str, object],
) -> CandidateWindow:
    window_value = observation.get("observation_window")
    if not isinstance(window_value, dict):
        raise ObservationSegmentError("candidate_window_missing")
    started_at = _utc_datetime(
        window_value.get("started_at"),
        code="candidate_started_at_invalid",
    )
    ends_at = _utc_datetime(
        window_value.get("ends_at"),
        code="candidate_ends_at_invalid",
    )
    observed_at = _utc_datetime(
        observation.get("observed_at"),
        code="candidate_anchor_invalid",
    )
    if (
        ends_at - started_at != timedelta(hours=24)
        or window_value.get("within_window") is not True
        or not started_at <= observed_at <= started_at
        + timedelta(seconds=FIRST_OBSERVATION_TOLERANCE_SECONDS)
    ):
        raise ObservationSegmentError("candidate_window_invalid")
    return CandidateWindow(
        started_at=started_at,
        ends_at=ends_at,
        cadence_anchor=observed_at,
    )


def normalize_candidate(
    *,
    started_at: str,
    ends_at: str,
    cadence_anchor: str,
) -> CandidateWindow:
    start = _utc_datetime(started_at, code="candidate_started_at_invalid")
    end = _utc_datetime(ends_at, code="candidate_ends_at_invalid")
    anchor = _utc_datetime(cadence_anchor, code="candidate_anchor_invalid")
    if (
        end - start != timedelta(hours=24)
        or not start <= anchor <= start
        + timedelta(seconds=FIRST_OBSERVATION_TOLERANCE_SECONDS)
        or anchor
        + timedelta(
            seconds=(TOTAL_OBSERVATIONS - 1) * OBSERVATION_INTERVAL_SECONDS
        )
        > end
    ):
        raise ObservationSegmentError("candidate_window_invalid")
    return CandidateWindow(started_at=start, ends_at=end, cadence_anchor=anchor)


def _observation_window_matches(
    observation: Mapping[str, object],
    candidate: CandidateWindow,
) -> bool:
    raw = observation.get("observation_window")
    if not isinstance(raw, dict):
        return False
    try:
        started_at = _utc_datetime(
            raw.get("started_at"),
            code="observation_started_at_invalid",
        )
        ends_at = _utc_datetime(
            raw.get("ends_at"),
            code="observation_ends_at_invalid",
        )
    except ObservationSegmentError:
        return False
    return (
        raw.get("duration_hours") == 24
        and raw.get("within_window") is True
        and started_at == candidate.started_at
        and ends_at == candidate.ends_at
    )


def bind_observation(
    observation: Mapping[str, object],
    *,
    identity: SegmentIdentity,
    candidate: CandidateWindow,
    slot_index: int,
) -> dict[str, object]:
    first_slot, last_slot = segment_slot_bounds(identity.segment_index)
    if slot_index < first_slot or slot_index > last_slot:
        raise ObservationSegmentError("segment_slot_out_of_range")
    expected_at = candidate.cadence_anchor + timedelta(
        seconds=slot_index * OBSERVATION_INTERVAL_SECONDS
    )
    observed_at = _utc_datetime(
        observation.get("observed_at"),
        code="observation_time_invalid",
    )
    lateness = (observed_at - expected_at).total_seconds()
    if (
        lateness < -1
        or lateness > MAX_SLOT_LATENESS_SECONDS
        or observed_at > candidate.ends_at
        or observation.get("workflow_revision") != identity.code_revision
        or not _observation_window_matches(observation, candidate)
    ):
        raise ObservationSegmentError("observation_slot_contract_failed")
    bound = dict(observation)
    bound["observation_chain"] = {
        "schema_version": SEGMENT_SCHEMA_VERSION,
        "chain_id": identity.chain_id,
        "segment_index": identity.segment_index,
        "segment_count": SEGMENT_COUNT,
        "slot_index": slot_index,
        "cadence_anchor": _utc_iso(candidate.cadence_anchor),
        "candidate_started_at": _utc_iso(candidate.started_at),
        "candidate_ends_at": _utc_iso(candidate.ends_at),
        "run_id": identity.run_id,
        "run_attempt": identity.run_attempt,
    }
    return bound


def canonical_jsonl(records: Sequence[Mapping[str, object]]) -> bytes:
    return "".join(
        json.dumps(
            record,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
        for record in records
    ).encode("utf-8")


def build_segment_manifest(
    *,
    identity: SegmentIdentity,
    candidate: CandidateWindow | None,
    records: Sequence[Mapping[str, object]],
    observations_sha256: str,
    status: str,
    completed_at: datetime,
    error_code: str | None,
) -> dict[str, object]:
    first_slot, last_slot = segment_slot_bounds(identity.segment_index)
    return {
        "schema_version": SEGMENT_SCHEMA_VERSION,
        "kind": SEGMENT_KIND,
        "status": status,
        "error_code": error_code,
        "chain_id": identity.chain_id,
        "segment_index": identity.segment_index,
        "segment_count": SEGMENT_COUNT,
        "code_revision": identity.code_revision,
        "run_id": identity.run_id,
        "run_attempt": identity.run_attempt,
        "predecessor_run_id": identity.predecessor_run_id,
        "predecessor_artifact_digest": identity.predecessor_artifact_digest,
        "candidate_started_at": (
            _utc_iso(candidate.started_at) if candidate is not None else None
        ),
        "candidate_ends_at": (
            _utc_iso(candidate.ends_at) if candidate is not None else None
        ),
        "cadence_anchor": (
            _utc_iso(candidate.cadence_anchor) if candidate is not None else None
        ),
        "first_slot_index": first_slot,
        "last_slot_index": last_slot,
        "expected_record_count": SEGMENT_COUNTS[identity.segment_index - 1],
        "record_count": len(records),
        "first_observed_at": (
            str(records[0].get("observed_at")) if records else None
        ),
        "last_observed_at": (
            str(records[-1].get("observed_at")) if records else None
        ),
        "observations_sha256": observations_sha256,
        "completed_at": _utc_iso(completed_at),
    }


def _sleep_until(
    target: datetime,
    *,
    clock: Callable[[], datetime],
    sleeper: Callable[[float], None],
) -> None:
    while True:
        remaining = (target - clock()).total_seconds()
        if remaining <= 0:
            break
        sleeper(min(remaining, 60.0))
    if (clock() - target).total_seconds() > MAX_SLOT_LATENESS_SECONDS:
        raise ObservationSegmentError("observation_slot_missed")


def _run_monitor(output_path: Path) -> tuple[int, dict[str, object]]:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "curator.global_alpha_monitor",
            "--evidence",
            str(output_path),
            "--require-active-pipeline",
        ],
        check=False,
    )
    try:
        decoded = json.loads(output_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ObservationSegmentError("monitor_evidence_invalid") from exc
    if not isinstance(decoded, dict):
        raise ObservationSegmentError("monitor_evidence_invalid")
    return completed.returncode, decoded


def dispatch_next_segment(
    *,
    token: str,
    repository: str,
    workflow: str,
    ref: str,
    inputs: Mapping[str, str],
) -> None:
    if (
        not token
        or REPOSITORY_PATTERN.fullmatch(repository) is None
        or not workflow.endswith(".yml")
        or "/" in workflow
        or not ref.strip()
    ):
        raise ObservationSegmentError("self_chain_dispatch_configuration_invalid")
    payload = json.dumps(
        {"ref": ref, "inputs": dict(inputs)},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    request = Request(
        "https://api.github.com/repos/"
        + repository
        + "/actions/workflows/"
        + workflow
        + "/dispatches",
        data=payload,
        method="POST",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
            "User-Agent": "bside-global-alpha-observation-chain/1.0",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urlopen(request, timeout=30) as response:  # noqa: S310
            if int(response.status) != 204:
                raise ObservationSegmentError("self_chain_dispatch_failed")
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        raise ObservationSegmentError("self_chain_dispatch_failed") from exc


def _next_inputs(
    *,
    identity: SegmentIdentity,
    candidate: CandidateWindow,
) -> dict[str, str]:
    return {
        "segment_index": str(identity.segment_index + 1),
        "chain_id": identity.chain_id,
        "expected_revision": identity.code_revision,
        "candidate_started_at": _utc_iso(candidate.started_at),
        "candidate_ends_at": _utc_iso(candidate.ends_at),
        "cadence_anchor": _utc_iso(candidate.cadence_anchor),
        "predecessor_run_id": identity.run_id,
    }


def run_segment(
    *,
    identity: SegmentIdentity,
    candidate: CandidateWindow | None,
    output_dir: Path,
    monitor: Callable[[Path], tuple[int, dict[str, object]]] = _run_monitor,
    dispatcher: Callable[[Mapping[str, str]], None] | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    sleeper: Callable[[float], None] = time.sleep,
) -> tuple[list[dict[str, object]], CandidateWindow]:
    first_slot, last_slot = segment_slot_bounds(identity.segment_index)
    records: list[dict[str, object]] = []
    dispatched = False
    monitor_path = output_dir / ".current-observation.json"
    for slot_index in range(first_slot, last_slot + 1):
        if candidate is not None:
            target = candidate.cadence_anchor + timedelta(
                seconds=slot_index * OBSERVATION_INTERVAL_SECONDS
            )
            _sleep_until(target, clock=clock, sleeper=sleeper)
        return_code, observation = monitor(monitor_path)
        if candidate is None:
            if identity.segment_index != 1 or slot_index != 0:
                raise ObservationSegmentError("candidate_window_missing")
            candidate = candidate_from_first_observation(observation)
        bound = bind_observation(
            observation,
            identity=identity,
            candidate=candidate,
            slot_index=slot_index,
        )
        records.append(bound)
        if return_code != 0 or observation.get("status") != "healthy":
            raise ObservationSegmentError("monitor_observation_failed")
        if (
            identity.segment_index < SEGMENT_COUNT
            and not dispatched
            and dispatcher is not None
        ):
            next_first_slot, _unused = segment_slot_bounds(
                identity.segment_index + 1
            )
            dispatch_at = candidate.cadence_anchor + timedelta(
                seconds=next_first_slot * OBSERVATION_INTERVAL_SECONDS
                - SUCCESSOR_DISPATCH_LEAD_SECONDS
            )
            if clock() >= dispatch_at:
                dispatcher(_next_inputs(identity=identity, candidate=candidate))
                dispatched = True
    if candidate is None:
        raise ObservationSegmentError("candidate_window_missing")
    if identity.segment_index < SEGMENT_COUNT and not dispatched:
        if dispatcher is None:
            raise ObservationSegmentError("self_chain_dispatcher_missing")
        dispatcher(_next_inputs(identity=identity, candidate=candidate))
    if identity.segment_index == SEGMENT_COUNT:
        _sleep_until(candidate.ends_at, clock=clock, sleeper=sleeper)
    return records, candidate


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one immutable segment of the Production Alpha observation chain",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--segment-index", type=int, required=True)
    parser.add_argument("--chain-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-attempt", type=int, required=True)
    parser.add_argument("--expected-revision", required=True)
    parser.add_argument("--candidate-started-at", default="")
    parser.add_argument("--candidate-ends-at", default="")
    parser.add_argument("--cadence-anchor", default="")
    parser.add_argument("--predecessor-run-id", default="")
    parser.add_argument("--predecessor-artifact-digest", default="")
    parser.add_argument("--repository", required=True)
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--ref", required=True)
    parser.add_argument("--github-token-env", default="GITHUB_TOKEN")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    observations_path = output_dir / "observations.jsonl"
    manifest_path = output_dir / "segment-manifest.json"
    records: list[dict[str, object]] = []
    candidate: CandidateWindow | None = None
    status = "failed"
    error_code: str | None = None
    identity: SegmentIdentity | None = None
    try:
        identity = normalize_identity(
            chain_id=args.chain_id,
            segment_index=args.segment_index,
            run_id=args.run_id,
            run_attempt=args.run_attempt,
            code_revision=args.expected_revision,
            predecessor_run_id=args.predecessor_run_id,
            predecessor_artifact_digest=args.predecessor_artifact_digest,
        )
        candidate_values = (
            args.candidate_started_at,
            args.candidate_ends_at,
            args.cadence_anchor,
        )
        if identity.segment_index == 1:
            if any(candidate_values):
                raise ObservationSegmentError("segment_seed_candidate_forbidden")
        else:
            if not all(candidate_values):
                raise ObservationSegmentError("candidate_window_missing")
            candidate = normalize_candidate(
                started_at=args.candidate_started_at,
                ends_at=args.candidate_ends_at,
                cadence_anchor=args.cadence_anchor,
            )

        token = str(os.environ.get(args.github_token_env) or "")

        def dispatch(inputs: Mapping[str, str]) -> None:
            dispatch_next_segment(
                token=token,
                repository=args.repository,
                workflow=args.workflow,
                ref=args.ref,
                inputs=inputs,
            )

        records, candidate = run_segment(
            identity=identity,
            candidate=candidate,
            output_dir=output_dir,
            dispatcher=dispatch,
        )
        status = "complete"
    except ObservationSegmentError as exc:
        error_code = str(exc)
    except Exception:
        error_code = "unexpected_segment_failure"
    finally:
        encoded = canonical_jsonl(records)
        observations_path.write_bytes(encoded)
        if identity is None:
            fallback_revision = str(args.expected_revision).strip().casefold()
            fallback_chain = str(args.chain_id).strip()
            fallback_run = str(args.run_id).strip()
            if SHA_PATTERN.fullmatch(fallback_revision) is None:
                fallback_revision = "0" * 40
            if POSITIVE_INTEGER_PATTERN.fullmatch(fallback_chain) is None:
                fallback_chain = "1"
            if POSITIVE_INTEGER_PATTERN.fullmatch(fallback_run) is None:
                fallback_run = "1"
            fallback_index = max(1, min(SEGMENT_COUNT, int(args.segment_index)))
            identity = SegmentIdentity(
                chain_id=fallback_chain,
                segment_index=fallback_index,
                run_id=fallback_run,
                run_attempt=int(args.run_attempt),
                code_revision=fallback_revision,
                predecessor_run_id=None,
                predecessor_artifact_digest=None,
            )
        manifest = build_segment_manifest(
            identity=identity,
            candidate=candidate,
            records=records,
            observations_sha256=hashlib.sha256(encoded).hexdigest(),
            status=status,
            completed_at=datetime.now(timezone.utc),
            error_code=error_code,
        )
        manifest_path.write_text(
            json.dumps(
                manifest,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
            newline="\n",
        )
        current = output_dir / ".current-observation.json"
        current.unlink(missing_ok=True)
    print(
        "Global Alpha observation segment "
        f"status={status} segment={args.segment_index}/{SEGMENT_COUNT} "
        f"records={len(records)} error={error_code or 'none'}"
    )
    return 0 if status == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
