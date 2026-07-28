from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping

import pytest
import yaml

from curator.global_alpha_observation_segment import (
    SEGMENT_COUNT,
    SEGMENT_COUNTS,
    TOTAL_OBSERVATIONS,
    ObservationSegmentError,
    bind_observation,
    candidate_from_first_observation,
    normalize_candidate,
    normalize_identity,
    run_segment,
    segment_slot_bounds,
)


REVISION = "a" * 40
START = datetime(2026, 7, 29, 0, 0, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[1]


def observation(now: datetime) -> dict[str, object]:
    return {
        "schema_version": 1,
        "observation_id": "global-alpha:" + now.strftime("%H%M%S").ljust(48, "0"),
        "observed_at": now.isoformat(),
        "status": "healthy",
        "workflow_revision": REVISION,
        "observation_window": {
            "duration_hours": 24,
            "started_at": START.isoformat(),
            "ends_at": (START + timedelta(hours=24)).isoformat(),
            "within_window": True,
            "elapsed_minutes": int((now - START).total_seconds() // 60),
        },
    }


class FakeClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def now(self) -> datetime:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)


def test_fixed_segments_cover_every_slot_once_and_stay_below_five_hours() -> None:
    slots: list[int] = []
    for segment_index in range(1, SEGMENT_COUNT + 1):
        first, last = segment_slot_bounds(segment_index)
        slots.extend(range(first, last + 1))
        assert last - first + 1 == SEGMENT_COUNTS[segment_index - 1]
        assert (last - first) * 5 < 300

    assert slots == list(range(TOTAL_OBSERVATIONS))
    assert TOTAL_OBSERVATIONS == 288


def test_seed_identity_and_candidate_are_fail_closed() -> None:
    identity = normalize_identity(
        chain_id="101",
        segment_index=1,
        run_id="101",
        run_attempt=1,
        code_revision=REVISION,
        predecessor_run_id="",
        predecessor_artifact_digest="",
    )
    candidate = candidate_from_first_observation(observation(START))

    assert identity.chain_id == "101"
    assert candidate.cadence_anchor == START
    with pytest.raises(ObservationSegmentError, match="seed_identity"):
        normalize_identity(
            chain_id="100",
            segment_index=1,
            run_id="101",
            run_attempt=1,
            code_revision=REVISION,
            predecessor_run_id="",
            predecessor_artifact_digest="",
        )
    with pytest.raises(ObservationSegmentError, match="candidate_window_invalid"):
        normalize_candidate(
            started_at=START.isoformat(),
            ends_at=(START + timedelta(hours=23)).isoformat(),
            cadence_anchor=START.isoformat(),
        )


def test_bound_observation_rejects_late_or_wrong_segment_slots() -> None:
    identity = normalize_identity(
        chain_id="101",
        segment_index=1,
        run_id="101",
        run_attempt=1,
        code_revision=REVISION,
        predecessor_run_id="",
        predecessor_artifact_digest="",
    )
    candidate = candidate_from_first_observation(observation(START))
    bound = bind_observation(
        observation(START),
        identity=identity,
        candidate=candidate,
        slot_index=0,
    )
    assert bound["observation_chain"]["slot_index"] == 0  # type: ignore[index]

    with pytest.raises(ObservationSegmentError, match="slot_contract"):
        bind_observation(
            observation(START + timedelta(minutes=7, seconds=1)),
            identity=identity,
            candidate=candidate,
            slot_index=1,
        )
    with pytest.raises(ObservationSegmentError, match="slot_out_of_range"):
        bind_observation(
            observation(START + timedelta(minutes=290)),
            identity=identity,
            candidate=candidate,
            slot_index=58,
        )


def test_first_segment_observes_58_slots_and_dispatches_successor_early(
    tmp_path: Path,
) -> None:
    clock = FakeClock(START)
    identity = normalize_identity(
        chain_id="101",
        segment_index=1,
        run_id="101",
        run_attempt=1,
        code_revision=REVISION,
        predecessor_run_id="",
        predecessor_artifact_digest="",
    )
    dispatched: list[Mapping[str, str]] = []

    def monitor(_path: Path) -> tuple[int, dict[str, object]]:
        return 0, observation(clock.now())

    records, candidate = run_segment(
        identity=identity,
        candidate=None,
        output_dir=tmp_path,
        monitor=monitor,
        dispatcher=dispatched.append,
        clock=clock.now,
        sleeper=clock.sleep,
    )

    assert len(records) == 58
    assert candidate.cadence_anchor == START
    assert len(dispatched) == 1
    assert dispatched[0]["segment_index"] == "2"
    assert dispatched[0]["chain_id"] == "101"
    assert dispatched[0]["predecessor_run_id"] == "101"
    assert clock.now() == START + timedelta(minutes=285)


def test_continuation_requires_exact_predecessor_digest() -> None:
    with pytest.raises(ObservationSegmentError, match="predecessor_identity"):
        normalize_identity(
            chain_id="101",
            segment_index=2,
            run_id="102",
            run_attempt=1,
            code_revision=REVISION,
            predecessor_run_id="101",
            predecessor_artifact_digest="",
        )
    identity = normalize_identity(
        chain_id="101",
        segment_index=2,
        run_id="102",
        run_attempt=1,
        code_revision=REVISION,
        predecessor_run_id="101",
        predecessor_artifact_digest="sha256:" + ("b" * 64),
    )
    assert identity.predecessor_run_id == "101"


def test_final_segment_stops_at_server_candidate_end(tmp_path: Path) -> None:
    clock = FakeClock(START + timedelta(minutes=1150))
    identity = normalize_identity(
        chain_id="101",
        segment_index=5,
        run_id="105",
        run_attempt=1,
        code_revision=REVISION,
        predecessor_run_id="104",
        predecessor_artifact_digest="sha256:" + ("b" * 64),
    )
    candidate = normalize_candidate(
        started_at=START.isoformat(),
        ends_at=(START + timedelta(hours=24)).isoformat(),
        cadence_anchor=START.isoformat(),
    )

    def monitor(_path: Path) -> tuple[int, dict[str, object]]:
        return 0, observation(clock.now())

    records, _candidate = run_segment(
        identity=identity,
        candidate=candidate,
        output_dir=tmp_path,
        monitor=monitor,
        dispatcher=None,
        clock=clock.now,
        sleeper=clock.sleep,
    )

    assert len(records) == 57
    assert records[-1]["observed_at"] == (
        START + timedelta(minutes=1435)
    ).isoformat()
    assert clock.now() == START + timedelta(hours=24)


def test_chain_workflow_is_manual_same_sha_bounded_and_preserves_failures() -> None:
    path = ROOT / ".github" / "workflows" / "global-alpha-observation-chain.yml"
    text = path.read_text(encoding="utf-8")
    workflow = yaml.load(text, Loader=yaml.BaseLoader)

    assert set(workflow["on"]) == {"workflow_dispatch"}
    assert workflow["permissions"] == {"actions": "write", "contents": "read"}
    job = workflow["jobs"]["observe-segment"]
    assert int(job["timeout-minutes"]) < 360
    assert "github.event.repository.default_branch" in job["if"]
    assert "GLOBAL_ALPHA_OBSERVATION_ENABLED" in job["if"]
    assert "GOVERNANCE_PIPELINE_MODE == 'shadow'" in job["if"]
    assert "run.conclusion !== \"success\"" in text
    assert "run.run_attempt !== 1" in text
    assert "run.event !== \"workflow_dispatch\"" in text
    assert "global-alpha-observation-chain.yml" in text
    assert "python -m curator.global_alpha_observation_segment" in text
    assert "if: always() && steps.initialize.outcome == 'success'" in text
    assert "actions/upload-artifact@" in text
    assert "BSIDE_ADMIN_TOKEN" not in text
    assert "BSIDE_RELEASE_AUTHORIZER_TOKEN" not in text
