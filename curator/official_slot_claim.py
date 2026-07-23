from __future__ import annotations

import argparse
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping
from urllib.parse import urlsplit, urlunsplit

import httpx

from .official_schedule import (
    INCREMENTAL_CRON_EXPRESSIONS,
    next_incremental_slot,
    slot_iso,
    slot_matches_incremental_schedule,
)


_REVISION_RE = re.compile(r"^[0-9a-f]{7,40}$")
_ENTITY_ID_RE = re.compile(r"^[0-9A-Za-z_.:-]{1,96}$")


class OfficialSlotClaimError(RuntimeError):
    """A scheduled run could not obtain an exact durable slot claim."""


class OfficialSlotClaimActivationError(OfficialSlotClaimError):
    """The server initialized the claim epoch without attributing the current run."""


@dataclass(frozen=True)
class OfficialSlotClaim:
    claim_id: str
    pipeline: str
    github_run_id: str
    github_run_attempt: int
    event_schedule: str
    scheduled_slot_at: str
    trigger_created_at: str
    claimed_at: str
    next_cadence_slot_at: str
    trigger_lag_seconds: int
    claim_lag_seconds: int
    late: bool
    status: str
    terminal_reason: str | None
    duplicate: bool


def _validated_api_base_url(raw: str) -> str:
    value = raw.strip()
    if not value:
        return ""
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise OfficialSlotClaimError(
            "official slot claim API URL must be absolute HTTPS without credentials, query, or fragment"
        )
    path = parsed.path.rstrip("/")
    if not path.endswith("/api/v1"):
        path += "/api/v1"
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def _timestamp(value: object, field: str) -> datetime:
    raw = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise OfficialSlotClaimError(f"slot claim ACK {field} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise OfficialSlotClaimError(f"slot claim ACK {field} must include a timezone")
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def _environment_request(environment: Mapping[str, str]) -> dict[str, object]:
    pipeline = "ingest-official"
    github_run_id = str(environment.get("GITHUB_RUN_ID", "")).strip()
    attempt_raw = str(environment.get("GITHUB_RUN_ATTEMPT", "")).strip()
    event_schedule = str(environment.get("CURATOR_EVENT_SCHEDULE", "")).strip()
    trigger_created_at = str(
        environment.get("CURATOR_GITHUB_RUN_CREATED_AT", "")
    ).strip()
    code_revision = str(
        environment.get("GITHUB_SHA", "")
        or environment.get("CURATOR_CODE_REVISION", "")
    ).strip().casefold()
    repair_expected_at = str(
        environment.get("CURATOR_OFFICIAL_SLOT_REPAIR_EXPECTED_AT", "")
    ).strip()
    if not github_run_id.isdigit() or not attempt_raw.isdigit() or int(attempt_raw) < 1:
        raise OfficialSlotClaimError("scheduled slot claim requires numeric GitHub run identity")
    if event_schedule not in INCREMENTAL_CRON_EXPRESSIONS:
        raise OfficialSlotClaimError("scheduled slot claim requires an incremental event schedule")
    trigger = _timestamp(trigger_created_at, "trigger_created_at")
    if not _REVISION_RE.fullmatch(code_revision):
        raise OfficialSlotClaimError("scheduled slot claim requires a 7-40 character Git SHA")
    request: dict[str, object] = {
        "action": "repair" if repair_expected_at else "claim",
        "pipeline": pipeline,
        "github_run_id": github_run_id,
        "github_run_attempt": int(attempt_raw),
        "event_schedule": event_schedule,
        "trigger_created_at": trigger.isoformat(),
        "code_revision": code_revision,
    }
    if repair_expected_at:
        expected = _timestamp(repair_expected_at, "expected_slot_at")
        if not slot_matches_incremental_schedule(expected, event_schedule):
            raise OfficialSlotClaimError(
                "repair expected slot does not belong to event schedule"
            )
        request["expected_slot_at"] = expected.isoformat()
    return request


class OfficialSlotClaimClient:
    def __init__(
        self,
        *,
        base_url: str | None = None,
        token: str | None = None,
        timeout: float = 15.0,
        max_ack_retries: int = 2,
        backoff_seconds: float = 0.25,
        transport: httpx.BaseTransport | None = None,
        client_factory: Callable[..., httpx.Client] = httpx.Client,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        configured_url = (
            base_url
            if base_url is not None
            else os.environ.get("BSIDE_API_BASE_URL", "")
            or os.environ.get("GOVERNANCE_API_BASE_URL", "")
        )
        self.base_url = _validated_api_base_url(configured_url).rstrip("/")
        self.token = (
            token if token is not None else os.environ.get("BSIDE_OPS_TOKEN", "")
        ).strip()
        if not self.base_url or not self.token:
            raise OfficialSlotClaimError(
                "official slot claim requires BSIDE_API_BASE_URL and BSIDE_OPS_TOKEN"
            )
        if timeout <= 0 or max_ack_retries < 0 or backoff_seconds < 0:
            raise ValueError("invalid official slot claim retry configuration")
        self.timeout = timeout
        self.max_ack_retries = max_ack_retries
        self.backoff_seconds = backoff_seconds
        self.transport = transport
        self.client_factory = client_factory
        self.sleeper = sleeper

    @property
    def endpoint(self) -> str:
        return f"{self.base_url}/ops/official-slot-claims"

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json; charset=utf-8",
        }

    @staticmethod
    def _json_object(response: httpx.Response) -> dict[str, object]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise OfficialSlotClaimError(
                f"official slot claim API returned invalid JSON (HTTP {response.status_code})"
            ) from exc
        if not isinstance(payload, dict):
            raise OfficialSlotClaimError("official slot claim API response must be an object")
        return payload

    def claim(self, request_payload: Mapping[str, object]) -> OfficialSlotClaim:
        request = dict(request_payload)
        expected_keys = {
            "action",
            "pipeline",
            "github_run_id",
            "github_run_attempt",
            "event_schedule",
            "trigger_created_at",
            "code_revision",
        }
        if request.get("action") == "repair":
            expected_keys.add("expected_slot_at")
        elif request.get("action") != "claim":
            raise OfficialSlotClaimError("official slot claim action is invalid")
        if set(request) != expected_keys:
            raise OfficialSlotClaimError("official slot claim request fields are not exact")
        encoded = json.dumps(request, separators=(",", ":"), sort_keys=True).encode("utf-8")
        response: httpx.Response | None = None
        last_error: Exception | None = None
        for retry in range(self.max_ack_retries + 1):
            try:
                with self.client_factory(
                    timeout=self.timeout,
                    transport=self.transport,
                    follow_redirects=False,
                ) as client:
                    response = client.post(
                        self.endpoint, content=encoded, headers=self._headers()
                    )
            except httpx.TransportError as exc:
                last_error = exc
                if retry < self.max_ack_retries:
                    self.sleeper(self.backoff_seconds * (2**retry))
                    continue
                break
            if 500 <= response.status_code <= 599 and retry < self.max_ack_retries:
                self.sleeper(self.backoff_seconds * (2**retry))
                continue
            break
        if response is None:
            raise OfficialSlotClaimError("official slot claim API did not acknowledge the run") from last_error
        payload = self._json_object(response)
        error = payload.get("error")
        error_code = (
            str(error.get("code") or "") if isinstance(error, dict) else str(error or "")
        )
        if response.status_code == 409 and error_code == "official_slot_claim_activated":
            raise OfficialSlotClaimActivationError(
                "official slot claim epoch was activated; this ambiguous run was not attributed"
            )
        if response.status_code != 200:
            raise OfficialSlotClaimError(
                f"official slot claim rejected (HTTP {response.status_code}): "
                f"{error_code or 'unknown_error'}"
            )
        return self._validate_ack(payload, request)

    @staticmethod
    def _validate_ack(
        payload: Mapping[str, object], request: Mapping[str, object]
    ) -> OfficialSlotClaim:
        if payload.get("ok") is not True or payload.get("accepted") != 1:
            raise OfficialSlotClaimError("official slot claim ACK omitted accepted=1")
        for field in ("pipeline", "github_run_id", "github_run_attempt", "event_schedule"):
            if payload.get(field) != request.get(field):
                raise OfficialSlotClaimError(f"official slot claim ACK changed {field}")
        github_run_attempt = payload.get("github_run_attempt")
        if type(github_run_attempt) is not int:
            raise OfficialSlotClaimError(
                "official slot claim ACK has an invalid github_run_attempt"
            )
        claim_id = str(payload.get("claim_id") or "")
        if _ENTITY_ID_RE.fullmatch(claim_id) is None:
            raise OfficialSlotClaimError("official slot claim ACK has an invalid claim_id")
        slot = _timestamp(payload.get("scheduled_slot_at"), "scheduled_slot_at")
        trigger = _timestamp(payload.get("trigger_created_at"), "trigger_created_at")
        claimed = _timestamp(payload.get("claimed_at"), "claimed_at")
        next_slot = _timestamp(payload.get("next_cadence_slot_at"), "next_cadence_slot_at")
        if slot_iso(trigger) != request.get("trigger_created_at"):
            raise OfficialSlotClaimError("official slot claim ACK changed trigger_created_at")
        schedule = str(request["event_schedule"])
        if not slot_matches_incremental_schedule(slot, schedule):
            raise OfficialSlotClaimError("claimed slot does not belong to event_schedule")
        if request.get("action") == "repair" and slot_iso(slot) != request.get(
            "expected_slot_at"
        ):
            raise OfficialSlotClaimError("repair ACK changed the exact expected slot")
        if next_slot != next_incremental_slot(slot):
            raise OfficialSlotClaimError("slot claim ACK has an invalid next cadence boundary")
        if trigger < slot or claimed < trigger:
            raise OfficialSlotClaimError("slot claim timestamps are not monotonic")
        trigger_lag = payload.get("trigger_lag_seconds")
        claim_lag = payload.get("claim_lag_seconds")
        late = payload.get("late")
        status = str(payload.get("status") or "").strip().casefold()
        terminal_raw = payload.get("terminal_reason")
        terminal_reason = (
            str(terminal_raw).strip().casefold() if terminal_raw not in (None, "") else None
        )
        duplicate = payload.get("duplicate")
        if (
            type(trigger_lag) is not int
            or trigger_lag != int((trigger - slot).total_seconds())
            or type(claim_lag) is not int
            or claim_lag != int((claimed - slot).total_seconds())
            or type(late) is not bool
            or late is not (claimed >= next_slot)
            or status not in {"claimed", "failed", "completed"}
            or (
                terminal_reason is not None
                and re.fullmatch(r"[a-z][a-z0-9_]{0,63}", terminal_reason) is None
            )
            or (terminal_reason is not None and status != "failed")
            or type(duplicate) is not bool
        ):
            raise OfficialSlotClaimError("slot claim ACK lag/late fields are inconsistent")
        return OfficialSlotClaim(
            claim_id=claim_id,
            pipeline=str(payload["pipeline"]),
            github_run_id=str(payload["github_run_id"]),
            github_run_attempt=github_run_attempt,
            event_schedule=schedule,
            scheduled_slot_at=slot_iso(slot),
            trigger_created_at=slot_iso(trigger),
            claimed_at=slot_iso(claimed),
            next_cadence_slot_at=slot_iso(next_slot),
            trigger_lag_seconds=trigger_lag,
            claim_lag_seconds=claim_lag,
            late=late,
            status=status,
            terminal_reason=terminal_reason,
            duplicate=duplicate,
        )


def claim_from_environment(
    environment: Mapping[str, str] | None = None,
    *,
    client: OfficialSlotClaimClient | None = None,
) -> OfficialSlotClaim:
    values = os.environ if environment is None else environment
    return (client or OfficialSlotClaimClient()).claim(_environment_request(values))


def append_github_environment(path: Path, claim: OfficialSlotClaim) -> None:
    rows = {
        "CURATOR_OFFICIAL_SLOT_CLAIM_ID": claim.claim_id,
        "CURATOR_OFFICIAL_SCHEDULED_SLOT_AT": claim.scheduled_slot_at,
        "CURATOR_OFFICIAL_SLOT_CLAIMED_AT": claim.claimed_at,
        "CURATOR_OFFICIAL_NEXT_CADENCE_SLOT_AT": claim.next_cadence_slot_at,
        "CURATOR_OFFICIAL_TRIGGER_LAG_SECONDS": str(claim.trigger_lag_seconds),
        "CURATOR_OFFICIAL_CLAIM_LAG_SECONDS": str(claim.claim_lag_seconds),
        "CURATOR_OFFICIAL_SLOT_LATE": "1" if claim.late else "0",
        "CURATOR_OFFICIAL_SLOT_TERMINAL_NOOP": (
            "1" if claim.status == "completed" or claim.terminal_reason is not None else "0"
        ),
        "CURATOR_GITHUB_RUN_ID": claim.github_run_id,
        "CURATOR_GITHUB_RUN_ATTEMPT": str(claim.github_run_attempt),
    }
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        for key, value in rows.items():
            if "\n" in value or "\r" in value:
                raise OfficialSlotClaimError("slot claim environment value contains a newline")
            handle.write(f"{key}={value}\n")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Claim one durable official-ingest schedule slot")
    parser.add_argument("--github-env", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        claim = claim_from_environment()
        append_github_environment(args.github_env, claim)
    except (OfficialSlotClaimError, OSError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(claim.__dict__, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
