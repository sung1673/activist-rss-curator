"""Durably record one actual GitHub Actions web distribution outcome.

The observation identity is deterministic for a workflow run attempt and an
operation name.  Retrying the POST is therefore safe: the operations API must
acknowledge either one insert or one exact duplicate with HTTP 202.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


ENDPOINT_PATH = "/ops/web-distribution-observations"
MAX_RESPONSE_BYTES = 64 * 1024
MAX_DURATION_MS = 3_600_000
SOURCE = "github_actions"
USER_AGENT = "BSIDE-Governance-Intelligence/1.0 support@bside.ai"
SHA_PATTERN = re.compile(r"^[a-f0-9]{40}$")
OPERATION_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,31}$")
RETRYABLE_HTTP_STATUSES = {408, 425, 429, 500, 502, 503, 504}


class DistributionObservationError(RuntimeError):
    """Raised when an observation cannot be validated or acknowledged."""


class RejectRedirectHandler(HTTPRedirectHandler):
    """Never forward the privileged bearer token to a redirect target."""

    def redirect_request(
        self,
        req: Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        del req, fp, code, msg, headers, newurl
        return None


@dataclass(frozen=True)
class WebDistributionObservation:
    observation_id: str
    observed_at: str
    distribution_target: str
    duration_ms: int
    succeeded: bool
    build_sha: str
    workflow_run_id: int
    workflow_run_attempt: int
    failure_detected_at: str | None = None
    source: str = SOURCE

    def as_api_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "observation_id": self.observation_id,
            "observed_at": self.observed_at,
            "distribution_target": self.distribution_target,
            "duration_ms": self.duration_ms,
            "succeeded": self.succeeded,
            "build_sha": self.build_sha,
            "workflow_run_id": self.workflow_run_id,
            "workflow_run_attempt": self.workflow_run_attempt,
            "failure_detected_at": self.failure_detected_at,
            "source": self.source,
        }
        return payload


def _utc_datetime(value: str, field: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise DistributionObservationError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DistributionObservationError(f"{field} must include a UTC offset")
    return parsed.astimezone(timezone.utc)


def _api_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _positive_integer(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise DistributionObservationError(f"{field} must be a positive integer")
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise DistributionObservationError(f"{field} must be a positive integer") from exc
    if parsed < 1:
        raise DistributionObservationError(f"{field} must be a positive integer")
    return parsed


def parse_succeeded(value: str) -> bool:
    normalized = value.strip().casefold()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise DistributionObservationError("succeeded must be exactly true or false")


def api_endpoint(base_url: str) -> str:
    normalized = base_url.strip().rstrip("/")
    parts = urlsplit(normalized)
    if (
        parts.scheme != "https"
        or not parts.netloc
        or parts.username is not None
        or parts.password is not None
        or parts.query
        or parts.fragment
    ):
        raise DistributionObservationError(
            "BSIDE_API_BASE_URL must be a credential-free, query-free HTTPS URL"
        )
    prefix = normalized if normalized.endswith("/api/v1") else normalized + "/api/v1"
    return prefix + ENDPOINT_PATH


def build_observation(
    *,
    target: str,
    succeeded: bool,
    observed_at: str,
    completed_at: str,
    build_sha: str,
    workflow_run_id: object,
    workflow_run_attempt: object,
    operation: str,
) -> WebDistributionObservation:
    normalized_target = target.strip().casefold()
    if normalized_target not in {"pages", "api"}:
        raise DistributionObservationError("distribution target must be pages or api")
    normalized_sha = build_sha.strip().casefold()
    if SHA_PATTERN.fullmatch(normalized_sha) is None:
        raise DistributionObservationError("build SHA must be a full 40-character Git SHA")
    normalized_operation = operation.strip().casefold()
    if OPERATION_PATTERN.fullmatch(normalized_operation) is None:
        raise DistributionObservationError(
            "operation must contain 1-32 lowercase letters, digits, dots, underscores, or hyphens"
        )

    run_id = _positive_integer(workflow_run_id, "workflow run ID")
    run_attempt = _positive_integer(workflow_run_attempt, "workflow run attempt")
    if run_attempt > 10_000:
        raise DistributionObservationError("workflow run attempt must not exceed 10000")
    observed = _utc_datetime(observed_at, "observed_at")
    completed = _utc_datetime(completed_at, "completed_at")
    elapsed_ms = int(round((completed - observed).total_seconds() * 1000))
    if elapsed_ms < 0 or elapsed_ms > MAX_DURATION_MS:
        raise DistributionObservationError(
            f"distribution duration must be between 0 and {MAX_DURATION_MS} milliseconds"
        )

    return WebDistributionObservation(
        observation_id=(
            f"github-actions:{run_id}:{run_attempt}:{normalized_target}:{normalized_operation}"
        ),
        observed_at=_api_timestamp(observed),
        distribution_target=normalized_target,
        duration_ms=elapsed_ms,
        succeeded=succeeded,
        build_sha=normalized_sha,
        workflow_run_id=run_id,
        workflow_run_attempt=run_attempt,
        failure_detected_at=None if succeeded else _api_timestamp(completed),
    )


def _integer_ack(payload: dict[str, Any], field: str) -> int:
    value = payload.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise DistributionObservationError(f"HTTP 202 response has invalid {field}")
    return value


def validate_acknowledgement(payload: object) -> dict[str, Any]:
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        raise DistributionObservationError("HTTP 202 response did not acknowledge the observation")
    accepted = _integer_ack(payload, "accepted_count")
    inserted = _integer_ack(payload, "inserted_count")
    duplicate = _integer_ack(payload, "duplicate_count")
    if accepted != 1 or inserted not in {0, 1} or duplicate not in {0, 1}:
        raise DistributionObservationError("HTTP 202 response has invalid observation counts")
    if accepted != inserted + duplicate:
        raise DistributionObservationError("HTTP 202 response counts do not balance")
    return payload


def _header(response: Any, name: str) -> str:
    headers = getattr(response, "headers", None)
    if headers is None:
        return ""
    try:
        value = headers.get(name, "")
    except (AttributeError, TypeError):
        return ""
    return str(value or "").strip()


def _content_type(response: Any) -> str:
    return _header(response, "Content-Type").partition(";")[0].strip().casefold()


def _redirect_host(response: Any) -> str:
    location = _header(response, "Location")
    if not location:
        return ""
    try:
        return str(urlsplit(location).hostname or "").casefold()
    except ValueError:
        return ""


def _safe_response_diagnostics(response: Any, *, status: int, body: bytes) -> str:
    """Return response metadata safe for Actions logs.

    The body and redirect URL are intentionally never included.  A bounded body
    digest is sufficient to correlate repeated edge/WAF responses.
    """

    return (
        f"status={status} "
        f"content_type={_content_type(response) or 'missing'} "
        f"body_length={len(body)} "
        f"body_sha256={hashlib.sha256(body).hexdigest()} "
        f"redirect_host={_redirect_host(response) or 'none'}"
    )


def _read_response_body(response: Any) -> bytes:
    body = response.read(MAX_RESPONSE_BYTES + 1)
    if len(body) > MAX_RESPONSE_BYTES:
        diagnostics = _safe_response_diagnostics(
            response,
            status=int(getattr(response, "status", 0)),
            body=body,
        )
        raise DistributionObservationError(
            f"operations API response exceeds 64 KiB ({diagnostics})"
        )
    return body


def _read_json_response(response: Any, *, status: int, body: bytes) -> object:
    diagnostics = _safe_response_diagnostics(response, status=status, body=body)
    if _content_type(response) != "application/json":
        raise DistributionObservationError(
            f"operations API returned a non-JSON content type ({diagnostics})"
        )
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DistributionObservationError(
            f"operations API returned invalid JSON ({diagnostics})"
        ) from exc


def submit_observation(
    *,
    base_url: str,
    token: str,
    observation: WebDistributionObservation,
    attempts: int = 3,
    timeout_seconds: float = 15,
    opener: Callable[..., Any] | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    if len(token.strip()) < 20 or any(character.isspace() for character in token.strip()):
        raise DistributionObservationError("BSIDE_OPS_TOKEN is missing or invalid")
    if attempts < 1:
        raise DistributionObservationError("POST attempts must be positive")
    endpoint = api_endpoint(base_url)
    body = json.dumps(
        {"observations": [observation.as_api_dict()]},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    request = Request(
        endpoint,
        data=body,
        method="POST",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token.strip()}",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        },
    )
    request_opener = opener or build_opener(RejectRedirectHandler()).open

    for attempt_number in range(1, attempts + 1):
        try:
            with request_opener(request, timeout=timeout_seconds) as response:
                status = int(response.status)
                response_body = _read_response_body(response)
        except HTTPError as exc:
            if exc.code in RETRYABLE_HTTP_STATUSES and attempt_number < attempts:
                sleeper(float(2 ** (attempt_number - 1)))
                continue
            try:
                error_body = _read_response_body(exc)
            except DistributionObservationError as diagnostic_error:
                raise diagnostic_error from exc
            diagnostics = _safe_response_diagnostics(
                exc,
                status=int(exc.code),
                body=error_body,
            )
            raise DistributionObservationError(
                f"operations API returned HTTP {exc.code} ({diagnostics})"
            ) from exc
        except (URLError, TimeoutError, OSError) as exc:
            if attempt_number < attempts:
                sleeper(float(2 ** (attempt_number - 1)))
                continue
            raise DistributionObservationError(
                f"operations API request failed after {attempts} attempts"
            ) from exc

        if status != 202:
            diagnostics = _safe_response_diagnostics(
                response,
                status=status,
                body=response_body,
            )
            raise DistributionObservationError(
                "operations API returned an unexpected status; "
                f"exactly 202 is required ({diagnostics})"
            )
        payload = _read_json_response(response, status=status, body=response_body)
        return validate_acknowledgement(payload)

    raise DistributionObservationError("operations API request was not attempted")


def _github_output(name: str, value: object) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT", "").strip()
    if not output_path:
        return
    with Path(output_path).open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(f"{name}={value}\n")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Record an actual Pages or API deployment outcome."
    )
    parser.add_argument("--target", required=True, choices=("pages", "api"))
    parser.add_argument("--operation", required=True)
    parser.add_argument("--succeeded", required=True, choices=("true", "false"))
    parser.add_argument("--observed-at", required=True)
    parser.add_argument("--completed-at")
    parser.add_argument("--api-base-url", default=os.environ.get("BSIDE_API_BASE_URL", ""))
    parser.add_argument("--ops-token", default=os.environ.get("BSIDE_OPS_TOKEN", ""))
    parser.add_argument("--build-sha", default=os.environ.get("GITHUB_SHA", ""))
    parser.add_argument("--workflow-run-id", default=os.environ.get("GITHUB_RUN_ID", ""))
    parser.add_argument(
        "--workflow-run-attempt", default=os.environ.get("GITHUB_RUN_ATTEMPT", "")
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    completed_at = args.completed_at or _api_timestamp(datetime.now(timezone.utc))
    try:
        observation = build_observation(
            target=args.target,
            succeeded=parse_succeeded(args.succeeded),
            observed_at=args.observed_at,
            completed_at=completed_at,
            build_sha=args.build_sha,
            workflow_run_id=args.workflow_run_id,
            workflow_run_attempt=args.workflow_run_attempt,
            operation=args.operation,
        )
        acknowledgement = submit_observation(
            base_url=args.api_base_url,
            token=args.ops_token,
            observation=observation,
        )
    except DistributionObservationError as exc:
        print(
            f"::error::Web distribution observation was not durably acknowledged: {exc}",
            file=sys.stderr,
        )
        return 1

    duplicate_count = int(acknowledgement["duplicate_count"])
    _github_output("observation_id", observation.observation_id)
    _github_output("duplicate_count", duplicate_count)
    print(
        f"Recorded {observation.distribution_target} distribution observation "
        f"{observation.observation_id} (duplicate_count={duplicate_count})."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
