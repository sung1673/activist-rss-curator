from __future__ import annotations

import hashlib
import json
import os
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


REPORT_PATH = Path(".watchdog-report.md")
USER_AGENT = "bside-governance-watchdog/2.0"
CONFIG_PATTERN = re.compile(
    r"^window\.__BSIDE_GOVERNANCE_CONFIG__=Object\.freeze\((\{.*\})\);\s*$"
)


@dataclass(frozen=True)
class AvailabilityObservation:
    observation_id: str
    route_template: str
    observed_at: str
    http_status: int
    duration_ms: int
    succeeded: bool
    build_sha: str
    source: str = "github_watchdog"
    error_class: str | None = None


def parse_timestamp(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def integer(value: object, default: int = 0) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


def unwrap_payload(payload: object) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    return data if isinstance(data, dict) else payload


def api_endpoint(base_url: str, suffix: str) -> str:
    normalized = base_url.rstrip("/")
    return normalized + suffix if normalized.endswith("/api/v1") else normalized + "/api/v1" + suffix


def fetch_health(base_url: str, token: str) -> dict[str, Any]:
    request = Request(
        api_endpoint(base_url, "/ops/health"),
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
            "User-Agent": USER_AGENT,
        },
    )
    with urlopen(request, timeout=15) as response:  # noqa: S310 - configured first-party endpoint
        return unwrap_payload(json.load(response))


def fetch_deployed_build_sha(web_base_url: str) -> str:
    request = Request(
        web_base_url.rstrip("/") + "/governance/config.js",
        headers={"Accept": "application/javascript", "User-Agent": USER_AGENT},
    )
    with urlopen(request, timeout=15) as response:  # noqa: S310 - configured first-party endpoint
        body = response.read(4097)
    if len(body) > 4096:
        raise ValueError("governance config.js exceeds the safe parsing limit")
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("governance config.js is not UTF-8") from exc
    match = CONFIG_PATTERN.fullmatch(text)
    if match is None:
        raise ValueError("governance config.js has an unexpected format")
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise ValueError("governance config.js contains invalid JSON") from exc
    build_sha = str(payload.get("buildSha") if isinstance(payload, dict) else "").casefold()
    if len(build_sha) != 40 or any(character not in "0123456789abcdef" for character in build_sha):
        raise ValueError("deployed governance buildSha must be a full Git SHA")
    return build_sha


def active_deployment_sha(payload: dict[str, Any]) -> str:
    deployment_status = str(payload.get("active_deployment_status") or "").strip().casefold()
    if deployment_status != "observed":
        raise ValueError(
            "authenticated active deployment status is " + (deployment_status or "missing")
        )
    deployment = payload.get("active_deployment")
    if not isinstance(deployment, dict):
        raise ValueError("health response has no authenticated active deployment")
    build_sha = str(deployment.get("build_sha") or "").strip().casefold()
    if len(build_sha) != 40 or any(character not in "0123456789abcdef" for character in build_sha):
        raise ValueError("authenticated active deployment build_sha must be a full Git SHA")
    if parse_timestamp(deployment.get("observed_at")) is None:
        raise ValueError("authenticated active deployment observed_at is invalid")
    if str(deployment.get("distribution_target") or "").strip().casefold() not in {
        "pages",
        "api",
    }:
        raise ValueError("authenticated active deployment target is invalid")
    return build_sha


def _valid_https_url(url: str) -> bool:
    parts = urlsplit(url)
    return (
        parts.scheme == "https"
        and bool(parts.netloc)
        and parts.username is None
        and parts.password is None
        and not parts.query
        and not parts.fragment
    )


def probe_url(*, url: str, route_template: str, build_sha: str) -> AvailabilityObservation:
    started = time.monotonic()
    status = 0
    error_class: str | None = None
    try:
        request = Request(url, headers={"Accept": "*/*", "User-Agent": USER_AGENT})
        with urlopen(request, timeout=15) as response:  # noqa: S310 - validated first-party HTTPS URL
            status = int(response.status)
            response.read(1024)
    except HTTPError as exc:
        status = int(exc.code)
        error_class = "HTTPError"
    except (URLError, TimeoutError, OSError) as exc:
        error_class = type(exc).__name__
    duration_ms = max(0, int(round((time.monotonic() - started) * 1000)))
    observed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    identity = "\x1f".join((build_sha, route_template, observed_at, str(status)))
    return AvailabilityObservation(
        observation_id="availability:" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:48],
        route_template=route_template,
        observed_at=observed_at,
        http_status=status,
        duration_ms=duration_ms,
        succeeded=200 <= status < 400,
        build_sha=build_sha,
        error_class=error_class,
    )


def submit_availability(
    base_url: str,
    token: str,
    observations: list[AvailabilityObservation],
) -> dict[str, Any]:
    body = json.dumps(
        {"observations": [asdict(observation) for observation in observations]},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    request = Request(
        api_endpoint(base_url, "/ops/availability-observations"),
        data=body,
        method="POST",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "User-Agent": USER_AGENT,
        },
    )
    with urlopen(request, timeout=15) as response:  # noqa: S310 - configured first-party endpoint
        return unwrap_payload(json.load(response))


def minutes_since(value: datetime | None, now: datetime) -> float | None:
    if value is None:
        return None
    return (now - value).total_seconds() / 60.0


def output_value(name: str, value: str) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        return
    with Path(output_path).open("a", encoding="utf-8") as output:
        output.write(f"{name}={value}\n")


def build_report(
    *,
    now: datetime,
    payload: dict[str, Any],
    reasons: list[str],
    ingest_age: float | None,
    observations: list[AvailabilityObservation],
) -> str:
    status = "INCIDENT" if reasons else "HEALTHY"
    lines = [
        f"## BSIDE governance pipeline watchdog: {status}",
        "",
        f"Checked at: `{now.isoformat()}`",
        f"Last successful ingest: `{payload.get('last_success_at') or 'unknown'}`",
        f"Ingest age (minutes): `{round(ingest_age, 1) if ingest_age is not None else 'unknown'}`",
        f"Measured deployed build: `{observations[0].build_sha if observations else 'unknown'}`",
        "",
        "### Web availability observations",
        "",
        "| Route template | HTTP | Duration (ms) | Result |",
        "|---|---:|---:|---|",
    ]
    for observation in observations:
        result = "ok" if observation.succeeded else (observation.error_class or "failed")
        lines.append(
            f"| `{observation.route_template}` | {observation.http_status} | "
            f"{observation.duration_ms} | {result} |"
        )
    if reasons:
        lines.extend(["", "### Incident reasons", ""])
        lines.extend(f"- {reason}" for reason in reasons)
    else:
        lines.extend(["", "Ingest freshness and all measured web routes are within budget."])
    return "\n".join(lines) + "\n"


def _public_routes(web_base_url: str, include_governance: bool) -> list[tuple[str, str]]:
    base = web_base_url.rstrip("/")
    routes = [("/", base + "/"), ("/feed.xml", base + "/feed.xml")]
    if include_governance:
        routes.append(("/governance/", base + "/governance/"))
    return routes


def main() -> int:
    now = datetime.now(timezone.utc)
    base_url = os.environ.get("BSIDE_API_BASE_URL", "").strip()
    web_base_url = os.environ.get("BSIDE_PUBLIC_WEB_URL", "").strip()
    token = os.environ.get("BSIDE_OPS_TOKEN", "").strip()
    build_sha = ""
    max_ingest_age = integer(os.environ.get("WATCHDOG_MAX_INGEST_AGE_MINUTES"), 90)
    include_governance = os.environ.get("WATCHDOG_GOVERNANCE_PAGES", "false").casefold() == "true"
    reasons: list[str] = []
    payload: dict[str, Any] = {}
    observations: list[AvailabilityObservation] = []

    missing = [
        name
        for name, value in (
            ("BSIDE_API_BASE_URL", base_url),
            ("BSIDE_PUBLIC_WEB_URL", web_base_url),
            ("BSIDE_OPS_TOKEN", token),
        )
        if not value
    ]
    if missing:
        reasons.append("Missing operational configuration: " + ", ".join(missing))
    if base_url and not _valid_https_url(base_url):
        reasons.append("BSIDE_API_BASE_URL must be a credential-free, query-free HTTPS URL")
    if web_base_url and not _valid_https_url(web_base_url):
        reasons.append("BSIDE_PUBLIC_WEB_URL must be a credential-free, query-free HTTPS URL")
    operational_config_valid = not missing and _valid_https_url(base_url) and _valid_https_url(
        web_base_url
    )
    if operational_config_valid:
        try:
            payload = fetch_health(base_url, token)
            if not payload:
                reasons.append("The health endpoint returned an empty or invalid data object")
        except HTTPError as exc:
            reasons.append(f"Health endpoint returned HTTP {exc.code}")
        except (URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
            reasons.append(f"Health endpoint request failed: {type(exc).__name__}: {exc}")

    if payload:
        try:
            build_sha = active_deployment_sha(payload)
        except ValueError as exc:
            reasons.append(str(exc))

    if include_governance and operational_config_valid and build_sha:
        try:
            public_build_sha = fetch_deployed_build_sha(web_base_url)
        except (HTTPError, URLError, TimeoutError, OSError, ValueError) as exc:
            reasons.append(
                f"Public governance revision could not be verified: {type(exc).__name__}: {exc}"
            )
        else:
            if public_build_sha != build_sha:
                reasons.append(
                    f"Public governance revision {public_build_sha} does not match authenticated "
                    f"active deployment {build_sha}"
                )

    # Availability is independent evidence. Once the authenticated immutable
    # deployment SHA is known, probe and submit even when freshness, public-SHA
    # verification, or another incident check has already failed.
    if operational_config_valid and build_sha:
        probe_targets = _public_routes(web_base_url, include_governance)
        probe_targets.append(("/api/v1/health", api_endpoint(base_url, "/health")))
        observations = [
            probe_url(url=url, route_template=route, build_sha=build_sha)
            for route, url in probe_targets
        ]
        for observation in observations:
            if not observation.succeeded:
                reasons.append(
                    f"{observation.route_template} availability probe failed "
                    f"(HTTP {observation.http_status}, {observation.error_class or 'unexpected status'})"
                )
        try:
            acknowledgement = submit_availability(base_url, token, observations)
            accepted = integer(acknowledgement.get("accepted_count"), -1)
            if accepted != len(observations):
                reasons.append(
                    f"Availability evidence ACK mismatch: expected {len(observations)}, got {accepted}"
                )
        except HTTPError as exc:
            reasons.append(f"Availability evidence endpoint returned HTTP {exc.code}")
        except (URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
            reasons.append(f"Availability evidence submission failed: {type(exc).__name__}: {exc}")

    last_success = parse_timestamp(payload.get("last_success_at"))
    ingest_age = minutes_since(last_success, now)

    if payload:
        official_sources = payload.get("official_sources")
        if not isinstance(official_sources, dict):
            reasons.append("The health response has no source-specific official ingest state")
        else:
            for source in ("dart", "kind"):
                source_state = official_sources.get(source)
                if not isinstance(source_state, dict):
                    reasons.append(f"The health response has no {source.upper()} ingest state")
                    continue
                # The API exposes this explicit scheduled-only field so a
                # successful manual/backfill/company-master run can never
                # refresh the production cadence watchdog accidentally.
                source_success = parse_timestamp(
                    source_state.get("last_scheduled_success_at")
                )
                source_age = minutes_since(source_success, now)
                if source_age is None:
                    reasons.append(f"{source.upper()} has no successful ingest timestamp")
                elif source_age < -5:
                    reasons.append(
                        f"{source.upper()} successful ingest timestamp is "
                        f"{-source_age:.1f} minutes in the future"
                    )
                elif source_age > max_ingest_age:
                    reasons.append(
                        f"No successful {source.upper()} ingest for {source_age:.1f} minutes "
                        f"(budget: {max_ingest_age})"
                    )

    REPORT_PATH.write_text(
        build_report(
            now=now,
            payload=payload,
            reasons=reasons,
            ingest_age=ingest_age,
            observations=observations,
        ),
        encoding="utf-8",
    )
    incident = bool(reasons)
    output_value("incident", "true" if incident else "false")
    output_value("report_path", REPORT_PATH.as_posix())
    print(REPORT_PATH.read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
