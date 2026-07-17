from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


REPORT_PATH = Path(".watchdog-report.md")


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
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def unwrap_payload(payload: object) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    return data if isinstance(data, dict) else payload


def fetch_health(base_url: str, token: str) -> dict[str, Any]:
    normalized = base_url.rstrip("/")
    suffix = "/ops/health" if normalized.endswith("/api/v1") else "/api/v1/ops/health"
    endpoint = normalized + suffix
    request = Request(
        endpoint,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "bside-governance-watchdog/1.0",
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
    outbox_age: float | None,
) -> str:
    status = "INCIDENT" if reasons else "HEALTHY"
    lines = [
        f"## BSIDE governance pipeline watchdog: {status}",
        "",
        f"Checked at: `{now.isoformat()}`",
        f"Last successful ingest: `{payload.get('last_success_at') or 'unknown'}`",
        f"Ingest age (minutes): `{round(ingest_age, 1) if ingest_age is not None else 'unknown'}`",
        f"Pending outbox: `{integer(payload.get('pending_outbox'))}`",
        f"Oldest pending item: `{payload.get('oldest_pending_at') or 'none'}`",
        f"Outbox age (minutes): `{round(outbox_age, 1) if outbox_age is not None else 'n/a'}`",
        f"Dead-letter count: `{integer(payload.get('dead_letter_count'))}`",
    ]
    if reasons:
        lines.extend(["", "### Incident reasons", ""])
        lines.extend(f"- {reason}" for reason in reasons)
    else:
        lines.extend(["", "All configured freshness and delivery thresholds are within budget."])
    return "\n".join(lines) + "\n"


def main() -> int:
    now = datetime.now(timezone.utc)
    base_url = os.environ.get("BSIDE_API_BASE_URL", "").strip()
    token = os.environ.get("BSIDE_OPS_TOKEN", "").strip()
    max_ingest_age = integer(os.environ.get("WATCHDOG_MAX_INGEST_AGE_MINUTES"), 90)
    max_outbox_age = integer(os.environ.get("WATCHDOG_MAX_OUTBOX_AGE_MINUTES"), 5)
    reasons: list[str] = []
    payload: dict[str, Any] = {}

    if not base_url or not token:
        missing = [name for name, value in (("BSIDE_API_BASE_URL", base_url), ("BSIDE_OPS_TOKEN", token)) if not value]
        reasons.append("Missing operational configuration: " + ", ".join(missing))
    elif not base_url.startswith("https://"):
        reasons.append("BSIDE_API_BASE_URL must use HTTPS")
    else:
        try:
            payload = fetch_health(base_url, token)
            if not payload:
                reasons.append("The health endpoint returned an empty or invalid data object")
        except HTTPError as exc:
            reasons.append(f"Health endpoint returned HTTP {exc.code}")
        except (URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
            reasons.append(f"Health endpoint request failed: {type(exc).__name__}: {exc}")

    last_success = parse_timestamp(payload.get("last_success_at"))
    oldest_pending = parse_timestamp(payload.get("oldest_pending_at"))
    ingest_age = minutes_since(last_success, now)
    outbox_age = minutes_since(oldest_pending, now)
    pending_outbox = integer(payload.get("pending_outbox"))
    dead_letter_count = integer(payload.get("dead_letter_count"))

    if payload:
        if ingest_age is None:
            reasons.append("The health response has no valid last_success_at timestamp")
        elif ingest_age < -5:
            reasons.append(f"Last successful ingest timestamp is {-ingest_age:.1f} minutes in the future")
        elif ingest_age > max_ingest_age:
            reasons.append(f"No successful ingest for {ingest_age:.1f} minutes (budget: {max_ingest_age})")
        if pending_outbox < 0:
            reasons.append("The health response has a negative pending_outbox count")
        if pending_outbox > 0 and outbox_age is None:
            reasons.append("The delivery queue is pending but oldest_pending_at is missing or invalid")
        elif pending_outbox > 0 and outbox_age is not None and outbox_age < -5:
            reasons.append(f"Oldest pending delivery timestamp is {-outbox_age:.1f} minutes in the future")
        elif pending_outbox > 0 and outbox_age is not None and outbox_age > max_outbox_age:
            reasons.append(f"Oldest delivery has waited {outbox_age:.1f} minutes (budget: {max_outbox_age})")
        if dead_letter_count < 0:
            reasons.append("The health response has a negative dead_letter_count")
        if dead_letter_count > 0:
            reasons.append(f"Delivery dead-letter queue contains {dead_letter_count} item(s)")

    REPORT_PATH.write_text(
        build_report(
            now=now,
            payload=payload,
            reasons=reasons,
            ingest_age=ingest_age,
            outbox_age=outbox_age,
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
