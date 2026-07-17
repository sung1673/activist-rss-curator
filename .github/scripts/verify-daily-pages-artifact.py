from __future__ import annotations

import json
import os
import re
import sys
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


API_ROOT = "https://api.github.com"
API_VERSION = "2022-11-28"
EXPECTED_WORKFLOW_PATH = ".github/workflows/daily.yml"
ALLOWED_EVENTS = frozenset({"schedule", "workflow_dispatch"})
KST = timezone(timedelta(hours=9), name="KST")
REPOSITORY_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


class VerificationError(RuntimeError):
    """A safe-to-print marker verification failure."""


def parse_github_timestamp(value: object) -> datetime | None:
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
        return None
    return parsed.astimezone(timezone.utc)


def resolve_kst_date(value: str | None, *, now: datetime | None = None) -> date:
    text = (value or "").strip()
    if not text:
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        return current.astimezone(KST).date()
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise VerificationError("BSIDE_KST_DATE must use YYYY-MM-DD format") from exc
    if parsed.isoformat() != text:
        raise VerificationError("BSIDE_KST_DATE must use YYYY-MM-DD format")
    return parsed


def require_repository(value: str) -> str:
    repository = value.strip()
    if not REPOSITORY_PATTERN.fullmatch(repository):
        raise VerificationError("GITHUB_REPOSITORY must use owner/repository format")
    return repository


def positive_integer(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value


def github_get_json(
    *,
    repository: str,
    token: str,
    path: str,
    query: dict[str, str] | None = None,
) -> dict[str, Any]:
    suffix = f"?{urlencode(query)}" if query else ""
    endpoint = f"{API_ROOT}/repos/{repository}{path}{suffix}"
    request = Request(
        endpoint,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "bside-daily-pages-marker/1.0",
            "X-GitHub-Api-Version": API_VERSION,
        },
    )
    try:
        with urlopen(request, timeout=15) as response:  # noqa: S310 - fixed GitHub API host
            payload = json.load(response)
    except HTTPError as exc:
        raise VerificationError(f"GitHub API returned HTTP {exc.code}") from exc
    except (URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
        raise VerificationError(f"GitHub API request failed ({type(exc).__name__})") from exc
    if not isinstance(payload, dict):
        raise VerificationError("GitHub API returned an invalid response")
    return payload


def normalize_workflow_path(value: object) -> str:
    path = str(value or "").strip().replace("\\", "/")
    if "@" in path:
        path = path.split("@", 1)[0]
    return path.lstrip("/")


def artifact_window(kst_date: date) -> tuple[datetime, datetime]:
    start = datetime.combine(kst_date, time(hour=5, minute=40), tzinfo=KST)
    end = datetime.combine(kst_date, time(hour=6, minute=5), tzinfo=KST)
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc)


def verify_run(
    *,
    repository: str,
    token: str,
    run_id: int,
) -> dict[str, object]:
    run = github_get_json(
        repository=repository,
        token=token,
        path=f"/actions/runs/{run_id}",
    )
    if positive_integer(run.get("id")) != run_id:
        raise VerificationError(f"run {run_id} returned an inconsistent id")
    if run.get("status") != "completed" or run.get("conclusion") != "success":
        raise VerificationError(f"run {run_id} is not completed successfully")
    event = str(run.get("event") or "")
    if event not in ALLOWED_EVENTS:
        raise VerificationError(f"run {run_id} has an unsupported trigger")

    workflow_id = positive_integer(run.get("workflow_id"))
    if workflow_id is None:
        raise VerificationError(f"run {run_id} has no valid workflow id")
    run_path = normalize_workflow_path(run.get("path"))
    if run_path != EXPECTED_WORKFLOW_PATH:
        raise VerificationError(f"run {run_id} is not from daily.yml")

    workflow = github_get_json(
        repository=repository,
        token=token,
        path=f"/actions/workflows/{workflow_id}",
    )
    if positive_integer(workflow.get("id")) != workflow_id:
        raise VerificationError(f"workflow {workflow_id} returned an inconsistent id")
    if normalize_workflow_path(workflow.get("path")) != EXPECTED_WORKFLOW_PATH:
        raise VerificationError(f"workflow {workflow_id} is not daily.yml")
    return {
        "run_id": run_id,
        "workflow_id": workflow_id,
        "workflow_path": EXPECTED_WORKFLOW_PATH,
        "event": event,
    }


def verify_daily_pages_marker(
    *,
    repository: str,
    token: str,
    kst_date: date,
) -> dict[str, object]:
    marker_name = f"governance-pages-ready-{kst_date.isoformat()}"
    payload = github_get_json(
        repository=repository,
        token=token,
        path="/actions/artifacts",
        query={"name": marker_name, "per_page": "100"},
    )
    raw_artifacts = payload.get("artifacts")
    if not isinstance(raw_artifacts, list):
        raise VerificationError("GitHub artifact response has no artifacts list")

    window_start, window_end = artifact_window(kst_date)
    candidates: list[tuple[datetime, dict[str, Any]]] = []
    for raw_artifact in raw_artifacts:
        if not isinstance(raw_artifact, dict) or raw_artifact.get("name") != marker_name:
            continue
        if raw_artifact.get("expired") is not False:
            continue
        created_at = parse_github_timestamp(raw_artifact.get("created_at"))
        if created_at is None or not window_start <= created_at <= window_end:
            continue
        candidates.append((created_at, raw_artifact))

    if not candidates:
        raise VerificationError(
            f"no unexpired {marker_name} artifact exists in the 05:40-06:05 KST window"
        )

    failures: list[str] = []
    for created_at, artifact in sorted(candidates, key=lambda item: item[0], reverse=True):
        artifact_id = positive_integer(artifact.get("id"))
        workflow_run = artifact.get("workflow_run")
        if artifact_id is None:
            failures.append("a candidate artifact has no valid id")
            continue
        if not isinstance(workflow_run, dict):
            failures.append(f"artifact {artifact_id} has no workflow run")
            continue
        run_id = positive_integer(workflow_run.get("id"))
        if run_id is None:
            failures.append(f"artifact {artifact_id} has no valid workflow run id")
            continue
        try:
            run_result = verify_run(repository=repository, token=token, run_id=run_id)
        except VerificationError as exc:
            failures.append(str(exc))
            continue
        return {
            "date": kst_date.isoformat(),
            "artifact_id": artifact_id,
            "artifact_name": marker_name,
            "created_at": created_at.isoformat().replace("+00:00", "Z"),
            **run_result,
        }

    detail = "; ".join(failures[:3]) or "candidate artifacts were invalid"
    raise VerificationError(f"no valid daily pages marker was found: {detail}")


def main() -> int:
    try:
        token = (os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or "").strip()
        if not token:
            raise VerificationError("GH_TOKEN or GITHUB_TOKEN is required")
        repository = require_repository(os.environ.get("GITHUB_REPOSITORY", ""))
        kst_date = resolve_kst_date(os.environ.get("BSIDE_KST_DATE"))
        result = verify_daily_pages_marker(
            repository=repository,
            token=token,
            kst_date=kst_date,
        )
    except VerificationError as exc:
        print(f"::error::Daily pages marker verification failed: {exc}", file=sys.stderr)
        return 1

    print(
        "pages_marker_verified=1"
        f" date={result['date']}"
        f" artifact_id={result['artifact_id']}"
        f" run_id={result['run_id']}"
        f" created_at={result['created_at']}"
        f" workflow={result['workflow_path']}"
        f" event={result['event']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
