from __future__ import annotations

import argparse
import json
import math
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Mapping
from urllib.parse import urlencode, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import httpx


KST = ZoneInfo("Asia/Seoul")
REVISION_RE = re.compile(r"^[0-9a-f]{40}$")
COUNT_FIELDS = (
    "official_evidence_total_count",
    "official_evidence_linked_count",
    "top_sensitive_total_count",
    "top_sensitive_reviewed_count",
    "original_language_total_count",
    "original_language_preserved_count",
    "source_right_total_count",
    "valid_source_right_count",
)
# These columns remain in schema version 7 for migration compatibility. They
# are deliberately frozen as zero and are never consumed by the release gate;
# protected human benchmark.json is the sole same-event ground truth.
RESERVED_BENCHMARK_FIELDS = (
    "same_story_evaluated_pair_count",
    "same_story_predicted_same_count",
    "same_story_true_positive_count",
)


class QualitySnapshotError(RuntimeError):
    """A mutable or incomplete production metric set cannot be frozen."""


def _api_base(raw: str) -> str:
    parsed = urlsplit(raw.strip())
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise QualitySnapshotError("quality API base must be an absolute credential-free HTTPS URL")
    path = parsed.path.rstrip("/")
    if not path.endswith("/api/v1"):
        path += "/api/v1"
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def _number(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise QualitySnapshotError(f"{field} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise QualitySnapshotError(f"{field} must be finite and non-negative")
    return result


def _count(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise QualitySnapshotError(f"{field} must be a non-negative integer")
    return value


def _json_object(response: httpx.Response, operation: str) -> dict[str, object]:
    try:
        payload = response.json()
    except ValueError as exc:
        raise QualitySnapshotError(f"{operation} returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise QualitySnapshotError(f"{operation} response must be an object")
    return payload


def build_observation(
    export: Mapping[str, object], *, observation_date: date, revision: str
) -> dict[str, object]:
    revision = revision.casefold()
    if not REVISION_RE.fullmatch(revision):
        raise QualitySnapshotError("revision must be a full lowercase Git SHA")
    if export.get("ok") is not True or export.get("evidence_source") != "production_db_export":
        raise QualitySnapshotError("quality source must be the production DB export")
    if export.get("is_synthetic") is not False or export.get("distribution_mode") != "web_only":
        raise QualitySnapshotError("synthetic or non-web-only quality evidence is forbidden")
    if export.get("release_state") not in {"preview", "live"}:
        raise QualitySnapshotError("quality snapshots require preview or live release state")
    expected_day = observation_date.isoformat()
    bounds = export.get("range")
    if not isinstance(bounds, dict) or bounds.get("from") != expected_day or bounds.get("to") != expected_day:
        raise QualitySnapshotError("quality export range must be exactly one completed KST day")
    revisions = export.get("code_revisions")
    if not isinstance(revisions, list) or revisions != [revision]:
        raise QualitySnapshotError("quality export must contain exactly the expected revision")
    operations = export.get("operations_days")
    if not isinstance(operations, list):
        raise QualitySnapshotError("quality export omitted operations_days")
    matches = [
        item
        for item in operations
        if isinstance(item, dict)
        and item.get("observation_date") == expected_day
        and str(item.get("code_revision") or "").casefold() == revision
    ]
    if len(matches) != 1:
        raise QualitySnapshotError("quality export must contain exactly one day/revision operation")
    operation = matches[0]
    assignment = operation.get("content_metric_assignment")
    if assignment not in {"database_corpus_snapshot", "immutable_quality_observation"}:
        raise QualitySnapshotError("content metrics are ambiguous or not attributable to one revision")
    if operation.get("content_scope") != "governance_corpus_2021_plus_kst_day_end_v2":
        raise QualitySnapshotError("quality operation has an invalid corpus scope")
    snapshot_at = operation.get("content_snapshot_at")
    if not isinstance(snapshot_at, str):
        raise QualitySnapshotError("quality operation omitted the KST day-end corpus snapshot time")
    try:
        parsed_snapshot = datetime.fromisoformat(snapshot_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise QualitySnapshotError("quality operation has an invalid corpus snapshot time") from exc
    if parsed_snapshot.tzinfo is None:
        raise QualitySnapshotError("quality operation corpus snapshot time requires a timezone")
    snapshot_kst = parsed_snapshot.astimezone(KST)
    if (
        snapshot_kst.date() != observation_date
        or snapshot_kst.strftime("%H:%M:%S") != "23:59:59"
        or snapshot_kst.microsecond != 0
    ):
        raise QualitySnapshotError("quality operation corpus snapshot must be the KST day end")
    dart_poll = _number(
        operation.get("dart_success_poll_interval_p95_minutes"),
        "dart_success_poll_interval_p95_minutes",
    )
    kind_value = operation.get("kind_observation_lag_p95_minutes")
    kind_observation_count = _count(
        operation.get("kind_observation_count"), "kind_observation_count"
    )
    kind_lag_sample_count = _count(
        operation.get("kind_lag_sample_count"), "kind_lag_sample_count"
    )
    if kind_observation_count == 0:
        if kind_lag_sample_count != 0 or kind_value is not None:
            raise QualitySnapshotError("a true KIND no-disclosure day must have zero samples and null lag")
        kind_lag: float | None = None
    else:
        if kind_lag_sample_count != kind_observation_count or kind_value is None:
            raise QualitySnapshotError("KIND observation timestamps are incomplete")
        kind_lag = _number(kind_value, "kind_observation_lag_p95_minutes")
    raw = operation.get("raw_counts")
    if not isinstance(raw, dict):
        raise QualitySnapshotError("quality operation omitted raw_counts")
    counts = {field: _count(raw.get(field), field) for field in COUNT_FIELDS}
    for numerator, denominator in (
        ("official_evidence_linked_count", "official_evidence_total_count"),
        ("top_sensitive_reviewed_count", "top_sensitive_total_count"),
        ("original_language_preserved_count", "original_language_total_count"),
        ("valid_source_right_count", "source_right_total_count"),
    ):
        if counts[numerator] > counts[denominator]:
            raise QualitySnapshotError(f"{numerator} exceeds {denominator}")
    counts.update({field: 0 for field in RESERVED_BENCHMARK_FIELDS})
    return {
        "observation_id": f"quality:{expected_day}:{revision}",
        "observation_date": expected_day,
        "code_revision": revision,
        "dart_success_poll_interval_p95_minutes": dart_poll,
        "kind_observation_lag_p95_minutes": kind_lag,
        "raw_counts": counts,
        "source": "production_quality_job",
    }


@dataclass
class QualitySnapshotClient:
    base_url: str
    token: str
    timeout: float = 30.0
    transport: httpx.BaseTransport | None = None
    client_factory: Callable[..., httpx.Client] = httpx.Client

    def __post_init__(self) -> None:
        self.base_url = _api_base(self.base_url)
        self.token = self.token.strip()
        if len(self.token) < 32:
            raise QualitySnapshotError("BSIDE_OPS_TOKEN must contain at least 32 characters")

    def _headers(self, *, content: bool = False) -> dict[str, str]:
        result = {"Accept": "application/json", "Authorization": f"Bearer {self.token}"}
        if content:
            result["Content-Type"] = "application/json; charset=utf-8"
        return result

    def export(self, observation_date: date, revision: str) -> dict[str, object]:
        day = observation_date.isoformat()
        query = urlencode({"from": day, "to": day, "code_revision": revision})
        with self.client_factory(timeout=self.timeout, transport=self.transport) as client:
            response = client.get(
                f"{self.base_url}/ops/release-evidence?{query}", headers=self._headers()
            )
        payload = _json_object(response, "quality export")
        if response.status_code != 200 or payload.get("ok") is not True:
            raise QualitySnapshotError(
                f"quality export failed (HTTP {response.status_code}): {payload.get('error') or 'unknown_error'}"
            )
        return payload

    def post(self, observation: Mapping[str, object]) -> dict[str, object]:
        body = json.dumps(
            {"observations": [dict(observation)]},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        with self.client_factory(timeout=self.timeout, transport=self.transport) as client:
            response = client.post(
                f"{self.base_url}/ops/quality-observations",
                content=body,
                headers=self._headers(content=True),
            )
        payload = _json_object(response, "quality snapshot write")
        accepted = payload.get("accepted_count")
        inserted = payload.get("inserted_count")
        duplicates = payload.get("duplicate_count")
        if (
            response.status_code != 202
            or payload.get("ok") is not True
            or accepted != 1
            or not isinstance(inserted, int)
            or not isinstance(duplicates, int)
            or inserted + duplicates != 1
        ):
            raise QualitySnapshotError(
                f"quality snapshot ACK mismatch (HTTP {response.status_code})"
            )
        return payload


def freeze_quality_snapshot(
    client: QualitySnapshotClient, *, observation_date: date, revision: str
) -> dict[str, object]:
    observation = build_observation(
        client.export(observation_date, revision),
        observation_date=observation_date,
        revision=revision,
    )
    ack = client.post(observation)
    verified = client.export(observation_date, revision)
    operations = verified.get("operations_days")
    if not isinstance(operations, list) or len(operations) != 1 or not isinstance(operations[0], dict):
        raise QualitySnapshotError("quality snapshot verification returned an ambiguous operation")
    operation = operations[0]
    if (
        operation.get("content_metric_assignment") != "immutable_quality_observation"
        or operation.get("quality_observation_id") != observation["observation_id"]
        or not re.fullmatch(r"[0-9a-f]{64}", str(operation.get("quality_payload_sha256") or ""))
    ):
        raise QualitySnapshotError("quality snapshot was not durably re-exported")
    return {
        "ok": True,
        "observation": observation,
        "quality_payload_sha256": operation["quality_payload_sha256"],
        "ack": ack,
        "verified_at": datetime.now(KST).isoformat(),
    }


def _default_day() -> date:
    return datetime.now(KST).date() - timedelta(days=1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Freeze one actual KST production quality day")
    parser.add_argument("--observation-date", default="")
    parser.add_argument("--revision", default=os.environ.get("GITHUB_SHA", ""))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    observation_date = date.fromisoformat(args.observation_date) if args.observation_date else _default_day()
    base = os.environ.get("BSIDE_API_BASE_URL", "") or os.environ.get(
        "GOVERNANCE_API_BASE_URL", ""
    )
    client = QualitySnapshotClient(
        base_url=base,
        token=os.environ.get("BSIDE_OPS_TOKEN", ""),
    )
    result = freeze_quality_snapshot(
        client,
        observation_date=observation_date,
        revision=str(args.revision).casefold(),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    written_observation = result.get("observation")
    if not isinstance(written_observation, dict):  # pragma: no cover - internal invariant
        raise QualitySnapshotError("quality snapshot result omitted its observation")
    print(json.dumps({"ok": True, "observation_id": written_observation["observation_id"]}))


if __name__ == "__main__":
    main()
