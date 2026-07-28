from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .global_alpha_pages_identity import (
    MAX_TERMINAL_ASSET_BYTES,
    PagesArtifactIdentityError,
    build_terminal_content_identity,
)


API_VERSION = "v2"
EVIDENCE_SCHEMA_VERSION = 1
MAX_RESPONSE_BYTES = 250_000
REQUEST_TIMEOUT_SECONDS = 15
OBSERVATION_WINDOW_HOURS = 24
USER_AGENT = "bside-global-alpha-watchdog/1.0"
COUNTRIES = ("KR", "US", "JP", "GB", "CA", "AU")
REQUIRED_ALPHA_SOURCE_POLICY = {
    "KR": ("connector:kr:dart", "market-wide"),
    "US": ("connector:us:sec-edgar", "market-wide"),
    "CA": ("connector:ca:issuer-ir", "link-only"),
    "AU": ("connector:au:asic-register", "link-only"),
}
OPTIONAL_ALPHA_SOURCE_POLICY = {
    "JP": ("connector:jp:edinet", "link-only"),
    "GB": ("connector:gb:companies-house", "link-only"),
}
ACTIVE_COVERAGE_MODES = frozenset(
    ("market-wide", "official-register", "selected-issuers")
)
VALID_COVERAGE_MODES = ACTIVE_COVERAGE_MODES | frozenset(
    ("link-only", "unavailable")
)
SHA_PATTERN = re.compile(r"^[a-f0-9]{40}$")
CONNECTOR_PATTERN = re.compile(r"^[A-Za-z0-9_.:\-]{1,96}$")
STATUS_PATTERN = re.compile(r"^[a-z0-9_\-]{1,40}$")
EVENT_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.:\-]{1,191}$")
CONFIG_PATTERN = re.compile(
    r"^window\.__BSIDE_GOVERNANCE_CONFIG__=Object\.freeze\((\{.*\})\);\s*$"
)
EARLY_ACCESS_RELEASE_CHANNEL = "production_alpha_early_access"


class MonitorContractError(ValueError):
    """Raised when a first-party endpoint violates its public contract."""


@dataclass(frozen=True)
class MonitorConfig:
    api_base_url: str
    web_base_url: str
    web_surface: str
    pipeline_mode: str
    ops_token: str
    preview_token: str
    code_revision: str


@dataclass(frozen=True)
class HttpProbe:
    http_status: int
    duration_ms: int
    payload: object | None = None
    text: str | None = None
    error_class: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.http_status == 200 and self.error_class is None


class MonitorHttpClient(Protocol):
    def get_json(self, url: str, *, token: str = "") -> HttpProbe: ...

    def get_text(self, url: str) -> HttpProbe: ...


class RejectRedirectHandler(HTTPRedirectHandler):
    """Never forward privileged first-party headers to a redirect target."""

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


class UrllibMonitorHttpClient:
    def __init__(self, *, timeout: int = REQUEST_TIMEOUT_SECONDS) -> None:
        self.timeout = max(1, min(30, int(timeout)))
        self.opener = build_opener(RejectRedirectHandler())

    def _get(self, url: str, *, accept: str, token: str = "") -> HttpProbe:
        started = time.monotonic()
        status = 0
        payload: bytes | None = None
        error_class: str | None = None
        try:
            headers = {"Accept": accept, "User-Agent": USER_AGENT}
            if token:
                headers["Authorization"] = f"Bearer {token}"
            request = Request(url, headers=headers)
            with self.opener.open(  # noqa: S310 - only validated first-party HTTPS bases are used
                request,
                timeout=self.timeout,
            ) as response:
                status = int(response.status)
                payload = response.read(MAX_RESPONSE_BYTES + 1)
                if len(payload) > MAX_RESPONSE_BYTES:
                    error_class = "response_too_large"
                    payload = None
        except HTTPError as exc:
            status = int(exc.code)
            error_class = "http_error"
        except (URLError, TimeoutError, OSError):
            error_class = "network_error"
        duration_ms = max(0, int(round((time.monotonic() - started) * 1000)))
        if payload is None:
            return HttpProbe(
                http_status=status,
                duration_ms=duration_ms,
                error_class=error_class,
            )
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError:
            return HttpProbe(
                http_status=status,
                duration_ms=duration_ms,
                error_class="invalid_utf8",
            )
        return HttpProbe(http_status=status, duration_ms=duration_ms, text=text)

    def get_json(self, url: str, *, token: str = "") -> HttpProbe:
        probe = self._get(url, accept="application/json", token=token)
        if not probe.succeeded:
            return probe
        try:
            payload = json.loads(probe.text or "")
        except json.JSONDecodeError:
            return HttpProbe(
                http_status=probe.http_status,
                duration_ms=probe.duration_ms,
                error_class="invalid_json",
            )
        return HttpProbe(
            http_status=probe.http_status,
            duration_ms=probe.duration_ms,
            payload=payload,
        )

    def get_text(self, url: str) -> HttpProbe:
        return self._get(url, accept="application/javascript")


def _utc_datetime(value: object, *, code: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise MonitorContractError(code) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00",
        "Z",
    )


def _valid_https_base(value: str) -> bool:
    parsed = urlsplit(value)
    return (
        parsed.scheme == "https"
        and bool(parsed.netloc)
        and parsed.username is None
        and parsed.password is None
        and not parsed.query
        and not parsed.fragment
    )


def normalize_api_base(value: str) -> str:
    candidate = value.strip().rstrip("/")
    if not _valid_https_base(candidate):
        raise ValueError("invalid_api_base_url")
    parsed = urlsplit(candidate)
    path = parsed.path.rstrip("/")
    for suffix in ("/api/v1", "/api/v2"):
        if path.endswith(suffix):
            path = path[: -len(suffix)]
            break
    path += "/api/v2"
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def normalize_web_base(value: str) -> str:
    candidate = value.strip().rstrip("/")
    if not _valid_https_base(candidate):
        raise ValueError("invalid_web_base_url")
    return candidate


def config_from_environment(values: Mapping[str, str] | None = None) -> MonitorConfig:
    source = os.environ if values is None else values
    pipeline_mode = str(source.get("GOVERNANCE_PIPELINE_MODE") or "").strip().casefold()
    if pipeline_mode not in {"shadow", "live"}:
        raise ValueError("inactive_pipeline_mode")
    code_revision = str(source.get("GITHUB_SHA") or "").strip().casefold()
    if SHA_PATTERN.fullmatch(code_revision) is None:
        raise ValueError("invalid_code_revision")
    ops_token = str(source.get("BSIDE_OPS_TOKEN") or "").strip()
    if not ops_token:
        raise ValueError("missing_ops_token")
    preview_token = str(source.get("GOVERNANCE_PREVIEW_TOKEN") or "").strip()
    if pipeline_mode == "shadow" and not preview_token:
        raise ValueError("missing_preview_token")
    public_web_base = normalize_web_base(
        str(source.get("BSIDE_PUBLIC_WEB_URL") or "https://news.bside.ai")
    )
    if pipeline_mode == "shadow":
        configured_preview = str(
            source.get("BSIDE_ALPHA_PREVIEW_WEB_URL") or ""
        ).strip()
        web_base_url = normalize_web_base(
            configured_preview or public_web_base + "/governance"
        )
        web_surface = "governance-preview"
    else:
        web_base_url = public_web_base
        web_surface = "public-root"
    return MonitorConfig(
        api_base_url=normalize_api_base(
            str(
                source.get("BSIDE_API_BASE_URL")
                or source.get("GOVERNANCE_API_BASE_URL")
                or ""
            )
        ),
        web_base_url=web_base_url,
        web_surface=web_surface,
        pipeline_mode=pipeline_mode,
        ops_token=ops_token,
        preview_token=preview_token,
        code_revision=code_revision,
    )


def _probe_evidence(probe: HttpProbe, *, contract_valid: bool) -> dict[str, object]:
    return {
        "http_status": probe.http_status,
        "duration_ms": probe.duration_ms,
        "transport_succeeded": probe.succeeded,
        "contract_valid": contract_valid,
        "error_class": probe.error_class,
    }


def _object_payload(probe: HttpProbe, *, code: str) -> dict[str, Any]:
    if not probe.succeeded:
        raise MonitorContractError(code + "_unavailable")
    if not isinstance(probe.payload, dict):
        raise MonitorContractError(code + "_contract")
    return probe.payload


def _validate_health(
    probe: HttpProbe,
    *,
    expected_revision: str,
) -> tuple[dict[str, object], str, str]:
    payload = _object_payload(probe, code="health")
    raw_revision = payload.get("code_revision")
    code_revision = raw_revision if isinstance(raw_revision, str) else ""
    if (
        payload.get("ok") is not True
        or payload.get("api_version") != API_VERSION
        or payload.get("service") != "bside-global-market-terminal"
        or SHA_PATTERN.fullmatch(code_revision) is None
    ):
        raise MonitorContractError("health_contract")
    if code_revision != expected_revision:
        raise MonitorContractError("api_revision_mismatch")
    checked = _utc_datetime(payload.get("time"), code="health_time_invalid")
    return (
        _probe_evidence(probe, contract_valid=True),
        _utc_iso(checked),
        code_revision,
    )


def _validate_release_state(
    probe: HttpProbe,
) -> tuple[dict[str, object], dict[str, object]]:
    payload = _object_payload(probe, code="release_state")
    data = payload.get("data")
    if (
        payload.get("ok") is not True
        or payload.get("api_version") != API_VERSION
        or not isinstance(data, dict)
        or data.get("release_state") not in {"closed", "preview", "live"}
        or not isinstance(data.get("state_version"), int)
        or isinstance(data.get("state_version"), bool)
        or int(data["state_version"]) < 0
    ):
        raise MonitorContractError("release_state_contract")
    updated_at = _utc_datetime(
        data.get("updated_at"),
        code="release_state_updated_at_invalid",
    )
    cutover_at = None
    if data.get("cutover_at") is not None:
        cutover_at = _utc_datetime(
            data.get("cutover_at"),
            code="release_state_cutover_at_invalid",
        )
    normalized = {
        "release_state": str(data["release_state"]),
        "state_version": int(data["state_version"]),
        "updated_at": _utc_iso(updated_at),
        "cutover_at": _utc_iso(cutover_at) if cutover_at is not None else None,
    }
    return _probe_evidence(probe, contract_valid=True), normalized


def _nonnegative_integer(value: object, *, code: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MonitorContractError(code)
    return value


def _positive_integer(value: object, *, code: str) -> int:
    result = _nonnegative_integer(value, code=code)
    if result < 1:
        raise MonitorContractError(code)
    return result


def _validate_optional_timestamp(value: object, *, code: str) -> str | None:
    if value is None:
        return None
    return _utc_iso(_utc_datetime(value, code=code))


def _normalize_source_item(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise MonitorContractError("source_item_contract")
    country = str(value.get("country") or "")
    coverage_mode = str(value.get("coverage_mode") or "")
    status = str(value.get("status") or "")
    public_status = str(value.get("public_status") or "")
    connector_id_value = value.get("connector_id")
    connector_id = None if connector_id_value is None else str(connector_id_value)
    if (
        country not in COUNTRIES
        or coverage_mode not in VALID_COVERAGE_MODES
        or STATUS_PATTERN.fullmatch(status) is None
        or STATUS_PATTERN.fullmatch(public_status) is None
        or (
            connector_id is not None
            and CONNECTOR_PATTERN.fullmatch(connector_id) is None
        )
        or not isinstance(value.get("fresh"), bool)
        or not isinstance(value.get("public_ready"), bool)
    ):
        raise MonitorContractError("source_item_contract")
    lag_value = value.get("lag_minutes")
    lag_minutes = (
        None
        if lag_value is None
        else _nonnegative_integer(lag_value, code="source_lag_contract")
    )
    return {
        "connector_id": connector_id,
        "country": country,
        "coverage_mode": coverage_mode,
        "status": status,
        "fresh": bool(value["fresh"]),
        "public_status": public_status,
        "public_ready": bool(value["public_ready"]),
        "lag_minutes": lag_minutes,
        "expected_cadence_minutes": _positive_integer(
            value.get("expected_cadence_minutes"),
            code="source_cadence_contract",
        ),
        "raw_count": _nonnegative_integer(
            value.get("raw_count"),
            code="source_raw_count_contract",
        ),
        "acknowledged_count": _nonnegative_integer(
            value.get("acknowledged_count"),
            code="source_ack_count_contract",
        ),
        "last_success_at": _validate_optional_timestamp(
            value.get("last_success_at"),
            code="source_last_success_at_invalid",
        ),
        "last_checked_at": _validate_optional_timestamp(
            value.get("last_checked_at"),
            code="source_last_checked_at_invalid",
        ),
    }


def _validate_sources(
    probe: HttpProbe,
) -> tuple[dict[str, object], str, tuple[dict[str, object], ...]]:
    payload = _object_payload(probe, code="sources_status")
    data = payload.get("data")
    meta = payload.get("meta")
    if (
        payload.get("ok") is not True
        or payload.get("api_version") != API_VERSION
        or not isinstance(data, dict)
        or not isinstance(data.get("items"), list)
        or not isinstance(meta, dict)
        or isinstance(meta.get("returned"), bool)
        or not isinstance(meta.get("returned"), int)
        or int(meta["returned"]) != len(data["items"])
    ):
        raise MonitorContractError("sources_status_contract")
    checked_at = _utc_iso(
        _utc_datetime(
            data.get("checked_at"),
            code="sources_status_checked_at_invalid",
        )
    )
    items = tuple(_normalize_source_item(item) for item in data["items"])
    if len(items) != len(COUNTRIES):
        raise MonitorContractError("source_country_row_count_mismatch")
    connector_ids = [
        str(item["connector_id"])
        for item in items
        if item["connector_id"] is not None
    ]
    if len(connector_ids) != len(set(connector_ids)):
        raise MonitorContractError("duplicate_source_connector")
    if {str(item["country"]) for item in items} != set(COUNTRIES):
        raise MonitorContractError("source_country_set_mismatch")
    by_country = {str(item["country"]): item for item in items}
    for country, (connector_id, coverage_mode) in (
        REQUIRED_ALPHA_SOURCE_POLICY.items()
    ):
        item = by_country[country]
        if (
            item["connector_id"] != connector_id
            or item["coverage_mode"] != coverage_mode
        ):
            raise MonitorContractError("required_source_policy_mismatch")
    for country, (connector_id, coverage_mode) in (
        OPTIONAL_ALPHA_SOURCE_POLICY.items()
    ):
        item = by_country[country]
        if (
            item["connector_id"] != connector_id
            or item["coverage_mode"] != coverage_mode
            or item["public_status"] != "coverage_unavailable"
            or item["public_ready"] is not False
            or item["raw_count"] != 0
            or item["acknowledged_count"] != 0
            or item["last_success_at"] is not None
            or item["last_checked_at"] is not None
        ):
            raise MonitorContractError("optional_source_policy_mismatch")
    required_ready = data.get("required_source_ready")
    expected_required_ids = {
        connector_id
        for connector_id, _coverage in REQUIRED_ALPHA_SOURCE_POLICY.values()
    }
    if (
        not isinstance(required_ready, dict)
        or set(required_ready) != expected_required_ids
        or any(not isinstance(value, bool) for value in required_ready.values())
        or data.get("all_required_ready")
        != all(bool(value) for value in required_ready.values())
    ):
        raise MonitorContractError("required_source_readiness_contract")
    for country, (connector_id, _coverage_mode) in (
        REQUIRED_ALPHA_SOURCE_POLICY.items()
    ):
        if required_ready[connector_id] is not (
            by_country[country]["public_ready"] is True
        ):
            raise MonitorContractError("required_source_readiness_mismatch")
    return _probe_evidence(probe, contract_valid=True), checked_at, items


def _validate_live(
    probe: HttpProbe,
) -> tuple[dict[str, object], int, str | None]:
    payload = _object_payload(probe, code="live")
    data = payload.get("data")
    meta = payload.get("meta")
    if (
        payload.get("ok") is not True
        or payload.get("api_version") != API_VERSION
        or not isinstance(data, dict)
        or not isinstance(data.get("items"), list)
        or len(data["items"]) > 1
        or not isinstance(meta, dict)
        or isinstance(meta.get("returned"), bool)
        or not isinstance(meta.get("returned"), int)
        or int(meta["returned"]) != len(data["items"])
    ):
        raise MonitorContractError("live_contract")
    event_id = None
    if data["items"]:
        first = data["items"][0]
        if (
            not isinstance(first, dict)
            or EVENT_ID_PATTERN.fullmatch(str(first.get("event_id") or "")) is None
        ):
            raise MonitorContractError("live_contract")
        event_id = str(first["event_id"])
    return (
        _probe_evidence(probe, contract_valid=True),
        len(data["items"]),
        event_id,
    )


def _validate_search(probe: HttpProbe) -> dict[str, object]:
    payload = _object_payload(probe, code="search")
    data = payload.get("data")
    meta = payload.get("meta")
    if (
        payload.get("ok") is not True
        or payload.get("api_version") != API_VERSION
        or not isinstance(data, dict)
        or not isinstance(data.get("items"), list)
        or len(data["items"]) > 1
        or not all(isinstance(item, dict) for item in data["items"])
        or not isinstance(meta, dict)
        or isinstance(meta.get("returned"), bool)
        or not isinstance(meta.get("returned"), int)
        or int(meta["returned"]) != len(data["items"])
    ):
        raise MonitorContractError("search_contract")
    return _probe_evidence(probe, contract_valid=True)


def _validate_event_detail(
    probe: HttpProbe,
    *,
    expected_event_id: str,
) -> dict[str, object]:
    payload = _object_payload(probe, code="event_detail")
    data = payload.get("data")
    event = data.get("event") if isinstance(data, dict) else None
    documents = data.get("documents") if isinstance(data, dict) else None
    if (
        payload.get("ok") is not True
        or payload.get("api_version") != API_VERSION
        or not isinstance(event, dict)
        or event.get("event_id") != expected_event_id
        or not isinstance(documents, list)
    ):
        raise MonitorContractError("event_detail_contract")
    return _probe_evidence(probe, contract_valid=True)


def _validate_root(probe: HttpProbe) -> dict[str, object]:
    text = probe.text
    if (
        not probe.succeeded
        or not isinstance(text, str)
        or '<main id="app"' not in text
        or 'data-nav="today"' not in text
        or 'src="./config.js"' not in text
        or 'src="./app.js"' not in text
    ):
        raise MonitorContractError(
            "public_root_unavailable"
            if not probe.succeeded
            else "public_root_contract"
        )
    return _probe_evidence(probe, contract_valid=True)


def _validate_build(
    probe: HttpProbe,
    *,
    expected_api_base: str,
) -> tuple[dict[str, object], str, str]:
    if not probe.succeeded or probe.text is None:
        raise MonitorContractError("build_config_unavailable")
    match = CONFIG_PATTERN.fullmatch(probe.text)
    if match is None:
        raise MonitorContractError("build_config_contract")
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise MonitorContractError("build_config_contract") from exc
    raw_build_sha = payload.get("buildSha") if isinstance(payload, dict) else None
    build_sha = raw_build_sha if isinstance(raw_build_sha, str) else ""
    if SHA_PATTERN.fullmatch(build_sha) is None:
        raise MonitorContractError("build_sha_invalid")
    if not isinstance(payload, dict):
        raise MonitorContractError("build_config_contract")
    if payload.get("releaseChannel") != EARLY_ACCESS_RELEASE_CHANNEL:
        raise MonitorContractError("build_release_channel_invalid")
    configured_v1 = payload.get("apiBase")
    configured_v2 = payload.get("apiV2Base")
    if not isinstance(configured_v1, str) or not configured_v1.strip():
        raise MonitorContractError("build_api_base_invalid")
    try:
        api_base = normalize_api_base(configured_v1)
        if configured_v2 is not None:
            if not isinstance(configured_v2, str) or not configured_v2.strip():
                raise ValueError("invalid_api_v2_base")
            explicit_v2 = normalize_api_base(configured_v2)
            if configured_v2 != explicit_v2 or explicit_v2 != api_base:
                raise ValueError("inconsistent_api_v2_base")
            api_base = explicit_v2
    except ValueError as exc:
        raise MonitorContractError("build_api_base_invalid") from exc
    if api_base != expected_api_base:
        raise MonitorContractError("build_api_base_mismatch")
    return _probe_evidence(probe, contract_valid=True), build_sha, api_base


def _validate_terminal_asset(
    probe: HttpProbe,
    *,
    asset_name: str,
) -> dict[str, object]:
    if not probe.succeeded or probe.text is None:
        raise MonitorContractError(f"terminal_{asset_name}_unavailable")
    encoded = probe.text.encode("utf-8")
    if not encoded or len(encoded) > MAX_TERMINAL_ASSET_BYTES:
        raise MonitorContractError(f"terminal_{asset_name}_contract")
    return _probe_evidence(probe, contract_valid=True)


def _source_is_unhealthy(item: Mapping[str, object]) -> bool:
    return (
        item.get("country") in REQUIRED_ALPHA_SOURCE_POLICY
        and item.get("connector_id") is not None
        and (
            item.get("public_status") != "active"
            or item.get("public_ready") is not True
        )
    )


def _window_evidence(
    *,
    release: Mapping[str, object],
    now: datetime,
) -> dict[str, object]:
    anchor_value = release.get("cutover_at") or release.get("updated_at")
    anchor = _utc_datetime(anchor_value, code="observation_window_anchor_invalid")
    end = anchor + timedelta(hours=OBSERVATION_WINDOW_HOURS)
    return {
        "duration_hours": OBSERVATION_WINDOW_HOURS,
        "started_at": _utc_iso(anchor),
        "ends_at": _utc_iso(end),
        "within_window": anchor <= now <= end,
        "elapsed_minutes": max(
            0,
            int((now - anchor).total_seconds() // 60),
        ),
    }


def _error_probe(probe: HttpProbe) -> dict[str, object]:
    return _probe_evidence(probe, contract_valid=False)


def run_monitor(
    config: MonitorConfig,
    *,
    client: MonitorHttpClient | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    http = client or UrllibMonitorHttpClient()
    observed = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    observed_at = _utc_iso(observed)
    reasons: list[str] = []
    warnings: list[str] = []
    probes: dict[str, object] = {}
    release: dict[str, object] = {}
    sources: tuple[dict[str, object], ...] = ()
    build_sha: str | None = None
    api_code_revision: str | None = None
    deployed_api_base: str | None = None
    live_count: int | None = None
    live_event_id: str | None = None
    terminal_content: dict[str, object] | None = None

    root_probe = http.get_text(config.web_base_url + "/")
    try:
        probes["public_root"] = _validate_root(root_probe)
    except MonitorContractError as exc:
        reasons.append(str(exc))
        probes["public_root"] = _error_probe(root_probe)

    health_probe = http.get_json(config.api_base_url + "/health")
    try:
        health_evidence, health_time, api_code_revision = _validate_health(
            health_probe,
            expected_revision=config.code_revision,
        )
        probes["health"] = {**health_evidence, "server_time": health_time}
    except MonitorContractError as exc:
        reasons.append(str(exc))
        probes["health"] = _error_probe(health_probe)

    release_probe = http.get_json(
        config.api_base_url + "/ops/release-state",
        token=config.ops_token,
    )
    try:
        release_evidence, release = _validate_release_state(release_probe)
        probes["release_state"] = release_evidence
        expected_state = "preview" if config.pipeline_mode == "shadow" else "live"
        if release["release_state"] != expected_state:
            reasons.append("pipeline_release_state_mismatch")
    except MonitorContractError as exc:
        reasons.append(str(exc))
        probes["release_state"] = _error_probe(release_probe)

    build_probe = http.get_text(config.web_base_url + "/config.js")
    try:
        build_evidence, build_sha, deployed_api_base = _validate_build(
            build_probe,
            expected_api_base=config.api_base_url,
        )
        probes["deployed_build"] = build_evidence
        if build_sha != config.code_revision:
            reasons.append("deployed_revision_mismatch")
    except MonitorContractError as exc:
        reasons.append(str(exc))
        probes["deployed_build"] = _error_probe(build_probe)

    app_probe = http.get_text(config.web_base_url + "/app.js")
    try:
        probes["terminal_app"] = _validate_terminal_asset(
            app_probe,
            asset_name="app",
        )
    except MonitorContractError as exc:
        reasons.append(str(exc))
        probes["terminal_app"] = _error_probe(app_probe)

    styles_probe = http.get_text(config.web_base_url + "/styles.css")
    try:
        probes["terminal_styles"] = _validate_terminal_asset(
            styles_probe,
            asset_name="styles",
        )
    except MonitorContractError as exc:
        reasons.append(str(exc))
        probes["terminal_styles"] = _error_probe(styles_probe)

    if (
        root_probe.succeeded
        and isinstance(root_probe.text, str)
        and build_probe.succeeded
        and isinstance(build_probe.text, str)
        and app_probe.succeeded
        and isinstance(app_probe.text, str)
        and styles_probe.succeeded
        and isinstance(styles_probe.text, str)
        and not any(
            reason.startswith("terminal_")
            or reason.startswith("public_root_")
            or reason.startswith("build_config_")
            for reason in reasons
        )
    ):
        try:
            terminal_content = build_terminal_content_identity(
                {
                    "index.html": root_probe.text.encode("utf-8"),
                    "config.js": build_probe.text.encode("utf-8"),
                    "app.js": app_probe.text.encode("utf-8"),
                    "styles.css": styles_probe.text.encode("utf-8"),
                }
            )
        except PagesArtifactIdentityError:
            reasons.append("terminal_content_identity_invalid")

    effective_state = str(
        release.get("release_state")
        or ("preview" if config.pipeline_mode == "shadow" else "live")
    )
    public_token = config.preview_token if effective_state == "preview" else ""
    if effective_state == "preview" and not public_token:
        reasons.append("missing_preview_token")
    else:
        sources_probe = http.get_json(
            config.api_base_url + "/sources/status",
            token=public_token,
        )
        try:
            source_evidence, checked_at, sources = _validate_sources(sources_probe)
            probes["sources_status"] = {
                **source_evidence,
                "checked_at": checked_at,
            }
        except MonitorContractError as exc:
            reasons.append(str(exc))
            probes["sources_status"] = _error_probe(sources_probe)

        live_probe = http.get_json(
            config.api_base_url + "/live?" + urlencode({"limit": 1}),
            token=public_token,
        )
        try:
            live_evidence, live_count, live_event_id = _validate_live(live_probe)
            probes["live"] = live_evidence
        except MonitorContractError as exc:
            reasons.append(str(exc))
            probes["live"] = _error_probe(live_probe)

        search_probe = http.get_json(
            config.api_base_url + "/search?"
            + urlencode({"q": "BSIDE", "limit": 1}),
            token=public_token,
        )
        try:
            probes["search"] = _validate_search(search_probe)
        except MonitorContractError as exc:
            reasons.append(str(exc))
            probes["search"] = _error_probe(search_probe)

        if live_event_id is not None:
            detail_probe = http.get_json(
                config.api_base_url + "/events/"
                + quote(live_event_id, safe=""),
                token=public_token,
            )
            try:
                probes["event_detail"] = _validate_event_detail(
                    detail_probe,
                    expected_event_id=live_event_id,
                )
            except MonitorContractError as exc:
                reasons.append(str(exc))
                probes["event_detail"] = _error_probe(detail_probe)
        else:
            probes["event_detail"] = {
                "skipped": True,
                "reason": "no_live_event_available",
            }

    unhealthy = tuple(item for item in sources if _source_is_unhealthy(item))
    unhealthy_market_wide = tuple(
        item for item in unhealthy if item["coverage_mode"] == "market-wide"
    )
    if len(unhealthy_market_wide) >= 2:
        reasons.append("multiple_market_wide_connectors_unhealthy")
    elif len(unhealthy_market_wide) == 1:
        warnings.append("single_market_wide_connector_unhealthy")
    if any(item["coverage_mode"] != "market-wide" for item in unhealthy):
        warnings.append("limited_coverage_connector_unhealthy")

    if live_count is None:
        event_state = "unknown"
    elif live_count > 0:
        event_state = "events_present"
    elif unhealthy:
        event_state = "source_outage"
    else:
        event_state = "no_events"

    observation_window: dict[str, object] | None = None
    if release:
        try:
            observation_window = _window_evidence(
                release=release,
                now=observed,
            )
        except MonitorContractError as exc:
            reasons.append(str(exc))
    if observation_window is not None and not observation_window["within_window"]:
        warnings.append("outside_initial_24_hour_window")

    reasons = list(dict.fromkeys(reasons))
    warnings = list(dict.fromkeys(warnings))
    if reasons:
        status = "incident"
    elif warnings:
        status = "degraded"
    else:
        status = "healthy"
    identity = "\x1f".join(
        (
            observed_at,
            config.pipeline_mode,
            config.web_surface,
            str(release.get("release_state") or "unknown"),
            build_sha or "unknown",
            api_code_revision or "unknown",
            deployed_api_base or "unknown",
            str((terminal_content or {}).get("sha256") or "unknown"),
        )
    )
    return {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "observation_id": "global-alpha:"
        + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:48],
        "observed_at": observed_at,
        "status": status,
        "pipeline_mode": config.pipeline_mode,
        "web_surface": config.web_surface,
        "release_state": release.get("release_state"),
        "release_state_version": release.get("state_version"),
        "deployed_build_sha": build_sha,
        "deployed_api_base": deployed_api_base,
        "terminal_content": terminal_content,
        "api_code_revision": api_code_revision,
        "workflow_revision": config.code_revision,
        "observation_window": observation_window,
        "event_availability": {
            "state": event_state,
            "returned": live_count,
            "meaning": (
                "No public event matched, while all monitored connectors were healthy."
                if event_state == "no_events"
                else (
                    "No public event matched and at least one monitored connector was unhealthy."
                    if event_state == "source_outage"
                    else None
                )
            ),
        },
        "source_summary": {
            "returned": len(sources),
            "unhealthy_count": len(unhealthy),
            "market_wide_count": sum(
                1 for item in sources if item["coverage_mode"] == "market-wide"
            ),
            "unhealthy_market_wide_count": len(unhealthy_market_wide),
        },
        "sources": list(sources),
        "probes": probes,
        "reasons": reasons,
        "warnings": warnings,
    }


def _configuration_failure_evidence(
    *,
    code: str,
    now: datetime,
    values: Mapping[str, str],
) -> dict[str, object]:
    revision = str(values.get("GITHUB_SHA") or "").strip().casefold()
    return {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "observation_id": "global-alpha:"
        + hashlib.sha256((_utc_iso(now) + "\x1f" + code).encode("utf-8")).hexdigest()[
            :48
        ],
        "observed_at": _utc_iso(now),
        "status": "incident",
        "pipeline_mode": str(
            values.get("GOVERNANCE_PIPELINE_MODE") or ""
        ).strip(),
        "web_surface": (
            "governance-preview"
            if str(values.get("GOVERNANCE_PIPELINE_MODE") or "")
            .strip()
            .casefold()
            == "shadow"
            else "public-root"
        ),
        "release_state": None,
        "release_state_version": None,
        "deployed_build_sha": None,
        "deployed_api_base": None,
        "terminal_content": None,
        "api_code_revision": None,
        "workflow_revision": revision if SHA_PATTERN.fullmatch(revision) else None,
        "observation_window": None,
        "event_availability": {
            "state": "unknown",
            "returned": None,
            "meaning": None,
        },
        "source_summary": {
            "returned": 0,
            "unhealthy_count": 0,
            "market_wide_count": 0,
            "unhealthy_market_wide_count": 0,
        },
        "sources": [],
        "probes": {},
        "reasons": [code],
        "warnings": [],
    }


def write_evidence(path: Path, evidence: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(
        evidence,
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
    )
    path.write_text(encoded + "\n", encoding="utf-8", newline="\n")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Observe the BSIDE Global Terminal Production Alpha",
    )
    parser.add_argument(
        "--evidence",
        default=os.environ.get("EVIDENCE_PATH", "global-alpha-observation.json"),
    )
    parser.add_argument(
        "--require-active-pipeline",
        action="store_true",
        help="Fail closed unless the pipeline mode resolves to shadow or live.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    now = datetime.now(timezone.utc)
    values = dict(os.environ)
    try:
        config = config_from_environment(values)
        evidence = run_monitor(config, now=now)
    except ValueError as exc:
        evidence = _configuration_failure_evidence(
            code=str(exc),
            now=now,
            values=values,
        )
    except Exception:
        # Evidence must survive unexpected failures. Do not serialize exception
        # messages because upstream libraries may include configured URLs.
        evidence = _configuration_failure_evidence(
            code="unexpected_monitor_failure",
            now=now,
            values=values,
        )
    write_evidence(Path(args.evidence), evidence)
    status = str(evidence.get("status") or "incident")
    raw_reasons = evidence.get("reasons", [])
    reasons = raw_reasons if isinstance(raw_reasons, list) else []
    reason_codes = ",".join(str(item) for item in reasons)
    print(
        "Global Alpha watchdog "
        f"status={status} evidence={Path(args.evidence).name} "
        f"reasons={reason_codes or 'none'}"
    )
    return 1 if status == "incident" else 0


if __name__ == "__main__":
    raise SystemExit(main())
