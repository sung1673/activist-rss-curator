from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request

import pytest

from curator.global_alpha_monitor import (
    HttpProbe,
    MonitorConfig,
    RejectRedirectHandler,
    config_from_environment,
    main,
    normalize_api_base,
    run_monitor,
)


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "global-alpha-watchdog.yml"
REVISION = "a" * 40
DEPLOYED = REVISION
MISMATCHED_DEPLOYED = "b" * 40
NOW = datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc)
CONNECTOR_IDS = {
    "KR": "connector:kr:dart",
    "US": "connector:us:sec-edgar",
    "JP": "connector:jp:edinet",
    "GB": "connector:gb:companies-house",
    "CA": "connector:ca:issuer-ir",
    "AU": "connector:au:asic-register",
}
REQUIRED_CONNECTOR_IDS = {
    CONNECTOR_IDS[country] for country in ("KR", "US", "CA", "AU")
}


def envelope(data: object) -> dict[str, object]:
    return {"ok": True, "data": data, "api_version": "v2"}


def source(
    country: str,
    coverage_mode: str,
    *,
    status: str = "active",
    fresh: bool = True,
    public_status: str | None = None,
    public_ready: bool | None = None,
    observed: bool = True,
    raw_count: int = 3,
) -> dict[str, object]:
    effective_public_status = public_status or status
    effective_public_ready = (
        public_ready
        if public_ready is not None
        else effective_public_status == "active" and fresh
    )
    return {
        "connector_id": CONNECTOR_IDS[country],
        "country": country,
        "source_name": f"{country} official source",
        "coverage_mode": coverage_mode,
        "status": status,
        "public_status": effective_public_status,
        "public_ready": effective_public_ready,
        "last_success_at": "2026-07-24T11:55:00Z" if observed else None,
        "last_checked_at": "2026-07-24T11:56:00Z" if observed else None,
        "last_error_class": None,
        "public_note": None,
        "lag_minutes": 5 if observed else None,
        "expected_cadence_minutes": 30,
        "fresh": fresh,
        "raw_count": raw_count,
        "acknowledged_count": raw_count,
    }


def healthy_sources() -> list[dict[str, object]]:
    return [
        source("KR", "market-wide"),
        source("US", "market-wide"),
        source(
            "JP",
            "link-only",
            status="inactive",
            fresh=False,
            public_status="coverage_unavailable",
            public_ready=False,
            observed=False,
            raw_count=0,
        ),
        source(
            "GB",
            "link-only",
            status="inactive",
            fresh=False,
            public_status="coverage_unavailable",
            public_ready=False,
            observed=False,
            raw_count=0,
        ),
        source("CA", "link-only"),
        source("AU", "link-only"),
    ]


class FakeHttpClient:
    def __init__(
        self,
        *,
        sources: list[dict[str, object]] | None = None,
        live_items: list[dict[str, object]] | None = None,
        release_state: str = "preview",
        overrides: dict[str, HttpProbe] | None = None,
    ) -> None:
        source_items = healthy_sources() if sources is None else sources
        ready_by_connector = {
            str(item["connector_id"]): item.get("public_ready") is True
            for item in source_items
            if item.get("connector_id") in REQUIRED_CONNECTOR_IDS
        }
        self.responses: dict[str, HttpProbe] = {
            "/health": HttpProbe(
                200,
                10,
                payload={
                    "ok": True,
                    "service": "bside-global-market-terminal",
                    "code_revision": REVISION,
                    "time": "2026-07-24T12:00:00Z",
                    "api_version": "v2",
                },
            ),
            "/ops/release-state": HttpProbe(
                200,
                11,
                payload=envelope(
                    {
                        "release_state": release_state,
                        "state_version": 4,
                        "updated_at": "2026-07-24T11:00:00Z",
                        "cutover_at": (
                            "2026-07-24T11:00:00Z"
                            if release_state == "live"
                            else None
                        ),
                    }
                ),
            ),
            "/sources/status": HttpProbe(
                200,
                12,
                payload={
                    **envelope(
                        {
                            "items": source_items,
                            "checked_at": "2026-07-24T12:00:00Z",
                            "required_source_ready": ready_by_connector,
                            "all_required_ready": all(ready_by_connector.values()),
                        }
                    ),
                    "meta": {"returned": len(source_items)},
                },
            ),
            "/live?limit=1": HttpProbe(
                200,
                13,
                payload={
                    **envelope({"items": live_items or []}),
                    "meta": {
                        "page": 1,
                        "limit": 1,
                        "returned": len(live_items or []),
                        "has_more": False,
                        "next_page": None,
                    },
                },
            ),
            "/search?q=BSIDE&limit=1": HttpProbe(
                200,
                13,
                payload={
                    **envelope({"items": []}),
                    "meta": {
                        "page": 1,
                        "limit": 1,
                        "returned": 0,
                        "has_more": False,
                        "next_page": None,
                    },
                },
            ),
            "/": HttpProbe(
                200,
                13,
                text=(
                    '<!doctype html><main id="app"></main>'
                    '<a data-nav="today"></a>'
                    '<script src="./config.js"></script>'
                    '<script src="./app.js"></script>'
                ),
            ),
            "/config.js": HttpProbe(
                200,
                14,
                text=(
                    "window.__BSIDE_GOVERNANCE_CONFIG__=Object.freeze("
                    '{"apiBase":"https://api.example.test/api/v1",'
                    '"webBase":"https://news.example.test",'
                    f'"buildSha":"{DEPLOYED}",'
                    '"releaseChannel":"production_alpha_early_access"'
                    "});\n"
                ),
            ),
            "/app.js": HttpProbe(
                200,
                15,
                text="window.__BSIDE_ALPHA_APP__=true;\n",
            ),
            "/styles.css": HttpProbe(
                200,
                16,
                text=":root{color:#111827}\n",
            ),
        }
        self.responses.update(overrides or {})
        self.calls: list[tuple[str, str]] = []

    @staticmethod
    def _key(url: str) -> str:
        if "/events/" in url and not url.endswith("/events/"):
            return "/events/:id"
        for suffix in (
            "/ops/release-state",
            "/sources/status",
            "/live?limit=1",
            "/search?q=BSIDE&limit=1",
            "/config.js",
            "/app.js",
            "/styles.css",
            "/health",
            "/",
        ):
            if url.endswith(suffix):
                return suffix
        raise AssertionError(f"unexpected fake URL: {url}")

    def get_json(self, url: str, *, token: str = "") -> HttpProbe:
        key = self._key(url)
        self.calls.append((key, token))
        return self.responses[key]

    def get_text(self, url: str) -> HttpProbe:
        key = self._key(url)
        self.calls.append((key, ""))
        return self.responses[key]


def monitor_config(*, mode: str = "shadow") -> MonitorConfig:
    return MonitorConfig(
        api_base_url="https://api.example.test/api/v2",
        web_base_url=(
            "https://news.example.test/governance"
            if mode == "shadow"
            else "https://news.example.test"
        ),
        web_surface=(
            "governance-preview" if mode == "shadow" else "public-root"
        ),
        pipeline_mode=mode,
        ops_token="ops-secret-must-never-be-serialized",
        preview_token="preview-secret-must-never-be-serialized",
        code_revision=REVISION,
    )


def test_api_base_normalization_is_v2_and_rejects_credentials() -> None:
    assert (
        normalize_api_base("https://example.test/activist/api.php/api/v1")
        == "https://example.test/activist/api.php/api/v2"
    )
    assert (
        normalize_api_base("https://example.test/activist/api.php")
        == "https://example.test/activist/api.php/api/v2"
    )
    with pytest.raises(ValueError, match="invalid_api_base_url"):
        normalize_api_base("https://user:password@example.test/api/v2")


def test_privileged_monitor_requests_never_follow_redirects() -> None:
    handler = RejectRedirectHandler()
    assert (
        handler.redirect_request(
            Request(
                "https://api.example.test/api/v2/ops/release-state",
                headers={"Authorization": "Bearer secret"},
            ),
            None,
            302,
            "Found",
            {},
            "https://attacker.example/collect",
        )
        is None
    )


def test_environment_is_fail_closed_and_requires_preview_token_in_shadow() -> None:
    base = {
        "BSIDE_API_BASE_URL": "https://api.example.test/api/v2",
        "BSIDE_PUBLIC_WEB_URL": "https://news.example.test",
        "GOVERNANCE_PIPELINE_MODE": "shadow",
        "BSIDE_OPS_TOKEN": "ops",
        "GITHUB_SHA": REVISION,
    }
    with pytest.raises(ValueError, match="missing_preview_token"):
        config_from_environment(base)
    with pytest.raises(ValueError, match="inactive_pipeline_mode"):
        config_from_environment({**base, "GOVERNANCE_PIPELINE_MODE": "off"})
    configured = config_from_environment(
        {**base, "GOVERNANCE_PREVIEW_TOKEN": "preview"}
    )
    assert configured.web_base_url == "https://news.example.test/governance"
    assert configured.web_surface == "governance-preview"


def test_healthy_empty_stream_is_explicitly_no_events() -> None:
    client = FakeHttpClient()
    evidence = run_monitor(monitor_config(), client=client, now=NOW)

    assert evidence["status"] == "healthy"
    assert evidence["event_availability"] == {
        "state": "no_events",
        "returned": 0,
        "meaning": "No public event matched, while all monitored connectors were healthy.",
    }
    assert evidence["release_state"] == "preview"
    assert evidence["web_surface"] == "governance-preview"
    assert evidence["deployed_build_sha"] == DEPLOYED
    assert evidence["terminal_content"]["file_count"] == 4
    assert len(str(evidence["terminal_content"]["sha256"])) == 64
    assert evidence["api_code_revision"] == REVISION
    assert evidence["deployed_api_base"] == monitor_config().api_base_url
    assert evidence["workflow_revision"] == REVISION
    assert evidence["source_summary"] == {
        "returned": 6,
        "unhealthy_count": 0,
        "market_wide_count": 2,
        "unhealthy_market_wide_count": 0,
    }
    assert evidence["observation_window"]["duration_hours"] == 24
    assert evidence["observation_window"]["within_window"] is True
    assert ("/sources/status", monitor_config().preview_token) in client.calls
    assert ("/live?limit=1", monitor_config().preview_token) in client.calls
    assert ("/search?q=BSIDE&limit=1", monitor_config().preview_token) in client.calls
    assert ("/", "") in client.calls
    assert ("/app.js", "") in client.calls
    assert ("/styles.css", "") in client.calls
    assert evidence["probes"]["event_detail"] == {
        "skipped": True,
        "reason": "no_live_event_available",
    }
    serialized = json.dumps(evidence)
    assert monitor_config().ops_token not in serialized
    assert monitor_config().preview_token not in serialized


def test_live_mode_does_not_send_preview_token_to_public_routes() -> None:
    client = FakeHttpClient(release_state="live")
    evidence = run_monitor(
        monitor_config(mode="live"),
        client=client,
        now=NOW,
    )

    assert evidence["status"] == "healthy"
    assert ("/sources/status", "") in client.calls
    assert ("/live?limit=1", "") in client.calls
    assert ("/search?q=BSIDE&limit=1", "") in client.calls


def test_live_event_detail_is_probed_with_the_same_public_credentials() -> None:
    event_id = "event:official:one"
    client = FakeHttpClient(
        live_items=[{"event_id": event_id}],
        overrides={
            "/events/:id": HttpProbe(
                200,
                10,
                payload=envelope(
                    {
                        "event": {"event_id": event_id},
                        "documents": [],
                    }
                ),
            ),
        },
    )
    evidence = run_monitor(monitor_config(), client=client, now=NOW)

    assert evidence["status"] == "healthy"
    assert evidence["event_availability"]["state"] == "events_present"
    assert evidence["probes"]["event_detail"]["contract_valid"] is True
    assert ("/events/:id", monitor_config().preview_token) in client.calls


def test_two_unhealthy_market_wide_connectors_fail_closed_and_mark_outage() -> None:
    items = healthy_sources()
    items[0] = source("KR", "market-wide", status="failed", fresh=False)
    items[1] = source("US", "market-wide", status="degraded", fresh=True)
    evidence = run_monitor(
        monitor_config(),
        client=FakeHttpClient(sources=items),
        now=NOW,
    )

    assert evidence["status"] == "incident"
    assert evidence["event_availability"]["state"] == "source_outage"
    assert (
        "multiple_market_wide_connectors_unhealthy" in evidence["reasons"]
    )
    assert evidence["source_summary"]["unhealthy_market_wide_count"] == 2


def test_optional_country_cannot_masquerade_as_ready_coverage() -> None:
    items = healthy_sources()
    items[2] = source("JP", "link-only")
    evidence = run_monitor(
        monitor_config(),
        client=FakeHttpClient(sources=items),
        now=NOW,
    )

    assert evidence["status"] == "incident"
    assert "optional_source_policy_mismatch" in evidence["reasons"]


def test_one_unhealthy_market_wide_connector_is_degraded_not_failed() -> None:
    items = healthy_sources()
    items[0] = source("KR", "market-wide", status="active", fresh=False)
    evidence = run_monitor(
        monitor_config(),
        client=FakeHttpClient(sources=items),
        now=NOW,
    )

    assert evidence["status"] == "degraded"
    assert evidence["reasons"] == []
    assert "single_market_wide_connector_unhealthy" in evidence["warnings"]
    assert evidence["event_availability"]["state"] == "source_outage"


def test_collection_health_cannot_hide_public_redistribution_block() -> None:
    items = healthy_sources()
    items[1] = source(
        "US",
        "market-wide",
        status="active",
        fresh=True,
        public_status="redistribution_blocked",
        public_ready=False,
    )
    evidence = run_monitor(
        monitor_config(),
        client=FakeHttpClient(sources=items),
        now=NOW,
    )

    assert evidence["status"] == "degraded"
    assert evidence["event_availability"]["state"] == "source_outage"
    assert evidence["source_summary"]["unhealthy_market_wide_count"] == 1
    assert "single_market_wide_connector_unhealthy" in evidence["warnings"]
    us_source = next(
        item for item in evidence["sources"] if item["country"] == "US"
    )
    assert us_source["status"] == "active"
    assert us_source["fresh"] is True
    assert us_source["public_status"] == "redistribution_blocked"
    assert us_source["public_ready"] is False


@pytest.mark.parametrize(
    ("override_key", "probe_key", "probe", "reason"),
    (
        (
            "/health",
            "health",
            HttpProbe(503, 20, error_class="http_error"),
            "health_unavailable",
        ),
        (
            "/sources/status",
            "sources_status",
            HttpProbe(
                200,
                20,
                payload={
                    **envelope(
                        {
                            "items": healthy_sources(),
                            "checked_at": "2026-07-24T12:00:00Z",
                        }
                    ),
                    "meta": {"returned": 99},
                },
            ),
            "sources_status_contract",
        ),
        (
            "/live?limit=1",
            "live",
            HttpProbe(
                200,
                20,
                payload={
                    **envelope({"items": []}),
                    "meta": {"returned": 1},
                },
            ),
            "live_contract",
        ),
    ),
)
def test_api_unavailability_or_contract_mismatch_is_an_incident(
    override_key: str,
    probe_key: str,
    probe: HttpProbe,
    reason: str,
) -> None:
    evidence = run_monitor(
        monitor_config(),
        client=FakeHttpClient(overrides={override_key: probe}),
        now=NOW,
    )

    assert evidence["status"] == "incident"
    assert reason in evidence["reasons"]
    assert evidence["probes"][probe_key]["contract_valid"] is False


def test_release_state_must_match_pipeline_mode() -> None:
    evidence = run_monitor(
        monitor_config(mode="live"),
        client=FakeHttpClient(release_state="preview"),
        now=NOW,
    )

    assert evidence["status"] == "incident"
    assert "pipeline_release_state_mismatch" in evidence["reasons"]


def test_deployed_pages_revision_must_match_observer_revision() -> None:
    config_probe = HttpProbe(
        200,
        14,
        text=(
            "window.__BSIDE_GOVERNANCE_CONFIG__=Object.freeze("
            '{"apiBase":"https://api.example.test/api/v2",'
            '"webBase":"https://news.example.test",'
            f'"buildSha":"{MISMATCHED_DEPLOYED}",'
            '"releaseChannel":"production_alpha_early_access"'
            "});\n"
        ),
    )
    evidence = run_monitor(
        monitor_config(),
        client=FakeHttpClient(
            overrides={"/config.js": config_probe},
        ),
        now=NOW,
    )

    assert evidence["status"] == "incident"
    assert "deployed_revision_mismatch" in evidence["reasons"]
    assert evidence["deployed_build_sha"] == MISMATCHED_DEPLOYED


def test_pages_config_must_identify_the_early_access_release_channel() -> None:
    config_probe = HttpProbe(
        200,
        14,
        text=(
            "window.__BSIDE_GOVERNANCE_CONFIG__=Object.freeze("
            '{"apiBase":"https://api.example.test/api/v1",'
            '"webBase":"https://news.example.test",'
            f'"buildSha":"{DEPLOYED}",'
            '"releaseChannel":"production_alpha"'
            "});\n"
        ),
    )
    evidence = run_monitor(
        monitor_config(),
        client=FakeHttpClient(overrides={"/config.js": config_probe}),
        now=NOW,
    )

    assert evidence["status"] == "incident"
    assert "build_release_channel_invalid" in evidence["reasons"]
    assert evidence["deployed_build_sha"] is None


def test_api_health_revision_must_match_observer_revision() -> None:
    evidence = run_monitor(
        monitor_config(),
        client=FakeHttpClient(
            overrides={
                "/health": HttpProbe(
                    200,
                    10,
                    payload={
                        "ok": True,
                        "service": "bside-global-market-terminal",
                        "code_revision": MISMATCHED_DEPLOYED,
                        "time": "2026-07-24T12:00:00Z",
                        "api_version": "v2",
                    },
                )
            }
        ),
        now=NOW,
    )

    assert evidence["status"] == "incident"
    assert "api_revision_mismatch" in evidence["reasons"]
    assert evidence["api_code_revision"] is None


def test_pages_config_must_point_to_the_probed_v2_api_base() -> None:
    config_probe = HttpProbe(
        200,
        14,
        text=(
            "window.__BSIDE_GOVERNANCE_CONFIG__=Object.freeze("
            '{"apiBase":"https://other.example.test/api/v1",'
            '"apiV2Base":"https://other.example.test/api/v2",'
            '"webBase":"https://news.example.test",'
            f'"buildSha":"{DEPLOYED}",'
            '"releaseChannel":"production_alpha_early_access"'
            "});\n"
        ),
    )
    evidence = run_monitor(
        monitor_config(),
        client=FakeHttpClient(overrides={"/config.js": config_probe}),
        now=NOW,
    )

    assert evidence["status"] == "incident"
    assert "build_api_base_mismatch" in evidence["reasons"]
    assert evidence["deployed_api_base"] is None


def test_cli_always_writes_sanitized_evidence_on_configuration_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    evidence_path = tmp_path / "observation.json"
    monkeypatch.setenv("EVIDENCE_PATH", str(evidence_path))
    monkeypatch.setenv("GOVERNANCE_PIPELINE_MODE", "shadow")
    monkeypatch.setenv("BSIDE_OPS_TOKEN", "do-not-print-ops")
    monkeypatch.setenv("GOVERNANCE_PREVIEW_TOKEN", "do-not-print-preview")
    monkeypatch.setenv("GITHUB_SHA", REVISION)
    monkeypatch.delenv("BSIDE_API_BASE_URL", raising=False)
    monkeypatch.delenv("GOVERNANCE_API_BASE_URL", raising=False)

    assert main(["--evidence", str(evidence_path)]) == 1

    evidence_text = evidence_path.read_text(encoding="utf-8")
    output = capsys.readouterr().out
    assert json.loads(evidence_text)["status"] == "incident"
    assert "do-not-print-ops" not in evidence_text + output
    assert "do-not-print-preview" not in evidence_text + output


def test_workflow_is_default_branch_only_non_cancelling_and_preserves_evidence() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")

    assert 'cron: "*/5 * * * *"' in text
    assert "github.ref_name == github.event.repository.default_branch" in text
    assert "vars.GOVERNANCE_PIPELINE_MODE == 'shadow'" in text
    assert "vars.GOVERNANCE_PIPELINE_MODE == 'live'" in text
    assert "vars.GLOBAL_ALPHA_OBSERVATION_ENABLED == 'true'" in text
    assert "vars.GLOBAL_ALPHA_OBSERVATION_ENABLED != 'false'" in text
    assert "GLOBAL_ALPHA_OBSERVATION_ENABLED: ${{ vars.GLOBAL_ALPHA_OBSERVATION_ENABLED }}" in text
    assert "KIND_CONNECTOR_MODE: ${{ vars.KIND_CONNECTOR_MODE }}" in text
    assert "python -m curator.operation_mode --github-output \"$GITHUB_OUTPUT\"" in text
    assert "steps.rollout.outputs.global_alpha_observation_enabled == 'true'" in text
    assert "timeout-minutes: 8" in text
    assert "cancel-in-progress: false" in text
    assert "if: always() && steps.initialize.outcome == 'success'" in text
    assert "actions/upload-artifact@" in text
    assert "--require-active-pipeline" in text
    assert "contents: read" in text
    assert "BSIDE_OPS_TOKEN: ${{ secrets.BSIDE_OPS_TOKEN }}" in text
    assert "BSIDE_ADMIN_TOKEN" not in text

    lowered = text.casefold()
    assert "telegram" not in lowered
    assert "rollback" not in lowered
    assert "gh variable set" not in lowered
    assert "issues: write" not in lowered
