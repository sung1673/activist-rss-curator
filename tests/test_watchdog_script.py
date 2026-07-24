from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType

import pytest


ROOT = Path(__file__).resolve().parents[1]


def load_watchdog() -> ModuleType:
    path = ROOT / ".github" / "scripts" / "watchdog.py"
    spec = importlib.util.spec_from_file_location("bside_watchdog", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_parse_timestamp_normalizes_utc() -> None:
    watchdog = load_watchdog()
    assert watchdog.parse_timestamp("2026-07-16T00:00:00Z") == datetime(2026, 7, 16, tzinfo=timezone.utc)
    assert watchdog.parse_timestamp("not-a-date") is None


def test_missing_configuration_emits_incident_output(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    watchdog = load_watchdog()
    output_path = tmp_path / "github-output.txt"
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("BSIDE_API_BASE_URL", raising=False)
    monkeypatch.delenv("BSIDE_OPS_TOKEN", raising=False)
    monkeypatch.setenv("GITHUB_OUTPUT", str(output_path))
    assert watchdog.main() == 0
    assert "incident=true" in output_path.read_text(encoding="utf-8")
    report = (tmp_path / ".watchdog-report.md").read_text(encoding="utf-8")
    assert "Missing operational configuration" in report


def test_report_lists_actual_web_observations() -> None:
    watchdog = load_watchdog()
    observation = watchdog.AvailabilityObservation(
        observation_id="availability:test",
        route_template="/api/v1/health",
        observed_at="2026-07-16T00:00:00+00:00",
        http_status=200,
        duration_ms=42,
        succeeded=True,
        build_sha="a" * 40,
    )
    report = watchdog.build_report(
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
        payload={"last_success_at": "2026-07-15T23:55:00Z"},
        reasons=["availability failure"],
        ingest_age=5,
        observations=[observation],
    )
    assert "INCIDENT" in report
    assert "`/api/v1/health`" in report
    assert "| 200 | 42 | ok |" in report


def test_watchdog_is_web_only_and_posts_raw_observations() -> None:
    source = (ROOT / ".github" / "scripts" / "watchdog.py").read_text(encoding="utf-8")
    assert "/ops/availability-observations" in source
    assert "pending_outbox" not in source
    assert "dead_letter_count" not in source


def test_watchdog_rejects_credential_bearing_operational_urls() -> None:
    watchdog = load_watchdog()

    assert watchdog._valid_https_url("https://example.test/api/v1")
    assert not watchdog._valid_https_url("https://user:password@example.test/api/v1")


def test_deployed_build_sha_is_read_from_strict_public_config(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    watchdog = load_watchdog()
    revision = "b" * 40

    class Response:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, *_args):  # type: ignore[no-untyped-def]
            return None

        def read(self, _limit: int) -> bytes:
            return (
                'window.__BSIDE_GOVERNANCE_CONFIG__=Object.freeze('
                f'{{"apiBase":"/api/v1","webBase":"https://example.test","buildSha":"{revision}"}});\n'
            ).encode("utf-8")

    monkeypatch.setattr(watchdog, "urlopen", lambda *_args, **_kwargs: Response())

    assert watchdog.fetch_deployed_build_sha("https://example.test") == revision


def test_deployed_build_sha_rejects_executable_or_partial_config(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    watchdog = load_watchdog()

    class Response:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, *_args):  # type: ignore[no-untyped-def]
            return None

        def read(self, _limit: int) -> bytes:
            return b'alert(1); window.__BSIDE_GOVERNANCE_CONFIG__={"buildSha":"abc"};'

    monkeypatch.setattr(watchdog, "urlopen", lambda *_args, **_kwargs: Response())

    with pytest.raises(ValueError, match="unexpected format"):
        watchdog.fetch_deployed_build_sha("https://example.test")


def test_active_sha_comes_from_authenticated_deployment_observation() -> None:
    watchdog = load_watchdog()
    assert watchdog.active_deployment_sha(
        {
            "active_deployment_status": "observed",
            "active_deployment": {
                "build_sha": "B" * 40,
                "observed_at": "2026-07-16T00:00:00Z",
                "distribution_target": "pages",
            }
        }
    ) == "b" * 40
    with pytest.raises(ValueError, match="authenticated active deployment"):
        watchdog.active_deployment_sha({})


def test_future_timestamps_fail_closed(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    watchdog = load_watchdog()
    output_path = tmp_path / "github-output.txt"
    future = datetime.now(timezone.utc) + timedelta(minutes=15)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BSIDE_API_BASE_URL", "https://api.example.test")
    monkeypatch.setenv("BSIDE_PUBLIC_WEB_URL", "https://www.example.test")
    monkeypatch.setenv("BSIDE_OPS_TOKEN", "token")
    monkeypatch.setenv("GITHUB_SHA", "a" * 40)
    monkeypatch.setenv("GITHUB_OUTPUT", str(output_path))
    monkeypatch.setattr(
        watchdog,
        "fetch_health",
        lambda *_args: {
            "last_success_at": future.isoformat(),
            "active_deployment_status": "observed",
            "active_deployment": {
                "build_sha": "b" * 40,
                "observed_at": (future - timedelta(minutes=15)).isoformat(),
                "distribution_target": "pages",
            },
            "official_sources": {
                "dart": {"last_scheduled_success_at": future.isoformat()},
                "kind": {"last_scheduled_success_at": future.isoformat()},
            },
        },
    )
    monkeypatch.setattr(
        watchdog,
        "probe_url",
        lambda *, url, route_template, build_sha: watchdog.AvailabilityObservation(
            observation_id="availability:" + route_template,
            route_template=route_template,
            observed_at=datetime.now(timezone.utc).isoformat(),
            http_status=200,
            duration_ms=1,
            succeeded=True,
            build_sha=build_sha,
        ),
    )
    monkeypatch.setattr(
        watchdog,
        "submit_availability",
        lambda _base, _token, observations: {"accepted_count": len(observations)},
    )

    assert watchdog.main() == 0
    assert "incident=true" in output_path.read_text(encoding="utf-8")
    report = (tmp_path / ".watchdog-report.md").read_text(encoding="utf-8")
    assert "timestamp is" in report and "in the future" in report


def test_one_fresh_source_cannot_hide_missing_other_official_source(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    watchdog = load_watchdog()
    output_path = tmp_path / "github-output.txt"
    now = datetime.now(timezone.utc)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BSIDE_API_BASE_URL", "https://api.example.test")
    monkeypatch.setenv("BSIDE_PUBLIC_WEB_URL", "https://www.example.test")
    monkeypatch.setenv("BSIDE_OPS_TOKEN", "token")
    monkeypatch.setenv("GITHUB_SHA", "a" * 40)
    monkeypatch.setenv("GITHUB_OUTPUT", str(output_path))
    monkeypatch.setattr(
        watchdog,
        "fetch_health",
        lambda *_args: {
            "last_success_at": None,
            "active_deployment_status": "observed",
            "active_deployment": {
                "build_sha": "b" * 40,
                "observed_at": now.isoformat(),
                "distribution_target": "pages",
            },
            "official_sources": {
                "dart": {"last_scheduled_success_at": now.isoformat()},
                "kind": {"last_scheduled_success_at": None},
            },
        },
    )
    monkeypatch.setattr(
        watchdog,
        "probe_url",
        lambda *, url, route_template, build_sha: watchdog.AvailabilityObservation(
            observation_id="availability:" + route_template,
            route_template=route_template,
            observed_at=now.isoformat(),
            http_status=200,
            duration_ms=1,
            succeeded=True,
            build_sha=build_sha,
        ),
    )
    monkeypatch.setattr(
        watchdog,
        "submit_availability",
        lambda _base, _token, observations: {"accepted_count": len(observations)},
    )

    assert watchdog.main() == 0
    assert "incident=true" in output_path.read_text(encoding="utf-8")
    report = (tmp_path / ".watchdog-report.md").read_text(encoding="utf-8")
    assert "KIND has no successful ingest timestamp" in report


def test_sha_mismatch_still_submits_observations_for_authenticated_active_build(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    watchdog = load_watchdog()
    output_path = tmp_path / "github-output.txt"
    now = datetime.now(timezone.utc)
    submitted: list[list[object]] = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BSIDE_API_BASE_URL", "https://api.example.test")
    monkeypatch.setenv("BSIDE_PUBLIC_WEB_URL", "https://www.example.test")
    monkeypatch.setenv("BSIDE_OPS_TOKEN", "token")
    monkeypatch.setenv("WATCHDOG_GOVERNANCE_PAGES", "true")
    monkeypatch.setenv("GITHUB_SHA", "f" * 40)
    monkeypatch.setenv("GITHUB_OUTPUT", str(output_path))
    monkeypatch.setattr(
        watchdog,
        "fetch_health",
        lambda *_args: {
            "last_success_at": now.isoformat(),
            "active_deployment_status": "observed",
            "active_deployment": {
                "build_sha": "a" * 40,
                "observed_at": now.isoformat(),
                "distribution_target": "pages",
            },
            "official_sources": {
                "dart": {"last_scheduled_success_at": now.isoformat()},
                "kind": {"last_scheduled_success_at": now.isoformat()},
            },
        },
    )
    monkeypatch.setattr(watchdog, "fetch_deployed_build_sha", lambda _url: "b" * 40)
    monkeypatch.setattr(
        watchdog,
        "probe_url",
        lambda *, url, route_template, build_sha: watchdog.AvailabilityObservation(
            observation_id="availability:" + route_template,
            route_template=route_template,
            observed_at=now.isoformat(),
            http_status=200,
            duration_ms=1,
            succeeded=True,
            build_sha=build_sha,
        ),
    )

    def submit(_base, _token, observations):  # type: ignore[no-untyped-def]
        submitted.append(list(observations))
        return {"accepted_count": len(observations)}

    monkeypatch.setattr(watchdog, "submit_availability", submit)

    assert watchdog.main() == 0
    assert "incident=true" in output_path.read_text(encoding="utf-8")
    assert len(submitted) == 1
    assert len(submitted[0]) == 4
    assert {observation.build_sha for observation in submitted[0]} == {"a" * 40}
    report = (tmp_path / ".watchdog-report.md").read_text(encoding="utf-8")
    assert "does not match authenticated active deployment" in report
