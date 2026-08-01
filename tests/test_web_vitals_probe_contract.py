from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / ".github" / "scripts" / "web-vitals-probe.mjs"
WORKFLOW = ROOT / ".github" / "workflows" / "web-vitals.yml"


def test_probe_uses_real_mobile_event_timing_without_secret_urls() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    assert 'Object.freeze(["/today", "/events", "/issuers", "/calendar"])' in source
    assert "RUNS_PER_ROUTE = 5" in source
    assert "MAX_JOURNEY_RETRIES = 1" in source
    assert 'devices["Pixel 5"]' in source
    assert 'install("largest-contentful-paint"' in source
    assert 'install("layout-shift"' in source
    assert 'install("event"' in source
    assert 'durationThreshold: 16' in source
    assert "entry.interactionId > 0" in source
    assert '.mobile-bottom-nav [data-nav="${destination}"]:visible' in source
    assert ".click({ timeout: 10_000 })" in source
    assert "state.flushEvents().inp > 0" in source
    assert "values.inp <= 0" in source
    assert 'source: "first_party"' in source
    assert "sessionStorage.setItem(key, token)" in source
    assert 'context.route("**/metrics/web-vitals"' in source
    assert "#preview=" not in source
    assert "searchParams.set" not in source
    assert "console.log" not in source


def test_probe_retries_only_transient_route_attempts_without_partial_submission() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    assert "class RouteAttemptError extends ProbeError" in source
    assert "error instanceof RouteAttemptError" in source
    assert "attemptNumber <= MAX_JOURNEY_RETRIES + 1" in source
    assert "context = await browser.newContext" in source
    assert 'substep = "click_bottom_navigation"' in source
    assert 'substep = "wait_for_inp"' in source
    assert "original_error_message_sha256" in source
    assert 'split(previewToken).join("[REDACTED]")' in source
    matrix_index = source.rindex("assertObservationMatrix(observations")
    submission_index = source.rindex("submitObservations(deployed.apiBase")
    assert matrix_index < submission_index
    assert source.index("observations.push(...measured.observations)") < matrix_index
    assert "journey_attempt_audit" in source


def test_workflow_runs_before_evidence_and_keeps_outbound_disabled() -> None:
    source = WORKFLOW.read_text(encoding="utf-8")
    parsed = yaml.load(source, Loader=yaml.BaseLoader)
    assert parsed["on"]["schedule"] == [{"cron": "0 14 * * *"}]
    assert parsed["permissions"] == {"contents": "read"}
    job = parsed["jobs"]["mobile-routes"]
    assert job["timeout-minutes"] == "30"
    commands = "\n".join(str(step.get("run", "")) for step in job["steps"])
    assert "git rev-parse HEAD" in commands
    assert "npx playwright install --with-deps chromium" in commands
    assert "web-vitals-probe.mjs" in commands
    probe_step = next(
        step for step in job["steps"] if step.get("name") == "Measure four real mobile SPA journeys and submit evidence"
    )
    assert probe_step["env"]["GOVERNANCE_PREVIEW_TOKEN"] == "${{ secrets.GOVERNANCE_PREVIEW_TOKEN }}"
    assert probe_step["env"]["ENABLE_TELEGRAM_DELIVERY"] == "${{ vars.ENABLE_TELEGRAM_DELIVERY }}"
    assert probe_step["env"]["ENABLE_GOVERNANCE_DELIVERY"] == "${{ vars.ENABLE_GOVERNANCE_DELIVERY }}"
    assert "TELEGRAM_BOT_TOKEN" not in source
    assert "TELEGRAM_CHAT_ID" not in source
    upload_step = next(
        step for step in job["steps"] if step.get("name") == "Upload privacy-minimal run receipt"
    )
    assert "if" not in upload_step


def test_probe_schedule_precedes_evidence_input_schedule() -> None:
    evidence = yaml.load(
        (ROOT / ".github" / "workflows" / "release-evidence-inputs.yml").read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )
    assert evidence["on"]["schedule"] == [{"cron": "35 15 * * *"}]
    # UTC 14:00 (KST 23:00) is 95 minutes before UTC 15:35 (KST 00:35).
    assert 14 * 60 < 15 * 60 + 35
