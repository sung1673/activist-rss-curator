from __future__ import annotations

import gzip
import shutil
from pathlib import Path

import pytest

from curator.governance_ui import (
    ASSET_GZIP_BUDGET_BYTES,
    HTML_BUDGET_BYTES,
    assert_asset_budget,
    build_governance_ui,
    config_javascript,
    configured_api_base,
    configured_build_sha,
    configured_web_base,
    normalize_api_base,
    normalize_build_sha,
    normalize_web_base,
)


ROOT = Path(__file__).resolve().parents[1]
UI = ROOT / "public" / "governance"


def test_api_base_normalization_supports_pretty_and_php_routes() -> None:
    assert normalize_api_base("/api/v1/") == "/api/v1"
    assert normalize_api_base("/backend") == "/backend/api/v1"
    assert normalize_api_base("https://api.example.com/activist/api.php") == (
        "https://api.example.com/activist/api.php/api/v1"
    )
    assert normalize_api_base("https://news.example.com/api/v1/") == "https://news.example.com/api/v1"


@pytest.mark.parametrize(
    "value",
    [
        "javascript:alert(1)",
        "//evil.example/api/v1",
        "https://user:secret@example.com/api/v1",
        "https://example.com/api/v1?token=secret",
        "https://example.com/api/v1#fragment",
    ],
)
def test_api_base_rejects_unsafe_values(value: str) -> None:
    with pytest.raises(ValueError):
        normalize_api_base(value)


def test_environment_api_base_priority(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("ACTIVIST_PUBLIC_API_URL", "https://legacy.example/api.php")
    monkeypatch.setenv("BSIDE_PUBLIC_API_V1_URL", "https://v1.example/api/v1")
    monkeypatch.setenv("GOVERNANCE_API_BASE_URL", "https://preferred.example/api/v1")
    assert configured_api_base() == "https://preferred.example/api/v1"
    assert configured_api_base("https://explicit.example/api/v1") == "https://explicit.example/api/v1"


def test_web_and_api_bases_are_configured_independently(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("GOVERNANCE_WEB_BASE_URL", "https://preview.news.example/governance/")
    assert configured_web_base() == "https://preview.news.example/governance/"
    assert normalize_web_base(configured_web_base()) == "https://preview.news.example/governance"
    assert normalize_web_base("https://news.bside.ai/") == "https://news.bside.ai"
    for value in ("/governance", "javascript:alert(1)", "https://user:secret@example.com", "https://example.com/?token=secret"):
        with pytest.raises(ValueError):
            normalize_web_base(value)


def test_config_javascript_contains_only_public_normalized_base() -> None:
    build_sha = "a" * 40
    javascript = config_javascript("https://api.example.com/root", build_sha=build_sha)
    assert javascript == (
        'window.__BSIDE_GOVERNANCE_CONFIG__=Object.freeze({"apiBase":"https://api.example.com/root/api/v1","webBase":"https://news.bside.ai","buildSha":"' + build_sha + '"});\n'
    )
    assert "secret" not in javascript.casefold()


def test_build_sha_uses_ci_revision_and_rejects_labels(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("GITHUB_SHA", "B" * 40)
    assert configured_build_sha() == "B" * 40
    assert normalize_build_sha(configured_build_sha()) == "b" * 40
    assert normalize_build_sha("development") == "development"
    with pytest.raises(ValueError):
        normalize_build_sha("main")


def test_build_writes_config_and_enforces_performance_budget(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("GITHUB_SHA", "c" * 40)
    target = tmp_path / "public" / "governance"
    target.mkdir(parents=True)
    for name in ("index.html", "app.js", "styles.css"):
        shutil.copyfile(UI / name, target / name)
    result = build_governance_ui(tmp_path, "https://api.example.com/api/v1", "https://web.example/governance")
    assert result["api_base"] == "https://api.example.com/api/v1"
    assert result["web_base"] == "https://web.example/governance"
    assert result["build_sha"] == "c" * 40
    assert result["html_bytes"] < HTML_BUDGET_BYTES
    assert result["asset_gzip_bytes"] < ASSET_GZIP_BUDGET_BYTES
    assert '"apiBase":"https://api.example.com/api/v1"' in (target / "config.js").read_text(encoding="utf-8")
    assert '"webBase":"https://web.example/governance"' in (target / "config.js").read_text(encoding="utf-8")
    assert '"buildSha":"' + "c" * 40 + '"' in (target / "config.js").read_text(encoding="utf-8")


def test_checked_in_assets_are_far_below_initial_budgets() -> None:
    budget = assert_asset_budget(UI)
    assert budget["html_bytes"] < 25_000
    assert budget["asset_gzip_bytes"] < 50_000
    direct_gzip = sum(
        len(gzip.compress((UI / name).read_bytes(), compresslevel=9))
        for name in ("app.js", "styles.css", "config.js")
    )
    assert direct_gzip == budget["asset_gzip_bytes"]


def test_ui_uses_safe_rendering_and_all_public_api_contracts() -> None:
    javascript = (UI / "app.js").read_text(encoding="utf-8")
    for unsafe in ("innerHTML", "insertAdjacentHTML", "document.write", "eval("):
        assert unsafe not in javascript
    assert "textContent" in javascript
    assert "sourceNode" in javascript
    for route in (
        'request("/today"',
        'request("/events"',
        'request("/companies"',
        'request(`/companies/${',
        'request(`/actors/${',
        'request(`/events/${',
        'request(`/campaigns/${',
        'request("/calendar"',
        'request("/search"',
        'request("/revisions"',
        'request("/feedback"',
    ):
        assert route in javascript


def test_ui_fail_closed_public_content_preview_and_large_body_contracts() -> None:
    javascript = (UI / "app.js").read_text(encoding="utf-8")
    assets = "\n".join((UI / name).read_text(encoding="utf-8") for name in ("index.html", "app.js", "styles.css", "config.js"))
    assert 'window.location.hash.startsWith("#preview=")' in javascript
    assert "sessionStorage.setItem(PREVIEW_SESSION_KEY" in javascript
    assert "headers.Authorization = `Bearer ${token}`" in javascript
    assert "history.replaceState" in javascript
    assert "preview_token" not in assets
    assert "localStorage" not in javascript
    assert "body_limit_bytes: DOCUMENT_BODY_CHUNK_BYTES" in javascript
    assert "body_truncated" in javascript
    assert "TextEncoder" in javascript
    assert 'String(event.verification_status || "") === "signal"' in javascript
    assert ".filter(isPublicEvent)" in javascript
    assert "eventScore" not in javascript
    assert "watchCandidate" not in javascript
    assert "!hiddenIds.has(item.event_id)" in javascript
    assert "new FormData(form)" in javascript
    assert '"actor_id", "event_type", "source_class", "verification_status", "from", "to"' in javascript
    assert "function installWebVitals" in javascript
    assert 'fetch(endpoint("/metrics/web-vitals")' in javascript
    assert "keepalive: true" in javascript
    assert "route_template: routeTemplate, metric, value, device_class: deviceClass, build_sha: buildSha" in javascript
    assert 'return `/${first}/:id`' in javascript
    assert "telegram" not in assets.casefold()
    assert "#/admin" not in assets.casefold()


def test_ui_has_actor_public_revision_and_right_of_reply_flows() -> None:
    javascript = (UI / "app.js").read_text(encoding="utf-8")
    html = (UI / "index.html").read_text(encoding="utf-8")
    assert "async function renderActor" in javascript
    assert 'request(`/actors/${' in javascript
    assert "async function renderRevisions" in javascript
    assert 'revision.is_public === false' in javascript
    assert 'revision.publication_status !== "published"' in javascript
    assert '["actor", "당사자 / Actor"]' in javascript
    assert 'feedback_type=right_of_reply&entity_type=actor' in javascript
    assert 'data-nav="revisions"' in html


def test_html_and_css_include_accessibility_and_language_policy() -> None:
    html = (UI / "index.html").read_text(encoding="utf-8")
    css = (UI / "styles.css").read_text(encoding="utf-8")
    assert 'class="skip-link"' in html
    assert 'aria-live="polite"' in html
    assert 'aria-label="주요 메뉴 / Primary navigation"' in html
    assert "원문 제목과 본문은 번역하지 않습니다" in html
    assert "Content-Security-Policy" in html
    assert "style=" not in html
    assert "prefers-reduced-motion" in css
    assert ":focus-visible" in css
    assert "min-height: 44px" in css


def test_daily_pages_configure_governance_api_without_secrets() -> None:
    workflow = (ROOT / ".github" / "workflows" / "daily.yml").read_text(encoding="utf-8")
    assert "python -m curator.governance_ui --root ." in workflow
    assert "GOVERNANCE_API_BASE_URL: ${{ vars.GOVERNANCE_API_BASE_URL }}" in workflow
    assert "ACTIVIST_PUBLIC_API_URL: ${{ vars.ACTIVIST_PUBLIC_API_URL }}" in workflow
    ui_step = workflow.split("- name: Configure public governance UI", 1)[1].split("- name:", 1)[0]
    assert "secrets." not in ui_step
