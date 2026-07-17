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
    normalize_api_base,
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


def test_config_javascript_contains_only_public_normalized_base() -> None:
    javascript = config_javascript("https://api.example.com/root")
    assert javascript == (
        'window.__BSIDE_GOVERNANCE_CONFIG__=Object.freeze({"apiBase":"https://api.example.com/root/api/v1"});\n'
    )
    assert "secret" not in javascript.casefold()


def test_build_writes_config_and_enforces_performance_budget(tmp_path: Path) -> None:
    target = tmp_path / "public" / "governance"
    target.mkdir(parents=True)
    for name in ("index.html", "app.js", "styles.css"):
        shutil.copyfile(UI / name, target / name)
    result = build_governance_ui(tmp_path, "https://api.example.com/api/v1")
    assert result["api_base"] == "https://api.example.com/api/v1"
    assert result["html_bytes"] < HTML_BUDGET_BYTES
    assert result["asset_gzip_bytes"] < ASSET_GZIP_BUDGET_BYTES
    assert '"apiBase":"https://api.example.com/api/v1"' in (target / "config.js").read_text(encoding="utf-8")


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
        'request("/events"',
        'request("/companies"',
        'request(`/companies/${',
        'request(`/events/${',
        'request(`/campaigns/${',
        'request("/calendar"',
        'request("/search"',
        'request("/feedback"',
    ):
        assert route in javascript


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
