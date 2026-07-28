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
        'window.__BSIDE_GOVERNANCE_CONFIG__=Object.freeze({"apiBase":"https://api.example.com/root/api/v1","webBase":"https://news.bside.ai","buildSha":"'
        + build_sha
        + '","releaseChannel":"production_alpha_early_access"});\n'
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
    assert result["release_channel"] == "production_alpha_early_access"
    assert result["html_bytes"] < HTML_BUDGET_BYTES
    assert result["asset_gzip_bytes"] < ASSET_GZIP_BUDGET_BYTES
    assert '"apiBase":"https://api.example.com/api/v1"' in (target / "config.js").read_text(encoding="utf-8")
    assert '"webBase":"https://web.example/governance"' in (target / "config.js").read_text(encoding="utf-8")
    assert '"buildSha":"' + "c" * 40 + '"' in (target / "config.js").read_text(encoding="utf-8")
    assert '"releaseChannel":"production_alpha_early_access"' in (
        target / "config.js"
    ).read_text(encoding="utf-8")


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
    assert 'importance: event.importance || "unknown"' in javascript
    assert 'importance: event.importance || "medium"' not in javascript
    for route in (
        'terminalRequest("/briefs/latest"',
        'terminalRequest("/live"',
        'terminalRequest("/sources/status"',
        'terminalRequest("/events"',
        'terminalRequest("/calendar"',
        'terminalRequest("/issuers"',
        'terminalRequest("/search"',
        "terminalRequest(path",
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
    assert 'String(event.verification_status || "") === "signal"' in javascript
    assert "topIds.has(item.event_id)" in javascript
    assert "new FormData(form)" in javascript
    assert '"actor_id", "event_type", "source_class", "verification_status", "from", "to"' in javascript
    assert "function installWebVitals" in javascript
    assert 'fetch(endpoint("/metrics/web-vitals")' in javascript
    assert "keepalive: true" in javascript
    assert "route_template: routeTemplate, metric, value, device_class: deviceClass, build_sha: buildSha" in javascript
    assert 'return `/${first}/:id`' in javascript
    assert "telegram" not in assets.casefold()
    assert "#/admin" not in assets.casefold()


def test_terminal_v2_fallback_filters_drawer_and_keyboard_contracts() -> None:
    javascript = (UI / "app.js").read_text(encoding="utf-8")
    html = (UI / "index.html").read_text(encoding="utf-8")
    css = (UI / "styles.css").read_text(encoding="utf-8")
    assert 'const v2BaseUrl = apiBase("v2")' in javascript
    assert 'anchor.dataset.apiVersion === "v2"' in javascript
    assert html.count('data-api-version="v2"') == 4
    for path in (
        "/api/v2/feeds/events.atom",
        "/api/v2/exports/events.csv",
        "/api/v2/exports/events.json",
        "/api/v2/openapi.yaml",
    ):
        assert path in html
    assert "function publicSourceState(source)" in javascript
    assert 'typeof source.public_status === "string"' in javascript
    assert "source.public_ready === true" in javascript
    assert "publicSourceState(item).ready" in javascript
    assert '[405, 410, 501]' in javascript
    assert "Number(error.status) === 404" in javascript
    assert 'payload.api_version !== "v2"' in javascript
    assert 'payload.ok !== true' in javascript
    assert 'error.code === "unsupported_v2_contract"' in javascript
    assert "error.status === 0" not in javascript
    assert '["not_found", "endpoint_not_found"]' in javascript
    assert 'terminalRequest("/search"' in javascript
    assert 'terminalRequest("/events"' in javascript
    assert 'terminalRequest("/calendar"' in javascript
    assert 'terminalRequest("/issuers"' in javascript
    assert "const result = await terminalRequest(path" in javascript
    assert 'fallback: () => request("/today"' in javascript
    assert 'fallback: () => request("/events"' in javascript
    assert 'request("/calendar"' in javascript
    assert 'values.set("market", currentMarket(query))' in javascript
    assert 'dataset: { eventDrawer: event.event_id || "" }' in javascript
    assert '["j", "J", "k", "K", "Enter"]' in javascript
    assert 'event.key === "Escape"' in javascript
    assert 'event.key === "/"' in javascript
    for market in ("GLOBAL", "KR", "US", "JP", "GB", "CA", "AU"):
        assert f'data-market="{market}"' in html
    assert 'href="#/issuers" data-nav="companies"' in html
    assert 'href="#/companies" data-nav="companies"' not in html
    assert 'id="mobile-menu-toggle"' in html
    assert 'id="mobile-menu"' in html
    assert "function toggleMobileMenu()" in javascript
    assert "closeMobileMenu({ restoreFocus: false })" in javascript
    assert 'id="event-drawer"' in html
    assert 'class="mobile-bottom-nav"' in html
    assert 'href="#/today?view=live" data-nav="live"' in html
    assert 'id: "terminal-live"' in javascript
    assert 'route.query.get("view") === "live"' in javascript
    assert 'event.target.closest("[data-nav=\'live\']")' in javascript
    assert "currentMarket(parseRoute().query)" in javascript
    assert "grid-template-areas: \"filters main rail\"" in css
    assert "min-height: 44px" in css
    assert "function mobileTerminalTabs" in javascript
    assert "function openTerminalFilterSheet" in javascript
    assert 'id: "terminal-filters"' in javascript
    assert "filter-sheet-open" in css
    assert "mobile-terminal-panel" in css
    assert "Watch / 새로 바뀐 사건" in javascript


def test_production_alpha_ui_keeps_jp_and_gb_link_only_and_unavailable() -> None:
    javascript = (UI / "app.js").read_text(encoding="utf-8")
    html = (UI / "index.html").read_text(encoding="utf-8")
    scope = javascript.split(
        "const ALPHA_MARKET_SCOPE = Object.freeze({",
        1,
    )[1].split("\n  });", 1)[0]
    for country in ("JP", "GB"):
        assert (
            f'{country}: Object.freeze({{ coverage_mode: "link-only", '
            'public_status: "coverage_unavailable", public_ready: false })'
        ) in scope
    assert 'JP: "EDINET 시장 전체 / Market-wide' not in javascript
    assert 'GB: "Companies House 공식 등록부 / Official register' not in javascript
    assert "if (policy && policy.public_ready === false)" in javascript
    assert "return { status: policy.public_status, ready: false };" in javascript
    assert "JP·GB는 링크 전용·현재 수집 불가" in html
    assert "JP·GB 시장 전체" not in html
    assert "GB 공식 등록부" not in html


def test_production_alpha_early_access_channel_is_visible_on_every_route() -> None:
    javascript = (UI / "app.js").read_text(encoding="utf-8")
    html = (UI / "index.html").read_text(encoding="utf-8")
    css = (UI / "styles.css").read_text(encoding="utf-8")
    assert '<body data-release-channel="production_alpha_early_access">' in html
    assert 'aria-label="출시 상태 / Release status"' in html
    assert "Production Alpha · Early Access" in html
    assert "KR·US 공식 시장 데이터 · CA·AU 공식 링크 · JP·GB 현재 수집 불가" in html
    assert 'config.releaseChannel || "production_alpha_early_access"' in javascript
    assert '"releaseChannel":"production_alpha_early_access"' in (
        UI / "config.js"
    ).read_text(encoding="utf-8")
    assert "document.body.dataset.releaseChannel = PUBLIC_UI_CONFIG.releaseChannel;" in javascript
    assert ".release-strip {" in css
    assert ".release-notice {" in css


def test_v2_issuer_identity_archive_and_calendar_never_alias_to_legacy_company() -> None:
    javascript = (UI / "app.js").read_text(encoding="utf-8")
    assert "function issuerOrCompanyRoute" in javascript
    assert 'return identity.kind === "issuer"' in javascript
    assert '`#/issuers/${encodeURIComponent(identity.id)}`' in javascript
    assert "company_id: event.company_id || event.issuer_id" not in javascript
    assert "company_id: event.company_id || \"\"" in javascript
    assert "issuer_id: event.issuer_id || \"\"" in javascript
    assert 'path === "/calendar"' in javascript
    assert 'path === "/issuers"' in javascript
    assert "/^\\/issuers\\/[A-Za-z0-9_.:%-]+$/.test(path)" in javascript
    assert "async function renderIssuers" in javascript
    assert "async function renderIssuer" in javascript
    assert "function normalizeCalendarItem" in javascript
    assert 'else if (first === "issuers" && second) await renderIssuer(second, signal)' in javascript
    assert 'else if (first === "issuers") await renderIssuers(route.query, signal)' in javascript


def test_v2_ui_validates_actor_roles_and_uses_exact_offset_continuations() -> None:
    javascript = (UI / "app.js").read_text(encoding="utf-8")
    assert "function isV2PageMeta(value)" in javascript
    assert "value.next_offset === value.offset + value.returned" in javascript
    assert "!isRecordArray(data.actors)" in javascript
    assert 'typeof actor.actor_role === "string"' in javascript
    assert 'return { kind: "offset", value: value.next_offset };' in javascript
    assert "function continuationParams(base, value, kind)" in javascript
    assert 'params[kind === "offset" ? "offset" : "page"] = value;' in javascript
    assert "continuationParams(v2Params, cursor, cursorKind)" in javascript
    assert "continuationParams(params, cursor, cursorKind)" in javascript


def test_today_date_filters_reject_inverted_ranges_and_undated_records() -> None:
    javascript = (UI / "app.js").read_text(encoding="utf-8")
    assert 'const invalid = Boolean(from.value && to.value && from.value > to.value);' in javascript
    assert 'to.setCustomValidity(invalid ? "종료일은 시작일보다 빠를 수 없습니다.' in javascript
    assert "if ((hasFrom || hasTo) && !eventDate) return false;" in javascript
    assert 'if (/^\\d{4}-\\d{2}-\\d{2}$/.test(value)) params[name] = value;' in javascript


def test_v2_ui_fails_closed_and_labels_title_provenance() -> None:
    javascript = (UI / "app.js").read_text(encoding="utf-8")
    assert "const TITLE_PROVENANCE_VALUES = new Set([" in javascript
    assert 'generated_metadata: "공시 메타데이터 표제 / Filing metadata label"' in javascript
    assert 'operator_metadata: "운영자 등록 표제 / Operator-entered label"' in javascript
    assert 'unknown: "제목 출처 미확인 / Title source unavailable"' in javascript
    assert "function isV2EventRecord(value)" in javascript
    assert "TITLE_PROVENANCE_VALUES.has(String(value.title_provenance || \"\"))" in javascript
    assert '? String(event.title_provenance)' in javascript
    assert ': "unknown"' in javascript
    assert 'label("titleProvenance", event.title_provenance)' in javascript
    assert "title_provenance: \"source\"" not in javascript


def test_terminal_uses_only_the_eight_canonical_v2_event_families() -> None:
    javascript = (UI / "app.js").read_text(encoding="utf-8")
    canonical = (
        "large_ownership",
        "meeting_and_vote",
        "tender_offer_and_mna",
        "capital_issuance",
        "capital_return",
        "board_and_compensation",
        "listing_status",
        "correction_and_withdrawal",
    )
    labels = javascript.split("eventType: {", 1)[1].split("},", 1)[0]
    for event_family in canonical:
        assert f"{event_family}:" in labels
    for legacy in (
        "five_percent_holding",
        "shareholder_proposal",
        "treasury_shares",
        "tender_offer",
        "value_up",
    ):
        assert f"{legacy}:" not in labels
    assert "const CANONICAL_EVENT_FAMILIES = new Set([" in javascript
    assert 'if (eventType) params.event_family = eventType' in javascript


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
    mobile = css.split("@media (max-width: 680px)", 1)[1]
    assert ".market-nav { position: static; }" in mobile
    assert ".mobile-terminal-tabs {" in mobile
    assert "top: 0;" in mobile


def test_daily_pages_configure_governance_api_without_secrets() -> None:
    workflow = (ROOT / ".github" / "workflows" / "daily.yml").read_text(encoding="utf-8")
    assert "python -m curator.governance_ui --root ." in workflow
    assert "GOVERNANCE_API_BASE_URL: ${{ vars.GOVERNANCE_API_BASE_URL }}" in workflow
    assert "ACTIVIST_PUBLIC_API_URL: ${{ vars.ACTIVIST_PUBLIC_API_URL }}" in workflow
    ui_step = workflow.split("- name: Configure public governance UI", 1)[1].split("- name:", 1)[0]
    assert "secrets." not in ui_step
