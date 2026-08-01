(() => {
  "use strict";

  const app = document.getElementById("app");
  const announcer = document.getElementById("announcer");
  if (!app || !announcer) return;
  const drawerShell = document.getElementById("event-drawer-shell");
  const drawer = document.getElementById("event-drawer");
  const drawerContent = document.getElementById("drawer-content");
  const drawerKicker = document.getElementById("drawer-kicker");
  const globalSearchForm = document.getElementById("global-search-form");
  const globalSearchInput = document.getElementById("global-search");
  const globalCoverage = document.getElementById("global-source-coverage");
  const mobileMenuToggle = document.getElementById("mobile-menu-toggle");
  const mobileMenu = document.getElementById("mobile-menu");

  const PREVIEW_SESSION_KEY = "bside.governance.preview";
  const MARKETS = new Set(["GLOBAL", "KR", "US", "JP", "GB", "CA", "AU"]);
  const ALPHA_MARKET_SCOPE = Object.freeze({
    KR: Object.freeze({ coverage_mode: "market-wide", public_status: "active", public_ready: true }),
    US: Object.freeze({ coverage_mode: "market-wide", public_status: "active", public_ready: true }),
    JP: Object.freeze({ coverage_mode: "link-only", public_status: "coverage_unavailable", public_ready: false }),
    GB: Object.freeze({ coverage_mode: "link-only", public_status: "coverage_unavailable", public_ready: false }),
    CA: Object.freeze({ coverage_mode: "link-only", public_status: "active", public_ready: true }),
    AU: Object.freeze({ coverage_mode: "link-only", public_status: "active", public_ready: true })
  });
  const REQUIRED_ALPHA_MARKETS = Object.freeze(["KR", "US", "CA", "AU"]);
  const OPTIONAL_ALPHA_MARKETS = Object.freeze(["JP", "GB"]);
  let drawerTrigger = null;
  let drawerController = null;
  let filterSheetTrigger = null;

  function closeMobileMenu(options) {
    const settings = options || {};
    if (!mobileMenu || !mobileMenuToggle || mobileMenu.hidden) return;
    mobileMenu.hidden = true;
    mobileMenuToggle.setAttribute("aria-expanded", "false");
    if (settings.restoreFocus !== false) mobileMenuToggle.focus();
  }

  function toggleMobileMenu() {
    if (!mobileMenu || !mobileMenuToggle) return;
    const opening = mobileMenu.hidden;
    mobileMenu.hidden = !opening;
    mobileMenuToggle.setAttribute("aria-expanded", opening ? "true" : "false");
    if (opening) {
      const firstLink = mobileMenu.querySelector("a[href]");
      if (firstLink) firstLink.focus();
    }
  }

  function capturePreviewToken() {
    if (!window.location.hash.startsWith("#preview=")) return;
    const fragment = new URLSearchParams(window.location.hash.slice(1));
    const token = String(fragment.get("preview") || "");
    try {
      if (/^[A-Za-z0-9._~-]{32,512}$/.test(token)) sessionStorage.setItem(PREVIEW_SESSION_KEY, token);
      else sessionStorage.removeItem(PREVIEW_SESSION_KEY);
    } catch (_error) {
      // A blocked session store must fail closed instead of leaving a token in the address bar.
    }
    history.replaceState(null, "", `${window.location.pathname}${window.location.search}#/today`);
  }

  function previewToken() {
    try {
      const token = String(sessionStorage.getItem(PREVIEW_SESSION_KEY) || "");
      return /^[A-Za-z0-9._~-]{32,512}$/.test(token) ? token : "";
    } catch (_error) {
      return "";
    }
  }

  capturePreviewToken();

  const config = window.__BSIDE_GOVERNANCE_CONFIG__ || {};
  const PUBLIC_UI_CONFIG = Object.freeze({
    releaseChannel: String(config.releaseChannel || "production_alpha_early_access")
  });
  document.body.dataset.releaseChannel = PUBLIC_UI_CONFIG.releaseChannel;
  const CANONICAL_EVENT_FAMILIES = new Set([
    "large_ownership",
    "meeting_and_vote",
    "tender_offer_and_mna",
    "capital_issuance",
    "capital_return",
    "board_and_compensation",
    "listing_status",
    "correction_and_withdrawal"
  ]);
  const TITLE_PROVENANCE_VALUES = new Set([
    "source",
    "generated_metadata",
    "operator_metadata"
  ]);
  const UTC_DATE_TIME_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/;
  const LEGACY_EVENT_FAMILY_MAP = Object.freeze({
    five_percent_holding: "large_ownership",
    shareholder_proposal: "meeting_and_vote",
    general_meeting: "meeting_and_vote",
    proposal_vote: "meeting_and_vote",
    tender_offer: "tender_offer_and_mna",
    merger: "tender_offer_and_mna",
    split: "tender_offer_and_mna",
    duplicate_listing: "tender_offer_and_mna",
    rights_issue: "capital_issuance",
    convertible_bond: "capital_issuance",
    bond_with_warrant: "capital_issuance",
    exchangeable_bond: "capital_issuance",
    dividend: "capital_return",
    treasury_shares: "capital_return",
    board: "board_and_compensation",
    executive_compensation: "board_and_compensation",
    delisting: "listing_status",
    trading_suspension: "listing_status"
  });
  const labels = {
    eventType: {
      large_ownership: "대량보유·보유목적 변경 / Ownership",
      meeting_and_vote: "주총·주주제안·의결 / Meetings & votes",
      tender_offer_and_mna: "공개매수·M&A·합병·분할 / Tender offers & M&A",
      capital_issuance: "증자·CB/BW/EB·자본 희석 / Capital issuance",
      capital_return: "배당·자사주 매입·소각 / Capital return",
      board_and_compensation: "이사·경영진·보수 / Board & compensation",
      listing_status: "거래정지·상장상태 / Listing status",
      correction_and_withdrawal: "정정·철회·취소 / Corrections & withdrawals"
    },
    importance: {
      critical: "시장 민감 / Market-sensitive",
      market_sensitive: "시장 민감 / Market-sensitive",
      high: "높음 / High",
      medium: "보통 / Medium",
      low: "낮음 / Low",
      unknown: "미분류 / Unclassified"
    },
    verification: {
      signal: "신호 / Signal",
      unverified: "미확인 / Unverified",
      corroborated: "추가 근거 / Corroborated",
      official: "공식 근거 / Official",
      confirmed: "확인 / Confirmed",
      disputed: "다툼 있음 / Disputed",
      corrected: "정정 / Corrected",
      withdrawn: "철회 / Withdrawn"
    },
    campaignStage: {
      initial_signal: "초기 신호 / Initial signal",
      private_engagement: "비공개 관여 / Private engagement",
      public_letter: "공개서한·질의 / Public letter",
      public_campaign: "공개 캠페인 / Public campaign",
      shareholder_proposal: "주주제안 / Shareholder proposal",
      proxy_vote: "위임·표결 / Proxy vote",
      resolution: "합의·결과 / Resolution",
      implementation_tracking: "이행 추적 / Implementation tracking",
      closed: "종료 / Closed"
    },
    outcome: {
      settled: "합의 / Settled",
      withdrawn: "철회 / Withdrawn",
      passed: "가결 / Passed",
      failed: "부결 / Failed",
      pending: "대기 / Pending"
    },
    sourceClass: {
      official_disclosure: "공식 공시 / Official disclosure",
      company_statement: "회사 입장 / Company statement",
      activist_statement: "행동주주 입장 / Activist statement",
      media_report: "언론 보도 / Media report",
      editorial_analysis: "편집 분석 / Editorial analysis"
    },
    titleProvenance: {
      source: "원문 제목 / Source title",
      generated_metadata: "공시 메타데이터 표제 / Filing metadata label",
      operator_metadata: "운영자 등록 표제 / Operator-entered label",
      unknown: "제목 출처 미확인 / Title source unavailable"
    },
    claimType: {
      actor_claim: "당사자 주장 / Actor claim",
      company_response: "회사 반론 / Company response",
      official_fact: "공식 사실 / Official fact",
      media_report: "보도 / Media report",
      editorial_interpretation: "편집 해석 / Editorial interpretation"
    },
    commitment: {
      planned: "계획 / Planned",
      announced: "발표 / Announced",
      in_progress: "진행 중 / In progress",
      met: "이행 / Met",
      partially_met: "부분 이행 / Partially met",
      missed: "미이행 / Missed",
      cancelled: "취소 / Cancelled"
    },
    actorType: {
      company: "회사 / Company",
      activist_shareholder: "행동주주 / Activist shareholder",
      institution: "기관 / Institution",
      shareholder_coalition: "주주연대 / Shareholder coalition",
      regulator: "규제기관 / Regulator",
      advisor: "자문사 / Advisor"
    },
    actorRole: {
      issuer: "발행사 / Issuer",
      target: "대상 / Target",
      proponent: "제안자 / Proponent",
      claimant: "주장 주체 / Claimant",
      respondent: "대응 주체 / Respondent",
      supporter: "지지 주체 / Supporter",
      advisor: "자문사 / Advisor",
      regulator: "규제기관 / Regulator",
      participant: "참여자 / Participant"
    },
    kind: {
      company: "기업 / Company",
      actor: "당사자 / Actor",
      event: "사건 / Event",
      campaign: "캠페인 / Campaign",
      document: "문서 / Document"
    },
    coverageMode: {
      "market-wide": "시장 전체 / Market-wide",
      "official-register": "공식 등록부 / Official register",
      "selected-issuers": "선별 기업 / Selected issuers",
      "link-only": "링크 전용·수동 메타데이터 / Link-only · manual metadata",
      unavailable: "수집 범위 없음 / Unavailable",
      official: "공식 근거 / Official evidence",
      media_only: "보도 전용 / Media only",
      partial: "일부 범위 / Partial"
    }
  };

  let routeController = null;

  class ApiError extends Error {
    constructor(message, code, status, apiVersion) {
      super(message);
      this.name = "ApiError";
      this.code = code || "request_failed";
      this.status = status || 0;
      this.apiVersion = apiVersion || "";
    }
  }

  function element(tag, options, children) {
    const node = document.createElement(tag);
    const settings = options || {};
    if (settings.className) node.className = settings.className;
    if (settings.text !== undefined && settings.text !== null) node.textContent = String(settings.text);
    if (settings.lang && /^[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})*$/.test(settings.lang)) node.lang = settings.lang;
    if (settings.attrs) {
      Object.entries(settings.attrs).forEach(([name, value]) => {
        if (value !== undefined && value !== null && value !== false) node.setAttribute(name, String(value));
      });
    }
    if (settings.dataset) {
      Object.entries(settings.dataset).forEach(([name, value]) => { node.dataset[name] = String(value); });
    }
    (children || []).forEach((child) => {
      if (child instanceof Node) node.append(child);
      else if (child !== undefined && child !== null) node.append(document.createTextNode(String(child)));
    });
    return node;
  }

  function fragment(children) {
    const value = document.createDocumentFragment();
    (children || []).forEach((child) => { if (child) value.append(child); });
    return value;
  }

  function sourceNode(tag, text, language, className) {
    return element(tag, { text: text || "—", lang: language || "", className: className || "source-text" });
  }

  function label(group, value) {
    const key = String(value || "");
    return (labels[group] && labels[group][key]) || key || "—";
  }

  function normalizedDate(value) {
    const text = String(value || "").trim();
    if (!text) return null;
    const iso = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(text) ? `${text.replace(" ", "T")}Z` : text;
    const date = new Date(iso);
    return Number.isNaN(date.getTime()) ? null : date;
  }

  function formatDate(value, withTime) {
    const date = normalizedDate(value);
    if (!date) return String(value || "—");
    const options = withTime
      ? { year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", timeZone: "Asia/Seoul" }
      : { year: "numeric", month: "short", day: "numeric", timeZone: "Asia/Seoul" };
    return new Intl.DateTimeFormat("ko-KR", options).format(date);
  }

  function apiBase(version) {
    try {
      const configured = version === "v2" && config.apiV2Base
        ? config.apiV2Base
        : config.apiBase || "/api/v1";
      const url = new URL(String(configured), window.location.origin);
      if (!/^https?:$/.test(url.protocol) || url.username || url.password || url.search || url.hash) throw new Error("unsafe API base");
      if (version === "v2" && !config.apiV2Base) {
        if (/\/api\/v1\/?$/.test(url.pathname)) url.pathname = url.pathname.replace(/\/api\/v1\/?$/, "/api/v2");
        else if (/\/api\.php\/api\/v1\/?$/.test(url.pathname)) url.pathname = url.pathname.replace(/\/api\.php\/api\/v1\/?$/, "/api.php/api/v2");
        else url.pathname = `${url.pathname.replace(/\/$/, "")}/api/v2`;
      }
      url.pathname = `${url.pathname.replace(/\/$/, "")}/`;
      return url;
    } catch (_error) {
      return new URL(version === "v2" ? "/api/v2/" : "/api/v1/", window.location.origin);
    }
  }

  const baseUrl = apiBase("v1");
  const v2BaseUrl = apiBase("v2");

  function endpoint(path, params, version) {
    const url = new URL(String(path || "").replace(/^\//, ""), version === "v2" ? v2BaseUrl : baseUrl);
    Object.entries(params || {}).forEach(([name, value]) => {
      if (value !== undefined && value !== null && String(value) !== "") url.searchParams.set(name, String(value));
    });
    return url;
  }

  document.querySelectorAll("[data-api-link]").forEach((anchor) => {
    const version = anchor.dataset.apiVersion === "v2" ? "v2" : "v1";
    anchor.href = endpoint(anchor.dataset.apiLink, {}, version).toString();
  });

  async function fetchPayload(url, options) {
    const settings = options || {};
    const headers = settings.body ? { "Content-Type": "application/json", Accept: "application/json" } : { Accept: "application/json" };
    const token = previewToken();
    if (token) headers.Authorization = `Bearer ${token}`;
    let response;
    try {
      response = await fetch(url, {
        method: settings.method || "GET",
        headers,
        body: settings.body ? JSON.stringify(settings.body) : undefined,
        credentials: "omit",
        cache: settings.method === "POST" ? "no-store" : "no-cache",
        referrerPolicy: "no-referrer",
        signal: settings.signal
      });
    } catch (error) {
      if (error && error.name === "AbortError") throw error;
      throw new ApiError("API request failed", "network_error", 0);
    }
    const contentType = response.headers.get("content-type") || "";
    let payload = null;
    if (contentType.includes("application/json")) {
      try {
        payload = await response.json();
      } catch (_error) {
        throw new ApiError("API returned invalid JSON", "invalid_json", response.status);
      }
    }
    if (!response.ok || !payload || payload.ok === false) {
      const code = payload && payload.error ? String(payload.error) : `http_${response.status}`;
      const apiVersion = payload && typeof payload.api_version === "string" ? payload.api_version : "";
      throw new ApiError("API request failed", code, response.status, apiVersion);
    }
    return payload;
  }

  async function request(path, options) {
    const settings = options || {};
    return fetchPayload(endpoint(path, settings.params), settings);
  }

  function shouldUseV1Fallback(error) {
    if (!error || error.name === "AbortError") return false;
    if (error.code === "unsupported_v2_contract") return true;
    if (Number(error.status) === 200 && ["invalid_json", "http_200"].includes(String(error.code))) return true;
    if ([405, 410, 501].includes(Number(error.status))) return true;
    return Number(error.status) === 404
      && (!error.apiVersion || ["not_found", "endpoint_not_found"].includes(String(error.code)));
  }

  function isRecord(value) {
    return Boolean(value) && typeof value === "object" && !Array.isArray(value);
  }

  function isRecordArray(value) {
    return Array.isArray(value) && value.every(isRecord);
  }

  function isUtcDateTime(value) {
    return typeof value === "string" && UTC_DATE_TIME_PATTERN.test(value);
  }

  function isNullableUtcDateTime(value) {
    return value === null || isUtcDateTime(value);
  }

  function isCoverageNotice(value) {
    if (value === null) return true;
    return (
      isRecord(value)
      && ["coverage_unavailable", "partial_coverage"].includes(String(value.reason || ""))
      && ["blocking", "warning"].includes(String(value.scope || ""))
      && typeof value.brief_id === "string"
      && isNullableUtcDateTime(value.cutoff_at)
      && isNullableUtcDateTime(value.published_at)
      && Array.isArray(value.unavailable_countries)
      && value.unavailable_countries.every((country) => typeof country === "string")
      && Array.isArray(value.unavailable_sources)
      && value.unavailable_sources.every((source) => typeof source === "string")
    );
  }

  function isV2EventRecord(value) {
    return (
      isRecord(value)
      && typeof value.event_id === "string"
      && TITLE_PROVENANCE_VALUES.has(String(value.title_provenance || ""))
      && isNullableUtcDateTime(value.occurred_at)
      && isNullableUtcDateTime(value.filed_at)
      && isNullableUtcDateTime(value.first_observed_at)
      && isUtcDateTime(value.updated_at)
      && isNullableUtcDateTime(value.deadline_at)
      && (
        value.actor_role === undefined
        || value.actor_role === null
        || typeof value.actor_role === "string"
      )
    );
  }

  function invalidV2Contract(path) {
    return new ApiError(`Unsupported API v2 contract for ${path}`, "unsupported_v2_contract", 200, "");
  }

  function isV2PageMeta(value) {
    return (
      isRecord(value)
      && Number.isInteger(value.offset)
      && value.offset >= 0
      && value.offset <= 10000
      && (value.page === null
        || (Number.isInteger(value.page) && value.page >= 1 && value.page <= 100))
      && Number.isInteger(value.limit)
      && value.limit >= 1
      && value.limit <= 100
      && Number.isInteger(value.returned)
      && value.returned >= 0
      && value.returned <= value.limit
      && typeof value.has_more === "boolean"
      && (value.next_page === null
        || (Number.isInteger(value.next_page) && value.next_page >= 2 && value.next_page <= 100))
      && (value.next_offset === null
        || (Number.isInteger(value.next_offset)
          && value.next_offset === value.offset + value.returned
          && value.next_offset <= 10000))
      && typeof value.continuation_limited === "boolean"
      && (!value.has_more || value.next_offset !== null || value.continuation_limited)
      && (!value.continuation_limited || (value.has_more && value.next_offset === null))
      && (value.has_more || (value.next_page === null && value.next_offset === null))
    );
  }

  function validateV2Payload(path, payload) {
    if (!isRecord(payload) || payload.api_version !== "v2" || payload.ok !== true || !isRecord(payload.data)) {
      throw invalidV2Contract(path);
    }
    const data = payload.data;
    if (path === "/briefs/latest") {
      if (
        typeof data.edition !== "string"
        || !isNullableUtcDateTime(data.cutoff_at)
        || !isNullableUtcDateTime(data.published_at)
        || !isNullableUtcDateTime(data.last_updated_at)
        || typeof data.stale !== "boolean"
        || !["", "no_approved_brief", "no_confirmed_material_events", "coverage_unavailable"].includes(String(data.empty_reason || ""))
        || !isCoverageNotice(data.coverage_notice)
        || !isRecordArray(data.top)
        || !isRecordArray(data.watch)
        || !isRecordArray(data.deadlines)
        || !isRecordArray(data.source_status)
        || !data.source_status.every((source) => (
          isNullableUtcDateTime(source.last_success_at)
          && isNullableUtcDateTime(source.last_checked_at)
        ))
        || ![...data.top, ...data.watch, ...data.deadlines].every(isV2EventRecord)
      ) {
        throw invalidV2Contract(path);
      }
      return payload;
    }
    if (path === "/live" || path === "/events" || path === "/calendar" || path === "/search") {
      if (
        !isRecordArray(data.items)
        || !data.items.every(isV2EventRecord)
        || (path === "/calendar" && !data.items.every((item) => typeof item.deadline_at === "string"))
        || !isV2PageMeta(payload.meta)
        || payload.meta.returned !== data.items.length
      ) {
        throw invalidV2Contract(path);
      }
      return payload;
    }
    if (path === "/sources/status") {
      if (!isRecordArray(data.items) || !isRecord(payload.meta)) throw invalidV2Contract(path);
      return payload;
    }
    if (path === "/issuers") {
      if (
        !isRecordArray(data.items)
        || !data.items.every((item) => typeof item.issuer_id === "string")
        || !isV2PageMeta(payload.meta)
        || payload.meta.returned !== data.items.length
      ) {
        throw invalidV2Contract(path);
      }
      return payload;
    }
    if (/^\/events\/[A-Za-z0-9_.:%-]+$/.test(path)) {
      if (
        !isRecord(data.event)
        || !isV2EventRecord(data.event)
        || !isRecordArray(data.actors)
        || !data.actors.every((actor) => typeof actor.actor_id === "string" && typeof actor.actor_role === "string")
        || !isRecordArray(data.documents)
        || !data.documents.every((documentItem) => (
          isNullableUtcDateTime(documentItem.filed_at)
          && isNullableUtcDateTime(documentItem.published_at)
        ))
        || !isRecordArray(data.observations)
        || !data.observations.every((observation) => (
          isUtcDateTime(observation.first_observed_at)
          && isUtcDateTime(observation.observed_at)
        ))
      ) {
        throw invalidV2Contract(path);
      }
      return payload;
    }
    if (/^\/issuers\/[A-Za-z0-9_.:%-]+$/.test(path)) {
      if (
        !isRecord(data.issuer)
        || typeof data.issuer.issuer_id !== "string"
        || !isRecordArray(data.identifiers)
        || !isRecordArray(data.listings)
        || !isRecordArray(data.events)
        || !data.events.every(isV2EventRecord)
      ) {
        throw invalidV2Contract(path);
      }
      return payload;
    }
    throw invalidV2Contract(path);
  }

  async function terminalRequest(path, options) {
    const settings = options || {};
    try {
      const payload = validateV2Payload(
        path,
        await fetchPayload(endpoint(path, settings.params, "v2"), settings)
      );
      return { payload, version: "v2" };
    } catch (error) {
      if (!settings.fallback || !shouldUseV1Fallback(error)) throw error;
      const payload = await settings.fallback();
      return { payload, version: "v1" };
    }
  }

  function metricRouteTemplate() {
    const raw = window.location.hash.startsWith("#/") ? window.location.hash.slice(1).split("?", 1)[0] : "/today";
    const segments = raw.split("/").filter(Boolean);
    const first = segments[0] || "today";
    const allowed = new Set(["today", "events", "companies", "issuers", "actors", "campaigns", "documents", "calendar", "search", "revisions", "feedback"]);
    if (!allowed.has(first)) return "/not-found";
    if (segments.length > 1 && ["events", "companies", "issuers", "actors", "campaigns", "documents"].includes(first)) return `/${first}/:id`;
    return `/${first}`;
  }

  function installWebVitals() {
    const buildSha = String(config.buildSha || "").toLowerCase();
    if (!/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(buildSha) || typeof PerformanceObserver !== "function") return;
    const routeTemplate = metricRouteTemplate();
    const deviceClass = window.matchMedia("(max-width: 767px)").matches ? "mobile" : "desktop";
    const values = { lcp: null, cls: 0, inp: null };
    const supported = new Set(PerformanceObserver.supportedEntryTypes || []);
    const observed = { lcp: false, cls: false, inp: false };
    const sent = new Set();
    const observers = [];

    const observe = (type, callback, options) => {
      if (!supported.has(type)) return false;
      try {
        const observer = new PerformanceObserver((list) => callback(list.getEntries()));
        observer.observe({ type, buffered: true, ...(options || {}) });
        observers.push({ observer, callback });
        return true;
      } catch (_error) {
        // Unsupported observer options must not affect the public reading experience.
        return false;
      }
    };

    observe("largest-contentful-paint", (entries) => {
      const latest = entries[entries.length - 1];
      if (latest && Number.isFinite(latest.startTime)) {
        values.lcp = latest.startTime;
        observed.lcp = true;
      }
    });
    observed.cls = observe("layout-shift", (entries) => {
      entries.forEach((entry) => {
        if (!entry.hadRecentInput && Number.isFinite(entry.value)) values.cls += entry.value;
      });
    });
    observe("event", (entries) => {
      entries.forEach((entry) => {
        if (entry.interactionId > 0 && Number.isFinite(entry.duration)) {
          values.inp = Math.max(values.inp || 0, entry.duration);
          observed.inp = true;
        }
      });
    }, { durationThreshold: 16 });

    const flush = () => {
      observers.forEach(({ observer, callback }) => {
        const entries = observer.takeRecords();
        if (entries.length) callback(entries);
      });
      Object.entries(values).forEach(([metric, rawValue]) => {
        if (sent.has(metric) || !observed[metric] || !Number.isFinite(rawValue)) return;
        const value = metric === "cls" ? Number(rawValue.toFixed(6)) : Number(rawValue.toFixed(3));
        const headers = { "Content-Type": "application/json", Accept: "application/json" };
        const token = previewToken();
        if (token) headers.Authorization = `Bearer ${token}`;
        sent.add(metric);
        void fetch(endpoint("/metrics/web-vitals"), {
          method: "POST",
          headers,
          body: JSON.stringify({ route_template: routeTemplate, metric, value, device_class: deviceClass, build_sha: buildSha }),
          credentials: "omit",
          cache: "no-store",
          keepalive: true,
          referrerPolicy: "no-referrer"
        }).catch(() => {});
      });
    };

    document.addEventListener("visibilitychange", () => { if (document.visibilityState === "hidden") flush(); });
    window.addEventListener("pagehide", flush, { once: true });
  }

  installWebVitals();

  function validEntityId(value) { return /^[A-Za-z0-9_.:-]{1,96}$/.test(String(value || "")); }
  function validCompanyId(value) { return /^\d{8}$/.test(String(value || "")); }

  function routeLink(text, hash, options) {
    return element("a", {
      text,
      lang: options && options.lang,
      className: options && options.className,
      attrs: { href: hash, ...((options && options.attrs) || {}) },
      dataset: (options && options.dataset) || {}
    });
  }

  function eventRouteLink(event, className) {
    return routeLink(
      event.title || "제목 없음",
      `#/events/${encodeURIComponent(event.event_id || "")}`,
      {
        lang: event.original_language,
        className,
        dataset: { eventDrawer: event.event_id || "" },
        attrs: { "aria-haspopup": "dialog" }
      }
    );
  }

  function issuerOrCompanyId(record) {
    if (record && validEntityId(record.issuer_id)) {
      return { kind: "issuer", id: String(record.issuer_id) };
    }
    if (record && validCompanyId(record.company_id)) {
      return { kind: "company", id: String(record.company_id) };
    }
    return null;
  }

  function issuerOrCompanyName(record) {
    return String(
      (record && (record.issuer_name || record.company_name || record.legal_name))
      || (record && (record.issuer_id || record.company_id))
      || "—"
    );
  }

  function issuerOrCompanyRoute(record) {
    const identity = issuerOrCompanyId(record);
    if (!identity) return "";
    return identity.kind === "issuer"
      ? `#/issuers/${encodeURIComponent(identity.id)}`
      : `#/companies/${encodeURIComponent(identity.id)}`;
  }

  function issuerOrCompanyLink(record, text, options) {
    const route = issuerOrCompanyRoute(record);
    const labelText = text || issuerOrCompanyName(record);
    return route
      ? routeLink(labelText, route, options)
      : element("span", { text: labelText, className: options && options.className });
  }

  function externalLink(text, href, className) {
    let safeHref = "";
    try {
      const url = new URL(String(href || ""), window.location.origin);
      if (/^https?:$/.test(url.protocol)) safeHref = url.toString();
    } catch (_error) {
      safeHref = "";
    }
    if (!safeHref) return element("span", { text, className });
    return element("a", { text, className, attrs: { href: safeHref, target: "_blank", rel: "noopener noreferrer" } });
  }

  function badge(value, group) {
    const key = String(value || "unknown");
    return element("span", { text: label(group, key), className: `badge badge--${key.replace(/[^a-z0-9_-]/gi, "")}` });
  }

  function metadata(items) {
    const row = element("div", { className: "meta-row" });
    items.filter((item) => item && item.value).forEach((item, index) => {
      if (index) row.append(element("span", { text: "·", attrs: { "aria-hidden": "true" } }));
      row.append(item.node || element("span", { text: item.value }));
    });
    return row;
  }

  function pageHeader(eyebrow, title, description, actions, language) {
    const copy = element("div", {}, [
      element("p", { text: eyebrow, className: "eyebrow" }),
      sourceNode("h1", title, language),
      description ? element("p", { text: description, className: "lede" }) : null
    ].filter(Boolean));
    const children = [copy];
    if (actions && actions.length) children.push(element("div", { className: "header-actions" }, actions));
    return element("header", { className: "page-header" }, children);
  }

  function sectionHeading(title, note, id) {
    return element("div", { className: "section-heading" }, [
      element("h2", { text: title, attrs: id ? { id } : {} }),
      note ? element("p", { text: note }) : null
    ].filter(Boolean));
  }

  function emptyState(title, message) {
    return element("div", { className: "empty-state" }, [
      element("h2", { text: title }),
      element("p", { text: message })
    ]);
  }

  function addLoadMore(section, pagination, loadPage) {
    const nextCursor = (value) => {
      if (value && Number.isInteger(value.next_offset) && value.next_offset >= 0) {
        return { kind: "offset", value: value.next_offset };
      }
      if (value && Number.isInteger(value.next_page) && value.next_page > 0) {
        return { kind: "page", value: value.next_page };
      }
      return null;
    };
    let cursor = nextCursor(pagination);
    if (!cursor) return;
    const button = element("button", { text: "더 보기 / Load more", attrs: { type: "button" } });
    button.addEventListener("click", async () => {
      button.disabled = true;
      try {
        const next = await loadPage(cursor.value, cursor.kind);
        cursor = nextCursor(next);
        if (!cursor) button.remove();
      } catch (error) {
        announceError(error);
      } finally {
        if (cursor) button.disabled = false;
      }
    });
    section.append(element("div", { className: "load-more" }, [button]));
  }

  function continuationParams(base, value, kind) {
    const params = { ...base };
    delete params.page;
    delete params.offset;
    params[kind === "offset" ? "offset" : "page"] = value;
    return params;
  }

  const PUBLIC_DOCUMENT_CLASSES = new Set([
    "official_disclosure",
    "company_statement",
    "activist_statement",
    "media_report",
    "editorial_analysis"
  ]);

  function isPublicEvent(event) {
    if (!event || String(event.verification_status || "") === "signal") return false;
    if (event.publication_status && event.publication_status !== "published") return false;
    if (event.review_status && !["approved", "not_required"].includes(event.review_status)) return false;
    return true;
  }

  function isPublicDocument(documentItem) {
    if (!documentItem || !PUBLIC_DOCUMENT_CLASSES.has(String(documentItem.source_class || ""))) return false;
    if (documentItem.publication_status && documentItem.publication_status !== "published") return false;
    return true;
  }

  function isPublicCampaign(campaign) {
    if (!campaign || String(campaign.stage || "") === "initial_signal") return false;
    if (campaign.publication_status && campaign.publication_status !== "published") return false;
    if (campaign.review_status && campaign.review_status !== "approved") return false;
    return true;
  }

  function eventFilterParams(query) {
    const params = { page: 1, limit: 50 };
    const allowed = ["company_id", "actor_id", "event_type", "source_class", "verification_status", "from", "to"];
    allowed.forEach((name) => {
      const value = String(query.get(name) || "").trim();
      if (value) params[name] = value;
    });
    return params;
  }

  function v2EventFilterParams(query) {
    const params = { page: 1, limit: 50 };
    const country = String(query.get("country") || query.get("market") || "").toUpperCase();
    if (MARKETS.has(country) && country !== "GLOBAL") params.country = country;
    const mappings = {
      issuer_id: "issuer_id",
      market_code: "market",
      event_family: "event_family",
      verification_status: "verification_status",
      change_type: "change_type",
      from: "from",
      to: "to"
    };
    Object.entries(mappings).forEach(([queryName, apiName]) => {
      const value = String(query.get(queryName) || "").trim();
      if (value) params[apiName] = value;
    });
    const legacyEventType = String(query.get("event_type") || "").trim();
    if (!params.event_family && legacyEventType) params.event_family = legacyEventType;
    return params;
  }

  function v2EventFilterForm(query, destination) {
    const country = element("select", { attrs: { id: "filter-country", name: "country" } });
    [["", "전체 국가 / All countries"], ...[...MARKETS].filter((value) => value !== "GLOBAL").map((value) => [value, value])]
      .forEach(([value, text]) => {
        const option = element("option", { text, attrs: { value } });
        if (value === String(query.get("country") || query.get("market") || "").toUpperCase()) option.selected = true;
        country.append(option);
      });
    const issuer = element("input", {
      attrs: {
        id: "filter-issuer",
        name: "issuer_id",
        value: query.get("issuer_id") || "",
        maxlength: "96",
        pattern: "[A-Za-z0-9_.:-]{1,96}",
        placeholder: "Global issuer ID"
      }
    });
    const type = element("select", { attrs: { id: "filter-event-family", name: "event_family" } });
    [["", "전체 유형 / All types"], ...Object.entries(labels.eventType)].forEach(([value, text]) => {
      const option = element("option", { text, attrs: { value } });
      if (value === String(query.get("event_family") || query.get("event_type") || "")) option.selected = true;
      type.append(option);
    });
    const status = element("select", { attrs: { id: "filter-v2-status", name: "verification_status" } });
    [["", "전체 상태 / All statuses"], ...Object.entries(labels.verification).filter(([value]) => value !== "signal")]
      .forEach(([value, text]) => {
        const option = element("option", { text, attrs: { value } });
        if (value === String(query.get("verification_status") || "")) option.selected = true;
        status.append(option);
      });
    const change = element("input", {
      attrs: {
        id: "filter-change-type",
        name: "change_type",
        value: query.get("change_type") || "",
        maxlength: "24",
        pattern: "[a-z][a-z0-9_]{1,23}",
        placeholder: "new / updated / corrected"
      }
    });
    const from = element("input", { attrs: { id: "filter-v2-from", type: "date", name: "from", value: query.get("from") || "" } });
    const to = element("input", { attrs: { id: "filter-v2-to", type: "date", name: "to", value: query.get("to") || "" } });
    const form = element("form", { className: "filter-form", attrs: { "aria-label": "글로벌 사건 필터 / Global event filters" } }, [
      element("div", { className: "filter-grid" }, [
        element("div", { className: "field" }, [element("label", { text: "국가 / Country", attrs: { for: "filter-country" } }), country]),
        element("div", { className: "field" }, [element("label", { text: "발행사 / Issuer", attrs: { for: "filter-issuer" } }), issuer]),
        element("div", { className: "field" }, [element("label", { text: "사건 유형 / Event family", attrs: { for: "filter-event-family" } }), type]),
        element("div", { className: "field" }, [element("label", { text: "확인 상태 / Status", attrs: { for: "filter-v2-status" } }), status]),
        element("div", { className: "field" }, [element("label", { text: "변경 유형 / Change", attrs: { for: "filter-change-type" } }), change]),
        element("div", { className: "field" }, [element("label", { text: "시작 / From", attrs: { for: "filter-v2-from" } }), from]),
        element("div", { className: "field" }, [element("label", { text: "종료 / To", attrs: { for: "filter-v2-to" } }), to])
      ]),
      element("div", { className: "form-actions" }, [
        element("button", { text: "필터 적용 / Apply", attrs: { type: "submit" } }),
        routeLink("초기화 / Reset", destination, { className: "text-link" })
      ])
    ]);
    form.addEventListener("submit", (event) => {
      event.preventDefault();
      if (!form.reportValidity()) return;
      const values = new URLSearchParams();
      new FormData(form).forEach((value, name) => {
        const text = String(value).trim();
        if (text) values.set(name, text);
      });
      window.location.hash = `${destination}${values.toString() ? `?${values.toString()}` : ""}`;
    });
    return form;
  }

  function eventFilterForm(query, destination) {
    const company = element("input", { attrs: { id: "filter-company", name: "company_id", value: query.get("company_id") || "", inputmode: "numeric", maxlength: "8", pattern: "[0-9]{8}", placeholder: "DART corp_code" } });
    const actor = element("input", { attrs: { id: "filter-actor", name: "actor_id", value: query.get("actor_id") || "", maxlength: "64", pattern: "[A-Za-z0-9_.:-]{1,64}", placeholder: "Actor ID" } });
    const type = element("select", { attrs: { id: "filter-event-type", name: "event_type" } });
    [["", "전체 유형 / All types"], ...Object.entries(labels.eventType)].forEach(([value, text]) => {
      const option = element("option", { text, attrs: { value } });
      if (value === String(query.get("event_type") || "")) option.selected = true;
      type.append(option);
    });
    const source = element("select", { attrs: { id: "filter-source", name: "source_class" } });
    [["", "전체 근거 / All evidence"], ...Object.entries(labels.sourceClass)].forEach(([value, text]) => {
      const option = element("option", { text, attrs: { value } });
      if (value === String(query.get("source_class") || "")) option.selected = true;
      source.append(option);
    });
    const status = element("select", { attrs: { id: "filter-status", name: "verification_status" } });
    [["", "전체 상태 / All statuses"], ...Object.entries(labels.verification).filter(([value]) => value !== "signal")].forEach(([value, text]) => {
      const option = element("option", { text, attrs: { value } });
      if (value === String(query.get("verification_status") || "")) option.selected = true;
      status.append(option);
    });
    const from = element("input", { attrs: { id: "filter-from", type: "date", name: "from", value: query.get("from") || "" } });
    const to = element("input", { attrs: { id: "filter-to", type: "date", name: "to", value: query.get("to") || "" } });
    const form = element("form", { className: "filter-form", attrs: { "aria-label": "사건 필터 / Event filters" } }, [
      element("div", { className: "filter-grid" }, [
        element("div", { className: "field" }, [element("label", { text: "회사 / Company", attrs: { for: "filter-company" } }), company]),
        element("div", { className: "field" }, [element("label", { text: "당사자 / Actor", attrs: { for: "filter-actor" } }), actor]),
        element("div", { className: "field" }, [element("label", { text: "사건 유형 / Event type", attrs: { for: "filter-event-type" } }), type]),
        element("div", { className: "field" }, [element("label", { text: "근거 / Evidence", attrs: { for: "filter-source" } }), source]),
        element("div", { className: "field" }, [element("label", { text: "확인 상태 / Status", attrs: { for: "filter-status" } }), status]),
        element("div", { className: "field" }, [element("label", { text: "시작 / From", attrs: { for: "filter-from" } }), from]),
        element("div", { className: "field" }, [element("label", { text: "종료 / To", attrs: { for: "filter-to" } }), to])
      ]),
      element("div", { className: "form-actions" }, [
        element("button", { text: "필터 적용 / Apply", attrs: { type: "submit" } }),
        routeLink("초기화 / Reset", destination, { className: "text-link" })
      ])
    ]);
    form.addEventListener("submit", (event) => {
      event.preventDefault();
      if (!form.reportValidity()) return;
      const values = new URLSearchParams();
      new FormData(form).forEach((value, name) => {
        const text = String(value).trim();
        if (text) values.set(name, text);
      });
      window.location.hash = `${destination}${values.toString() ? `?${values.toString()}` : ""}`;
    });
    return form;
  }

  function currentMarket(query) {
    const value = String(query.get("market") || "GLOBAL").toUpperCase();
    return MARKETS.has(value) ? value : "GLOBAL";
  }

  function marketCoverageDescription(market) {
    const descriptions = {
      GLOBAL: "KR·US 시장 전체 / market-wide · CA·AU 링크 전용 / link-only · JP·GB 링크 전용·현재 수집 불가 / coverage unavailable",
      KR: "OpenDART 시장 전체 / Market-wide",
      US: "SEC EDGAR 시장 전체 / Market-wide",
      JP: "링크 전용·현재 수집 불가 / Link-only · coverage unavailable · 공개 준비 아님 / public_ready=false",
      GB: "링크 전용·현재 수집 불가 / Link-only · coverage unavailable · 공개 준비 아님 / public_ready=false",
      CA: "링크 전용·수동 메타데이터 / Link-only · manual metadata · SEDAR+ 전문 제외",
      AU: "ASIC 링크 전용·수동 메타데이터 / Link-only · manual metadata · ASX 전문 제외"
    };
    return `Production Alpha · ${descriptions[market] || descriptions.GLOBAL}`;
  }

  function marketForEvent(event) {
    const explicit = String(
      event.country
      || event.country_code
      || event.company_country_code
      || event.exchange_country_code
      || ""
    ).toUpperCase();
    if (MARKETS.has(explicit) && explicit !== "GLOBAL") return explicit;
    const market = String(event.market || event.market_code || "").toUpperCase();
    if (/KOSPI|KOSDAQ|KONEX|KRX/.test(market)) return "KR";
    if (/NYSE|NASDAQ|AMEX|OTC|US/.test(market)) return "US";
    if (/TSE|TOKYO|JP/.test(market)) return "JP";
    if (/LSE|LONDON|GB|UK/.test(market)) return "GB";
    if (/TSX|CSE|CA/.test(market)) return "CA";
    if (/ASX|AU/.test(market)) return "AU";
    return /^\d{8}$/.test(String(event.company_id || event.issuer_id || "")) ? "KR" : "";
  }

  function eventCoverageMode(event) {
    const policy = ALPHA_MARKET_SCOPE[marketForEvent(event)];
    if (policy && policy.public_ready === false) return policy.coverage_mode;
    const explicit = String(event.coverage_mode || "").trim();
    if (explicit) return explicit;
    const inferred = policy && policy.coverage_mode;
    if (inferred) return inferred;
    if (Number(event.official_evidence_count || 0) > 0 || event.verification_status === "official") return "official";
    return Number(event.media_count || 0) > 0 ? "media_only" : "unavailable";
  }

  function canonicalEventFamily(value, verificationStatus, changeType) {
    const key = String(value || "").trim();
    if (CANONICAL_EVENT_FAMILIES.has(key)) return key;
    if (
      ["corrected", "withdrawn"].includes(String(verificationStatus || ""))
      || ["corrected", "withdrawn"].includes(String(changeType || ""))
    ) {
      return "correction_and_withdrawal";
    }
    return LEGACY_EVENT_FAMILY_MAP[key] || "";
  }

  function normalizeEvent(item) {
    const event = item && typeof item === "object" ? item : {};
    const eventFamily = canonicalEventFamily(
      event.event_family || event.event_type || event.category,
      event.verification_status,
      event.change_type
    );
    return {
      ...event,
      event_id: event.event_id || (event.item_type === "event" ? event.entity_id : "") || event.id || "",
      issuer_id: event.issuer_id || "",
      company_id: event.company_id || "",
      company_name: event.company_name || event.issuer_name || "",
      issuer_name: event.issuer_name || "",
      event_type: eventFamily,
      event_family: eventFamily,
      title: String(event.title || ""),
      title_provenance: TITLE_PROVENANCE_VALUES.has(String(event.title_provenance || ""))
        ? String(event.title_provenance)
        : "unknown",
      original_language: event.original_language || "",
      occurred_at: event.occurred_at || event.filed_at || event.first_observed_at || event.updated_at || "",
      verification_status: event.verification_status || "unverified",
      importance: event.importance || "unknown",
      market: event.market || "",
      country: event.country || event.country_code || "",
      change_summary: event.change_summary || event.summary || "",
      current_status: event.current_status || event.status || "",
      actor_name: event.actor_name || "",
      actor_role: event.actor_role || "",
      filed_at: event.filed_at || "",
      first_observed_at: event.first_observed_at || "",
      updated_at: event.updated_at || "",
      deadline_at: event.deadline_at || event.scheduled_at || "",
      official_evidence_count: Number(
        event.official_evidence_count
        || (event.has_official_evidence === true ? 1 : 0)
      ),
      media_count: Number(event.media_count || 0),
      coverage_mode: event.coverage_mode || "",
      source_url: event.source_url || ""
    };
  }

  function terminalFilterParams(query) {
    const params = {};
    const eventType = String(query.get("event_type") || "").trim();
    const verification = String(query.get("verification_status") || "").trim();
    const q = String(query.get("q") || "").trim();
    if (eventType) params.event_family = eventType;
    if (verification) params.verification_status = verification;
    if (q) params.q = q;
    ["from", "to"].forEach((name) => {
      const value = String(query.get(name) || "").trim();
      if (/^\d{4}-\d{2}-\d{2}$/.test(value)) params[name] = value;
    });
    const market = currentMarket(query);
    if (market !== "GLOBAL") params.country = market;
    return params;
  }

  function matchesTerminalFilters(event, query) {
    const market = currentMarket(query);
    if (market !== "GLOBAL" && marketForEvent(event) !== market) return false;
    const eventType = String(query.get("event_type") || "");
    if (eventType && String(event.event_type || "") !== eventType) return false;
    const verification = String(query.get("verification_status") || "");
    if (verification && String(event.verification_status || "") !== verification) return false;
    const eventDate = normalizedDate(event.occurred_at);
    const from = String(query.get("from") || "");
    const to = String(query.get("to") || "");
    const hasFrom = /^\d{4}-\d{2}-\d{2}$/.test(from);
    const hasTo = /^\d{4}-\d{2}-\d{2}$/.test(to);
    if ((hasFrom || hasTo) && !eventDate) return false;
    if (eventDate && hasFrom) {
      const fromDate = normalizedDate(`${from}T00:00:00Z`);
      if (fromDate && eventDate < fromDate) return false;
    }
    if (eventDate && hasTo) {
      const toDate = normalizedDate(`${to}T23:59:59.999Z`);
      if (toDate && eventDate > toDate) return false;
    }
    const q = String(query.get("q") || "").trim().toLocaleLowerCase();
    if (q) {
      const haystack = [
        event.title,
        event.company_name,
        event.ticker,
        event.actor_name,
        event.change_summary
      ].join(" ").toLocaleLowerCase();
      if (!haystack.includes(q)) return false;
    }
    return true;
  }

  function syncMarketTabs(query) {
    const active = currentMarket(query);
    document.querySelectorAll("[data-market]").forEach((link) => {
      const market = String(link.dataset.market || "GLOBAL");
      const next = new URLSearchParams(query);
      next.set("market", market);
      link.href = `#/today?${next.toString()}`;
      if (market === active) link.setAttribute("aria-current", "page");
      else link.removeAttribute("aria-current");
    });
  }

  function terminalFilterForm(query) {
    const q = element("input", {
      attrs: {
        id: "terminal-filter-q",
        name: "q",
        type: "search",
        value: query.get("q") || "",
        maxlength: "100",
        placeholder: "기업·당사자·제목"
      }
    });
    const type = element("select", { attrs: { id: "terminal-filter-type", name: "event_type" } });
    [["", "전체 사건"], ...Object.entries(labels.eventType)].forEach(([value, text]) => {
      const option = element("option", { text, attrs: { value } });
      if (value === String(query.get("event_type") || "")) option.selected = true;
      type.append(option);
    });
    const verification = element("select", { attrs: { id: "terminal-filter-verification", name: "verification_status" } });
    [
      ["", "전체 근거"],
      ["official", "공식 근거"],
      ["confirmed", "복수 확인"],
      ["corroborated", "추가 근거"],
      ["corrected", "정정"]
    ].forEach(([value, text]) => {
      const option = element("option", { text, attrs: { value } });
      if (value === String(query.get("verification_status") || "")) option.selected = true;
      verification.append(option);
    });
    const from = element("input", {
      attrs: {
        id: "terminal-filter-from",
        name: "from",
        type: "date",
        value: query.get("from") || ""
      }
    });
    const to = element("input", {
      attrs: {
        id: "terminal-filter-to",
        name: "to",
        type: "date",
        value: query.get("to") || ""
      }
    });
    const validateDateRange = () => {
      const invalid = Boolean(from.value && to.value && from.value > to.value);
      to.setCustomValidity(invalid ? "종료일은 시작일보다 빠를 수 없습니다. / To must be on or after From." : "");
      return !invalid;
    };
    from.addEventListener("input", validateDateRange);
    to.addEventListener("input", validateDateRange);
    const form = element("form", {
      className: "terminal-filter-form",
      attrs: { "aria-label": "오늘의 사건 필터 / Today event filters" }
    }, [
      element("div", { className: "field" }, [
        element("label", { text: "검색 / Search", attrs: { for: "terminal-filter-q" } }),
        q
      ]),
      element("div", { className: "field" }, [
        element("label", { text: "사건 유형 / Event", attrs: { for: "terminal-filter-type" } }),
        type
      ]),
      element("div", { className: "field" }, [
        element("label", { text: "근거 상태 / Evidence", attrs: { for: "terminal-filter-verification" } }),
        verification
      ]),
      element("div", { className: "field" }, [
        element("label", { text: "시작 / From", attrs: { for: "terminal-filter-from" } }),
        from
      ]),
      element("div", { className: "field" }, [
        element("label", { text: "종료 / To", attrs: { for: "terminal-filter-to" } }),
        to
      ]),
      element("div", { className: "form-actions" }, [
        element("button", { text: "적용", attrs: { type: "submit" } }),
        routeLink("초기화", `#/today?market=${currentMarket(query)}`, { className: "text-link" })
      ])
    ]);
    form.addEventListener("submit", (event) => {
      event.preventDefault();
      validateDateRange();
      if (!form.reportValidity()) return;
      const values = new URLSearchParams();
      values.set("market", currentMarket(query));
      new FormData(form).forEach((value, name) => {
        const text = String(value).trim();
        if (text) values.set(name, text);
      });
      window.location.hash = `#/today?${values.toString()}`;
    });
    return form;
  }

  function mobileTerminalTabs() {
    const targets = [
      ["오늘", "top-title"],
      ["Live", "live-title"],
      ["Watch", "mobile-watch-title"],
      ["기한", "mobile-deadline-title"]
    ];
    return element("nav", {
      className: "mobile-terminal-tabs",
      attrs: { "aria-label": "오늘의 데이터 구간 / Daily terminal sections" }
    }, [
      ...targets.map(([text, target]) => element("button", {
        text,
        attrs: { type: "button", "data-scroll-target": target }
      })),
      element("button", {
        text: "필터",
        className: "mobile-filter-toggle",
        attrs: {
          type: "button",
          "aria-controls": "terminal-filters",
          "aria-expanded": "false"
        }
      })
    ]);
  }

  function closeTerminalFilterSheet({ restoreFocus = true } = {}) {
    const panel = document.getElementById("terminal-filters");
    const trigger = document.querySelector(".mobile-filter-toggle");
    if (!panel || !panel.classList.contains("is-open")) return;
    panel.classList.remove("is-open");
    panel.setAttribute("aria-hidden", "true");
    panel.removeAttribute("role");
    panel.removeAttribute("aria-modal");
    document.body.classList.remove("filter-sheet-open");
    if (trigger) trigger.setAttribute("aria-expanded", "false");
    if (restoreFocus && filterSheetTrigger instanceof HTMLElement && filterSheetTrigger.isConnected) {
      filterSheetTrigger.focus();
    }
    filterSheetTrigger = null;
  }

  function openTerminalFilterSheet(trigger) {
    const panel = document.getElementById("terminal-filters");
    if (!panel || !window.matchMedia("(max-width: 680px)").matches) return;
    filterSheetTrigger = trigger instanceof HTMLElement ? trigger : null;
    panel.classList.add("is-open");
    panel.setAttribute("aria-hidden", "false");
    panel.setAttribute("role", "dialog");
    panel.setAttribute("aria-modal", "true");
    document.body.classList.add("filter-sheet-open");
    if (trigger) trigger.setAttribute("aria-expanded", "true");
    const firstControl = panel.querySelector("input, select, button, a[href]");
    if (firstControl instanceof HTMLElement) firstControl.focus({ preventScroll: true });
  }

  function installTodayTerminalControls() {
    const panel = document.getElementById("terminal-filters");
    if (panel) {
      if (window.matchMedia("(max-width: 680px)").matches) panel.setAttribute("aria-hidden", "true");
      else {
        panel.removeAttribute("aria-hidden");
        panel.removeAttribute("role");
        panel.removeAttribute("aria-modal");
      }
    }
    document.querySelectorAll("[data-scroll-target]").forEach((button) => {
      button.addEventListener("click", () => {
        const target = document.getElementById(String(button.dataset.scrollTarget || ""));
        if (target) target.scrollIntoView({ block: "start", behavior: "smooth" });
      });
    });
    const toggle = document.querySelector(".mobile-filter-toggle");
    if (toggle) toggle.addEventListener("click", () => openTerminalFilterSheet(toggle));
    document.querySelectorAll("[data-filter-sheet-close]").forEach((button) => {
      button.addEventListener("click", () => closeTerminalFilterSheet());
    });
  }

  function sourceCoverageBadge(event) {
    const official = Math.max(0, Number(event.official_evidence_count || 0));
    const media = Math.max(0, Number(event.media_count || 0));
    const coverageMode = eventCoverageMode(event);
    const parts = [label("coverageMode", coverageMode)];
    if (official) parts.push(`공식 ${official}`);
    else if (event.verification_status === "official") parts.push("공식 근거");
    if (media) parts.push(`보도 ${media}`);
    return element("span", {
      text: parts.join(" · "),
      className: "coverage-badge",
      dataset: { coverage: coverageMode },
      attrs: { "aria-label": `소스 커버리지: ${parts.join(", ")}` }
    });
  }

  function eventActorSummary(event) {
    const actorName = String(event.actor_name || "").trim();
    if (!actorName) return "";
    const actorRole = String(event.actor_role || "").trim();
    return actorRole
      ? `${actorName} · ${label("actorRole", actorRole)}`
      : actorName;
  }

  function eventTimestampMetadata(event, withTime = true) {
    return [
      event.filed_at ? `접수 / Filed ${formatDate(event.filed_at, withTime)}` : "",
      event.first_observed_at ? `최초 관측 / First seen ${formatDate(event.first_observed_at, withTime)}` : "",
      event.updated_at ? `갱신 / Updated ${formatDate(event.updated_at, withTime)}` : ""
    ].filter(Boolean);
  }

  function terminalEvent(event, rank, mobileCoverageContext = null) {
    const eventLink = eventRouteLink(event);
    const market = marketForEvent(event) || event.market || "—";
    return element("li", {
      className: "terminal-event",
      dataset: { importance: event.importance || "unknown", eventId: event.event_id || "" }
    }, [
      element("div", {
        text: String(rank).padStart(2, "0"),
        className: "terminal-event__rank",
        attrs: { "aria-label": `${rank}위` }
      }),
      element("div", { className: "terminal-event__body" }, [
        element("div", { className: "terminal-event__overline" }, [
          issuerOrCompanyLink(
            event,
            issuerOrCompanyName(event),
            { className: "terminal-event__company" }
          ),
          event.ticker ? element("span", { text: `${event.ticker} · ${market}` }) : element("span", { text: market }),
          element("span", { text: label("eventType", event.event_type) })
        ]),
        element("h3", {}, [eventLink]),
        event.change_summary
          ? sourceNode("p", event.change_summary, event.original_language, "terminal-event__summary")
          : null,
        element("div", { className: "terminal-event__footer" }, [
          sourceCoverageBadge(event),
          element("span", { text: label("titleProvenance", event.title_provenance) }),
          eventActorSummary(event) ? element("span", { text: `당사자 / Actor ${eventActorSummary(event)}` }) : null,
          event.current_status ? element("span", { text: event.current_status }) : null,
          event.deadline_at ? element("span", { text: `기한 ${formatDate(event.deadline_at, false)}` }) : null,
          ...eventTimestampMetadata(event).map((text) => element("span", { text }))
        ].filter(Boolean))
      ].filter(Boolean)),
      mobileCoverageContext
    ].filter(Boolean));
  }

  function liveEventRow(event) {
    return element("li", { className: "live-row" }, [
      element("time", {
        text: formatDate(event.updated_at || event.occurred_at, true),
        attrs: { datetime: event.updated_at || event.occurred_at || "" }
      }),
      element("div", {}, [
        element("h3", {}, [eventRouteLink(event)]),
        element("p", {
          text: [
            issuerOrCompanyName(event),
            label("eventType", event.event_type),
            label("titleProvenance", event.title_provenance),
            eventActorSummary(event)
          ].filter(Boolean).join(" · ")
        }),
        eventTimestampMetadata(event).length
          ? element("p", { text: eventTimestampMetadata(event).join(" · "), className: "event-timestamps" })
          : null
      ]),
      sourceCoverageBadge(event)
    ]);
  }

  function deadlineItem(event) {
    return element("li", { className: "deadline-item" }, [
      element("time", {
        text: formatDate(event.deadline_at || event.scheduled_at, false),
        attrs: { datetime: event.deadline_at || event.scheduled_at || "" }
      }),
      element("div", {}, [
        event.event_id
          ? eventRouteLink(event)
          : sourceNode("strong", event.title || "기한", event.original_language),
        element("p", { text: `${issuerOrCompanyName(event)} · ${label("eventType", event.event_type)}` })
      ])
    ]);
  }

  function publicSourceState(source) {
    const policy = ALPHA_MARKET_SCOPE[String(source.country || "").toUpperCase()];
    if (policy && policy.public_ready === false) {
      return { status: policy.public_status, ready: false };
    }
    const hasPublicContract = typeof source.public_status === "string"
      || typeof source.public_ready === "boolean";
    const status = String(
      hasPublicContract ? source.public_status || "unknown" : source.status || "unknown"
    ).toLowerCase();
    const ready = hasPublicContract
      ? source.public_ready === true
      : source.fresh === true && ["healthy", "ok", "active"].includes(status);
    return { status, ready };
  }

  function sourceStatusItem(source) {
    const publicState = publicSourceState(source);
    const status = publicState.status;
    const policy = ALPHA_MARKET_SCOPE[String(source.country || "").toUpperCase()];
    const publicNote = typeof source.public_note === "string"
      ? source.public_note.trim()
      : "";
    const coverage = label(
      "coverageMode",
      policy && policy.public_ready === false
        ? policy.coverage_mode
        : source.coverage_mode || "unavailable"
    );
    const lag = source.lag_minutes === null || source.lag_minutes === undefined || source.lag_minutes === ""
      ? Number.NaN
      : Number(source.lag_minutes);
    const detail = Number.isFinite(lag)
      ? `지연 ${Math.max(0, Math.round(lag))}분`
      : `확인 ${formatDate(source.last_checked_at || source.last_success_at, true)}`;
    return element("li", { className: "source-status-item", dataset: { status } }, [
      element("span", { className: "status-dot", attrs: { "aria-hidden": "true" } }),
      element("div", {}, [
        sourceNode("strong", source.source_name || source.connector_id || "공식 소스", source.original_language),
        element("p", { text: `${source.country || "GLOBAL"} · ${coverage} · ${status} · ${detail}` }),
        publicNote ? element("p", { text: publicNote, className: "source-public-note" }) : null
      ])
    ]);
  }

  function updateSourceCoverage(sources, events, market = "GLOBAL") {
    if (!globalCoverage) return;
    const rows = Array.isArray(sources) ? sources : [];
    const publicStates = rows.map(publicSourceState);
    const healthy = publicStates.filter((item) => item.ready).length;
    const linkOnly = rows.filter((item) => item.coverage_mode === "link-only").length;
    const down = publicStates.filter((item) => [
      "down", "error", "failed", "blocked_rights", "redistribution_blocked",
      "excluded_source", "inactive", "stale"
    ].includes(item.status)).length;
    if (rows.length && market === "GLOBAL") {
      const countryRows = (country) => rows.filter((item) => (
        String(item.country || "").toUpperCase() === country
      ));
      const requiredReady = REQUIRED_ALPHA_MARKETS.filter((country) => (
        countryRows(country).some((item) => publicSourceState(item).ready)
      ));
      const requiredDown = REQUIRED_ALPHA_MARKETS.filter((country) => (
        countryRows(country).some((item) => [
          "down", "error", "failed", "blocked_rights", "redistribution_blocked",
          "excluded_source", "inactive", "stale"
        ].includes(publicSourceState(item).status))
      ));
      const optionalReady = OPTIONAL_ALPHA_MARKETS.filter((country) => (
        countryRows(country).some((item) => publicSourceState(item).ready)
      ));
      const optionalUnavailable = OPTIONAL_ALPHA_MARKETS.filter((country) => !optionalReady.includes(country));
      globalCoverage.lastChild.textContent = [
        `필수 ${requiredReady.length}/${REQUIRED_ALPHA_MARKETS.length} 정상`,
        `선택 ${optionalReady.length}/${OPTIONAL_ALPHA_MARKETS.length} 제공`,
        optionalUnavailable.length ? `${optionalUnavailable.join("·")} 미지원` : "",
        linkOnly ? `링크 전용 ${linkOnly}` : ""
      ].filter(Boolean).join(" · ");
      globalCoverage.dataset.health = requiredDown.length
        ? "down"
        : requiredReady.length < REQUIRED_ALPHA_MARKETS.length ? "degraded" : "healthy";
      return;
    }
    if (rows.length) {
      globalCoverage.lastChild.textContent = `${healthy}/${rows.length} 소스 정상${linkOnly ? ` · 링크 전용 ${linkOnly}` : ""}`;
      globalCoverage.dataset.health = down ? "down" : healthy < rows.length ? "degraded" : "healthy";
      return;
    }
    const visible = Array.isArray(events) ? events : [];
    const official = visible.filter((item) => Number(item.official_evidence_count || 0) > 0 || item.verification_status === "official").length;
    globalCoverage.lastChild.textContent = `${official}/${visible.length || 0} 공식 근거 연결`;
    globalCoverage.dataset.health = visible.length && official < visible.length ? "degraded" : "healthy";
  }

  function terminalEmpty(message) {
    return element("p", { text: message, className: "terminal-empty" });
  }

  function eventCard(event, rank) {
    const titleLink = routeLink(event.title || "제목 없음", `#/events/${encodeURIComponent(event.event_id || "")}`, { lang: event.original_language });
    const company = issuerOrCompanyLink(event);
    return element("article", {
      className: "event-card",
      dataset: { importance: event.importance || "unknown" }
    }, [
      element("div", { className: "event-card__top" }, [
        rank ? element("span", { text: String(rank).padStart(2, "0"), className: "rank", attrs: { "aria-label": `${rank}위` } }) : element("span"),
        element("div", { className: "badge-row" }, [badge(event.importance, "importance"), badge(event.verification_status, "verification")])
      ]),
      element("h3", {}, [titleLink]),
      metadata([
        { node: company, value: issuerOrCompanyName(event) },
        { value: label("eventType", event.event_type) },
        eventActorSummary(event) ? { value: `당사자 / Actor ${eventActorSummary(event)}` } : null,
        { value: label("coverageMode", eventCoverageMode(event)) },
        { value: label("titleProvenance", event.title_provenance) },
        { value: formatDate(event.occurred_at, false) },
        ...eventTimestampMetadata(event, false).map((value) => ({ value })),
        event.deadline_at ? { value: `기한 / Deadline ${formatDate(event.deadline_at, false)}` } : null
      ])
    ]);
  }

  function compactEvent(event) {
    const actor = eventActorSummary(event);
    return element("li", { className: "compact-item" }, [
      routeLink(event.title || "제목 없음", `#/events/${encodeURIComponent(event.event_id || "")}`, { lang: event.original_language }),
      element("p", {
        text: [
          issuerOrCompanyName(event),
          actor ? `당사자 / Actor ${actor}` : "",
          label("verification", event.verification_status),
          label("titleProvenance", event.title_provenance),
          ...eventTimestampMetadata(event, false)
        ].filter(Boolean).join(" · ")
      })
    ]);
  }

  function archiveEvent(event) {
    return element("article", { className: "archive-row" }, [
      element("h3", {}, [routeLink(event.title || "제목 없음", `#/events/${encodeURIComponent(event.event_id || "")}`, { lang: event.original_language })]),
      metadata([
        { node: issuerOrCompanyLink(event), value: issuerOrCompanyName(event) },
        { value: label("eventType", event.event_type) },
        eventActorSummary(event) ? { value: `당사자 / Actor ${eventActorSummary(event)}` } : null,
        { value: label("verification", event.verification_status) },
        { value: label("coverageMode", eventCoverageMode(event)) },
        { value: label("titleProvenance", event.title_provenance) },
        { value: formatDate(event.occurred_at, false) },
        ...eventTimestampMetadata(event, false).map((value) => ({ value }))
      ])
    ]);
  }

  async function renderToday(query, signal) {
    const market = currentMarket(query);
    const params = terminalFilterParams(query);
    const sourceParams = market === "GLOBAL" ? {} : { country: market };
    const [briefResult, liveResult, sourceResult] = await Promise.all([
      terminalRequest("/briefs/latest", {
        params: { edition: market === "GLOBAL" ? "global" : market },
        signal,
        fallback: () => request("/today", { signal })
      }),
      terminalRequest("/live", {
        params: { ...params, page: 1, limit: 50 },
        signal,
        fallback: () => request("/events", { params: { page: 1, limit: 50 }, signal })
      }),
      terminalRequest("/sources/status", {
        params: sourceParams,
        signal,
        fallback: async () => ({ ok: true, data: { items: [] } })
      })
    ]);

    const briefEnvelope = briefResult.payload || {};
    const brief = briefResult.version === "v2" ? (briefEnvelope.data || {}) : briefEnvelope;
    const liveEnvelope = liveResult.payload || {};
    const liveData = liveResult.version === "v2" ? (liveEnvelope.data || {}) : liveEnvelope;
    const sourceEnvelope = sourceResult.payload || {};
    const sourceData = sourceResult.version === "v2" ? (sourceEnvelope.data || {}) : sourceEnvelope;

    const top = (Array.isArray(brief.top) ? brief.top : [])
      .map(normalizeEvent)
      .filter((item) => (
        isPublicEvent(item)
        && Number(item.official_evidence_count || 0) > 0
        && matchesTerminalFilters(item, query)
      ))
      .slice(0, 5);
    const topIds = new Set(top.map((item) => item.event_id));
    const watch = (Array.isArray(brief.watch) ? brief.watch : [])
      .map(normalizeEvent)
      .filter((item) => isPublicEvent(item) && matchesTerminalFilters(item, query) && !topIds.has(item.event_id));
    const rawLive = Array.isArray(liveData.items)
      ? liveData.items
      : Array.isArray(liveData.data)
        ? liveData.data
        : Array.isArray(brief.recent)
          ? brief.recent
          : [];
    const watchIds = new Set(watch.map((item) => item.event_id));
    const live = rawLive.map(normalizeEvent)
      .filter((item) => isPublicEvent(item) && matchesTerminalFilters(item, query) && !topIds.has(item.event_id))
      .filter((item) => !watchIds.has(item.event_id))
      .filter((item, index, rows) => rows.findIndex((candidate) => candidate.event_id === item.event_id) === index)
      .slice(0, 30);

    let deadlines = (Array.isArray(brief.deadlines) ? brief.deadlines : [])
      .map(normalizeEvent)
      .filter((item) => isPublicEvent(item) && matchesTerminalFilters(item, query));
    if (!deadlines.length && briefResult.version === "v1") {
      const now = new Date();
      const future = new Date(now.getTime() + 30 * 86400000);
      try {
        const calendarPayload = await request("/calendar", {
          params: { from: isoDate(now), to: isoDate(future), page: 1, limit: 50 },
          signal
        });
        deadlines = (Array.isArray(calendarPayload.data) ? calendarPayload.data : [])
          .map(normalizeEvent)
          .filter((item) => item.verification_status !== "signal" && matchesTerminalFilters(item, query));
      } catch (error) {
        if (error && error.name === "AbortError") throw error;
      }
    }
    if (!deadlines.length) {
      deadlines = [...top, ...live].filter((item) => item.deadline_at);
    }
    deadlines = deadlines
      .filter((item, index, rows) => rows.findIndex((candidate) => candidate.event_id === item.event_id) === index)
      .sort((a, b) => {
        const left = normalizedDate(a.deadline_at || a.scheduled_at);
        const right = normalizedDate(b.deadline_at || b.scheduled_at);
        return (left ? left.getTime() : Number.MAX_SAFE_INTEGER) - (right ? right.getTime() : Number.MAX_SAFE_INTEGER);
      })
      .slice(0, 8);

    const sources = (Array.isArray(sourceData.items) ? sourceData.items : Array.isArray(brief.source_status) ? brief.source_status : [])
      .filter((item) => market === "GLOBAL" || !item.country || String(item.country).toUpperCase() === market)
      .slice(0, 12);
    const allVisibleEvents = [...top, ...live];
    updateSourceCoverage(sources, allVisibleEvents, market);
    syncMarketTabs(query);

    const publishedAt = brief.last_updated_at || brief.published_at || brief.cutoff_at || "";
    const rawCoverageNotice = isRecord(brief.coverage_notice) ? brief.coverage_notice : null;
    const noticeCountries = rawCoverageNotice && Array.isArray(rawCoverageNotice.unavailable_countries)
      ? rawCoverageNotice.unavailable_countries.map((country) => String(country).toUpperCase())
      : [];
    const coverageNotice = (
      rawCoverageNotice
      && !(
        rawCoverageNotice.scope === "warning"
        && market !== "GLOBAL"
        && noticeCountries.length > 0
        && !noticeCountries.includes(market)
      )
    ) ? rawCoverageNotice : null;
    const coverageBlocking = coverageNotice && coverageNotice.scope === "blocking";
    const sourceUnavailable = brief.empty_reason === "coverage_unavailable"
      || coverageBlocking
      || (sources.length > 0 && !sources.some((item) => publicSourceState(item).ready));
    const topEmptyMessage = sourceUnavailable
      ? "공식 소스 수집 상태를 확인할 수 없습니다. 소스 상태를 먼저 확인해 주세요."
      : "오늘 확인된 중요 사건 없음";
    const unavailableCountries = coverageNotice && Array.isArray(coverageNotice.unavailable_countries)
      ? coverageNotice.unavailable_countries.join(" · ")
      : "";
    const noticeTimestamp = coverageNotice
      ? coverageNotice.cutoff_at || coverageNotice.published_at || brief.cutoff_at
      : brief.cutoff_at;
    const createCoverageAlert = () => coverageNotice ? element("section", {
      className: `coverage-alert coverage-alert--${coverageBlocking ? "blocking" : "warning"}`,
      attrs: {
        role: coverageBlocking ? "alert" : "status",
        "aria-live": coverageBlocking ? "assertive" : "polite"
      },
      dataset: { coverageScope: coverageBlocking ? "blocking" : "warning" }
    }, [
      element("strong", {
        text: coverageBlocking
          ? "공식 소스 수집 장애 / Source coverage unavailable"
          : "일부 공식 소스 지연 / Partial source coverage"
      }),
      element("p", {
        text: coverageBlocking
          ? "최신 발행본의 수집 범위를 확인할 수 없어 Top 5를 공개하지 않습니다."
          : "확인 가능한 공식 소스의 사건은 계속 표시하지만 일부 시장 정보가 누락될 수 있습니다."
      }),
      element("p", {
        text: [
          unavailableCountries ? `영향 국가 ${unavailableCountries}` : "",
          noticeTimestamp ? `기준 ${formatDate(noticeTimestamp, true)}` : ""
        ].filter(Boolean).join(" · "),
        className: "coverage-alert__meta"
      })
    ]) : null;
    const createStaleAlert = () => brief.stale === true ? element("section", {
      className: "coverage-alert coverage-alert--stale",
      attrs: { role: "status" },
      dataset: { briefStale: "true" }
    }, [
      element("strong", { text: "과거 브리프 스냅샷 / Stale brief snapshot" }),
      element("p", {
        text: `이 발행본은 36시간보다 오래되었습니다. 기준 ${formatDate(brief.cutoff_at, true)}`
      })
    ]) : null;
    const createCoverageScopeNote = () => element("p", {
      text: marketCoverageDescription(market),
      className: "coverage-scope-note",
      attrs: { "data-market-coverage": market }
    });
    const mobileCoverageContext = element("div", {
      className: "mobile-coverage-context"
    }, [
      createCoverageAlert(),
      createStaleAlert(),
      createCoverageScopeNote()
    ].filter(Boolean));
    const desktopCoverageContext = element("div", {
      className: `desktop-coverage-context${top.length ? "" : " desktop-coverage-context--mobile-visible"}`
    }, [
      createCoverageAlert(),
      createStaleAlert(),
      createCoverageScopeNote()
    ].filter(Boolean));
    const topSection = element("section", {
      className: "terminal-panel",
      attrs: { "aria-labelledby": "top-title" }
    }, [
      element("div", { className: "terminal-panel__header terminal-panel__header--top" }, [
        element("h2", { text: "Top 5", attrs: { id: "top-title" } }),
        element("p", { text: "공식 근거·시장 영향 기준" }),
        element("time", {
          text: publishedAt ? `업데이트 ${formatDate(publishedAt, true)}` : "최신 공개 기록",
          className: "mobile-edition-time",
          attrs: { datetime: publishedAt }
        })
      ]),
      top.length
        ? element("ol", { className: "terminal-top-list" }, top.map((item, index) => (
          terminalEvent(item, index + 1, index === 0 ? mobileCoverageContext : null)
        )))
        : terminalEmpty(topEmptyMessage)
    ]);
    const liveSection = element("section", {
      className: "terminal-panel",
      attrs: { id: "terminal-live", "aria-labelledby": "live-title" }
    }, [
      element("div", { className: "terminal-panel__header" }, [
        element("h2", { text: "Live / 최신 변화", attrs: { id: "live-title" } }),
        element("p", { text: `${live.length}건` })
      ]),
      live.length
        ? element("ul", { className: "live-list" }, live.map(liveEventRow))
        : terminalEmpty("새로 확인된 공개 변화가 없습니다.")
    ]);
    const filterPanel = element("aside", {
      className: "terminal-panel terminal-filters",
      attrs: {
        id: "terminal-filters",
        "aria-labelledby": "terminal-filter-title",
        "data-filter-sheet": "true"
      }
    }, [
      element("div", { className: "terminal-panel__header" }, [
        element("h2", { text: "필터 / Filters", attrs: { id: "terminal-filter-title" } }),
        element("p", { text: "URL에 저장" }),
        element("button", {
          text: "닫기",
          className: "filter-sheet-close",
          attrs: { type: "button", "data-filter-sheet-close": "true" }
        })
      ]),
      terminalFilterForm(query)
    ]);
    const deadlinePanel = element("section", {
      className: "terminal-panel desktop-rail-panel",
      attrs: { "aria-labelledby": "deadline-title" }
    }, [
      element("div", { className: "terminal-panel__header" }, [
        element("h2", { text: "주요 기한 / Deadlines", attrs: { id: "deadline-title" } }),
        routeLink("전체", "#/calendar", { className: "field-hint" })
      ]),
      deadlines.length
        ? element("ul", { className: "deadline-list" }, deadlines.map(deadlineItem))
        : terminalEmpty("30일 내 공개 기한이 없습니다.")
    ]);
    const watchPanel = element("section", {
      className: "terminal-panel desktop-rail-panel",
      attrs: { "aria-labelledby": "watch-title" }
    }, [
      element("div", { className: "terminal-panel__header" }, [
        element("h2", { text: "Watch / 주시", attrs: { id: "watch-title" } }),
        element("p", { text: `${watch.length}건` })
      ]),
      watch.length
        ? element("ul", { className: "live-list" }, watch.slice(0, 8).map(liveEventRow))
        : terminalEmpty("현재 공개된 주시 사건이 없습니다.")
    ]);
    const mobileWatchPanel = element("section", {
      className: "terminal-panel mobile-terminal-panel",
      attrs: { "aria-labelledby": "mobile-watch-title" }
    }, [
      element("div", { className: "terminal-panel__header" }, [
        element("h2", { text: "Watch / 새로 바뀐 사건", attrs: { id: "mobile-watch-title" } }),
        element("p", { text: `${watch.length}건` })
      ]),
      watch.length
        ? element("ul", { className: "live-list" }, watch.slice(0, 8).map(liveEventRow))
        : terminalEmpty("새로 바뀐 주시 사건이 없습니다.")
    ]);
    const mobileDeadlinePanel = element("section", {
      className: "terminal-panel mobile-terminal-panel",
      attrs: { "aria-labelledby": "mobile-deadline-title" }
    }, [
      element("div", { className: "terminal-panel__header" }, [
        element("h2", { text: "임박 기한 / Deadlines", attrs: { id: "mobile-deadline-title" } }),
        routeLink("전체", "#/calendar", { className: "field-hint" })
      ]),
      deadlines.length
        ? element("ul", { className: "deadline-list" }, deadlines.map(deadlineItem))
        : terminalEmpty("30일 내 공개 기한이 없습니다.")
    ]);
    const sourcePanel = element("section", {
      className: "terminal-panel",
      attrs: { "aria-labelledby": "source-status-title" }
    }, [
      element("div", { className: "terminal-panel__header" }, [
        element("h2", { text: "소스 상태 / Sources", attrs: { id: "source-status-title" } }),
        element("p", { text: sources.length ? `${sources.length}개` : "근거 연결" })
      ]),
      sources.length
        ? element("ul", { className: "source-status-list" }, sources.map(sourceStatusItem))
        : terminalEmpty("공식 근거 배지로 사건별 커버리지를 확인하세요.")
    ]);

    app.replaceChildren(
      element("header", { className: "terminal-header" }, [
        element("div", { className: "terminal-header__title" }, [
          element("h1", { text: "주주·자본시장 데일리" }),
          element("p", { text: `${market} · 공시와 공식자료 우선` })
        ]),
        element("time", {
          text: publishedAt ? `업데이트 ${formatDate(publishedAt, true)}` : "최신 공개 기록",
          className: "edition-time",
          attrs: { datetime: publishedAt }
        })
      ]),
      desktopCoverageContext,
      mobileTerminalTabs(),
      element("div", { className: "terminal-grid" }, [
        element("div", { className: "terminal-main" }, [
          topSection,
          mobileWatchPanel,
          mobileDeadlinePanel,
          liveSection
        ]),
        filterPanel,
        element("aside", { className: "terminal-rail rail-stack", attrs: { "aria-label": "기한과 소스 상태 / Deadlines and source status" } }, [
          deadlinePanel,
          watchPanel,
          sourcePanel
        ])
      ]),
      element("button", {
        className: "filter-sheet-backdrop",
        attrs: {
          type: "button",
          "data-filter-sheet-close": "true",
          "aria-label": "필터 닫기 / Close filters"
        }
      })
    );
    installTodayTerminalControls();
  }

  async function renderEvents(query, signal) {
    const v2Params = v2EventFilterParams(query);
    const v1Params = eventFilterParams(query);
    const result = await terminalRequest("/events", {
      params: v2Params,
      signal,
      fallback: () => request("/events", { params: v1Params, signal })
    });
    const payload = result.payload;
    const rawEvents = result.version === "v2"
      ? payload.data.items
      : Array.isArray(payload.data) ? payload.data : [];
    const events = rawEvents.map(normalizeEvent).filter(isPublicEvent);
    const list = element("div", { className: "data-list", attrs: { id: "event-results" } }, events.map(archiveEvent));
    const results = element("section", { className: "section-block", attrs: { "aria-labelledby": "event-results-title" } }, [
      sectionHeading("공개 사건 / Public events", `${events.length}개 표시`, "event-results-title"),
      events.length ? list : emptyState("조건에 맞는 사건이 없습니다.", "필터를 줄이거나 기간을 넓혀 다시 확인해 주세요.")
    ]);
    const pagination = result.version === "v2" ? payload.meta : payload.pagination;
    if (events.length) {
      addLoadMore(results, pagination, async (cursor, cursorKind) => {
        if (result.version === "v2") {
          const moreResult = await terminalRequest("/events", {
            params: continuationParams(v2Params, cursor, cursorKind)
          });
          moreResult.payload.data.items
            .map(normalizeEvent)
            .filter(isPublicEvent)
            .forEach((item) => list.append(archiveEvent(item)));
          return moreResult.payload.meta;
        }
        const more = await request("/events", { params: { ...v1Params, page: cursor } });
        (Array.isArray(more.data) ? more.data : [])
          .map(normalizeEvent)
          .filter(isPublicEvent)
          .forEach((item) => list.append(archiveEvent(item)));
        return more.pagination;
      });
    }
    const exportParams = { ...(result.version === "v2" ? v2Params : v1Params) };
    delete exportParams.page;
    delete exportParams.limit;
    const exportVersion = result.version === "v2" ? "v2" : "v1";
    app.replaceChildren(
      pageHeader(
        "EVENTS / 사건",
        result.version === "v2" ? "글로벌 자본시장 사건 전체 기록" : "거버넌스 사건 전체 기록",
        result.version === "v2"
          ? "국가·발행사·사건 유형·근거·기간·상태 필터는 현재 주소에 보존되어 공유할 수 있습니다."
          : "회사·당사자·유형·근거·기간·상태 필터는 현재 주소에 보존되어 공유할 수 있습니다.",
        [
          externalLink("Atom", endpoint("/feeds/events.atom", exportParams, exportVersion).toString(), "text-link"),
          externalLink("CSV", endpoint("/exports/events.csv", exportParams, exportVersion).toString(), "text-link"),
          externalLink("JSON", endpoint("/exports/events.json", exportParams, exportVersion).toString(), "text-link")
        ]
      ),
      result.version === "v2"
        ? v2EventFilterForm(query, "#/events")
        : eventFilterForm(query, "#/events"),
      results
    );
  }

  function companyRow(company) {
    const title = company.legal_name || company.short_name || company.company_id || "—";
    const aliases = Array.isArray(company.aliases) && company.aliases.length ? ` · ${company.aliases.join(", ")}` : "";
    return element("article", { className: "company-row" }, [
      element("h3", {}, [routeLink(title, `#/companies/${encodeURIComponent(company.company_id || "")}`)]),
      metadata([
        { value: company.stock_code || "" },
        { value: company.market || "" },
        { value: `사건 ${Number(company.event_count || 0)} / Events` },
        { value: `진행 캠페인 ${Number(company.active_campaign_count || 0)} / Active` }
      ]),
      company.legal_name_en ? element("p", { text: company.legal_name_en, lang: "en", className: "field-hint" }) : null,
      aliases ? element("p", { text: aliases.replace(/^ · /, ""), className: "field-hint" }) : null
    ].filter(Boolean));
  }

  async function renderCompanies(query, signal) {
    const q = String(query.get("q") || "").trim();
    const params = { page: 1, limit: 50 };
    if (q.length >= 2) params.q = q;
    const payload = await request("/companies", { params, signal });
    const companies = Array.isArray(payload.data) ? payload.data : [];
    const input = element("input", { attrs: { type: "search", name: "q", value: q, minlength: "2", maxlength: "100", placeholder: "회사명 또는 종목코드", "aria-label": "회사 검색 / Search companies" } });
    const form = element("form", { className: "inline-form", attrs: { role: "search" } }, [
      input,
      element("button", { text: "찾기 / Find", attrs: { type: "submit" } })
    ]);
    form.addEventListener("submit", (event) => {
      event.preventDefault();
      const value = input.value.trim();
      window.location.hash = value ? `#/companies?q=${encodeURIComponent(value)}` : "#/companies";
    });
    const companyList = element("div", { className: "data-list" }, companies.map(companyRow));
    const resultsSection = element("section", { className: "section-block", attrs: { "aria-labelledby": "company-results-title" } }, [
      sectionHeading(q ? `“${q}” 검색 결과` : "전체 기업 / All companies", `${companies.length}개 표시`, "company-results-title"),
      companies.length ? companyList : emptyState("기업을 찾지 못했습니다.", "회사명 또는 종목코드를 다시 확인해 주세요.")
    ]);
    if (companies.length) {
      addLoadMore(resultsSection, payload.pagination, async (page) => {
        const more = await request("/companies", { params: { ...params, page } });
        (Array.isArray(more.data) ? more.data : []).forEach((item) => companyList.append(companyRow(item)));
        return more.pagination;
      });
    }
    app.replaceChildren(
      pageHeader("COMPANIES / 기업", "기업별 거버넌스 기록", "DART corp_code를 기준으로 사건, 캠페인, 약속과 이행을 연결합니다."),
      element("section", { className: "filter-form", attrs: { "aria-label": "기업 검색" } }, [form]),
      resultsSection
    );
  }

  function issuerRow(issuer) {
    const title = issuer.legal_name || issuer.short_name || issuer.issuer_id || "—";
    return element("article", { className: "company-row" }, [
      element("h3", {}, [
        routeLink(title, `#/issuers/${encodeURIComponent(issuer.issuer_id || "")}`, {
          lang: issuer.original_language
        })
      ]),
      metadata([
        { value: issuer.ticker || "" },
        { value: issuer.market || "" },
        { value: issuer.country_code || "" },
        { value: `사건 ${Number(issuer.event_count || 0)} / Events` }
      ]),
      issuer.legal_name_en && issuer.legal_name_en !== title
        ? element("p", { text: issuer.legal_name_en, lang: "en", className: "field-hint" })
        : null
    ].filter(Boolean));
  }

  async function renderIssuers(query, signal) {
    const q = String(query.get("q") || "").trim();
    const country = String(query.get("country") || "").toUpperCase();
    const params = { page: 1, limit: 50 };
    if (q.length >= 2) params.q = q;
    if (MARKETS.has(country) && country !== "GLOBAL") params.country = country;
    const result = await terminalRequest("/issuers", { params, signal });
    const payload = result.payload;
    const issuers = payload.data.items;
    const input = element("input", {
      attrs: {
        type: "search",
        name: "q",
        value: q,
        minlength: "2",
        maxlength: "100",
        placeholder: "발행사명 / Issuer name",
        "aria-label": "발행사 검색 / Search issuers"
      }
    });
    const countrySelect = element("select", { attrs: { name: "country", "aria-label": "국가 / Country" } });
    [["", "전체 국가 / All countries"], ...[...MARKETS].filter((value) => value !== "GLOBAL").map((value) => [value, value])]
      .forEach(([value, text]) => {
        const option = element("option", { text, attrs: { value } });
        if (value === country) option.selected = true;
        countrySelect.append(option);
      });
    const form = element("form", { className: "inline-form", attrs: { role: "search" } }, [
      input,
      countrySelect,
      element("button", { text: "찾기 / Find", attrs: { type: "submit" } })
    ]);
    form.addEventListener("submit", (event) => {
      event.preventDefault();
      if (!form.reportValidity()) return;
      const values = new URLSearchParams();
      if (input.value.trim()) values.set("q", input.value.trim());
      if (countrySelect.value) values.set("country", countrySelect.value);
      window.location.hash = `#/issuers${values.toString() ? `?${values.toString()}` : ""}`;
    });
    const list = element("div", { className: "data-list" }, issuers.map(issuerRow));
    const results = element("section", { className: "section-block", attrs: { "aria-labelledby": "issuer-results-title" } }, [
      sectionHeading(q ? `“${q}” 검색 결과` : "글로벌 발행사 / Global issuers", `${issuers.length}개 표시`, "issuer-results-title"),
      issuers.length ? list : emptyState("발행사를 찾지 못했습니다.", "발행사명 또는 국가 조건을 다시 확인해 주세요.")
    ]);
    if (issuers.length) {
      addLoadMore(results, payload.meta, async (cursor, cursorKind) => {
        const more = await terminalRequest("/issuers", {
          params: continuationParams(params, cursor, cursorKind)
        });
        more.payload.data.items.forEach((item) => list.append(issuerRow(item)));
        return more.payload.meta;
      });
    }
    app.replaceChildren(
      pageHeader(
        "ISSUERS / 발행사",
        "글로벌 발행사별 자본시장 기록",
        "국가별 공식 식별자와 상장 정보, 공개 사건을 하나의 발행사 기록으로 연결합니다."
      ),
      element("section", { className: "filter-form", attrs: { "aria-label": "발행사 검색" } }, [form]),
      results
    );
  }

  function identifierRow(identifier) {
    return element("article", { className: "archive-row" }, [
      element("h3", { text: identifier.identifier_value || "—" }),
      metadata([
        { value: identifier.identifier_type || "" },
        { value: identifier.market || "" },
        Number(identifier.is_primary) === 1 ? { value: "Primary" } : null
      ])
    ]);
  }

  function listingRow(listing) {
    return element("article", { className: "archive-row" }, [
      element("h3", { text: [listing.ticker, listing.market].filter(Boolean).join(" · ") || listing.listing_id || "—" }),
      metadata([
        { value: listing.country_code || "" },
        { value: listing.isin || "" },
        { value: listing.currency_code || "" },
        { value: listing.listing_status || "" }
      ])
    ]);
  }

  async function renderIssuer(issuerId, signal) {
    if (!validEntityId(issuerId)) throw new ApiError("Invalid issuer ID", "invalid_issuer_id", 400);
    const path = `/issuers/${encodeURIComponent(issuerId)}`;
    const result = await terminalRequest(path, { signal });
    const data = result.payload.data;
    const issuer = data.issuer;
    const identifiers = data.identifiers;
    const listings = data.listings;
    const events = data.events.map(normalizeEvent).filter(isPublicEvent);
    const homepage = issuer.homepage_url
      ? externalLink("발행사 웹사이트 / Website", issuer.homepage_url, "text-link")
      : null;
    const timelineList = element("div", { className: "card-list" }, events.map((item) => eventCard(item)));
    app.replaceChildren(
      pageHeader(
        `${issuer.country_code || "GLOBAL"} · ${issuer.listing_status || "issuer"}`,
        issuer.legal_name || issuer.short_name || issuerId,
        [issuer.legal_name_en, listings[0] && listings[0].ticker, listings[0] && listings[0].market]
          .filter(Boolean)
          .join(" · "),
        [
          homepage,
          routeLink("전체 사건 / All events", `#/events?issuer_id=${encodeURIComponent(issuerId)}`, { className: "text-link" })
        ].filter(Boolean),
        issuer.original_language
      ),
      element("div", { className: "detail-grid" }, [
        element("div", {}, [
          element("section", { attrs: { "aria-labelledby": "issuer-timeline-title" } }, [
            sectionHeading("자본시장 타임라인 / Timeline", `${events.length}개 표시`, "issuer-timeline-title"),
            events.length
              ? timelineList
              : emptyState("공개 사건이 없습니다.", "공식 근거와 검수가 완료된 사건만 표시됩니다.")
          ])
        ]),
        element("aside", { className: "detail-sidebar", attrs: { "aria-label": "발행사 식별 정보 / Issuer identity" } }, [
          facts([
            { label: "발행사 ID / Issuer ID", value: issuer.issuer_id },
            { label: "국가 / Country", value: issuer.country_code },
            { label: "상장 상태 / Listing", value: issuer.listing_status },
            { label: "기준 언어 / Language", value: issuer.original_language },
            { label: "마스터 갱신 / Master updated", value: issuer.master_modified_at ? formatDate(issuer.master_modified_at, true) : "" }
          ]),
          element("section", { className: "section-block" }, [
            sectionHeading("공식 식별자 / Identifiers", `${identifiers.length}개`),
            identifiers.length
              ? element("div", { className: "data-list" }, identifiers.map(identifierRow))
              : element("p", { text: "공개된 식별자가 없습니다." })
          ]),
          element("section", { className: "section-block" }, [
            sectionHeading("상장 정보 / Listings", `${listings.length}개`),
            listings.length
              ? element("div", { className: "data-list" }, listings.map(listingRow))
              : element("p", { text: "공개된 상장 정보가 없습니다." })
          ])
        ])
      ])
    );
  }

  function facts(items) {
    const dl = element("dl", { className: "facts" });
    items.filter((item) => item && item.value !== undefined && item.value !== null && item.value !== "").forEach((item) => {
      dl.append(element("div", {}, [element("dt", { text: item.label }), item.node || element("dd", { text: item.value })]));
    });
    return dl;
  }

  function campaignRow(campaign) {
    return element("article", { className: "archive-row" }, [
      element("h3", {}, [routeLink(campaign.title || "제목 없음", `#/campaigns/${encodeURIComponent(campaign.campaign_id || "")}`, { lang: campaign.original_language })]),
      metadata([
        { value: label("campaignStage", campaign.stage) },
        campaign.outcome ? { value: label("outcome", campaign.outcome) } : null,
        { value: formatDate(campaign.started_at, false) }
      ])
    ]);
  }

  function commitmentCard(item) {
    return element("article", { className: "commitment-card" }, [
      element("div", { className: "badge-row" }, [badge(item.status, "commitment")]),
      sourceNode("h3", item.commitment_text, item.original_language),
      item.actual_action ? sourceNode("p", item.actual_action, item.original_language) : null,
      metadata([
        item.target_at ? { value: `기한 / Target ${formatDate(item.target_at, false)}` } : null
      ])
    ].filter(Boolean));
  }

  async function renderCompany(companyId, signal) {
    if (!validCompanyId(companyId)) throw new ApiError("Invalid company ID", "invalid_company_id", 400);
    const [payload, eventPayload] = await Promise.all([
      request(`/companies/${encodeURIComponent(companyId)}`, { signal }),
      request("/events", { params: { company_id: companyId, page: 1, limit: 50 }, signal })
    ]);
    const data = payload.data || {};
    const company = data.company || {};
    const events = (Array.isArray(eventPayload.data) ? eventPayload.data : [])
      .filter(isPublicEvent)
      .map((item) => ({ ...item, company_id: companyId, company_name: item.company_name || company.legal_name }));
    const campaigns = (Array.isArray(data.campaigns) ? data.campaigns : []).filter(isPublicCampaign);
    const commitments = Array.isArray(data.commitments) ? data.commitments : [];
    const homepage = company.homepage_url ? externalLink("회사 웹사이트 / Website", company.homepage_url, "text-link") : null;
    const timelineList = element("div", { className: "card-list", attrs: { id: "company-timeline" } }, events.map((item) => eventCard(item)));
    const timelineSection = element("section", { className: "section-block", attrs: { "aria-labelledby": "timeline-title" } }, [
      sectionHeading("거버넌스 타임라인 / Timeline", `${events.length}개 표시`, "timeline-title"),
      events.length ? timelineList : emptyState("공개 사건이 없습니다.", "검수 완료된 사건이 추가되면 표시됩니다.")
    ]);
    if (events.length) {
      addLoadMore(timelineSection, eventPayload.pagination, async (page) => {
        const more = await request("/events", { params: { company_id: companyId, page, limit: 50 } });
        (Array.isArray(more.data) ? more.data : []).filter(isPublicEvent).forEach((item) => {
          timelineList.append(eventCard({ ...item, company_id: companyId, company_name: item.company_name || company.legal_name }));
        });
        return more.pagination;
      });
    }
    app.replaceChildren(
      pageHeader(
        `${company.market || "COMPANY"}${company.stock_code ? ` · ${company.stock_code}` : ""}`,
        company.legal_name || companyId,
        company.legal_name_en || "기업별 사건·캠페인·약속 이행 기록",
        [homepage, routeLink("정정 요청 / Feedback", `#/feedback?entity_type=company&entity_id=${encodeURIComponent(companyId)}`, { className: "text-link" })].filter(Boolean)
      ),
      element("div", { className: "detail-grid" }, [
        element("div", {}, [
          element("section", { attrs: { "aria-labelledby": "campaigns-title" } }, [
            sectionHeading("현재 캠페인 / Campaigns", `${campaigns.length}개`, "campaigns-title"),
            campaigns.length ? element("div", { className: "data-list" }, campaigns.map(campaignRow)) : emptyState("공개 캠페인이 없습니다.", "새 캠페인이 확인되면 표시됩니다.")
          ]),
          timelineSection,
          element("section", { className: "section-block", attrs: { "aria-labelledby": "commitments-title" } }, [
            sectionHeading("밸류업 약속·이행 / Commitments", `${commitments.length}개`, "commitments-title"),
            commitments.length ? element("div", { className: "data-list" }, commitments.map(commitmentCard)) : emptyState("등록된 약속이 없습니다.", "회사 계획과 실제 이행이 연결되면 표시됩니다.")
          ])
        ]),
        element("aside", { className: "detail-sidebar", attrs: { "aria-label": "기업 정보 / Company information" } }, [
          element("h2", { text: "기업 정보 / Company" }),
          facts([
            { label: "DART corp_code", value: company.company_id || companyId },
            { label: "종목코드 / Ticker", value: company.stock_code },
            { label: "시장 / Market", value: company.market },
            { label: "약칭 / Short name", value: company.short_name },
            { label: "최종 갱신 / Updated", value: formatDate(company.updated_at, true) }
          ])
        ])
      ])
    );
  }

  async function renderActor(actorId, signal) {
    if (!validEntityId(actorId)) throw new ApiError("Invalid actor ID", "invalid_actor_id", 400);
    const [payload, eventPayload] = await Promise.all([
      request(`/actors/${encodeURIComponent(actorId)}`, { signal }),
      request("/events", { params: { actor_id: actorId, page: 1, limit: 50 }, signal })
    ]);
    const data = payload.data || {};
    const actor = data.actor || data;
    const campaigns = (Array.isArray(data.campaigns) ? data.campaigns : []).filter(isPublicCampaign);
    const events = (Array.isArray(eventPayload.data) ? eventPayload.data : []).filter(isPublicEvent);
    const eventList = element("div", { className: "card-list", attrs: { id: "actor-events" } }, events.map((item) => eventCard(item)));
    const eventSection = element("section", { attrs: { "aria-labelledby": "actor-events-title" } }, [
      sectionHeading("관련 사건 / Events", `${events.length}개 표시`, "actor-events-title"),
      events.length ? eventList : emptyState("공개 사건이 없습니다.", "이 당사자와 연결된 검수 완료 사건이 표시됩니다.")
    ]);
    if (events.length) {
      addLoadMore(eventSection, eventPayload.pagination, async (page) => {
        const more = await request("/events", { params: { actor_id: actorId, page, limit: 50 } });
        (Array.isArray(more.data) ? more.data : []).filter(isPublicEvent).forEach((item) => eventList.append(eventCard(item)));
        return more.pagination;
      });
    }
    const aliases = Array.isArray(actor.aliases) ? actor.aliases.join(", ") : "";
    app.replaceChildren(
      pageHeader(
        label("actorType", actor.actor_type),
        actor.display_name || actorId,
        actor.display_name_en || "당사자별 공식 근거·사건·캠페인 기록",
        [
          actor.homepage_url ? externalLink("공식 웹사이트 / Website", actor.homepage_url, "text-link") : null,
          routeLink("당사자 답변 / Right of reply", `#/feedback?feedback_type=right_of_reply&entity_type=actor&entity_id=${encodeURIComponent(actorId)}`, { className: "text-link" })
        ].filter(Boolean),
        actor.original_language
      ),
      element("div", { className: "detail-grid" }, [
        element("div", {}, [
          eventSection,
          element("section", { className: "section-block", attrs: { "aria-labelledby": "actor-campaigns-title" } }, [
            sectionHeading("관련 캠페인 / Campaigns", `${campaigns.length}개`, "actor-campaigns-title"),
            campaigns.length ? element("div", { className: "data-list" }, campaigns.map(campaignRow)) : emptyState("공개 캠페인이 없습니다.", "검수 승인된 캠페인이 연결되면 표시됩니다.")
          ])
        ]),
        element("aside", { className: "detail-sidebar", attrs: { "aria-label": "당사자 정보 / Actor information" } }, [
          element("h2", { text: "당사자 정보 / Actor" }),
          facts([
            { label: "유형 / Type", value: label("actorType", actor.actor_type) },
            { label: "Actor ID", value: actor.actor_id || actorId },
            { label: "국가 / Country", value: actor.country_code },
            { label: "별칭 / Aliases", value: aliases },
            actor.company_id ? { label: "연결 회사 / Company", node: element("dd", {}, [routeLink(actor.company_name || actor.company_id, `#/companies/${encodeURIComponent(actor.company_id)}`)]) } : null,
            { label: "최종 갱신 / Updated", value: formatDate(actor.updated_at, true) }
          ])
        ])
      ])
    );
  }

  function timeline(items) {
    return element("ol", { className: "timeline" }, items.map((item) => element("li", {}, [
      element("time", { text: formatDate(item.occurred_at, true), attrs: { datetime: item.occurred_at || "" } }),
      sourceNode("h3", item.title || item.entry_type, item.original_language),
      item.description ? sourceNode("p", item.description, item.original_language) : null
    ].filter(Boolean))));
  }

  function documentRow(documentItem) {
    const detail = routeLink(documentItem.title || "문서", `#/documents/${encodeURIComponent(documentItem.document_id || "")}`, { lang: documentItem.original_language });
    return element("article", { className: "document-row" }, [
      element("div", { className: "badge-row" }, [badge(documentItem.source_class, "sourceClass"), badge(documentItem.verification_status, "verification")]),
      element("h3", {}, [detail]),
      metadata([
        { value: documentItem.document_type || "" },
        { value: formatDate(documentItem.published_at, false) },
        { value: documentItem.version_no ? `v${documentItem.version_no}` : "" }
      ]),
      documentItem.original_url ? externalLink("원문 열기 / Open source", documentItem.original_url, "text-link") : null
    ].filter(Boolean));
  }

  function claimCard(claim) {
    return element("article", { className: "claim-card" }, [
      element("div", { className: "badge-row" }, [badge(claim.claim_type, "claimType")]),
      claim.actor_name ? element("h3", { text: claim.actor_name }) : null,
      sourceNode("p", claim.claim_text, claim.original_language),
      claim.document_title ? element("p", {}, [
        element("strong", { text: "근거 / Evidence: " }),
        claim.original_url ? externalLink(claim.document_title, claim.original_url) : element("span", { text: claim.document_title })
      ]) : null,
      claim.evidence_locator ? element("p", { text: claim.evidence_locator, className: "field-hint" }) : null
    ].filter(Boolean));
  }

  function isPublicRevision(revision) {
    if (!revision || revision.is_public === false) return false;
    if (revision.visibility && revision.visibility !== "public") return false;
    if (revision.publication_status && revision.publication_status !== "published") return false;
    return true;
  }

  function revisionEntityRoute(revision) {
    const type = String(revision.entity_type || "");
    const id = String(revision.entity_id || "");
    if (!validEntityId(id)) return "";
    if (type === "company" && validCompanyId(id)) return `#/companies/${encodeURIComponent(id)}`;
    if (["actor", "event", "campaign", "document"].includes(type)) return `#/${type === "actor" ? "actors" : `${type}s`}/${encodeURIComponent(id)}`;
    return "";
  }

  function revisionRow(revision) {
    const entityRoute = revisionEntityRoute(revision);
    const title = revision.title || revision.reason || "공개 정정";
    return element("article", { className: "revision-row" }, [
      element("div", { className: "badge-row" }, [badge("corrected", "verification")]),
      element("h3", {}, [entityRoute ? routeLink(title, entityRoute, { lang: revision.original_language }) : sourceNode("span", title, revision.original_language)]),
      revision.reason ? sourceNode("p", revision.reason, revision.original_language) : null,
      revision.before_value !== undefined && revision.before_value !== null ? element("div", { className: "revision-change" }, [
        element("strong", { text: "변경 전 / Before" }),
        sourceNode("p", revision.before_value, revision.original_language)
      ]) : null,
      revision.after_value !== undefined && revision.after_value !== null ? element("div", { className: "revision-change" }, [
        element("strong", { text: "변경 후 / After" }),
        sourceNode("p", revision.after_value, revision.original_language)
      ]) : null,
      metadata([
        { value: revision.entity_type || "record" },
        { value: revision.field_name || "record" },
        { value: revision.company_name || revision.company_id || "" },
        { value: formatDate(revision.published_at, true) }
      ])
    ].filter(Boolean));
  }

  async function renderRevisions(query, signal) {
    const params = { page: 1, limit: 50 };
    ["company_id", "entity_type", "from", "to"].forEach((name) => {
      const value = String(query.get(name) || "").trim();
      if (value) params[name] = value;
    });
    const payload = await request("/revisions", { params, signal });
    const revisions = (Array.isArray(payload.data) ? payload.data : []).filter(isPublicRevision);
    const list = element("div", { className: "data-list", attrs: { id: "public-revisions" } }, revisions.map(revisionRow));
    const company = element("input", { attrs: { id: "revision-company", name: "company_id", value: query.get("company_id") || "", inputmode: "numeric", maxlength: "8", pattern: "[0-9]{8}", placeholder: "DART corp_code" } });
    const entityType = element("select", { attrs: { id: "revision-entity", name: "entity_type" } });
    [["", "전체 기록 / All records"], ["company", "기업 / Company"], ["actor", "당사자 / Actor"], ["event", "사건 / Event"], ["campaign", "캠페인 / Campaign"], ["document", "문서 / Document"]].forEach(([value, text]) => {
      const option = element("option", { text, attrs: { value } });
      if (value === String(query.get("entity_type") || "")) option.selected = true;
      entityType.append(option);
    });
    const from = element("input", { attrs: { id: "revision-from", type: "date", name: "from", value: query.get("from") || "" } });
    const to = element("input", { attrs: { id: "revision-to", type: "date", name: "to", value: query.get("to") || "" } });
    const form = element("form", { className: "filter-form", attrs: { "aria-label": "정정 이력 필터 / Revision filters" } }, [
      element("div", { className: "filter-grid" }, [
        element("div", { className: "field" }, [element("label", { text: "회사 / Company", attrs: { for: "revision-company" } }), company]),
        element("div", { className: "field" }, [element("label", { text: "기록 유형 / Record type", attrs: { for: "revision-entity" } }), entityType]),
        element("div", { className: "field" }, [element("label", { text: "시작 / From", attrs: { for: "revision-from" } }), from]),
        element("div", { className: "field" }, [element("label", { text: "종료 / To", attrs: { for: "revision-to" } }), to])
      ]),
      element("div", { className: "form-actions" }, [element("button", { text: "필터 적용 / Apply", attrs: { type: "submit" } }), routeLink("초기화 / Reset", "#/revisions", { className: "text-link" })])
    ]);
    form.addEventListener("submit", (event) => {
      event.preventDefault();
      if (!form.reportValidity()) return;
      const values = new URLSearchParams();
      new FormData(form).forEach((value, name) => { if (String(value).trim()) values.set(name, String(value).trim()); });
      window.location.hash = `#/revisions${values.toString() ? `?${values.toString()}` : ""}`;
    });
    const results = element("section", { className: "section-block", attrs: { "aria-labelledby": "revision-results-title" } }, [
      sectionHeading("공개 정정 / Published revisions", `${revisions.length}개 표시`, "revision-results-title"),
      revisions.length ? list : emptyState("공개된 정정이 없습니다.", "편집 승인 후 공개된 정정만 이 목록에 나타납니다.")
    ]);
    if (revisions.length) {
      addLoadMore(results, payload.pagination, async (page) => {
        const more = await request("/revisions", { params: { ...params, page } });
        (Array.isArray(more.data) ? more.data : []).filter(isPublicRevision).forEach((item) => list.append(revisionRow(item)));
        return more.pagination;
      });
    }
    app.replaceChildren(
      pageHeader("REVISIONS / 정정 이력", "공개 정정 로그", "내부 승인·검수 기록은 제외하고, 공개된 사실 기록의 변경만 시간순으로 표시합니다."),
      form,
      results
    );
  }

  async function renderEvent(eventId, signal) {
    if (!validEntityId(eventId)) throw new ApiError("Invalid event ID", "invalid_event_id", 400);
    const path = `/events/${encodeURIComponent(eventId)}`;
    const result = await terminalRequest(path, {
      signal,
      fallback: () => request(path, { signal })
    });
    const payload = result.payload;
    const data = payload.data || {};
    const event = normalizeEvent(data.event || {});
    if (!isPublicEvent(event)) throw new ApiError("Signal events are not public", "event_not_found", 404);
    const actors = Array.isArray(data.actors) ? data.actors : [];
    const claims = Array.isArray(data.claims) ? data.claims : [];
    const documents = (Array.isArray(data.documents) ? data.documents : []).filter(isPublicDocument);
    const entries = Array.isArray(data.timeline) ? data.timeline : [];
    const revisions = (Array.isArray(data.revisions) ? data.revisions : []).filter(isPublicRevision);

    const actorList = element("ul", { className: "compact-list" }, actors.map((actor) => element("li", { className: "compact-item" }, [
      actor.actor_id
        ? routeLink(actor.display_name || actor.actor_id, `#/actors/${encodeURIComponent(actor.actor_id)}`, { lang: actor.original_language })
        : sourceNode("strong", actor.display_name, actor.original_language),
      element("p", { text: `${label("actorType", actor.actor_type)} · ${actor.actor_role || "participant"}` }),
      actor.display_name_en ? element("p", { text: actor.display_name_en, lang: "en" }) : null
    ].filter(Boolean))));

    app.replaceChildren(
      pageHeader(
        label("eventType", event.event_type),
        event.title || eventId,
        `${issuerOrCompanyName(event)} · ${formatDate(event.occurred_at, true)}`,
        [
          issuerOrCompanyRoute(event)
            ? routeLink("발행사 기록 / Issuer", issuerOrCompanyRoute(event), { className: "text-link" })
            : null,
          routeLink("정정·답변 / Feedback", `#/feedback?entity_type=event&entity_id=${encodeURIComponent(eventId)}`, { className: "text-link" })
        ].filter(Boolean),
        event.original_language
      ),
      element("div", { className: "detail-grid" }, [
        element("div", {}, [
          element("section", { attrs: { "aria-labelledby": "claims-title" } }, [
            sectionHeading("주장·반론·공식 사실 / Claims & evidence", `${claims.length}개`, "claims-title"),
            claims.length ? element("div", { className: "data-list" }, claims.map(claimCard)) : emptyState("공개된 주장 기록이 없습니다.", "공식 근거나 승인된 기록이 연결되면 표시됩니다.")
          ]),
          element("section", { className: "section-block", attrs: { "aria-labelledby": "documents-title" } }, [
            sectionHeading("공식 근거·보도 / Sources", `${documents.length}개`, "documents-title"),
            documents.length ? element("div", { className: "data-list" }, documents.map(documentRow)) : emptyState("공개 가능한 근거가 없습니다.", "이용권한과 검수가 확인된 문서만 표시됩니다.")
          ]),
          element("section", { className: "section-block", attrs: { "aria-labelledby": "event-timeline-title" } }, [
            sectionHeading("사건 이력 / Timeline", `${entries.length}개`, "event-timeline-title"),
            entries.length ? timeline(entries) : emptyState("등록된 이력이 없습니다.", "후속 조치와 결과가 확인되면 이어서 표시됩니다.")
          ]),
          revisions.length ? element("section", { className: "section-block" }, [
            sectionHeading("정정 이력 / Revisions", `${revisions.length}개`),
            element("div", { className: "data-list" }, revisions.map((revision) => element("article", { className: "archive-row" }, [
              element("h3", { text: revision.reason }),
              metadata([{ value: revision.field_name || "record" }, { value: formatDate(revision.published_at, true) }])
            ])))
          ]) : null
        ].filter(Boolean)),
        element("aside", { className: "detail-sidebar", attrs: { "aria-label": "사건 상태 / Event status" } }, [
          element("div", { className: "badge-row" }, [badge(event.importance, "importance"), badge(event.verification_status, "verification")]),
          facts([
            {
              label: "발행사 / Issuer",
              value: issuerOrCompanyName(event),
              node: element("dd", {}, [issuerOrCompanyLink(event)])
            },
            { label: "발생 / Occurred", value: formatDate(event.occurred_at, true) },
            { label: "대표 당사자 / Representative actor", value: eventActorSummary(event) },
            { label: "접수 / Filed", value: event.filed_at ? formatDate(event.filed_at, true) : "—" },
            { label: "최초 관측 / First observed", value: event.first_observed_at ? formatDate(event.first_observed_at, true) : "—" },
            { label: "마지막 변경 / Last updated", value: event.updated_at ? formatDate(event.updated_at, true) : "—" },
            { label: "기한 / Deadline", value: event.deadline_at ? formatDate(event.deadline_at, true) : "—" },
            { label: "사건 ID / Event ID", value: event.event_id || eventId },
            { label: "언어 / Language", value: event.original_language || "—" },
            { label: "제목 출처 / Title source", value: label("titleProvenance", event.title_provenance) },
            { label: "수집 범위 / Coverage", value: label("coverageMode", eventCoverageMode(event)) }
          ]),
          element("section", { className: "section-block" }, [
            element("h2", { text: "당사자 / Parties" }),
            actors.length ? actorList : element("p", { text: "등록된 당사자가 없습니다." })
          ])
        ])
      ])
    );
  }

  function voteCard(vote) {
    return element("article", { className: "vote-card" }, [
      element("div", { className: "badge-row" }, [badge(vote.result, "outcome")]),
      sourceNode("h3", vote.agenda_title, vote.original_language),
      metadata([
        { value: formatDate(vote.meeting_at, true) },
        vote.agenda_no ? { value: `의안 ${vote.agenda_no}` } : null,
        vote.recommendation_source ? { value: vote.recommendation_source } : null
      ]),
      vote.recommendation ? sourceNode("p", vote.recommendation, vote.original_language) : null,
      (vote.votes_for !== null && vote.votes_for !== undefined) ? element("p", { text: `찬성 ${vote.votes_for}% · 반대 ${vote.votes_against || 0}% · 기권 ${vote.votes_abstain || 0}%`, className: "field-hint" }) : null
    ].filter(Boolean));
  }

  async function renderCampaign(campaignId, signal) {
    if (!validEntityId(campaignId)) throw new ApiError("Invalid campaign ID", "invalid_campaign_id", 400);
    const payload = await request(`/campaigns/${encodeURIComponent(campaignId)}`, { signal });
    const data = payload.data || {};
    const campaign = data.campaign || {};
    if (!isPublicCampaign(campaign)) throw new ApiError("Signal campaigns are not public", "campaign_not_found", 404);
    const entries = Array.isArray(data.timeline) ? data.timeline : [];
    const votes = Array.isArray(data.votes) ? data.votes : [];
    const commitments = Array.isArray(data.commitments) ? data.commitments : [];
    app.replaceChildren(
      pageHeader(
        label("campaignStage", campaign.stage),
        campaign.title || campaignId,
        `${campaign.company_name || campaign.company_id || ""}${campaign.lead_actor_name ? ` · ${campaign.lead_actor_name}` : ""}`,
        [
          campaign.company_id ? routeLink("기업 기록 / Company", `#/companies/${encodeURIComponent(campaign.company_id)}`, { className: "text-link" }) : null,
          routeLink("정정·답변 / Feedback", `#/feedback?entity_type=campaign&entity_id=${encodeURIComponent(campaignId)}`, { className: "text-link" })
        ].filter(Boolean),
        campaign.original_language
      ),
      element("div", { className: "detail-grid" }, [
        element("div", {}, [
          campaign.demand_text ? element("section", {}, [
            sectionHeading("요구사항 / Demands"),
            sourceNode("p", campaign.demand_text, campaign.original_language, "long-text")
          ]) : null,
          element("section", { className: "section-block" }, [
            sectionHeading("캠페인 이력 / Timeline", `${entries.length}개`),
            entries.length ? timeline(entries) : emptyState("등록된 이력이 없습니다.", "공개된 캠페인 진행 이력이 추가되면 표시됩니다.")
          ]),
          element("section", { className: "section-block" }, [
            sectionHeading("주주제안·표결 / Proposals & votes", `${votes.length}개`),
            votes.length ? element("div", { className: "data-list" }, votes.map(voteCard)) : emptyState("표결 기록이 없습니다.", "주총 결과가 확인되면 표시됩니다.")
          ]),
          element("section", { className: "section-block" }, [
            sectionHeading("합의·이행 / Outcomes", `${commitments.length}개`),
            commitments.length ? element("div", { className: "data-list" }, commitments.map(commitmentCard)) : emptyState("이행 기록이 없습니다.", "합의 또는 회사 약속의 후속 조치를 추적합니다.")
          ])
        ].filter(Boolean)),
        element("aside", { className: "detail-sidebar", attrs: { "aria-label": "캠페인 상태 / Campaign status" } }, [
          element("div", { className: "badge-row" }, [badge(campaign.stage, "campaignStage"), campaign.outcome ? badge(campaign.outcome, "outcome") : null].filter(Boolean)),
          facts([
            { label: "대상 / Company", node: element("dd", {}, [routeLink(campaign.company_name || campaign.company_id, `#/companies/${encodeURIComponent(campaign.company_id || "")}`)]) },
            campaign.lead_actor_id ? { label: "제안 주체 / Lead", node: element("dd", {}, [routeLink(campaign.lead_actor_name || campaign.lead_actor_id, `#/actors/${encodeURIComponent(campaign.lead_actor_id)}`)]) } : { label: "제안 주체 / Lead", value: campaign.lead_actor_name },
            { label: "시작 / Started", value: formatDate(campaign.started_at, false) },
            { label: "종료 / Ended", value: campaign.ended_at ? formatDate(campaign.ended_at, false) : "진행 중 / Active" },
            { label: "캠페인 ID", value: campaign.campaign_id || campaignId }
          ])
        ])
      ])
    );
  }

  const DOCUMENT_BODY_CHUNK_BYTES = 65_536;

  function documentBodyPage(documentItem, requestedOffset) {
    const body = String(documentItem.body_text || documentItem.body_excerpt || "");
    const page = documentItem.body_page && typeof documentItem.body_page === "object" ? documentItem.body_page : {};
    const truncated = Boolean(documentItem.body_truncated || page.has_more);
    let nextOffset = Number(documentItem.body_next_offset ?? page.next_offset);
    if (!Number.isFinite(nextOffset) || nextOffset <= requestedOffset) {
      const returnedBytes = Number(documentItem.body_bytes_returned ?? page.returned_bytes);
      const measuredBytes = typeof TextEncoder === "function" ? new TextEncoder().encode(body).byteLength : 0;
      const increment = Number.isFinite(returnedBytes) && returnedBytes > 0 ? returnedBytes : measuredBytes;
      nextOffset = truncated && increment > 0 ? requestedOffset + increment : 0;
    }
    return { body, truncated, nextOffset };
  }

  async function renderDocument(documentId, signal) {
    if (!validEntityId(documentId)) throw new ApiError("Invalid document ID", "invalid_document_id", 400);
    const payload = await request(`/documents/${encodeURIComponent(documentId)}`, { params: { include: "body", body_offset: 0, body_limit_bytes: DOCUMENT_BODY_CHUNK_BYTES }, signal });
    const documentItem = payload.data || {};
    if (!isPublicDocument(documentItem)) throw new ApiError("Document is not public", "document_not_found", 404);
    const firstPage = documentBodyPage(documentItem, 0);
    const bodyNode = sourceNode("div", firstPage.body || "본문이 공개되지 않았습니다. / Body not available.", documentItem.original_language, "long-text");
    const bodyStatus = element("p", {
      text: firstPage.truncated ? "본문 일부를 표시하고 있습니다. / Body truncated; more is available." : "",
      className: "body-page-status",
      attrs: { role: "status", "aria-live": "polite" }
    });
    const bodySection = element("section", { attrs: { "aria-labelledby": "document-body" } }, [
      element("h2", { text: "원문 / Source text", attrs: { id: "document-body" } }),
      bodyNode,
      bodyStatus
    ]);
    if (firstPage.truncated && firstPage.nextOffset > 0) {
      let nextOffset = firstPage.nextOffset;
      const button = element("button", { text: "다음 본문 불러오기 / Load next body page", attrs: { type: "button" } });
      button.addEventListener("click", async () => {
        button.disabled = true;
        const requestedOffset = nextOffset;
        try {
          const more = await request(`/documents/${encodeURIComponent(documentId)}`, { params: { include: "body", body_offset: requestedOffset, body_limit_bytes: DOCUMENT_BODY_CHUNK_BYTES } });
          const nextDocument = more.data || {};
          const nextPage = documentBodyPage(nextDocument, requestedOffset);
          if (nextPage.body) bodyNode.append(document.createTextNode(nextPage.body));
          nextOffset = nextPage.nextOffset;
          bodyStatus.textContent = nextPage.truncated ? "본문 일부를 표시하고 있습니다. / Body truncated; more is available." : "본문 전체를 불러왔습니다. / Complete body loaded.";
          if (!nextPage.truncated || !nextOffset) button.remove();
        } catch (error) {
          bodyStatus.textContent = errorMessage(error && error.code);
        } finally {
          if (button.isConnected) button.disabled = false;
        }
      });
      bodySection.append(element("div", { className: "load-more" }, [button]));
    }
    app.replaceChildren(
      pageHeader(
        label("sourceClass", documentItem.source_class),
        documentItem.title || documentId,
        `${documentItem.company_name || ""} · ${formatDate(documentItem.published_at, true)}`,
        [
          documentItem.original_url ? externalLink("원문 열기 / Open source", documentItem.original_url, "text-link") : null,
          routeLink("정정 요청 / Feedback", `#/feedback?entity_type=document&entity_id=${encodeURIComponent(documentId)}`, { className: "text-link" })
        ].filter(Boolean),
        documentItem.original_language
      ),
      element("div", { className: "detail-grid" }, [
        bodySection,
        element("aside", { className: "detail-sidebar" }, [
          element("div", { className: "badge-row" }, [badge(documentItem.verification_status, "verification")]),
          facts([
            { label: "문서 유형 / Type", value: documentItem.document_type },
            { label: "외부 ID / External ID", value: documentItem.external_id },
            { label: "버전 / Version", value: documentItem.version_no },
            { label: "언어 / Language", value: documentItem.original_language },
            { label: "본문 일부 / Truncated", value: firstPage.truncated ? "예 / Yes" : "아니오 / No" },
            { label: "수집 / Retrieved", value: formatDate(documentItem.retrieved_at, true) }
          ])
        ])
      ])
    );
  }

  function isoDate(date) {
    return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
  }

  function normalizeCalendarItem(item, version) {
    if (version !== "v2") return item;
    const event = normalizeEvent(item);
    return {
      ...event,
      item_type: "event",
      entity_id: event.event_id,
      category: event.event_family,
      scheduled_at: event.deadline_at
    };
  }

  function calendarItem(item) {
    const date = normalizedDate(item.scheduled_at);
    const dateLabel = date ? new Intl.DateTimeFormat("ko-KR", { month: "short", day: "numeric", weekday: "short", timeZone: "Asia/Seoul" }).format(date) : "—";
    const title = item.item_type === "event" && validEntityId(item.entity_id)
      ? routeLink(item.title || "사건", `#/events/${encodeURIComponent(item.entity_id)}`, { lang: item.original_language })
      : sourceNode("span", item.title, item.original_language);
    return element("article", { className: "calendar-item" }, [
      element("div", { text: dateLabel, className: "calendar-date" }),
      element("div", {}, [
        element("div", { className: "badge-row" }, [badge(item.category, "eventType")]),
        element("h3", {}, [title]),
        metadata([
          { node: issuerOrCompanyLink(item), value: issuerOrCompanyName(item) },
          { value: formatDate(item.scheduled_at, true) }
        ])
      ])
    ]);
  }

  async function renderCalendar(query, signal) {
    const now = new Date();
    const future = new Date(now.getTime() + 90 * 86400000);
    const from = /^\d{4}-\d{2}-\d{2}$/.test(query.get("from") || "") ? query.get("from") : isoDate(now);
    const to = /^\d{4}-\d{2}-\d{2}$/.test(query.get("to") || "") ? query.get("to") : isoDate(future);
    const v2Params = { ...v2EventFilterParams(query), from, to, page: 1, limit: 100 };
    const v1Params = { from, to, page: 1, limit: 100 };
    const result = await terminalRequest("/calendar", {
      params: v2Params,
      signal,
      fallback: () => request("/calendar", { params: v1Params, signal })
    });
    const payload = result.payload;
    const rawItems = result.version === "v2"
      ? payload.data.items
      : Array.isArray(payload.data) ? payload.data : [];
    const items = rawItems
      .map((item) => normalizeCalendarItem(item, result.version))
      .filter((item) => item.verification_status !== "signal");
    const fromInput = element("input", { attrs: { id: "calendar-from", type: "date", name: "from", value: from, required: "required" } });
    const toInput = element("input", { attrs: { id: "calendar-to", type: "date", name: "to", value: to, required: "required" } });
    const form = element("form", { className: "field-grid" }, [
      element("div", { className: "field" }, [element("label", { text: "시작 / From", attrs: { for: "calendar-from" } }), fromInput]),
      element("div", { className: "field" }, [element("label", { text: "종료 / To", attrs: { for: "calendar-to" } }), toInput]),
      element("div", { className: "field--full" }, [element("button", { text: "기간 적용 / Apply", attrs: { type: "submit" } })])
    ]);
    form.addEventListener("submit", (event) => {
      event.preventDefault();
      const values = new URLSearchParams(query);
      values.set("from", fromInput.value);
      values.set("to", toInput.value);
      window.location.hash = `#/calendar?${values.toString()}`;
    });
    const groups = new Map();
    items.forEach((item) => {
      const month = String(item.scheduled_at || "").slice(0, 7) || "기타";
      if (!groups.has(month)) groups.set(month, []);
      groups.get(month).push(item);
    });
    const groupNodes = [...groups.entries()].map(([month, values]) => element("section", { className: "calendar-group" }, [
      element("h2", { text: month }),
      element("div", { className: "data-list" }, values.map(calendarItem))
    ]));
    const calendarResults = element("div", { attrs: { id: "calendar-results" } }, groupNodes);
    const pagination = result.version === "v2" ? payload.meta : payload.pagination;
    if (items.length) {
      addLoadMore(calendarResults, pagination, async (cursor, cursorKind) => {
        let moreItems;
        let morePagination;
        if (result.version === "v2") {
          const more = await terminalRequest("/calendar", {
            params: continuationParams(v2Params, cursor, cursorKind)
          });
          moreItems = more.payload.data.items.map((item) => normalizeCalendarItem(item, "v2"));
          morePagination = more.payload.meta;
        } else {
          const more = await request("/calendar", { params: { ...v1Params, page: cursor } });
          moreItems = (Array.isArray(more.data) ? more.data : []).map((item) => normalizeCalendarItem(item, "v1"));
          morePagination = more.pagination;
        }
        moreItems
          .filter((item) => item.verification_status !== "signal")
          .forEach((item) => calendarResults.append(calendarItem(item)));
        return morePagination;
      });
    }
    app.replaceChildren(
      pageHeader(
        "CALENDAR / 캘린더",
        result.version === "v2" ? "글로벌 주총·공개매수·주요 기한" : "주총·공개매수·주요 기한",
        "사건 기한과 의안 표결 일정을 시간순으로 확인합니다."
      ),
      element("section", { className: "filter-form", attrs: { "aria-label": "캘린더 기간" } }, [form]),
      items.length ? calendarResults : emptyState("해당 기간의 일정이 없습니다.", "기간을 넓혀 다시 확인해 주세요.")
    );
  }

  function resultRoute(item) {
    if (item.kind === "company") return `#/companies/${encodeURIComponent(item.entity_id || "")}`;
    if (item.kind === "issuer") return `#/issuers/${encodeURIComponent(item.entity_id || "")}`;
    if (item.kind === "actor") return `#/actors/${encodeURIComponent(item.entity_id || "")}`;
    if (item.kind === "event") return `#/events/${encodeURIComponent(item.entity_id || "")}`;
    if (item.kind === "campaign") return `#/campaigns/${encodeURIComponent(item.entity_id || "")}`;
    if (item.kind === "document") return `#/documents/${encodeURIComponent(item.entity_id || "")}`;
    return "";
  }

  function searchResult(item) {
    const route = resultRoute(item);
    const title = route
      ? routeLink(item.title || item.entity_id || "결과", route, { lang: item.original_language })
      : sourceNode("span", item.title || item.entity_id || "결과", item.original_language);
    const subtitle = item.kind === "actor" ? label("actorType", item.subtitle)
      : item.kind === "event" ? label("eventType", item.subtitle)
        : item.kind === "campaign" ? label("campaignStage", item.subtitle)
          : item.kind === "document" ? label("sourceClass", item.subtitle)
            : item.subtitle;
    return element("article", { className: "search-result" }, [
      element("div", { className: "badge-row" }, [badge(item.kind, "kind")]),
      element("h3", {}, [title]),
      metadata([
        { value: subtitle || "" },
        { value: item.issuer_id || item.company_id || "" },
        { value: formatDate(item.occurred_at || item.sort_at, false) }
      ])
    ]);
  }

  function normalizeSearchResult(item, version) {
    if (version !== "v2") {
      if (!item || item.kind !== "event") return item;
      return {
        ...item,
        subtitle: canonicalEventFamily(
          item.subtitle || item.event_family || item.event_type,
          item.verification_status,
          item.change_type
        )
      };
    }
    const event = normalizeEvent(item);
    return {
      ...event,
      kind: "event",
      entity_id: event.event_id,
      subtitle: event.event_family,
      sort_at: event.updated_at || event.occurred_at
    };
  }

  async function renderSearch(query, signal) {
    const q = String(query.get("q") || "").trim();
    const input = element("input", { attrs: { type: "search", name: "q", value: q, minlength: "2", maxlength: "100", required: "required", placeholder: "기업·행동주주·사건·공시 검색", "aria-label": "통합 검색어 / Search query" } });
    const form = element("form", { className: "inline-form", attrs: { role: "search" } }, [input, element("button", { text: "검색 / Search", attrs: { type: "submit" } })]);
    form.addEventListener("submit", (event) => {
      event.preventDefault();
      if (input.reportValidity()) window.location.hash = `#/search?q=${encodeURIComponent(input.value.trim())}`;
    });
    let results = [];
    let resultPayload = null;
    let resultVersion = "";
    if (q.length >= 2) {
      const result = await terminalRequest("/search", {
        params: { q, page: 1, limit: 50 },
        signal,
        fallback: () => request("/search", { params: { q, page: 1, limit: 50 }, signal })
      });
      resultPayload = result.payload;
      resultVersion = result.version;
      const rawResults = resultVersion === "v2"
        ? resultPayload.data.items
        : Array.isArray(resultPayload.data) ? resultPayload.data : [];
      results = rawResults
        .map((item) => normalizeSearchResult(item, resultVersion))
        .filter((item) => item.kind !== "event" || item.verification_status !== "signal");
    }
    const resultList = element("div", { className: "data-list" }, results.map(searchResult));
    const resultSection = q.length >= 2 ? element("section", { className: "section-block" }, [
      sectionHeading(`“${q}” 검색 결과`, `${results.length}개 표시`),
      results.length ? resultList : emptyState("검색 결과가 없습니다.", "다른 회사명, 사건명 또는 공시 제목을 입력해 주세요.")
    ]) : emptyState("검색어를 입력해 주세요.", "두 글자 이상 입력하면 공개 기록 전체를 검색합니다.");
    if (resultPayload && results.length) {
      addLoadMore(resultSection, resultVersion === "v2" ? resultPayload.meta : resultPayload.pagination, async (cursor, cursorKind) => {
        const moreResult = await terminalRequest("/search", {
          params: continuationParams({ q, page: 1, limit: 50 }, cursor, cursorKind),
          fallback: () => request("/search", { params: { q, page: cursor, limit: 50 } })
        });
        const morePayload = moreResult.payload;
        const moreItems = moreResult.version === "v2"
          ? morePayload.data.items
          : Array.isArray(morePayload.data) ? morePayload.data : [];
        moreItems
          .map((item) => normalizeSearchResult(item, moreResult.version))
          .filter((item) => item.kind !== "event" || item.verification_status !== "signal")
          .forEach((item) => resultList.append(searchResult(item)));
        return moreResult.version === "v2" ? morePayload.meta : morePayload.pagination;
      });
    }
    app.replaceChildren(
      pageHeader("SEARCH / 검색", "통합 검색", "회사, 행동주주, 사건 유형, 공시와 캠페인을 한 번에 찾습니다."),
      element("section", { className: "filter-form" }, [form]),
      resultSection
    );
  }

  function selectField(labelText, name, values, selected) {
    const select = element("select", { attrs: { name, id: `feedback-${name}` } });
    values.forEach(([value, text]) => {
      const option = element("option", { text, attrs: { value } });
      if (value === selected) option.selected = true;
      select.append(option);
    });
    return element("div", { className: "field" }, [element("label", { text: labelText, attrs: { for: `feedback-${name}` } }), select]);
  }

  function feedbackError(code) {
    const messages = {
      invalid_feedback_type: "접수 유형을 확인해 주세요. / Check feedback type.",
      invalid_message_length: "내용은 10자 이상 10,000자 이하로 입력해 주세요. / Message must be 10–10,000 characters.",
      entity_type_and_id_required_together: "대상 유형과 ID를 함께 입력해 주세요. / Entity type and ID are required together.",
      invalid_entity_reference: "대상 기록을 확인해 주세요. / Check the referenced record.",
      feedback_rate_limited: "잠시 후 다시 시도해 주세요. / Please try again later."
    };
    return messages[code] || "접수 중 오류가 발생했습니다. 잠시 후 다시 시도해 주세요. / Submission failed. Please try again later.";
  }

  function renderFeedback(query) {
    const entityType = String(query.get("entity_type") || "");
    const entityId = String(query.get("entity_id") || "");
    const requestedFeedbackType = String(query.get("feedback_type") || "");
    const feedbackType = ["correction", "right_of_reply", "source_rights", "general"].includes(requestedFeedbackType) ? requestedFeedbackType : "correction";
    const typeField = selectField("접수 유형 / Request type", "feedback_type", [
      ["correction", "정정 요청 / Correction"],
      ["right_of_reply", "당사자 답변 / Right of reply"],
      ["source_rights", "소스 권한 / Source rights"],
      ["general", "일반 문의 / General"]
    ], feedbackType);
    const entityField = selectField("대상 유형 / Entity type", "entity_type", [
      ["", "선택 안 함 / None"],
      ["company", "기업 / Company"],
      ["actor", "당사자 / Actor"],
      ["event", "사건 / Event"],
      ["campaign", "캠페인 / Campaign"],
      ["document", "문서 / Document"]
    ], entityType);
    const idInput = element("input", { attrs: { id: "feedback-entity-id", name: "entity_id", value: entityId, maxlength: "96", pattern: "[A-Za-z0-9_.:-]{1,96}" } });
    const nameInput = element("input", { attrs: { id: "feedback-name", name: "submitter_name", maxlength: "191", autocomplete: "name" } });
    const contactInput = element("input", { attrs: { id: "feedback-contact", name: "submitter_contact", maxlength: "320", autocomplete: "email", placeholder: "email or contact" } });
    const messageInput = element("textarea", { attrs: { id: "feedback-message", name: "message", minlength: "10", maxlength: "10000", required: "required" } });
    const evidenceInput = element("textarea", { attrs: { id: "feedback-evidence", name: "evidence_urls", maxlength: "10000", placeholder: "https://…\nhttps://…" } });
    const websiteInput = element("input", { attrs: { id: "feedback-website", name: "website", tabindex: "-1", autocomplete: "off" } });
    const status = element("p", { className: "form-status", attrs: { role: "status", "aria-live": "polite" } });
    const submit = element("button", { text: "비공개 접수 / Submit privately", attrs: { type: "submit" } });
    const form = element("form", { className: "feedback-form" }, [
      element("div", { className: "field-grid" }, [
        typeField,
        entityField,
        element("div", { className: "field field--full" }, [element("label", { text: "대상 ID / Entity ID", attrs: { for: "feedback-entity-id" } }), idInput]),
        element("div", { className: "field" }, [element("label", { text: "이름 / Name", attrs: { for: "feedback-name" } }), nameInput]),
        element("div", { className: "field" }, [element("label", { text: "연락처 / Contact", attrs: { for: "feedback-contact" } }), contactInput]),
        element("div", { className: "field field--full" }, [element("label", { text: "요청 내용 / Message", attrs: { for: "feedback-message" } }), messageInput, element("span", { text: "사실관계와 원하는 정정 또는 답변 내용을 구체적으로 적어 주세요.", className: "field-hint" })]),
        element("div", { className: "field field--full" }, [element("label", { text: "근거 URL / Evidence URLs", attrs: { for: "feedback-evidence" } }), evidenceInput, element("span", { text: "한 줄에 하나씩, 최대 10개", className: "field-hint" })]),
        element("div", { className: "sr-only", attrs: { "aria-hidden": "true" } }, [element("label", { text: "Website", attrs: { for: "feedback-website" } }), websiteInput])
      ]),
      element("p", { text: "제출 내용은 자동 공개되지 않으며 편집 검수 전까지 비공개로 보관됩니다. / Submissions stay private pending editorial review.", className: "form-note" }),
      element("div", { className: "form-actions" }, [submit, status])
    ]);
    const applyReplyRequirements = () => {
      const required = form.elements.feedback_type.value === "right_of_reply";
      nameInput.required = required;
      contactInput.required = required;
      nameInput.setAttribute("aria-required", String(required));
      contactInput.setAttribute("aria-required", String(required));
    };
    form.elements.feedback_type.addEventListener("change", applyReplyRequirements);
    applyReplyRequirements();
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!form.reportValidity()) return;
      const selectedType = form.elements.entity_type.value;
      const selectedId = idInput.value.trim();
      if ((selectedType && !selectedId) || (!selectedType && selectedId)) {
        status.textContent = feedbackError("entity_type_and_id_required_together");
        return;
      }
      const evidenceUrls = evidenceInput.value.split(/\r?\n/).map((value) => value.trim()).filter(Boolean).slice(0, 10);
      const body = {
        feedback_type: form.elements.feedback_type.value,
        message: messageInput.value.trim(),
        submitter_name: nameInput.value.trim(),
        submitter_contact: contactInput.value.trim(),
        evidence_urls: evidenceUrls,
        website: websiteInput.value
      };
      if (selectedType && selectedId) {
        body.entity_type = selectedType;
        body.entity_id = selectedId;
      }
      submit.disabled = true;
      status.textContent = "접수 중 / Submitting…";
      try {
        const payload = await request("/feedback", { method: "POST", body });
        form.reset();
        applyReplyRequirements();
        status.textContent = `접수되었습니다. 검수 상태: ${payload.status || "pending"} / Submitted for private review.`;
      } catch (error) {
        status.textContent = feedbackError(error.code);
      } finally {
        submit.disabled = false;
      }
    });
    app.replaceChildren(
      pageHeader("FEEDBACK / 정정·답변", "정정 요청과 당사자 답변", "사실 오류, 당사자 입장, 소스 권한 문제를 편집팀에 비공개로 제출합니다."),
      form
    );
  }

  function closeEventDrawer() {
    if (!drawerShell || drawerShell.hidden) return;
    if (drawerController) drawerController.abort();
    drawerController = null;
    drawerShell.hidden = true;
    document.body.classList.remove("drawer-open");
    if (drawerTrigger && drawerTrigger.isConnected) drawerTrigger.focus();
    drawerTrigger = null;
    announcer.textContent = "사건 상세를 닫았습니다.";
  }

  function drawerDocuments(documents) {
    const publicDocuments = (Array.isArray(documents) ? documents : []).filter(isPublicDocument).slice(0, 10);
    if (!publicDocuments.length) return null;
    return element("section", { className: "drawer-section", attrs: { "aria-labelledby": "drawer-sources" } }, [
      element("h3", { text: "공식 근거·보도 / Sources", attrs: { id: "drawer-sources" } }),
      element("div", { className: "data-list" }, publicDocuments.map(documentRow))
    ]);
  }

  function drawerClaims(claims) {
    const rows = Array.isArray(claims) ? claims.slice(0, 10) : [];
    if (!rows.length) return null;
    return element("section", { className: "drawer-section", attrs: { "aria-labelledby": "drawer-claims" } }, [
      element("h3", { text: "주장·반론·공식 사실 / Claims", attrs: { id: "drawer-claims" } }),
      element("div", { className: "data-list" }, rows.map(claimCard))
    ]);
  }

  async function openEventDrawer(eventId, trigger) {
    if (!drawerShell || !drawer || !drawerContent || !drawerKicker || !validEntityId(eventId)) return;
    if (drawerController) drawerController.abort();
    drawerController = new AbortController();
    drawerTrigger = trigger || document.activeElement;
    drawerShell.hidden = false;
    document.body.classList.add("drawer-open");
    drawerKicker.textContent = "공식 근거를 불러오는 중입니다.";
    drawerContent.replaceChildren(
      element("h2", { text: "사건 상세를 불러오고 있습니다.", attrs: { id: "drawer-title" } }),
      element("p", { text: "원문 제목과 근거 상태를 확인하고 있습니다.", className: "drawer-summary" })
    );
    drawer.focus({ preventScroll: true });
    try {
      const result = await terminalRequest(`/events/${encodeURIComponent(eventId)}`, {
        signal: drawerController.signal,
        fallback: () => request(`/events/${encodeURIComponent(eventId)}`, { signal: drawerController.signal })
      });
      const envelope = result.payload || {};
      const data = envelope.data || {};
      const detail = data.event ? data : { event: data };
      const event = normalizeEvent(detail.event || {});
      if (!isPublicEvent(event)) throw new ApiError("Signal events are not public", "event_not_found", 404);
      drawerKicker.textContent = `${issuerOrCompanyName(event)} · ${label("eventType", event.event_type)}`;
      const sourceAction = event.source_url
        ? externalLink("원문 근거 / Open source", event.source_url, "text-link")
        : null;
      const heading = sourceNode("h2", event.title || eventId, event.original_language);
      heading.id = "drawer-title";
      drawerContent.replaceChildren(
        element("div", { className: "badge-row" }, [
          badge(event.verification_status, "verification"),
          sourceCoverageBadge(event)
        ]),
        heading,
        event.change_summary
          ? sourceNode("p", event.change_summary, event.original_language, "drawer-summary")
          : element("p", { text: "확인된 사실과 연결 근거를 아래에서 확인하세요.", className: "drawer-summary" }),
        facts([
          {
            label: "발행사 / Issuer",
            value: issuerOrCompanyName(event),
            node: element("dd", {}, [issuerOrCompanyLink(event)])
          },
          { label: "시장 / Market", value: [event.ticker, event.market || marketForEvent(event)].filter(Boolean).join(" · ") },
          { label: "당사자·역할 / Actor & role", value: eventActorSummary(event) },
          { label: "현재 상태 / Status", value: event.current_status },
          { label: "발생 / Occurred", value: formatDate(event.occurred_at, true) },
          { label: "접수 / Filed", value: event.filed_at ? formatDate(event.filed_at, true) : "—" },
          { label: "최초 관측 / First observed", value: event.first_observed_at ? formatDate(event.first_observed_at, true) : "—" },
          { label: "마지막 변경 / Last updated", value: event.updated_at ? formatDate(event.updated_at, true) : "—" },
          { label: "기한 / Deadline", value: event.deadline_at ? formatDate(event.deadline_at, true) : "—" },
          { label: "언어 / Language", value: event.original_language || "—" },
          { label: "제목 출처 / Title source", value: label("titleProvenance", event.title_provenance) },
          { label: "수집 범위 / Coverage", value: label("coverageMode", eventCoverageMode(event)) }
        ]),
        element("div", { className: "drawer-actions" }, [
          routeLink("전체 사건 기록 / Full record", `#/events/${encodeURIComponent(eventId)}`, { className: "button" }),
          sourceAction,
          routeLink("정정·답변 / Feedback", `#/feedback?entity_type=event&entity_id=${encodeURIComponent(eventId)}`, { className: "text-link" })
        ].filter(Boolean)),
        drawerClaims(detail.claims),
        drawerDocuments(detail.documents)
      );
      announcer.textContent = `${event.title || "사건"} 상세를 열었습니다.`;
    } catch (error) {
      if (error && error.name === "AbortError") return;
      drawerKicker.textContent = "DATA UNAVAILABLE";
      drawerContent.replaceChildren(
        element("h2", { text: "사건 상세를 불러오지 못했습니다.", attrs: { id: "drawer-title" } }),
        element("p", { text: errorMessage(error && error.code), className: "drawer-summary" }),
        routeLink("전체 사건 기록으로 이동", `#/events/${encodeURIComponent(eventId)}`, { className: "text-link" })
      );
    }
  }

  function visibleEventLinks() {
    return [...document.querySelectorAll("[data-event-drawer]")]
      .filter((link) => link.dataset.eventDrawer && link.getClientRects().length > 0);
  }

  function isTypingTarget(target) {
    return target instanceof HTMLInputElement
      || target instanceof HTMLTextAreaElement
      || target instanceof HTMLSelectElement
      || (target instanceof HTMLElement && target.isContentEditable);
  }

  function installTerminalInteractions() {
    if (mobileMenuToggle && mobileMenu) {
      mobileMenuToggle.addEventListener("click", toggleMobileMenu);
      mobileMenu.addEventListener("click", (event) => {
        if (event.target instanceof Element && event.target.closest("a[href]")) {
          closeMobileMenu({ restoreFocus: false });
        }
      });
    }
    if (globalSearchForm && globalSearchInput) {
      globalSearchForm.addEventListener("submit", (event) => {
        event.preventDefault();
        const value = globalSearchInput.value.trim();
        if (value.length >= 2) window.location.hash = `#/search?q=${encodeURIComponent(value)}`;
      });
    }
    document.addEventListener("click", (event) => {
      const liveLink = event.target instanceof Element
        ? event.target.closest("[data-nav='live']")
        : null;
      if (
        liveLink
        && !event.defaultPrevented
        && event.button === 0
        && !event.metaKey
        && !event.ctrlKey
        && !event.shiftKey
        && !event.altKey
      ) {
        event.preventDefault();
        const destination = `#/today?market=${encodeURIComponent(currentMarket(parseRoute().query))}&view=live`;
        if (window.location.hash === destination) {
          const liveSection = document.getElementById("terminal-live");
          if (liveSection) liveSection.scrollIntoView({ block: "start", behavior: "smooth" });
        } else {
          window.location.hash = destination;
        }
        return;
      }
      const link = event.target instanceof Element ? event.target.closest("[data-event-drawer]") : null;
      if (!link || event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
      const eventId = String(link.dataset.eventDrawer || "");
      if (!validEntityId(eventId)) return;
      event.preventDefault();
      void openEventDrawer(eventId, link);
    });
    document.querySelectorAll("[data-drawer-close]").forEach((button) => {
      button.addEventListener("click", closeEventDrawer);
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && mobileMenu && !mobileMenu.hidden) {
        event.preventDefault();
        closeMobileMenu();
        return;
      }
      if (event.key === "Escape" && drawerShell && !drawerShell.hidden) {
        event.preventDefault();
        closeEventDrawer();
        return;
      }
      if (event.key === "Escape" && document.body.classList.contains("filter-sheet-open")) {
        event.preventDefault();
        closeTerminalFilterSheet();
        return;
      }
      if (event.key === "Tab" && document.body.classList.contains("filter-sheet-open")) {
        const filterPanel = document.getElementById("terminal-filters");
        if (!filterPanel) return;
        const focusable = [...filterPanel.querySelectorAll("a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex='-1'])")]
          .filter((node) => node.getClientRects().length > 0);
        if (!focusable.length) {
          event.preventDefault();
          filterPanel.focus();
          return;
        }
        const first = focusable[0];
        const last = focusable[focusable.length - 1];
        if (event.shiftKey && document.activeElement === first) {
          event.preventDefault();
          last.focus();
        } else if (!event.shiftKey && document.activeElement === last) {
          event.preventDefault();
          first.focus();
        }
        return;
      }
      if (event.key === "Tab" && drawerShell && !drawerShell.hidden && drawer) {
        const focusable = [...drawer.querySelectorAll("a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex='-1'])")]
          .filter((node) => node.getClientRects().length > 0);
        if (!focusable.length) {
          event.preventDefault();
          drawer.focus();
          return;
        }
        const first = focusable[0];
        const last = focusable[focusable.length - 1];
        if (event.shiftKey && document.activeElement === first) {
          event.preventDefault();
          last.focus();
        } else if (!event.shiftKey && document.activeElement === last) {
          event.preventDefault();
          first.focus();
        }
        return;
      }
      if (isTypingTarget(event.target)) return;
      if (event.key === "/" && globalSearchInput) {
        event.preventDefault();
        globalSearchInput.focus();
        globalSearchInput.select();
        return;
      }
      if (!["j", "J", "k", "K", "Enter"].includes(event.key)) return;
      const links = visibleEventLinks();
      if (!links.length) return;
      const current = links.indexOf(document.activeElement);
      if (event.key === "Enter" && current >= 0) {
        event.preventDefault();
        links[current].click();
        return;
      }
      if (!["j", "J", "k", "K"].includes(event.key)) return;
      event.preventDefault();
      const direction = event.key.toLowerCase() === "j" ? 1 : -1;
      const nextIndex = current < 0
        ? direction > 0 ? 0 : links.length - 1
        : (current + direction + links.length) % links.length;
      links[nextIndex].focus({ preventScroll: true });
      links[nextIndex].scrollIntoView({ block: "center", behavior: "smooth" });
    });
  }

  function renderNotFound() {
    app.replaceChildren(element("section", { className: "error-state" }, [
      element("p", { text: "404", className: "eyebrow" }),
      element("h1", { text: "페이지를 찾을 수 없습니다." }),
      element("p", { text: "주소를 확인하거나 오늘 기록으로 돌아가 주세요. / Page not found." }),
      routeLink("오늘 기록 / Today", "#/today", { className: "button" })
    ]));
  }

  function errorMessage(code) {
    const known = {
      company_not_found: "기업 기록을 찾을 수 없습니다. / Company not found.",
      issuer_not_found: "발행사 기록을 찾을 수 없습니다. / Issuer not found.",
      actor_not_found: "당사자 기록을 찾을 수 없습니다. / Actor not found.",
      event_not_found: "사건 기록을 찾을 수 없습니다. / Event not found.",
      campaign_not_found: "캠페인 기록을 찾을 수 없습니다. / Campaign not found.",
      document_not_found: "공개 가능한 문서를 찾을 수 없습니다. / Public document not found.",
      response_budget_exceeded: "응답 범위를 줄여 다시 시도해 주세요. / Narrow the request and try again."
    };
    return known[code] || "기록을 불러오지 못했습니다. 잠시 후 다시 시도해 주세요. / Unable to load records.";
  }

  function renderError(error) {
    const code = error && error.code ? error.code : "request_failed";
    app.replaceChildren(element("section", { className: "error-state" }, [
      element("p", { text: "DATA UNAVAILABLE", className: "eyebrow" }),
      element("h1", { text: "기록을 불러오지 못했습니다." }),
      element("p", { text: errorMessage(code) }),
      element("code", { text: code, className: "error-code" }),
      element("div", {}, [element("button", { text: "다시 시도 / Retry", attrs: { type: "button" } })])
    ]));
    const retry = app.querySelector("button");
    if (retry) retry.addEventListener("click", navigate);
  }

  function announceError(error) {
    announcer.textContent = errorMessage(error && error.code);
  }

  function parseRoute() {
    const raw = window.location.hash.startsWith("#/") ? window.location.hash.slice(1) : "/today";
    const question = raw.indexOf("?");
    const path = question >= 0 ? raw.slice(0, question) : raw;
    const query = new URLSearchParams(question >= 0 ? raw.slice(question + 1) : "");
    const segments = path.split("/").filter(Boolean).map((segment) => {
      try { return decodeURIComponent(segment); } catch (_error) { return ""; }
    });
    return { path, query, segments };
  }

  function activeNav(route) {
    const first = route.segments[0] || "today";
    const active = first === "today" && route.query.get("view") === "live"
      ? "live"
      : first === "documents" || first === "campaigns" || first === "actors"
        ? "events"
        : first === "issuers" ? "companies" : first;
    document.querySelectorAll("[data-nav]").forEach((link) => {
      if (link.dataset.nav === active) link.setAttribute("aria-current", "page");
      else link.removeAttribute("aria-current");
    });
  }

  function renderLoading() {
    app.setAttribute("aria-busy", "true");
    app.replaceChildren(element("section", { className: "loading-state" }, [
      element("p", { text: "LOADING / 불러오는 중", className: "eyebrow" }),
      element("h1", { text: "공개 기록을 확인하고 있습니다." }),
      element("p", { text: "공식 근거와 최신 상태를 불러옵니다. / Loading evidence and status." })
    ]));
  }

  function finishNavigation(route) {
    app.removeAttribute("aria-busy");
    activeNav(route);
    const heading = app.querySelector("h1");
    const title = heading ? heading.textContent : "거버넌스 인텔리전스";
    document.title = `${title} | BSIDE Market Intelligence`;
    announcer.textContent = `${title} 페이지를 열었습니다.`;
    if (heading) {
      heading.setAttribute("tabindex", "-1");
      heading.focus({ preventScroll: true });
    }
    window.scrollTo({ top: 0, behavior: "auto" });
    if (
      route.segments[0] === "today"
      && route.query.get("view") === "live"
    ) {
      const liveSection = document.getElementById("terminal-live");
      if (liveSection) {
        liveSection.scrollIntoView({ block: "start", behavior: "auto" });
      }
    }
  }

  async function navigate() {
    if (!window.location.hash.startsWith("#/")) {
      window.location.hash = "#/today";
      return;
    }
    if (routeController) routeController.abort();
    if (drawerShell && !drawerShell.hidden) closeEventDrawer();
    closeMobileMenu({ restoreFocus: false });
    closeTerminalFilterSheet({ restoreFocus: false });
    routeController = new AbortController();
    const signal = routeController.signal;
    const route = parseRoute();
    renderLoading();
    try {
      const [first, second] = route.segments;
      if (!first || first === "today") await renderToday(route.query, signal);
      else if (first === "issuers" && second) await renderIssuer(second, signal);
      else if (first === "issuers") await renderIssuers(route.query, signal);
      else if (first === "companies" && second) await renderCompany(second, signal);
      else if (first === "companies") await renderCompanies(route.query, signal);
      else if (first === "events" && second) await renderEvent(second, signal);
      else if (first === "events") await renderEvents(route.query, signal);
      else if (first === "actors" && second) await renderActor(second, signal);
      else if (first === "campaigns" && second) await renderCampaign(second, signal);
      else if (first === "documents" && second) await renderDocument(second, signal);
      else if (first === "calendar") await renderCalendar(route.query, signal);
      else if (first === "search") await renderSearch(route.query, signal);
      else if (first === "revisions") await renderRevisions(route.query, signal);
      else if (first === "feedback") renderFeedback(route.query);
      else renderNotFound();
      finishNavigation(route);
    } catch (error) {
      if (error && error.name === "AbortError") return;
      renderError(error);
      finishNavigation(route);
    }
  }

  installTerminalInteractions();
  window.addEventListener("hashchange", navigate);
  navigate();
})();
