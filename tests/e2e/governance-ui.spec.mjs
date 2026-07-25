import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { readFile } from "node:fs/promises";

async function expectViewportPng(path, viewport) {
  const png = await readFile(path);
  expect(png.subarray(1, 4).toString("ascii")).toBe("PNG");
  expect(png.subarray(12, 16).toString("ascii")).toBe("IHDR");
  expect({
    width: png.readUInt32BE(16),
    height: png.readUInt32BE(20)
  }).toEqual(viewport);
}

async function stabilizeViewportScreenshot(page) {
  await page.mouse.move(1, 1);
  const previous = await page.evaluate(() => {
    if (document.activeElement instanceof HTMLElement) document.activeElement.blur();
    const root = document.documentElement;
    const saved = {
      value: root.style.getPropertyValue("scroll-behavior"),
      priority: root.style.getPropertyPriority("scroll-behavior")
    };
    root.style.setProperty("scroll-behavior", "auto", "important");
    root.scrollTop = 0;
    document.body.scrollTop = 0;
    window.scrollTo(0, 0);
    return saved;
  });
  await expect.poll(() => page.evaluate(() => window.scrollY)).toBe(0);
  await page.evaluate(
    () => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)))
  );
  return async () => {
    await page.evaluate((saved) => {
      if (saved.value) {
        document.documentElement.style.setProperty(
          "scroll-behavior",
          saved.value,
          saved.priority
        );
      } else {
        document.documentElement.style.removeProperty("scroll-behavior");
      }
    }, previous);
  };
}

const events = [
  {
    event_id: "event:treasury",
    company_id: "00126380",
    company_name: "삼성전자",
    event_type: "capital_return",
    title: "자기주식취득결정",
    original_language: "ko",
    occurred_at: "2026-07-16T00:30:00Z",
    deadline_at: "2026-08-01T00:00:00Z",
    importance: "critical",
    verification_status: "official",
    has_official_evidence: true
  },
  {
    event_id: "event:proposal",
    company_id: "00126380",
    company_name: "삼성전자",
    event_type: "meeting_and_vote",
    title: "Shareholder proposal submitted",
    original_language: "en",
    occurred_at: "2026-07-15T02:00:00Z",
    importance: "high",
    verification_status: "confirmed",
    has_official_evidence: true
  },
  ...["large_ownership", "board_and_compensation", "capital_issuance"].map((eventType, index) => ({
    event_id: `event:${eventType}`,
    company_id: "00126380",
    company_name: "삼성전자",
    event_type: eventType,
    title: `공식 사건 ${index + 1}`,
    original_language: "ko",
    occurred_at: `2026-07-${14 - index}T01:00:00Z`,
    importance: "medium",
    verification_status: "official",
    has_official_evidence: true
  })),
  {
    event_id: "event:watch",
    company_id: "00126380",
    company_name: "삼성전자",
    event_type: "listing_status",
    title: "추가 확인이 필요한 공개 신호",
    original_language: "ko",
    occurred_at: "2026-07-10T01:00:00Z",
    importance: "low",
    verification_status: "signal"
  }
];

const v2Events = [
  {
    event_id: "event:treasury",
    issuer_id: "00126380",
    issuer_name: "삼성전자",
    ticker: "005930",
    market: "KOSPI",
    country: "KR",
    event_family: "capital_return",
    verification_status: "official",
    change_type: "new",
    title: "자기주식취득결정",
    original_language: "ko",
    change_summary: "회사가 자기주식 취득 계획과 주요 기한을 공시했다.",
    current_status: "이사회 결의",
    actor_name: "삼성전자",
    actor_role: "issuer",
    occurred_at: "2026-07-24T00:30:00Z",
    filed_at: "2026-07-24T00:31:00Z",
    first_observed_at: "2026-07-24T00:33:00Z",
    updated_at: "2026-07-24T00:35:00Z",
    deadline_at: "2026-08-01T00:00:00Z",
    official_evidence_count: 2,
    media_count: 3,
    coverage_mode: "market-wide",
    source_url: "https://dart.fss.or.kr/"
  },
  {
    event_id: "event:us-proposal",
    issuer_id: "us:exxon",
    issuer_name: "Exxon Mobil",
    ticker: "XOM",
    market: "NYSE",
    country: "US",
    event_family: "meeting_and_vote",
    verification_status: "confirmed",
    change_type: "updated",
    title: "Shareholder proposal filed on board accountability",
    title_provenance: "generated_metadata",
    original_language: "en",
    change_summary: "The filing adds a vote deadline and the issuer response.",
    current_status: "Filed",
    actor_name: "Investor coalition",
    actor_role: "proponent",
    occurred_at: "2026-07-24T01:00:00Z",
    updated_at: "2026-07-24T01:10:00Z",
    deadline_at: "2026-08-05T00:00:00Z",
    official_evidence_count: 1,
    media_count: 4,
    coverage_mode: "market-wide",
    source_url: "https://www.sec.gov/"
  },
  {
    event_id: "event:jp-board",
    issuer_id: "jp:toyota",
    issuer_name: "トヨタ自動車",
    ticker: "7203",
    market: "TSE",
    country: "JP",
    event_family: "board_and_compensation",
    verification_status: "official",
    change_type: "new",
    title: "取締役会構成に関する開示",
    original_language: "ja",
    change_summary: "取締役候補者と株主総会の日程を開示した。",
    current_status: "開示",
    actor_name: "トヨタ自動車",
    actor_role: "issuer",
    occurred_at: "2026-07-24T02:00:00Z",
    updated_at: "2026-07-24T02:05:00Z",
    deadline_at: "2026-08-12T00:00:00Z",
    official_evidence_count: 1,
    media_count: 1,
    coverage_mode: "market-wide",
    source_url: "https://www.jpx.co.jp/"
  },
  {
    event_id: "event:gb-tender",
    issuer_id: "gb:barclays",
    issuer_name: "Barclays",
    ticker: "BARC",
    market: "LSE",
    country: "GB",
    event_family: "capital_issuance",
    verification_status: "official",
    change_type: "updated",
    title: "Statement of capital filed",
    original_language: "en",
    change_summary: "Companies House recorded an updated statement of capital.",
    current_status: "Filed",
    actor_name: "Barclays",
    actor_role: "issuer",
    occurred_at: "2026-07-23T22:00:00Z",
    updated_at: "2026-07-24T02:20:00Z",
    deadline_at: "2026-08-09T00:00:00Z",
    official_evidence_count: 1,
    media_count: 2,
    coverage_mode: "official-register"
  },
  {
    event_id: "event:ca-value",
    issuer_id: "ca:shopify",
    issuer_name: "Shopify",
    ticker: "SHOP",
    market: "TSX",
    country: "CA",
    event_family: "capital_return",
    verification_status: "corroborated",
    change_type: "follow_up",
    title: "Capital allocation commitment enters implementation",
    original_language: "en",
    change_summary: "The company reported the first action against its capital allocation commitment.",
    current_status: "Implementation",
    actor_name: "Shopify",
    actor_role: "issuer",
    occurred_at: "2026-07-23T21:00:00Z",
    updated_at: "2026-07-24T02:35:00Z",
    official_evidence_count: 1,
    media_count: 1,
    coverage_mode: "link-only"
  },
  {
    event_id: "event:au-dividend",
    issuer_id: "au:bhp",
    issuer_name: "BHP Group",
    ticker: "BHP",
    market: "ASX",
    country: "AU",
    event_family: "board_and_compensation",
    verification_status: "official",
    change_type: "new",
    title: "Director appointment registered",
    original_language: "en",
    change_summary: "ASIC register link metadata records a director appointment.",
    current_status: "Registered",
    actor_name: "BHP Group",
    actor_role: "issuer",
    occurred_at: "2026-07-24T03:00:00Z",
    updated_at: "2026-07-24T03:05:00Z",
    deadline_at: null,
    official_evidence_count: 1,
    media_count: 0,
    coverage_mode: "link-only",
    source_url: "https://www.asic.gov.au/"
  },
  {
    event_id: "event:private-signal",
    issuer_id: "us:signal",
    issuer_name: "Signal Corp",
    ticker: "SIG",
    market: "NASDAQ",
    country: "US",
    event_family: "listing_status",
    verification_status: "signal",
    title: "Unverified signal",
    original_language: "en",
    updated_at: "2026-07-24T03:10:00Z",
    official_evidence_count: 0,
    media_count: 1,
    coverage_mode: "media_only"
  }
].map((item) => ({
  title_provenance: "source",
  occurred_at: null,
  filed_at: null,
  first_observed_at: null,
  deadline_at: null,
  ...item
}));

const sourceStatus = [
  { connector_id: "dart", country: "KR", source_name: "OpenDART", coverage_mode: "market-wide", status: "active", fresh: true, public_status: "active", public_ready: true, last_success_at: "2026-07-24T03:05:00Z", last_checked_at: "2026-07-24T03:10:00Z", lag_minutes: 5, error_class: null, public_note: "OpenDART governance disclosure scope" },
  { connector_id: "sec", country: "US", source_name: "SEC EDGAR", coverage_mode: "market-wide", status: "active", fresh: true, public_status: "active", public_ready: true, last_success_at: "2026-07-24T03:00:00Z", last_checked_at: "2026-07-24T03:10:00Z", lag_minutes: 10, error_class: null, public_note: "SEC Latest Filings Atom intraday discovery plus completed-day index reconciliation; allowlisted governance forms only" },
  { connector_id: "jpx", country: "JP", source_name: "EDINET", coverage_mode: "market-wide", status: "active", fresh: true, public_status: "redistribution_blocked", public_ready: false, last_success_at: "2026-07-24T02:30:00Z", last_checked_at: "2026-07-24T03:10:00Z", lag_minutes: 40, error_class: null, public_note: "EDINET document-type allowlist; TDnet excluded" },
  { connector_id: "companies-house", country: "GB", source_name: "Companies House", coverage_mode: "official-register", status: "active", fresh: true, public_status: "active", public_ready: true, last_success_at: "2026-07-24T02:55:00Z", last_checked_at: "2026-07-24T03:10:00Z", lag_minutes: 15, error_class: null, public_note: "Companies House configured company-number scope; RNS excluded" },
  { connector_id: "ca-ir", country: "CA", source_name: "Canadian issuer IR manual links", coverage_mode: "link-only", status: "active", fresh: true, public_status: "active", public_ready: true, last_success_at: "2026-07-24T02:50:00Z", last_checked_at: "2026-07-24T03:10:00Z", lag_minutes: 20, error_class: null, public_note: "Manual issuer-controlled IR link metadata only; SEDAR+ excluded" },
  { connector_id: "asic", country: "AU", source_name: "ASIC manual register links", coverage_mode: "link-only", status: "pending_rights", fresh: false, public_status: "blocked_rights", public_ready: false, last_success_at: null, last_checked_at: "2026-07-24T03:10:00Z", lag_minutes: null, error_class: "source_right_required", public_note: "Manual asic.gov.au link metadata only; ASX excluded" }
];

const v2Issuers = [
  {
    issuer_id: "00126380",
    country_code: "KR",
    legal_name: "삼성전자",
    legal_name_en: "Samsung Electronics",
    short_name: "삼성전자",
    original_language: "ko",
    homepage_url: "https://www.samsung.com/",
    listing_status: "listed",
    market: "KOSPI",
    ticker: "005930",
    event_count: 1
  },
  {
    issuer_id: "us:exxon",
    country_code: "US",
    legal_name: "Exxon Mobil",
    legal_name_en: "Exxon Mobil",
    short_name: "Exxon",
    original_language: "en",
    homepage_url: "https://corporate.exxonmobil.com/",
    listing_status: "listed",
    market: "NYSE",
    ticker: "XOM",
    event_count: 1
  },
  {
    issuer_id: "jp:toyota",
    country_code: "JP",
    legal_name: "トヨタ自動車",
    legal_name_en: "Toyota Motor",
    short_name: "トヨタ",
    original_language: "ja",
    homepage_url: "https://global.toyota/",
    listing_status: "listed",
    market: "TSE",
    ticker: "7203",
    event_count: 1
  }
];

function json(route, body, status = 200) {
  return route.fulfill({
    status,
    contentType: "application/json; charset=utf-8",
    body: JSON.stringify(body)
  });
}

function v2Json(route, body, status = 200) {
  const normalized = (
    body
    && body.ok === true
    && body.data
    && Array.isArray(body.data.items)
    && body.meta
  ) ? {
      ...body,
      meta: {
        offset: 0,
        next_offset: null,
        next_page: null,
        continuation_limited: false,
        ...body.meta,
        returned: body.data.items.length
      }
    } : body;
  return json(route, { ...normalized, api_version: "v2" }, status);
}

async function mockV2Api(page) {
  await page.route("**/api/v2/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = decodeURIComponent(url.pathname.replace(/^.*\/api\/v2/, ""));
    const country = String(url.searchParams.get("country") || "").toUpperCase();
    const visible = v2Events.filter((item) => !country || item.country === country);
    if (path === "/briefs/latest" && request.method() === "GET") {
      const visibleSources = sourceStatus.filter((item) => !country || item.country === country);
      const unavailableSources = visibleSources.filter((item) => item.public_ready !== true);
      const unavailableCountries = [...new Set(unavailableSources.map((item) => item.country))];
      return v2Json(route, {
        ok: true,
        data: {
          schema_version: 1,
          brief_id: "brief:2026-07-24",
          edition: country || "global",
          cutoff_at: "2026-07-24T03:10:00Z",
          published_at: "2026-07-24T03:12:00Z",
          last_updated_at: "2026-07-24T03:12:00Z",
          build_sha: "a".repeat(40),
          stale: false,
          coverage_notice: unavailableSources.length ? {
            reason: "partial_coverage",
            scope: "warning",
            brief_id: "brief:2026-07-24",
            cutoff_at: "2026-07-24T03:10:00Z",
            published_at: "2026-07-24T03:12:00Z",
            unavailable_countries: unavailableCountries,
            unavailable_sources: unavailableSources.map((item) => item.source_name)
          } : null,
          top: [...visible.filter((item) => item.verification_status !== "signal").slice(0, 5), ...visible.filter((item) => item.verification_status === "signal")],
          watch: [],
          deadlines: visible.filter((item) => item.deadline_at),
          source_status: visibleSources,
          empty_reason: null
        },
        meta: { page: 1, limit: 50, returned: visible.length, has_more: false }
      });
    }
    if (path === "/live" && request.method() === "GET") {
      return v2Json(route, {
        ok: true,
        data: { items: visible.slice().reverse() },
        meta: { page: 1, limit: 50, returned: visible.length, has_more: false }
      });
    }
    if (path === "/events" && request.method() === "GET") {
      return v2Json(route, {
        ok: true,
        data: { items: visible.filter((item) => item.verification_status !== "signal") },
        meta: { page: 1, limit: 50, returned: visible.length, has_more: false, next_page: null }
      });
    }
    if (path === "/calendar" && request.method() === "GET") {
      const items = visible.filter((item) => item.verification_status !== "signal" && item.deadline_at);
      return v2Json(route, {
        ok: true,
        data: { items },
        meta: { page: 1, limit: 100, returned: items.length, has_more: false, next_page: null }
      });
    }
    if (path === "/issuers" && request.method() === "GET") {
      const query = String(url.searchParams.get("q") || "").toLocaleLowerCase();
      const items = v2Issuers.filter((item) => (
        (!country || item.country_code === country)
        && (!query || [item.legal_name, item.legal_name_en, item.short_name].join(" ").toLocaleLowerCase().includes(query))
      ));
      return v2Json(route, {
        ok: true,
        data: { items },
        meta: { page: 1, limit: 50, returned: items.length, has_more: false, next_page: null }
      });
    }
    if (path.startsWith("/issuers/") && request.method() === "GET") {
      const issuerId = path.slice("/issuers/".length);
      const issuer = v2Issuers.find((item) => item.issuer_id === issuerId);
      if (!issuer) return v2Json(route, { ok: false, error: "issuer_not_found" }, 404);
      return v2Json(route, {
        ok: true,
        data: {
          issuer,
          identifiers: [
            { identifier_type: issuer.country_code === "US" ? "cik" : "ticker", identifier_value: issuer.country_code === "US" ? "0000034088" : issuer.ticker, market: issuer.market, is_primary: 1 }
          ],
          listings: [
            { listing_id: `listing:${issuer.issuer_id}`, country_code: issuer.country_code, market: issuer.market, ticker: issuer.ticker, isin: "", currency_code: issuer.country_code === "US" ? "USD" : "KRW", listing_status: "listed", is_primary: 1 }
          ],
          events: v2Events.filter((item) => item.issuer_id === issuer.issuer_id && item.verification_status !== "signal")
        }
      });
    }
    if (path === "/sources/status" && request.method() === "GET") {
      const items = sourceStatus.filter((item) => !country || item.country === country);
      return v2Json(route, {
        ok: true,
        data: { items },
        meta: { page: 1, limit: 50, returned: items.length, has_more: false }
      });
    }
    if (path === "/search" && request.method() === "GET") {
      const query = String(url.searchParams.get("q") || "").toLocaleLowerCase();
      const items = visible.filter((item) => [
        item.issuer_name,
        item.title,
        item.event_family,
        item.actor_name
      ].join(" ").toLocaleLowerCase().includes(query));
      return v2Json(route, {
        ok: true,
        data: { items },
        meta: { page: 1, limit: 50, returned: items.length, has_more: false, next_page: null }
      });
    }
    if (path.startsWith("/events/") && request.method() === "GET") {
      const eventId = path.slice("/events/".length);
      const item = v2Events.find((event) => event.event_id === eventId);
      if (!item) return v2Json(route, { ok: false, error: "event_not_found" }, 404);
      return v2Json(route, {
        ok: true,
        data: {
          event: item,
          actors: [{ actor_id: "actor:company", actor_type: "company", display_name: item.issuer_name, original_language: item.original_language, actor_role: "target" }],
          claims: [{ claim_id: "claim:fact", claim_type: "official_fact", claim_text: item.event_id === "event:treasury" ? "회사는 자기주식 취득을 결정했다." : item.change_summary, original_language: item.original_language, document_title: item.title, original_url: item.source_url }],
          documents: [{ document_id: `source:${item.event_id}`, title: item.title, original_language: item.original_language, source_class: "official_disclosure", verification_status: "official", document_type: item.event_family, version_no: 1, filed_at: item.filed_at, published_at: item.occurred_at, original_url: item.source_url }],
          observations: [],
          timeline: []
        },
        meta: { page: 1, limit: 1, returned: 1, has_more: false }
      });
    }
    return v2Json(route, { ok: false, error: "not_found" }, 404);
  });
}

async function mockPublicApi(page) {
  await page.route("**/api/v1/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname.replace(/^.*\/api\/v1/, "");
    if (path === "/today" && request.method() === "GET") {
      return json(route, {
        ok: true,
        ranking_policy: { version: "today-v1", archive_endpoint: "/events" },
        top: events.slice(0, 5),
        watch: [events[5]]
      });
    }
    if (path === "/events" && request.method() === "GET") {
      return json(route, { ok: true, data: events, pagination: { page: 1, has_more: false, next_page: null } });
    }
    if (path === "/events/event%3Atreasury" || path === "/events/event:treasury") {
      return json(route, {
        ok: true,
        data: {
          event: events[0],
          actors: [{ actor_id: "actor:company", actor_type: "company", display_name: "삼성전자", original_language: "ko", actor_role: "target" }],
          claims: [{ claim_id: "claim:fact", claim_type: "official_fact", claim_text: "회사는 자기주식 취득을 결정했다.", original_language: "ko", document_title: "자기주식취득결정", original_url: "https://dart.fss.or.kr/" }],
          documents: [{ document_id: "dart:202607160001", title: "자기주식취득결정", original_language: "ko", source_class: "official_disclosure", verification_status: "official", document_type: "treasury_shares", version_no: 1, published_at: "2026-07-16T00:30:00Z", original_url: "https://dart.fss.or.kr/" }],
          timeline: [{ timeline_entry_id: "timeline:one", occurred_at: "2026-07-16T00:30:00Z", title: "공시 접수", original_language: "ko" }],
          revisions: []
        }
      });
    }
    if (path === "/actors/actor%3Acompany" || path === "/actors/actor:company") {
      return json(route, {
        ok: true,
        data: {
          actor: { actor_id: "actor:company", actor_type: "company", display_name: "삼성전자", original_language: "ko", country_code: "KR" },
          campaigns: []
        }
      });
    }
    if (path === "/revisions") {
      return json(route, {
        ok: true,
        data: [
          { revision_id: "revision:public", entity_type: "event", entity_id: "event:treasury", title: "공개 정정", reason: "접수 시각 정정", publication_status: "published", is_public: true, published_at: "2026-07-17T00:00:00Z", original_language: "ko" },
          { revision_id: "revision:internal", entity_type: "event", entity_id: "event:treasury", title: "내부 승인", reason: "internal", publication_status: "draft", is_public: false, published_at: "2026-07-17T00:00:00Z", original_language: "ko" }
        ],
        pagination: { page: 1, has_more: false, next_page: null }
      });
    }
    if (path === "/documents/document%3Alarge" || path === "/documents/document:large") {
      const offset = Number(url.searchParams.get("body_offset") || 0);
      return json(route, {
        ok: true,
        data: {
          document_id: "document:large",
          title: "Large source document",
          original_language: "en",
          source_class: "official_disclosure",
          verification_status: "official",
          body_text: offset === 0 ? "First body page. " : "Second body page.",
          body_truncated: offset === 0,
          body_next_offset: offset === 0 ? 17 : null
        }
      });
    }
    if (path === "/search") {
      return json(route, {
        ok: true,
        data: [{ kind: "event", entity_id: "event:treasury", title: "자기주식취득결정", subtitle: "treasury_shares", company_id: "00126380", occurred_at: "2026-07-16T00:30:00Z" }],
        pagination: { page: 1, has_more: false, next_page: null }
      });
    }
    if (path === "/calendar") {
      return json(route, {
        ok: true,
        data: [
          {
            item_type: "event",
            entity_id: "event:treasury",
            company_id: "00126380",
            company_name: "삼성전자",
            category: "capital_return",
            title: "자기주식취득결정",
            original_language: "ko",
            scheduled_at: "2026-08-01T00:00:00Z",
            verification_status: "official"
          }
        ],
        pagination: { page: 1, has_more: false, next_page: null }
      });
    }
    if (path === "/feedback" && request.method() === "POST") {
      return json(route, { ok: true, feedback_id: "feedback:test", status: "pending", is_public: false }, 202);
    }
    return json(route, { ok: true, data: [], pagination: { page: 1, has_more: false, next_page: null } });
  });
}

async function installWebVitalsObservers(page) {
  await page.addInitScript(() => {
    const metrics = { lcp: 0, cls: 0, inp: 0 };
    window.__BSIDE_TEST_VITALS__ = metrics;
    window.__BSIDE_TEST_OBSERVERS__ = [];

    const observe = (type, callback, options = {}) => {
      if (!PerformanceObserver.supportedEntryTypes.includes(type)) return;
      const observer = new PerformanceObserver((list) => callback(list.getEntries()));
      observer.observe({ type, buffered: true, ...options });
      // Retain observers for the full journey. Some headless runs otherwise
      // garbage-collect the local observer before the buffered LCP callback.
      window.__BSIDE_TEST_OBSERVERS__.push(observer);
    };

    observe("largest-contentful-paint", (entries) => {
      const latest = entries[entries.length - 1];
      if (latest) metrics.lcp = latest.startTime;
    });
    observe("layout-shift", (entries) => {
      for (const entry of entries) {
        if (!entry.hadRecentInput) metrics.cls += entry.value;
      }
    });
    observe("event", (entries) => {
      for (const entry of entries) {
        if (entry.interactionId > 0) metrics.inp = Math.max(metrics.inp, entry.duration);
      }
    }, { durationThreshold: 16 });
  });
}

async function expectNoCriticalAccessibilityViolations(page) {
  const results = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"])
    .analyze();
  expect(results.violations, JSON.stringify(results.violations, null, 2)).toEqual([]);
}

test.beforeEach(async ({ page }) => {
  await installWebVitalsObservers(page);
  await mockV2Api(page);
  await mockPublicApi(page);
});

test("today to evidence journey preserves source language and accessibility @webkit-smoke", async ({ page }) => {
  await page.goto("#/today");
  const topPanel = page.locator(".terminal-main .terminal-panel").first();
  await expect(page.getByRole("heading", { name: "Top 5" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Live / 최신 변화" })).toBeVisible();
  await expect(topPanel.getByRole("link", { name: "자기주식취득결정", exact: true })).toBeVisible();
  await expect(topPanel.getByRole("link", { name: "Shareholder proposal filed on board accountability", exact: true })).toHaveAttribute("lang", "en");
  await expect(topPanel.getByText("공시 메타데이터 표제 / Filing metadata label", { exact: true })).toBeVisible();
  await expect(topPanel.getByRole("link", { name: "取締役会構成に関する開示", exact: true })).toHaveAttribute("lang", "ja");
  await expect(page.getByText("Unverified signal", { exact: true })).toHaveCount(0);
  await expect(topPanel.locator(".coverage-badge").first()).toContainText("시장 전체 / Market-wide");
  await expect(topPanel.locator(".coverage-badge").first()).toContainText("공식 2 · 보도 3");
  await expect(topPanel.getByText("당사자 / Actor 삼성전자 · 발행사 / Issuer", { exact: true })).toBeVisible();
  await expect(topPanel.getByText(/접수 \/ Filed/).first()).toBeVisible();
  await expect(topPanel.getByText(/최초 관측 \/ First seen/).first()).toBeVisible();
  await expect(topPanel.getByText(/갱신 \/ Updated/).first()).toBeVisible();
  await expect(page.getByRole("heading", { name: "소스 상태 / Sources" })).toBeVisible();
  await expect(page.locator(".source-status-item[data-status='redistribution_blocked']")).toContainText("EDINET");
  await expect(page.locator(".source-status-item").filter({ hasText: "Canadian issuer IR manual links" })).toContainText("링크 전용·수동 메타데이터 / Link-only · manual metadata");
  await expect(page.locator(".source-public-note")).toContainText([
    "SEC Latest Filings Atom intraday discovery plus completed-day index reconciliation; allowlisted governance forms only",
    "Companies House configured company-number scope; RNS excluded",
    "Manual issuer-controlled IR link metadata only; SEDAR+ excluded",
    "Manual asic.gov.au link metadata only; ASX excluded"
  ]);
  await expect(page.locator("#global-source-coverage")).toContainText("4/6");
  await expect(page.locator("#global-source-coverage")).toContainText("링크 전용 2");
  await expect(page.locator("[data-coverage-scope='warning']")).toContainText("일부 공식 소스 지연");
  await expect(page.locator("[data-coverage-scope='warning']")).toContainText("JP · AU");
  await expect(page.locator(".coverage-scope-note")).toContainText("CA·AU 링크 전용 / link-only");
  await expect(page.locator("a[data-api-link='/feeds/events.atom']")).toHaveAttribute("href", /\/api\/v2\/feeds\/events\.atom$/);
  await expect(page.locator("a[data-api-link='/exports/events.csv']")).toHaveAttribute("href", /\/api\/v2\/exports\/events\.csv$/);
  await expect(page.locator("a[data-api-link='/exports/events.json']")).toHaveAttribute("href", /\/api\/v2\/exports\/events\.json$/);
  await expect(page.locator("a[data-api-link='/openapi.yaml']")).toHaveAttribute("href", /\/api\/v2\/openapi\.yaml$/);
  await expectNoCriticalAccessibilityViolations(page);

  await topPanel.getByRole("link", { name: "자기주식취득결정", exact: true }).click();
  await expect(page).toHaveURL(/#\/today$/);
  const dialog = page.getByRole("dialog");
  await expect(dialog).toBeVisible();
  await expect(dialog.getByRole("heading", { name: "자기주식취득결정", exact: true }).first()).toHaveAttribute("lang", "ko");
  await expect(dialog.getByText("원문 제목 / Source title", { exact: true })).toBeVisible();
  await expect(dialog.getByText("시장 전체 / Market-wide", { exact: true }).first()).toBeVisible();
  await expect(dialog.getByText("당사자·역할 / Actor & role", { exact: true })).toBeVisible();
  await expect(dialog.getByText("삼성전자 · 발행사 / Issuer", { exact: true })).toBeVisible();
  await expect(dialog.getByText("접수 / Filed", { exact: true })).toBeVisible();
  await expect(dialog.getByText("최초 관측 / First observed", { exact: true })).toBeVisible();
  await expect(dialog.getByText("마지막 변경 / Last updated", { exact: true })).toBeVisible();
  await expect(page.getByRole("heading", { name: "주장·반론·공식 사실 / Claims" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "공식 근거·보도 / Sources" })).toBeVisible();
  await expect(page.getByText("회사는 자기주식 취득을 결정했다.", { exact: true })).toHaveAttribute("lang", "ko");
  await expectNoCriticalAccessibilityViolations(page);
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toBeHidden();
});

test("healthy empty edition is distinct from a source outage", async ({ page }) => {
  const healthySources = sourceStatus.map((item) => ({
    ...item,
    status: "active",
    fresh: true,
    public_status: "active",
    public_ready: true,
    last_success_at: item.last_success_at || "2026-07-24T03:05:00Z"
  }));
  await page.route("**/api/v2/briefs/latest**", (route) => v2Json(route, {
    ok: true,
    data: {
      schema_version: 1,
      brief_id: "brief:healthy-empty",
      edition: "global",
      cutoff_at: "2026-07-24T03:10:00Z",
      published_at: "2026-07-24T03:12:00Z",
      last_updated_at: "2026-07-24T03:12:00Z",
      build_sha: "b".repeat(40),
      stale: false,
      coverage_notice: null,
      top: [],
      watch: [],
      deadlines: [],
      source_status: healthySources,
      empty_reason: "no_confirmed_material_events"
    }
  }));

  await page.goto("#/today");
  await expect(page.locator("[data-coverage-scope]")).toHaveCount(0);
  await expect(page.getByText("오늘 확인된 중요 사건 없음", { exact: true })).toBeVisible();
  await expect(page.getByText("공식 소스 수집 장애 / Source coverage unavailable", { exact: true })).toHaveCount(0);
});

test("canonical unavailable edition blocks Top and identifies stale snapshot", async ({ page }) => {
  const unavailableSources = sourceStatus.map((item) => ({
    ...item,
    fresh: false,
    public_status: "stale",
    public_ready: false
  }));
  await page.route("**/api/v2/briefs/latest**", (route) => v2Json(route, {
    ok: true,
    data: {
      schema_version: 1,
      brief_id: "brief:coverage-unavailable",
      edition: "global",
      cutoff_at: "2026-07-20T03:10:00Z",
      published_at: "2026-07-20T03:12:00Z",
      last_updated_at: "2026-07-20T03:12:00Z",
      build_sha: "c".repeat(40),
      stale: true,
      coverage_notice: {
        reason: "coverage_unavailable",
        scope: "blocking",
        brief_id: "brief:coverage-unavailable",
        cutoff_at: "2026-07-20T03:10:00Z",
        published_at: "2026-07-20T03:12:00Z",
        unavailable_countries: ["KR", "US", "JP", "GB", "CA", "AU"],
        unavailable_sources: unavailableSources.map((item) => item.source_name)
      },
      top: [],
      watch: [],
      deadlines: [],
      source_status: unavailableSources,
      empty_reason: "coverage_unavailable"
    }
  }));

  await page.goto("#/today");
  const outage = page.getByRole("alert");
  await expect(outage).toContainText("공식 소스 수집 장애 / Source coverage unavailable");
  await expect(outage).toContainText("Top 5를 공개하지 않습니다");
  await expect(outage).toContainText("KR · US · JP · GB · CA · AU");
  await expect(page.locator("[data-brief-stale='true']")).toContainText("과거 브리프 스냅샷");
  await expect(page.locator("[data-brief-stale='true']")).toContainText("2026");
  await expect(page.getByText("오늘 확인된 중요 사건 없음", { exact: true })).toHaveCount(0);
  await expect(page.getByText("공식 소스 수집 상태를 확인할 수 없습니다. 소스 상태를 먼저 확인해 주세요.", { exact: true })).toBeVisible();
});

test("market filters, URL state, keyboard shortcuts, and responsive terminal layout @webkit-smoke", async ({ page }, testInfo) => {
  await page.goto("#/today?market=US");
  const topPanel = page.locator(".terminal-main .terminal-panel").first();
  await expect(page.getByRole("link", { name: "US", exact: true })).toHaveAttribute("aria-current", "page");
  await expect(topPanel.getByRole("link", { name: "Shareholder proposal filed on board accountability", exact: true })).toBeVisible();
  await expect(page.getByRole("link", { name: "자기주식취득결정", exact: true })).toHaveCount(0);
  await expect(topPanel.locator(".coverage-badge").first()).toContainText("공식 1 · 보도 4");
  await expect(page.locator(".coverage-scope-note")).toContainText("SEC EDGAR 시장 전체 / Market-wide");
  await expect(page.locator("[data-coverage-scope='warning']")).toHaveCount(0);
  const firstEvent = await page.locator(".terminal-event").first().boundingBox();
  expect(firstEvent).not.toBeNull();
  expect(firstEvent.y).toBeGreaterThanOrEqual(0);
  expect(firstEvent.y).toBeLessThanOrEqual(300);

  if (testInfo.project.name.includes("mobile")) {
    await expect(page.locator(".mobile-terminal-tabs")).toBeVisible();
    const filterToggle = page.getByRole("button", { name: "필터", exact: true });
    await filterToggle.click();
    const filterDialog = page.getByRole("dialog", { name: "필터 / Filters" });
    await expect(filterDialog).toBeVisible();
    await expect(filterDialog).toHaveAttribute("aria-modal", "true");
    await expect(filterToggle).toHaveAttribute("aria-expanded", "true");
    const closeFilter = filterDialog.getByRole("button", { name: "닫기", exact: true });
    await expect(closeFilter).toBeFocused();
    await page.keyboard.press("Shift+Tab");
    await expect(filterDialog.getByRole("link", { name: "초기화", exact: true })).toBeFocused();
    await page.keyboard.press("Tab");
    await expect(closeFilter).toBeFocused();
  }
  await page.getByLabel("검색 / Search").fill("Exxon");
  await page.getByLabel("사건 유형 / Event").selectOption("meeting_and_vote");
  await page.getByLabel("시작 / From").fill("2026-07-25");
  await page.getByLabel("종료 / To").fill("2026-07-24");
  await page.getByRole("button", { name: "적용", exact: true }).click();
  await expect(page).toHaveURL(/#\/today\?market=US$/);
  await expect.poll(() => page.getByLabel("종료 / To").evaluate((input) => input.validationMessage)).not.toBe("");
  await page.getByLabel("시작 / From").fill("2026-07-23");
  await page.getByLabel("종료 / To").fill("2026-07-24");
  await page.getByRole("button", { name: "적용", exact: true }).click();
  await expect(page).toHaveURL(/market=US/);
  await expect(page).toHaveURL(/q=Exxon/);
  await expect(page).toHaveURL(/event_type=meeting_and_vote/);
  await expect(page).toHaveURL(/from=2026-07-23/);
  await expect(page).toHaveURL(/to=2026-07-24/);
  await expect(page.locator(".loading")).toHaveCount(0);
  await expect(topPanel.getByRole("link", { name: "Shareholder proposal filed on board accountability", exact: true })).toBeVisible();

  await page.keyboard.press("j");
  await expect(topPanel.getByRole("link", { name: "Shareholder proposal filed on board accountability", exact: true })).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("dialog")).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toBeHidden();
  await page.keyboard.press("/");
  await expect(page.getByRole("searchbox", { name: "통합 검색 / Global search" })).toBeFocused();

  if (testInfo.project.name.includes("mobile")) {
    await expect(page.locator(".mobile-bottom-nav")).toBeVisible();
    await expect(page.locator(".mobile-terminal-tabs")).toBeVisible();
    await expect(page.locator("#terminal-filters")).toBeHidden();
    const positions = await page.evaluate(() => ({
      main: document.querySelector(".terminal-main").getBoundingClientRect().top,
      rail: document.querySelector(".terminal-rail").getBoundingClientRect().top,
      marketHeight: document.querySelector("[data-market='US']").getBoundingClientRect().height,
      bottomTarget: document.querySelector(".mobile-bottom-nav a").getBoundingClientRect().height,
      sectionTarget: document.querySelector(".mobile-terminal-tabs button").getBoundingClientRect().height
    }));
    expect(positions.main).toBeLessThan(positions.rail);
    expect(positions.marketHeight).toBeGreaterThanOrEqual(44);
    expect(positions.bottomTarget).toBeGreaterThanOrEqual(44);
    expect(positions.sectionTarget).toBeGreaterThanOrEqual(44);
    await page.getByRole("button", { name: "필터", exact: true }).click();
    await expect(page.locator("#terminal-filters")).toBeVisible();
    await page.locator(".filter-sheet-close").click();
    await expect(page.locator("#terminal-filters")).toBeHidden();
    await expect(page.getByRole("button", { name: "필터", exact: true })).toBeFocused();
  } else if (testInfo.project.name.includes("tablet")) {
    const positions = await page.evaluate(() => ({
      filtersLeft: document.querySelector(".terminal-filters").getBoundingClientRect().left,
      filtersTop: document.querySelector(".terminal-filters").getBoundingClientRect().top,
      mainLeft: document.querySelector(".terminal-main").getBoundingClientRect().left,
      mainBottom: document.querySelector(".terminal-main").getBoundingClientRect().bottom,
      railLeft: document.querySelector(".terminal-rail").getBoundingClientRect().left,
      marketHeight: document.querySelector("[data-market='US']").getBoundingClientRect().height
    }));
    expect(Math.abs(positions.filtersLeft - positions.mainLeft)).toBeLessThanOrEqual(1);
    expect(positions.mainLeft).toBeLessThan(positions.railLeft);
    expect(positions.filtersTop).toBeGreaterThanOrEqual(positions.mainBottom);
    expect(positions.marketHeight).toBeGreaterThanOrEqual(44);
  } else {
    const positions = await page.evaluate(() => ({
      filters: document.querySelector(".terminal-filters").getBoundingClientRect().left,
      main: document.querySelector(".terminal-main").getBoundingClientRect().left,
      rail: document.querySelector(".terminal-rail").getBoundingClientRect().left
    }));
    expect(positions.filters).toBeLessThan(positions.main);
    expect(positions.main).toBeLessThan(positions.rail);
  }
  if (testInfo.project.name.endsWith("-chromium")) {
    const screenshotPath = testInfo.outputPath(`today-${testInfo.project.name}.png`);
    const restoreScrollBehavior = await stabilizeViewportScreenshot(page);
    try {
      await page.screenshot({ path: screenshotPath });
      await expectViewportPng(screenshotPath, testInfo.project.use.viewport);
    } finally {
      await restoreScrollBehavior();
    }
  }
  const liveNavigation = page.locator("[data-nav='live']:visible").first();
  await expect(liveNavigation).toBeVisible();
  await liveNavigation.click();
  await expect(page).toHaveURL(/market=US/);
  await expect(page).toHaveURL(/view=live/);
  await expect(liveNavigation).toHaveAttribute("aria-current", "page");
  await expect(page.getByRole("heading", { name: "Live / 최신 변화" })).toBeVisible();
  expect(await page.locator("#terminal-live").evaluate((node) => {
    const bounds = node.getBoundingClientRect();
    return bounds.top < window.innerHeight && bounds.bottom > 0;
  })).toBe(true);
  // Run the document-wide target-size scan from a stable top position; when
  // the Live anchor scrolls a compact tablet view, otherwise valid links above
  // the viewport can sit partially behind the sticky market navigation.
  await page.evaluate(() => window.scrollTo({ top: 0, behavior: "auto" }));
  await expectNoCriticalAccessibilityViolations(page);
});

test("v2 unsupported endpoints gracefully fall back to the public v1 contracts", async ({ page }) => {
  const requests = [];
  page.on("request", (request) => {
    if (request.url().includes("/api/v2/") || request.url().includes("/api/v1/")) requests.push(request.url());
  });
  await page.route("**/api/v2/**", (route) => json(route, { ok: false, error: "not_found" }, 404));
  await page.goto("#/today?market=KR");
  const topPanel = page.locator(".terminal-main .terminal-panel").first();
  await expect(page.getByRole("heading", { name: "Top 5" })).toBeVisible();
  await expect(topPanel.getByRole("link", { name: "자기주식취득결정", exact: true })).toBeVisible();
  await expect(topPanel.getByRole("link", { name: "Shareholder proposal submitted", exact: true })).toHaveAttribute("lang", "en");
  await expect(topPanel.locator(".coverage-badge").first()).toContainText("시장 전체 / Market-wide");
  await expect(page.getByText("추가 확인이 필요한 공개 신호", { exact: true })).toHaveCount(0);
  await expect.poll(() => requests.some((url) => url.includes("/api/v2/briefs/latest"))).toBe(true);
  await expect.poll(() => requests.some((url) => url.includes("/api/v1/today"))).toBe(true);
  await expect.poll(() => requests.some((url) => url.includes("/api/v1/events"))).toBe(true);
  await expectNoCriticalAccessibilityViolations(page);
});

test("an unversioned HTTP 200 is unsupported and uses the controlled v1 fallback", async ({ page }) => {
  const requests = [];
  page.on("request", (request) => {
    if (request.url().includes("/api/v2/") || request.url().includes("/api/v1/")) requests.push(request.url());
  });
  await page.route("**/api/v2/**", (route) => json(route, { ok: true, status: "healthy" }));

  await page.goto("#/today?market=KR");
  const topPanel = page.locator(".terminal-main .terminal-panel").first();
  await expect(topPanel.getByRole("link", { name: "자기주식취득결정", exact: true })).toBeVisible();
  await expect.poll(() => requests.some((url) => url.includes("/api/v2/briefs/latest"))).toBe(true);
  await expect.poll(() => requests.some((url) => url.includes("/api/v1/today"))).toBe(true);

  await page.route("**/api/v2/search**", (route) => v2Json(route, {
    ok: true,
    data: { items: "not-an-array" },
    meta: { page: 1, limit: 50, returned: 0, has_more: false }
  }));
  await page.goto("#/search?q=삼성전자");
  await expect(page.getByRole("link", { name: "자기주식취득결정", exact: true })).toBeVisible();
  await expect.poll(() => requests.some((url) => url.includes("/api/v1/search"))).toBe(true);
});

test("real v2 release and authorization failures stay fail-closed", async ({ page }) => {
  const v1Requests = [];
  page.on("request", (request) => {
    if (request.url().includes("/api/v1/")) v1Requests.push(request.url());
  });
  await page.route("**/api/v2/**", (route) => v2Json(
    route,
    { ok: false, error: "global_terminal_release_closed" },
    503
  ));

  await page.goto("#/today");
  await expect(page.getByText("DATA UNAVAILABLE", { exact: true })).toBeVisible();
  await expect(page.locator(".error-code")).toHaveText("global_terminal_release_closed");
  const headingSize = await page.getByRole("heading", { name: "기록을 불러오지 못했습니다.", exact: true }).evaluate(
    (node) => Number.parseFloat(getComputedStyle(node).fontSize)
  );
  expect(headingSize).toBeLessThanOrEqual((page.viewportSize()?.width || 1440) <= 680 ? 26 : 30);
  await expect.poll(() => v1Requests.length).toBe(0);

  await page.route("**/api/v2/**", (route) => v2Json(
    route,
    { ok: false, error: "preview_token_required" },
    401
  ));
  await page.goto("#/search?q=삼성전자");
  await expect(page.locator(".error-code")).toHaveText("preview_token_required");
  await expect.poll(() => v1Requests.some((url) => url.includes("/api/v1/search"))).toBe(false);

  await page.route("**/api/v2/**", (route) => v2Json(
    route,
    { ok: false, error: "insufficient_role" },
    403
  ));
  await page.goto("#/events/event%3Atreasury");
  await expect(page.locator(".error-code")).toHaveText("insufficient_role");
  await expect.poll(() => v1Requests.some((url) => url.includes("/api/v1/events/event%3Atreasury"))).toBe(false);
});

test("global search and full event records use valid v2 contracts first @webkit-smoke", async ({ page }) => {
  const requests = [];
  page.on("request", (request) => requests.push(request.url()));

  await page.goto("#/search?q=삼성전자");
  await expect(page.getByRole("link", { name: "자기주식취득결정", exact: true })).toBeVisible();
  await expect.poll(() => requests.some((url) => url.includes("/api/v2/search"))).toBe(true);
  expect(requests.some((url) => url.includes("/api/v1/search"))).toBe(false);

  requests.length = 0;
  await page.goto("#/events/event%3Atreasury");
  await expect(page.locator("h1").filter({ hasText: "자기주식취득결정" })).toHaveAttribute("lang", "ko");
  await expect(page.getByRole("heading", { name: "공식 근거·보도 / Sources" })).toBeVisible();
  await expect.poll(() => requests.some((url) => url.includes("/api/v2/events/event%3Atreasury"))).toBe(true);
  expect(requests.some((url) => url.includes("/api/v1/events/event%3Atreasury"))).toBe(false);
});

test("global archive, issuer identity, and issuer detail use v2 without legacy company aliasing @webkit-smoke", async ({ page }, testInfo) => {
  const requests = [];
  page.on("request", (request) => requests.push(request.url()));

  await page.goto("#/today");
  if (["desktop-chromium", "webkit-smoke"].includes(testInfo.project.name)) {
    await page.locator(".primary-nav [data-nav='companies']").click();
  } else {
    await page.getByRole("button", { name: "메뉴 Menu", exact: true }).click();
    await expect(page.locator("#mobile-menu")).toBeVisible();
    await page.locator("#mobile-menu [data-nav='companies']").click();
  }
  await expect(page).toHaveURL(/#\/issuers$/);
  await expect(page.getByRole("heading", { name: "글로벌 발행사별 자본시장 기록" })).toBeVisible();
  await expect.poll(() => requests.some((url) => url.includes("/api/v2/issuers"))).toBe(true);
  expect(requests.some((url) => url.includes("/api/v1/companies"))).toBe(false);

  requests.length = 0;
  await page.goto("#/events?country=US");
  await expect(page.getByRole("heading", { name: "글로벌 자본시장 사건 전체 기록" })).toBeVisible();
  const issuerLink = page.getByRole("link", { name: "Exxon Mobil", exact: true }).first();
  await expect(issuerLink).toHaveAttribute("href", "#/issuers/us%3Aexxon");
  await expect(page.locator("a[href='#/companies/us%3Aexxon']")).toHaveCount(0);
  await expect.poll(() => requests.some((url) => url.includes("/api/v2/events") && url.includes("country=US"))).toBe(true);
  expect(requests.some((url) => url.includes("/api/v1/events"))).toBe(false);

  await issuerLink.click();
  await expect(page).toHaveURL(/#\/issuers\/us%3Aexxon$/);
  await expect(page.locator("h1").filter({ hasText: "Exxon Mobil" })).toHaveAttribute("lang", "en");
  await expect(page.getByText("0000034088", { exact: true })).toBeVisible();
  await expect(page.getByRole("heading", { name: "자본시장 타임라인 / Timeline" })).toBeVisible();
  await expect.poll(() => requests.some((url) => url.includes("/api/v2/issuers/us%3Aexxon"))).toBe(true);
  expect(requests.some((url) => url.includes("/api/v1/companies"))).toBe(false);
  await expectNoCriticalAccessibilityViolations(page);
});

test("v2 load-more follows next_offset without skipping a byte-fitted row", async ({ page }) => {
  const requests = [];
  page.on("request", (request) => {
    if (request.url().includes("/api/v2/events")) requests.push(request.url());
  });
  const publicEvents = v2Events.filter((item) => item.verification_status !== "signal").slice(0, 2);
  await page.route("**/api/v2/events**", (route) => {
    const url = new URL(route.request().url());
    const offset = Number(url.searchParams.get("offset") || 0);
    const item = publicEvents[offset] ? [publicEvents[offset]] : [];
    return v2Json(route, {
      ok: true,
      data: { items: item },
      meta: {
        page: offset ? null : 1,
        offset,
        limit: 50,
        returned: item.length,
        has_more: offset === 0,
        next_page: null,
        next_offset: offset === 0 ? 1 : null,
        continuation_limited: false
      }
    });
  });

  await page.goto("#/events");
  await expect(page.getByRole("link", { name: publicEvents[0].title, exact: true })).toBeVisible();
  await expect(page.getByRole("link", { name: publicEvents[1].title, exact: true })).toHaveCount(0);
  await page.getByRole("button", { name: "더 보기 / Load more" }).click();
  await expect(page.getByRole("link", { name: publicEvents[1].title, exact: true })).toBeVisible();
  await expect.poll(() => requests.some((url) => (
    url.includes("offset=1") && !url.includes("page=2")
  ))).toBe(true);
});

test("global calendar uses the v2 deadline contract and preserves issuer routes @webkit-smoke", async ({ page }) => {
  const requests = [];
  page.on("request", (request) => requests.push(request.url()));

  await page.goto("#/calendar?country=US&from=2026-07-24&to=2026-09-01");
  await expect(page.getByRole("heading", { name: "글로벌 주총·공개매수·주요 기한" })).toBeVisible();
  await expect(page.getByRole("link", { name: "Shareholder proposal filed on board accountability", exact: true })).toBeVisible();
  await expect(page.getByRole("link", { name: "Exxon Mobil", exact: true })).toHaveAttribute("href", "#/issuers/us%3Aexxon");
  await expect.poll(() => requests.some((url) => (
    url.includes("/api/v2/calendar")
    && url.includes("country=US")
    && url.includes("from=2026-07-24")
  ))).toBe(true);
  expect(requests.some((url) => url.includes("/api/v1/calendar"))).toBe(false);
  await expectNoCriticalAccessibilityViolations(page);
});

test("malformed unversioned v2 archive and calendar payloads use only the controlled v1 fallback", async ({ page }) => {
  const requests = [];
  page.on("request", (request) => requests.push(request.url()));
  await page.route("**/api/v2/events**", (route) => json(route, {
    ok: true,
    data: { items: "not-an-array" },
    meta: { page: 1, limit: 50, returned: 0, has_more: false }
  }));
  await page.route("**/api/v2/calendar**", (route) => json(route, {
    ok: true,
    data: { items: "not-an-array" },
    meta: { page: 1, limit: 100, returned: 0, has_more: false }
  }));

  await page.goto("#/events");
  await expect(page.getByRole("heading", { name: "거버넌스 사건 전체 기록" })).toBeVisible();
  await expect(page.getByRole("link", { name: "삼성전자", exact: true }).first()).toHaveAttribute("href", "#/companies/00126380");
  await expect.poll(() => requests.some((url) => url.includes("/api/v1/events"))).toBe(true);

  await page.goto("#/calendar?from=2026-07-24&to=2026-09-01");
  await expect(page.getByRole("heading", { name: "주총·공개매수·주요 기한" })).toBeVisible();
  await expect(page.getByRole("link", { name: "삼성전자", exact: true })).toHaveAttribute("href", "#/companies/00126380");
  await expect.poll(() => requests.some((url) => url.includes("/api/v1/calendar"))).toBe(true);
});

test("v2 events reject database-native timestamps and use the controlled v1 fallback", async ({ page }) => {
  const requests = [];
  page.on("request", (request) => requests.push(request.url()));
  await page.route("**/api/v2/events**", (route) => v2Json(route, {
    ok: true,
    data: {
      items: [{
        ...v2Events[0],
        updated_at: "2026-07-24 00:35:00"
      }]
    },
    meta: { page: 1, limit: 50, returned: 1, has_more: false }
  }));

  await page.goto("#/events");
  await expect(page.getByRole("heading", { name: "거버넌스 사건 전체 기록" })).toBeVisible();
  await expect(page.getByRole("link", { name: "삼성전자", exact: true }).first()).toHaveAttribute("href", "#/companies/00126380");
  await expect.poll(() => requests.some((url) => url.includes("/api/v1/events"))).toBe(true);
});

test("preview fragment is scrubbed and actor, public revisions, and body pages remain private-safe", async ({ page }) => {
  const token = "preview-token-abcdefghijklmnopqrstuvwxyz-0123456789";
  const authorizations = [];
  page.on("request", (request) => {
    if (request.url().includes("/api/v1/") || request.url().includes("/api/v2/")) authorizations.push(request.headers().authorization || "");
  });
  await page.goto(`#preview=${token}`);
  await expect(page).toHaveURL(/#\/today$/);
  await expect(page).not.toHaveURL(/preview-token/);
  await expect.poll(() => authorizations.some((value) => value === `Bearer ${token}`)).toBe(true);
  await expect(page.getByText("Unverified signal", { exact: true })).toHaveCount(0);

  await page.goto("#/actors/actor%3Acompany");
  await expect(page.getByRole("heading", { name: "삼성전자" })).toHaveAttribute("lang", "ko");
  await expect(page.getByRole("link", { name: "당사자 답변 / Right of reply" })).toBeVisible();

  await page.goto("#/revisions");
  await expect(page.getByText("공개 정정", { exact: true })).toBeVisible();
  await expect(page.getByText("내부 승인", { exact: true })).toHaveCount(0);

  await page.goto("#/documents/document%3Alarge");
  await expect(page.getByRole("heading", { name: "Large source document" })).toHaveAttribute("lang", "en");
  await expect(page.getByText("First body page.", { exact: true })).toHaveAttribute("lang", "en");
  await page.getByRole("button", { name: "다음 본문 불러오기 / Load next body page" }).click();
  await expect(page.getByText(/First body page\. Second body page\./)).toBeVisible();
});

test("web vitals telemetry contains only route template, metric, value, device class, and build SHA", async ({ page }) => {
  const buildSha = "a".repeat(40);
  const observations = [];
  await page.route("**/governance/config.js", (route) => route.fulfill({
    status: 200,
    contentType: "text/javascript; charset=utf-8",
    body: `window.__BSIDE_GOVERNANCE_CONFIG__=Object.freeze({"apiBase":"/api/v1","webBase":"https://news.bside.ai","buildSha":"${buildSha}"});`
  }));
  await page.route("**/api/v1/metrics/web-vitals", async (route) => {
    observations.push(route.request().postDataJSON());
    return json(route, { ok: true }, 202);
  });

  await page.goto("#/search?q=private-query-must-not-be-collected");
  await expect(page.getByRole("heading", { name: "통합 검색" })).toBeVisible();
  await page.evaluate(() => window.dispatchEvent(new PageTransitionEvent("pagehide")));
  await expect.poll(() => observations.some((item) => item.metric === "cls")).toBe(true);
  for (const item of observations) {
    expect(Object.keys(item).sort()).toEqual(["build_sha", "device_class", "metric", "route_template", "value"]);
    expect(item.route_template).toBe("/search");
    expect(JSON.stringify(item)).not.toContain("private-query-must-not-be-collected");
    expect(item.build_sha).toBe(buildSha);
    expect(["desktop", "mobile"]).toContain(item.device_class);
  }
});

test("search, private feedback, responsive layout, and local performance budgets", async ({ page }) => {
  await page.goto("#/search");
  await expect(page.getByRole("searchbox", { name: "통합 검색어 / Search query" })).toBeVisible();
  await page.waitForFunction(
    () => {
      const metrics = window.__BSIDE_TEST_VITALS__;
      if (!metrics) return false;

      // Capture LCP before the first user interaction. Chromium finalizes LCP
      // when input begins, and deferring this check until after the form
      // journey made parallel headless runs intermittently miss the callback.
      if (metrics.lcp <= 0) {
        const entries = performance.getEntriesByType("largest-contentful-paint");
        const latest = entries[entries.length - 1];
        if (latest) metrics.lcp = latest.startTime;
      }
      return metrics.lcp > 0;
    },
    null,
    // This timeout waits for the observer callback; the measured LCP itself
    // is still required below to stay within the 2.5 second product budget.
    { timeout: 10_000 }
  );
  await page.getByRole("searchbox", { name: "통합 검색어 / Search query" }).fill("삼성전자");
  await page.getByRole("button", { name: "검색 / Search", exact: true }).click();
  await expect(page.getByRole("link", { name: "자기주식취득결정", exact: true })).toBeVisible();

  await page.goto("#/feedback?entity_type=event&entity_id=event%3Atreasury");
  await page.getByLabel("요청 내용 / Message").fill("공시 원문의 날짜와 현재 기록을 다시 확인해 주세요.");
  await page.getByRole("button", { name: "비공개 접수 / Submit privately" }).click();
  await expect(page.locator(".form-status")).toContainText("pending");
  await expect(page.getByText(/자동 공개되지 않으며/)).toBeVisible();
  // Playwright scrolls the submit button into view. Reset before the full-page
  // target-size scan so the sticky market navigation does not partially cover
  // an otherwise 44px-high select near the top of the form.
  await page.evaluate(() => window.scrollTo({ top: 0, behavior: "auto" }));
  await expectNoCriticalAccessibilityViolations(page);

  const layout = await page.evaluate(() => ({
    clientWidth: document.documentElement.clientWidth,
    scrollWidth: document.documentElement.scrollWidth,
    transferBytes: performance.getEntriesByType("resource").reduce((sum, entry) => sum + (entry.transferSize || 0), 0),
    vitals: window.__BSIDE_TEST_VITALS__
  }));
  expect(layout.scrollWidth).toBeLessThanOrEqual(layout.clientWidth);
  expect(layout.transferBytes).toBeLessThan(250_000);
  expect(layout.vitals.lcp).toBeGreaterThan(0);
  expect(layout.vitals.lcp).toBeLessThanOrEqual(2_500);
  expect(layout.vitals.inp).toBeLessThanOrEqual(200);
  expect(layout.vitals.cls).toBeLessThanOrEqual(0.1);
});
