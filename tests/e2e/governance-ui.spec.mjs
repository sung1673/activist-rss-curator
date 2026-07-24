import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

const events = [
  {
    event_id: "event:treasury",
    company_id: "00126380",
    company_name: "삼성전자",
    event_type: "treasury_shares",
    title: "자기주식취득결정",
    original_language: "ko",
    occurred_at: "2026-07-16T00:30:00Z",
    deadline_at: "2026-08-01T00:00:00Z",
    importance: "critical",
    verification_status: "official"
  },
  {
    event_id: "event:proposal",
    company_id: "00126380",
    company_name: "삼성전자",
    event_type: "shareholder_proposal",
    title: "Shareholder proposal submitted",
    original_language: "en",
    occurred_at: "2026-07-15T02:00:00Z",
    importance: "high",
    verification_status: "confirmed"
  },
  ...["dividend", "board", "value_up"].map((eventType, index) => ({
    event_id: `event:${eventType}`,
    company_id: "00126380",
    company_name: "삼성전자",
    event_type: eventType,
    title: `공식 사건 ${index + 1}`,
    original_language: "ko",
    occurred_at: `2026-07-${14 - index}T01:00:00Z`,
    importance: "medium",
    verification_status: "official"
  })),
  {
    event_id: "event:watch",
    company_id: "00126380",
    company_name: "삼성전자",
    event_type: "other",
    title: "추가 확인이 필요한 공개 신호",
    original_language: "ko",
    occurred_at: "2026-07-10T01:00:00Z",
    importance: "low",
    verification_status: "signal"
  }
];

function json(route, body, status = 200) {
  return route.fulfill({
    status,
    contentType: "application/json; charset=utf-8",
    body: JSON.stringify(body)
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
  await mockPublicApi(page);
});

test("today to evidence journey preserves source language and accessibility", async ({ page }) => {
  await page.goto("#/today");
  await expect(page.getByRole("heading", { name: "Top 5" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Watch" })).toBeVisible();
  await expect(page.getByRole("link", { name: "자기주식취득결정", exact: true })).toBeVisible();
  await expect(page.getByRole("link", { name: "Shareholder proposal submitted", exact: true })).toHaveAttribute("lang", "en");
  await expect(page.getByText("추가 확인이 필요한 공개 신호", { exact: true })).toHaveCount(0);
  await expectNoCriticalAccessibilityViolations(page);

  await page.getByRole("link", { name: "자기주식취득결정", exact: true }).click();
  await expect(page).toHaveURL(/#\/events\/event%3Atreasury$/);
  await expect(page.getByRole("heading", { name: "주장·반론·공식 사실 / Claims & evidence" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "공식 근거·보도 / Sources" })).toBeVisible();
  await expect(page.getByText("회사는 자기주식 취득을 결정했다.", { exact: true })).toHaveAttribute("lang", "ko");
  await expectNoCriticalAccessibilityViolations(page);
});

test("preview fragment is scrubbed and actor, public revisions, and body pages remain private-safe", async ({ page }) => {
  const token = "preview-token-abcdefghijklmnopqrstuvwxyz-0123456789";
  const authorizations = [];
  page.on("request", (request) => {
    if (request.url().includes("/api/v1/")) authorizations.push(request.headers().authorization || "");
  });
  await page.goto(`#preview=${token}`);
  await expect(page).toHaveURL(/#\/today$/);
  await expect(page).not.toHaveURL(/preview-token/);
  await expect.poll(() => authorizations.some((value) => value === `Bearer ${token}`)).toBe(true);
  await expect(page.getByText("추가 확인이 필요한 공개 신호", { exact: true })).toHaveCount(0);

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
