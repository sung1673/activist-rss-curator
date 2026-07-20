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
  await expectNoCriticalAccessibilityViolations(page);

  await page.getByRole("link", { name: "자기주식취득결정", exact: true }).click();
  await expect(page).toHaveURL(/#\/events\/event%3Atreasury$/);
  await expect(page.getByRole("heading", { name: "주장·반론·공식 사실 / Claims & evidence" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "공식 근거·보도 / Sources" })).toBeVisible();
  await expect(page.getByText("회사는 자기주식 취득을 결정했다.", { exact: true })).toHaveAttribute("lang", "ko");
  await expectNoCriticalAccessibilityViolations(page);
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
    navigationMs: performance.getEntriesByType("navigation")[0]?.duration || 0,
    vitals: window.__BSIDE_TEST_VITALS__
  }));
  expect(layout.scrollWidth).toBeLessThanOrEqual(layout.clientWidth);
  expect(layout.transferBytes).toBeLessThan(250_000);
  expect(layout.navigationMs).toBeLessThan(2_500);
  expect(layout.vitals.lcp).toBeGreaterThan(0);
  expect(layout.vitals.lcp).toBeLessThanOrEqual(2_500);
  expect(layout.vitals.inp).toBeLessThanOrEqual(200);
  expect(layout.vitals.cls).toBeLessThanOrEqual(0.1);
});
