import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { readFile, stat, writeFile } from "node:fs/promises";

const PREVIEW_SESSION_KEY = "bside.governance.preview";
const SHA_PATTERN = /^[0-9a-f]{40}$/;
const TOKEN_PATTERN = /^[A-Za-z0-9._~-]{32,512}$/;

function requiredEnvironment() {
  const token = String(process.env.GOVERNANCE_PREVIEW_TOKEN || "").trim();
  if (!TOKEN_PATTERN.test(token)) {
    throw new Error("GOVERNANCE_PREVIEW_TOKEN must be a valid 32-512 character preview token");
  }
  const expectedSha = String(process.env.PREVIEW_EXPECTED_BUILD_SHA || process.env.GITHUB_SHA || "")
    .trim()
    .toLowerCase();
  if (!SHA_PATTERN.test(expectedSha)) {
    throw new Error("PREVIEW_EXPECTED_BUILD_SHA must be the immutable 40-character workflow revision");
  }
  const apiV1 = normalizeApiV1(process.env.BSIDE_API_BASE_URL);
  return {
    token,
    expectedSha,
    apiV1,
    apiV2: apiV1.replace(/\/api\/v1$/, "/api/v2")
  };
}

function normalizeApiV1(value) {
  const raw = String(value || "").trim();
  if (!raw) throw new Error("BSIDE_API_BASE_URL is required");
  let parsed;
  try {
    parsed = new URL(raw);
  } catch (_error) {
    throw new Error("BSIDE_API_BASE_URL must be a valid URL");
  }
  if (
    parsed.protocol !== "https:"
    || parsed.username
    || parsed.password
    || parsed.search
    || parsed.hash
    || !/\/api\/v1\/?$/.test(parsed.pathname)
  ) {
    throw new Error("BSIDE_API_BASE_URL must be a credential-free HTTPS URL ending in /api/v1");
  }
  parsed.pathname = parsed.pathname.replace(/\/+$/, "");
  return parsed.toString().replace(/\/$/, "");
}

function endpointPath(response, apiV2) {
  try {
    const actual = new URL(response.url());
    const base = new URL(apiV2);
    if (actual.origin !== base.origin) return "";
    const prefix = base.pathname.replace(/\/$/, "");
    if (actual.pathname !== prefix && !actual.pathname.startsWith(`${prefix}/`)) return "";
    return actual.pathname.slice(prefix.length) || "/";
  } catch (_error) {
    return "";
  }
}

function waitForV2(page, apiV2, path) {
  return page.waitForResponse((response) => endpointPath(response, apiV2) === path);
}

async function assertV2Response(response, expectedPath) {
  expect(new URL(response.url()).pathname.endsWith(expectedPath)).toBe(true);
  expect(response.status()).toBe(200);
  expect(response.headers()["x-bside-api-version"]).toBe("v2");
  const body = await response.body();
  expect(body.byteLength).toBeLessThanOrEqual(250_000);
  const payload = JSON.parse(body.toString("utf8"));
  expect(payload.api_version).toBe("v2");
  expect(payload.ok).toBe(true);
  return {
    path: expectedPath,
    status: response.status(),
    response_bytes: body.byteLength,
    api_version: payload.api_version
  };
}

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

async function navigateWithResponses(page, hash, apiV2, paths) {
  const pending = paths.map((path) => waitForV2(page, apiV2, path));
  await page.goto(hash, { waitUntil: "domcontentloaded" });
  const responses = await Promise.all(pending);
  const observations = [];
  for (let index = 0; index < responses.length; index += 1) {
    observations.push(await assertV2Response(responses[index], paths[index]));
  }
  await expect(page.locator("#app")).not.toHaveAttribute("aria-busy", "true");
  await expect(page.locator(".error-code")).toHaveCount(0);
  return observations;
}

async function browserHealth(page, apiV2, expectedSha) {
  const result = await page.evaluate(async ({ apiBase, storageKey }) => {
    const token = String(sessionStorage.getItem(storageKey) || "");
    const response = await fetch(`${apiBase.replace(/\/$/, "")}/health`, {
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${token}`
      },
      cache: "no-store",
      credentials: "omit",
      redirect: "error",
      referrerPolicy: "no-referrer"
    });
    const body = await response.arrayBuffer();
    const payload = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(body));
    return {
      status: response.status,
      xBsideApiVersion: String(response.headers.get("x-bside-api-version") || ""),
      xResponseBytes: Number(response.headers.get("x-response-bytes") || "0"),
      responseBytes: body.byteLength,
      apiVersion: String(payload?.api_version || ""),
      codeRevision: String(payload?.code_revision || "").toLowerCase(),
      ok: payload?.ok === true
    };
  }, { apiBase: apiV2, storageKey: PREVIEW_SESSION_KEY });
  expect(result.status).toBe(200);
  expect(result.xBsideApiVersion).toBe("v2");
  expect(result.responseBytes).toBeGreaterThan(0);
  expect(result.responseBytes).toBeLessThanOrEqual(250_000);
  expect(result.xResponseBytes).toBe(result.responseBytes);
  expect(result.apiVersion).toBe("v2");
  expect(result.ok).toBe(true);
  expect(result.codeRevision).toBe(expectedSha);
  return {
    path: "/health",
    status: result.status,
    response_bytes: result.responseBytes,
    api_version: result.apiVersion,
    code_revision: result.codeRevision
  };
}

test("remote Production Alpha preview renders real v2 data without mocks", async (
  { page, context },
  testInfo
) => {
  const environment = requiredEnvironment();
  const observed = [];
  const v1Fallbacks = [];
  const project = testInfo.project.name;
  const screenshotPath = testInfo.outputPath(`preview-${project}-${environment.expectedSha}.png`);
  const tracePath = testInfo.outputPath(`preview-${project}-${environment.expectedSha}.zip`);
  const receiptPath = testInfo.outputPath(`preview-${project}-${environment.expectedSha}.json`);
  let completed = false;
  let traceStarted = false;
  let axeSeriousCount = null;
  let axeCriticalCount = null;
  let firstImportantEventTopPx = null;
  let apiBudgetReceipts = [];

  await context.addInitScript(
    ({ key, token }) => {
      sessionStorage.setItem(key, token);
    },
    { key: PREVIEW_SESSION_KEY, token: environment.token }
  );

  page.on("response", (response) => {
    try {
      const actual = new URL(response.url());
      const v1 = new URL(environment.apiV1);
      const prefix = v1.pathname.replace(/\/$/, "");
      if (
        actual.origin === v1.origin
        && (actual.pathname === prefix || actual.pathname.startsWith(`${prefix}/`))
      ) {
        v1Fallbacks.push(actual.pathname.slice(prefix.length) || "/");
      }
    } catch (_error) {
      // Invalid browser response URLs are ignored; Playwright only emits parsed HTTP(S) URLs here.
    }
  });

  try {
    await context.tracing.start({
      name: `global-alpha-preview-${project}-${environment.expectedSha}`,
      screenshots: true,
      snapshots: true,
      sources: true
    });
    traceStarted = true;

    observed.push(...await navigateWithResponses(
      page,
      "#/today",
      environment.apiV2,
      ["/briefs/latest", "/live", "/sources/status"]
    ));

    const publicConfig = await page.evaluate(() => {
      const value = window.__BSIDE_GOVERNANCE_CONFIG__ || {};
      return {
        apiBase: String(value.apiBase || ""),
        buildSha: String(value.buildSha || "").toLowerCase(),
        releaseChannel: String(value.releaseChannel || "")
      };
    });
    expect(publicConfig.buildSha).toBe(environment.expectedSha);
    expect(normalizeApiV1(publicConfig.apiBase)).toBe(environment.apiV1);
    expect(publicConfig.releaseChannel).toBe("production_alpha_early_access");
    observed.push(await browserHealth(page, environment.apiV2, environment.expectedSha));
    apiBudgetReceipts = await page.evaluate(async ({ apiBase, storageKey }) => {
      const token = String(sessionStorage.getItem(storageKey) || "");
      const routes = [
        "/briefs/latest?edition=global",
        "/live?limit=100",
        "/events?limit=100",
        "/issuers?limit=100",
        "/calendar?limit=100",
        "/search?q=capital",
        "/sources/status",
        "/exports/events.json?limit=100",
        "/exports/events.csv?limit=100",
        "/feeds/events.atom?limit=100"
      ];
      const results = [];
      for (const route of routes) {
        const response = await fetch(`${apiBase.replace(/\/$/, "")}${route}`, {
          headers: {
            Accept: "*/*",
            Authorization: `Bearer ${token}`
          },
          cache: "no-store",
          credentials: "omit",
          referrerPolicy: "no-referrer"
        });
        const bytes = (await response.arrayBuffer()).byteLength;
        results.push({
          route,
          http_status: response.status,
          size_bytes: bytes,
          api_version_header: String(
            response.headers.get("x-bside-api-version") || ""
          ),
          response_bytes_header: Number(
            response.headers.get("x-response-bytes") || "0"
          )
        });
      }
      return results;
    }, { apiBase: environment.apiV2, storageKey: PREVIEW_SESSION_KEY });
    expect(apiBudgetReceipts).toHaveLength(10);
    for (const receipt of apiBudgetReceipts) {
      expect(receipt.http_status).toBe(200);
      expect(receipt.size_bytes).toBeGreaterThan(0);
      expect(receipt.size_bytes).toBeLessThanOrEqual(250_000);
      expect(receipt.api_version_header).toBe("v2");
      expect(receipt.response_bytes_header).toBe(receipt.size_bytes);
    }
    expect(page.url().includes(environment.token)).toBe(false);
    expect(new URL(page.url()).search).toBe("");

    const topEvents = page.locator(".terminal-top-list [data-event-drawer]:visible");
    await expect(topEvents).toHaveCount(5);
    const firstEvent = topEvents.first();
    await expect(firstEvent).toBeVisible();
    const firstEventBox = await firstEvent.boundingBox();
    expect(firstEventBox).not.toBeNull();
    firstImportantEventTopPx = firstEventBox.y;
    if (testInfo.project.use.viewport?.width === 390) {
      expect(firstImportantEventTopPx).toBeLessThanOrEqual(300);
    }
    const accessibility = await new AxeBuilder({ page }).analyze();
    axeSeriousCount = accessibility.violations.filter(
      (item) => item.impact === "serious"
    ).length;
    axeCriticalCount = accessibility.violations.filter(
      (item) => item.impact === "critical"
    ).length;
    expect(axeSeriousCount).toBe(0);
    expect(axeCriticalCount).toBe(0);
    const eventId = String(await firstEvent.getAttribute("data-event-drawer") || "");
    expect(eventId.length).toBeGreaterThan(0);
    const eventTitle = String(await firstEvent.textContent() || "").trim();
    expect(eventTitle.length).toBeGreaterThanOrEqual(2);
    const eventPath = `/events/${encodeURIComponent(eventId)}`;
    const detailPending = waitForV2(page, environment.apiV2, eventPath);
    await firstEvent.click();
    observed.push(await assertV2Response(await detailPending, eventPath));
    await expect(page.locator("#event-drawer")).toBeVisible();
    await expect(page.locator("#drawer-title")).not.toBeEmpty();
    await page.locator(".icon-button[data-drawer-close]").click();
    await expect(page.locator("#event-drawer-shell")).toBeHidden();

    observed.push(...await navigateWithResponses(page, "#/issuers", environment.apiV2, ["/issuers"]));
    const firstIssuer = page.locator(".company-row a[href^='#/issuers/']").first();
    await expect(firstIssuer).toBeVisible();
    const issuerHref = String(await firstIssuer.getAttribute("href") || "");
    const issuerTitle = String(await firstIssuer.textContent() || "").trim();
    expect(issuerHref.startsWith("#/issuers/")).toBe(true);
    const encodedIssuerId = issuerHref.slice("#/issuers/".length);
    const issuerPath = `/issuers/${encodedIssuerId}`;
    const issuerPending = waitForV2(page, environment.apiV2, issuerPath);
    await firstIssuer.click();
    observed.push(await assertV2Response(await issuerPending, issuerPath));
    await expect(page.locator("h1")).toContainText(issuerTitle);

    const searchTerm = issuerTitle.length >= 2 ? issuerTitle.slice(0, 80) : eventTitle.slice(0, 80);
    const searchPending = waitForV2(page, environment.apiV2, "/search");
    await page.goto(`#/search?q=${encodeURIComponent(searchTerm)}`);
    observed.push(await assertV2Response(await searchPending, "/search"));
    await expect(page.locator("main h1")).toBeVisible();
    await expect(page.locator(".search-result").first()).toBeVisible();

    observed.push(...await navigateWithResponses(page, "#/calendar", environment.apiV2, ["/calendar"]));
    await expect(page.locator("main h1")).toBeVisible();
    await expect(page.locator("#calendar-results, .empty-state").first()).toBeVisible();

    observed.push(...await navigateWithResponses(
      page,
      "#/today",
      environment.apiV2,
      ["/briefs/latest", "/live", "/sources/status"]
    ));
    const finalTopEvents = page.locator(".terminal-top-list [data-event-drawer]:visible");
    await expect(finalTopEvents).toHaveCount(5);
    const finalFirstEventBox = await finalTopEvents.first().boundingBox();
    expect(finalFirstEventBox).not.toBeNull();
    firstImportantEventTopPx = finalFirstEventBox.y;
    if (testInfo.project.use.viewport?.width === 390) {
      expect(firstImportantEventTopPx).toBeLessThanOrEqual(300);
    }
    expect(v1Fallbacks).toEqual([]);
    expect(page.url().includes(environment.token)).toBe(false);
    const restoreScrollBehavior = await stabilizeViewportScreenshot(page);
    try {
      await page.screenshot({ path: screenshotPath });
      await expectViewportPng(screenshotPath, testInfo.project.use.viewport);
    } finally {
      await restoreScrollBehavior();
    }
    completed = true;
  } finally {
    await page.evaluate((key) => sessionStorage.removeItem(key), PREVIEW_SESSION_KEY).catch(() => {});
    if (traceStarted) {
      await context.tracing.stop({ path: tracePath }).catch(() => {});
    }
    const receipt = {
      schema_version: 1,
      evidence_type: "global_alpha_remote_preview_smoke",
      status: completed ? "completed" : "failed",
      code_revision: environment.expectedSha,
      ui_build_sha: completed ? environment.expectedSha : null,
      api_code_revision: completed ? environment.expectedSha : null,
      project,
      viewport: testInfo.project.use.viewport || null,
      browser_name: "chromium",
      used_mock_routes: false,
      preview_token_in_url: false,
      v1_fallback_count: v1Fallbacks.length,
      visual_regression_passed: completed,
      axe_serious_count: axeSeriousCount,
      axe_critical_count: axeCriticalCount,
      first_important_event_top_px: firstImportantEventTopPx,
      api_budget_receipts: apiBudgetReceipts,
      observations: observed
    };
    await writeFile(receiptPath, `${JSON.stringify(receipt, null, 2)}\n`, "utf8");
  }

  expect(completed).toBe(true);
  expect((await stat(screenshotPath)).size).toBeGreaterThan(0);
  expect((await stat(tracePath)).size).toBeGreaterThan(0);
  expect((await stat(receiptPath)).size).toBeGreaterThan(0);
});
