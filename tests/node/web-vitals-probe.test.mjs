import assert from "node:assert/strict";
import http from "node:http";
import test from "node:test";

import { chromium } from "playwright";

import {
  MAX_API_BATCH,
  METRIC_NAMES,
  ProbeError,
  ROUTE_TEMPLATES,
  RUNS_PER_ROUTE,
  assertObservationMatrix,
  chunkObservations,
  deployedConfigUrl,
  governanceRouteUrl,
  interactionDestination,
  kstDate,
  observerInitScript,
  parseDeployedConfig,
  submitObservations,
} from "../../.github/scripts/web-vitals-probe.mjs";


const SHA = "a".repeat(40);
const API = "https://alignpe.gabia.io/activist/api.php/api/v1";
const WEB = "https://news.bside.ai";


function observationMatrix() {
  const rows = [];
  for (const route of ROUTE_TEMPLATES) {
    for (let run = 0; run < RUNS_PER_ROUTE; run += 1) {
      for (const metric of METRIC_NAMES) {
        rows.push({
          route_template: route,
          measured_at: "2026-07-22T12:00:00.000Z",
          metric,
          value: metric === "CLS" ? 0 : 100,
          device_class: "mobile",
          build_sha: SHA,
          source: "first_party",
        });
      }
    }
  }
  return rows;
}


test("deployed config requires an exact full revision and API base", () => {
  const source = `window.__BSIDE_GOVERNANCE_CONFIG__=Object.freeze({"apiBase":"${API}","webBase":"${WEB}","buildSha":"${SHA}","releaseChannel":"production_alpha_early_access"});\n`;
  assert.deepEqual(parseDeployedConfig(source, SHA, API, WEB), {
    apiBase: API,
    buildSha: SHA,
    releaseChannel: "production_alpha_early_access",
    webBase: WEB,
  });
  assert.throws(() => parseDeployedConfig(source, "a".repeat(39), API, WEB), ProbeError);
  assert.throws(() => parseDeployedConfig(source.replace(SHA, "b".repeat(40)), SHA, API, WEB), ProbeError);
  assert.throws(() => parseDeployedConfig(source, SHA, `${API}/wrong`, WEB), ProbeError);
  assert.throws(() => parseDeployedConfig(source, SHA, API, "https://other.example"), ProbeError);
  assert.throws(
    () => parseDeployedConfig(
      source.replace("production_alpha_early_access", "production_alpha"),
      SHA,
      API,
      WEB,
    ),
    ProbeError,
  );
});


test("route URLs contain only the SPA fragment and never a preview credential", () => {
  assert.equal(deployedConfigUrl(WEB).href, `${WEB}/governance/config.js`);
  for (const route of ROUTE_TEMPLATES) {
    const url = governanceRouteUrl(WEB, route);
    assert.equal(url.search, "");
    assert.equal(url.hash, `#${route}`);
    assert.equal(url.href.includes("preview"), false);
  }
});


test("mobile journeys choose a bottom-navigation destination", () => {
  assert.deepEqual(
    Object.fromEntries(ROUTE_TEMPLATES.map((route) => [
      route,
      interactionDestination(route),
    ])),
    {
      "/today": "live",
      "/events": "calendar",
      "/issuers": "calendar",
      "/calendar": "today",
    },
  );
  assert.throws(() => interactionDestination("/unsupported"), ProbeError);
});


test("matrix requires exactly four routes times five real samples for every metric", () => {
  const rows = observationMatrix();
  assert.equal(rows.length, 60);
  assert.doesNotThrow(() => assertObservationMatrix(rows, SHA, "2026-07-22"));
  assert.throws(() => assertObservationMatrix(rows.slice(1), SHA, "2026-07-22"), ProbeError);
  const unsupported = structuredClone(rows);
  unsupported[0].source = "synthetic";
  assert.throws(() => assertObservationMatrix(unsupported, SHA, "2026-07-22"), ProbeError);
  const crossedMidnight = structuredClone(rows);
  crossedMidnight[0].measured_at = "2026-07-22T15:00:00.000Z";
  assert.throws(() => assertObservationMatrix(crossedMidnight, SHA, "2026-07-22"), ProbeError);
});


test("sixty observations are split into API batches no larger than fifty", () => {
  const chunks = chunkObservations(observationMatrix());
  assert.deepEqual(chunks.map((chunk) => chunk.length), [MAX_API_BATCH, 10]);
  assert.throws(() => chunkObservations([], 51), ProbeError);
});


test("submission requires an exact 202 acknowledgement for every batch", async () => {
  const calls = [];
  const fakeFetch = async (url, options) => {
    const payload = JSON.parse(options.body);
    calls.push({ url: String(url), options, count: payload.observations.length });
    return new Response(JSON.stringify({
      ok: true,
      accepted_count: payload.observations.length,
      stored_identifiers: false,
    }), { status: 202, headers: { "content-type": "application/json" } });
  };
  const result = await submitObservations(API, "x".repeat(32), observationMatrix(), fakeFetch);
  assert.deepEqual(result, { acceptedCount: 60, batchSizes: [50, 10] });
  assert.deepEqual(calls.map((call) => call.count), [50, 10]);
  assert.ok(calls.every((call) => !call.url.includes("x".repeat(32))));
  assert.ok(calls.every((call) => call.options.headers.Authorization === `Bearer ${"x".repeat(32)}`));

  await assert.rejects(
    submitObservations(API, "x".repeat(32), observationMatrix(), async () => (
      new Response(JSON.stringify({ ok: true, accepted_count: 49, stored_identifiers: false }), {
        status: 202,
        headers: { "content-type": "application/json" },
      })
    )),
    ProbeError,
  );
});


test("KST date assignment is independent of the runner timezone", () => {
  assert.equal(kstDate("2026-07-22T14:59:59Z"), "2026-07-22");
  assert.equal(kstDate("2026-07-22T15:00:00Z"), "2026-07-23");
});


test("Chromium produces LCP, CLS support, and INP from a trusted click", { timeout: 30_000 }, async () => {
  const server = http.createServer((_request, response) => {
    response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
    response.end(`<!doctype html>
      <meta name="viewport" content="width=device-width,initial-scale=1">
      <main><h1>Governance performance journey</h1><button id="interact">Open records</button><div id="rows"></div></main>
      <script>
        document.querySelector('#interact').addEventListener('click', () => {
          const target = document.querySelector('#rows');
          for (let index = 0; index < 2500; index += 1) {
            const row = document.createElement('span');
            row.textContent = 'record ' + index;
            target.append(row);
          }
          target.getBoundingClientRect();
        });
      </script>`);
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address === "object");
  const browser = await chromium.launch({ headless: true });
  try {
    const context = await browser.newContext({ viewport: { width: 393, height: 851 } });
    await context.addInitScript(observerInitScript);
    const page = await context.newPage();
    await page.goto(`http://127.0.0.1:${address.port}/`, { waitUntil: "load" });
    await page.waitForTimeout(500);
    await page.evaluate(() => window.__BSIDE_PRODUCTION_VITALS_PROBE__.freezePaint());
    await page.locator("#interact").click();
    await page.waitForFunction(() => window.__BSIDE_PRODUCTION_VITALS_PROBE__.flushEvents().inp > 0);
    const values = await page.evaluate(() => window.__BSIDE_PRODUCTION_VITALS_PROBE__.flushEvents());
    assert.equal(values.supported["largest-contentful-paint"], true);
    assert.equal(values.supported["layout-shift"], true);
    assert.equal(values.supported.event, true);
    assert.ok(values.lcp > 0);
    assert.ok(values.inp > 0);
    assert.ok(values.cls >= 0);
    await context.close();
  } finally {
    await browser.close();
    await new Promise((resolve) => server.close(resolve));
  }
});
