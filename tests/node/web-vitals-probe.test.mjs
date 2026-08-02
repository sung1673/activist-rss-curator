import assert from "node:assert/strict";
import http from "node:http";
import test from "node:test";

import { chromium } from "playwright";

import {
  MAX_API_BATCH,
  MAX_JOURNEY_RETRIES,
  METRIC_NAMES,
  ProbeError,
  ROUTE_TEMPLATES,
  RouteAttemptError,
  RUNS_PER_ROUTE,
  assertObservationMatrix,
  chunkObservations,
  deployedConfigUrl,
  governanceRouteUrl,
  interactionDestination,
  kstDate,
  measureJourneyWithRetry,
  observerInitScript,
  parseDeployedConfig,
  runProbe,
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


function journeyObservations({ routeTemplate, buildSha = SHA }) {
  const measuredAt = new Date().toISOString();
  return METRIC_NAMES.map((metric) => ({
    route_template: routeTemplate,
    measured_at: measuredAt,
    metric,
    value: metric === "CLS" ? 0 : 100,
    device_class: "mobile",
    build_sha: buildSha,
    source: "first_party",
  }));
}


function routeFailure(options, attemptNumber, message = "transient timeout") {
  const cause = new Error(message);
  cause.name = "TimeoutError";
  return new RouteAttemptError({
    routeTemplate: options.routeTemplate,
    runNumber: options.runNumber,
    attemptNumber,
    substep: "wait_for_inp",
    cause,
    previewToken: options.previewToken,
  });
}


function probeEnvironment() {
  return {
    BSIDE_PUBLIC_WEB_URL: WEB,
    BSIDE_API_BASE_URL: API,
    PROBE_EXPECTED_BUILD_SHA: SHA,
    GOVERNANCE_PREVIEW_TOKEN: "x".repeat(32),
    ENABLE_TELEGRAM_DELIVERY: "false",
    ENABLE_GOVERNANCE_DELIVERY: "false",
  };
}


function probeFetch(postedBatches) {
  return async (_url, options = {}) => {
    if (options.method === "POST") {
      const payload = JSON.parse(options.body);
      postedBatches.push(payload.observations);
      return new Response(JSON.stringify({
        ok: true,
        accepted_count: payload.observations.length,
        stored_identifiers: false,
      }), { status: 202, headers: { "content-type": "application/json" } });
    }
    const config = {
      apiBase: API,
      webBase: WEB,
      buildSha: SHA,
      releaseChannel: "production_alpha_early_access",
    };
    return new Response(
      `window.__BSIDE_GOVERNANCE_CONFIG__=Object.freeze(${JSON.stringify(config)});\n`,
      { status: 200, headers: { "content-type": "application/javascript" } },
    );
  };
}


const fakeBrowserFactory = {
  async launch() {
    return { async close() {} };
  },
};


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


test("all mobile journeys use the real Live bottom-navigation destination", () => {
  assert.deepEqual(
    Object.fromEntries(ROUTE_TEMPLATES.map((route) => [
      route,
      interactionDestination(route),
    ])),
    {
      "/today": "live",
      "/events": "live",
      "/issuers": "live",
      "/calendar": "live",
    },
  );
  assert.throws(() => interactionDestination("/unsupported"), ProbeError);
});


test("matrix requires all twenty journeys and five real samples per route and metric", () => {
  const rows = observationMatrix();
  assert.equal(ROUTE_TEMPLATES.length * RUNS_PER_ROUTE, 20);
  assert.equal(rows.length, 60);
  for (const route of ROUTE_TEMPLATES) {
    assert.equal(rows.filter((row) => row.route_template === route).length, 15);
  }
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


test("a retryable journey failure gets one fresh attempt and keeps only successful observations", async () => {
  const attempts = [];
  const retries = [];
  const options = {
    routeTemplate: "/issuers",
    runNumber: 1,
    buildSha: SHA,
    previewToken: "secret-token-" + "x".repeat(32),
  };
  const result = await measureJourneyWithRetry(
    {},
    options,
    async (_browser, attemptOptions) => {
      attempts.push(attemptOptions.attemptNumber);
      if (attemptOptions.attemptNumber === 1) {
        throw routeFailure(
          options,
          1,
          `wait timed out for ${options.previewToken}`,
        );
      }
      return journeyObservations(attemptOptions);
    },
    (error) => retries.push(error),
  );
  assert.equal(MAX_JOURNEY_RETRIES, 1);
  assert.deepEqual(attempts, [1, 2]);
  assert.equal(result.attemptCount, 2);
  assert.equal(result.observations.length, 3);
  assert.ok(result.observations.every((item) => item.route_template === "/issuers"));
  assert.equal(result.retryAudit.length, 1);
  assert.equal(result.retryAudit[0].substep, "wait_for_inp");
  assert.match(result.retryAudit[0].original_error_message_sha256, /^[a-f0-9]{64}$/);
  assert.equal(retries.length, 1);
  assert.equal(retries[0].originalErrorMessage.includes(options.previewToken), false);
  assert.equal(retries[0].originalErrorMessage.includes("[REDACTED]"), true);
});


test("a journey stops after two retryable failures and preserves both attempt audits", async () => {
  const options = {
    routeTemplate: "/calendar",
    runNumber: 5,
    buildSha: SHA,
    previewToken: "x".repeat(32),
  };
  let calls = 0;
  await assert.rejects(
    measureJourneyWithRetry(
      {},
      options,
      async (_browser, attemptOptions) => {
        calls += 1;
        throw routeFailure(options, attemptOptions.attemptNumber);
      },
      () => {},
    ),
    (error) => {
      assert.ok(error instanceof RouteAttemptError);
      assert.equal(error.substep, "wait_for_inp");
      assert.equal(error.cause.name, "TimeoutError");
      assert.deepEqual(error.attempts.map((item) => item.failed_attempt), [1, 2]);
      return true;
    },
  );
  assert.equal(calls, 2);
});


test("explicit probe failures are fail-closed and are never retried", async () => {
  let calls = 0;
  await assert.rejects(
    measureJourneyWithRetry(
      {},
      { routeTemplate: "/today", runNumber: 1 },
      async () => {
        calls += 1;
        throw new ProbeError("route_render_failed");
      },
      () => assert.fail("non-retryable failure reached retry callback"),
    ),
    (error) => error instanceof ProbeError && error.code === "route_render_failed",
  );
  assert.equal(calls, 1);
});


test("a final journey failure submits no telemetry", async () => {
  const postedBatches = [];
  const attempts = new Map();
  await assert.rejects(
    runProbe({
      env: probeEnvironment(),
      fetchImpl: probeFetch(postedBatches),
      browserFactory: fakeBrowserFactory,
      measureAttempt: async (_browser, options) => {
        const key = `${options.routeTemplate}|${options.runNumber}`;
        attempts.set(key, (attempts.get(key) || 0) + 1);
        if (key === "/issuers|1") {
          throw routeFailure(options, options.attemptNumber);
        }
        return journeyObservations(options);
      },
      onRetry: () => {},
    }),
    RouteAttemptError,
  );
  assert.equal(attempts.get("/issuers|1"), 2);
  assert.equal(postedBatches.length, 0);
});


test("a recovered journey still submits exactly one complete sixty-observation matrix", async () => {
  const postedBatches = [];
  let firstAttemptFailed = false;
  const summary = await runProbe({
    env: probeEnvironment(),
    fetchImpl: probeFetch(postedBatches),
    browserFactory: fakeBrowserFactory,
    measureAttempt: async (_browser, options) => {
      if (!firstAttemptFailed && options.routeTemplate === "/events" && options.runNumber === 3) {
        firstAttemptFailed = true;
        throw routeFailure(options, options.attemptNumber);
      }
      return journeyObservations(options);
    },
    onRetry: () => {},
  });
  assert.deepEqual(postedBatches.map((batch) => batch.length), [50, 10]);
  assert.equal(postedBatches.flat().length, 60);
  assert.equal(summary.observation_count, 60);
  assert.equal(summary.accepted_count, 60);
  assert.equal(summary.journey_attempt_audit.successful_journey_count, 20);
  assert.equal(summary.journey_attempt_audit.total_attempt_count, 21);
  assert.equal(summary.journey_attempt_audit.retry_count, 1);
  assert.equal(summary.journey_attempt_audit.max_retries_per_journey, 1);
  assert.deepEqual(
    summary.journey_attempt_audit.retried_journeys.map((item) => [
      item.route_template,
      item.run_number,
      item.failed_attempt,
    ]),
    [["/events", 3, 1]],
  );
  assert.equal(summary.measured_metrics.lcp.sample_count, 20);
  assert.equal(summary.measured_metrics.inp.sample_count, 20);
  assert.equal(summary.measured_metrics.cls.sample_count, 20);
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


test("Chromium first-input preserves a trusted INP when event entries are unavailable", { timeout: 30_000 }, async () => {
  const server = http.createServer((_request, response) => {
    response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
    response.end(`<!doctype html>
      <meta name="viewport" content="width=device-width,initial-scale=1">
      <main><h1>Fast governance journey</h1><button id="interact">Open</button></main>`);
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
    await page.waitForTimeout(250);
    await page.evaluate(() => {
      const state = window.__BSIDE_PRODUCTION_VITALS_PROBE__;
      state.freezePaint();
      state.observers.event.disconnect();
    });
    await page.locator("#interact").click();
    await page.waitForFunction(() => window.__BSIDE_PRODUCTION_VITALS_PROBE__.flushEvents().inp > 0);
    const values = await page.evaluate(() => window.__BSIDE_PRODUCTION_VITALS_PROBE__.flushEvents());
    assert.equal(values.supported["first-input"], true);
    assert.equal(values.supported.event, true);
    assert.ok(values.inp > 0);
    await context.close();
  } finally {
    await browser.close();
    await new Promise((resolve) => server.close(resolve));
  }
});
