import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

import { chromium, devices } from "playwright";


export const ROUTE_TEMPLATES = Object.freeze(["/today", "/events", "/issuers", "/calendar"]);
export const METRIC_NAMES = Object.freeze(["LCP", "INP", "CLS"]);
export const RUNS_PER_ROUTE = 5;
export const MAX_API_BATCH = 50;
const PREVIEW_SESSION_KEY = "bside.governance.preview";
const CONFIG_PREFIX = "window.__BSIDE_GOVERNANCE_CONFIG__=Object.freeze(";
const EXPECTED_RELEASE_CHANNEL = "production_alpha_early_access";
const SHA_RE = /^[a-f0-9]{40}$/;
const TOKEN_RE = /^[A-Za-z0-9._~-]{32,512}$/;


export class ProbeError extends Error {
  constructor(code) {
    super(code);
    this.name = "ProbeError";
    this.code = code;
  }
}


function requiredText(value, code) {
  const text = String(value || "").trim();
  if (!text) throw new ProbeError(code);
  return text;
}


export function normalizeHttpsBase(value, kind) {
  let url;
  try {
    url = new URL(requiredText(value, `missing_${kind}_base`));
  } catch (error) {
    if (error instanceof ProbeError) throw error;
    throw new ProbeError(`invalid_${kind}_base`);
  }
  if (url.protocol !== "https:" || url.username || url.password || url.search || url.hash) {
    throw new ProbeError(`unsafe_${kind}_base`);
  }
  url.pathname = url.pathname.replace(/\/+$/, "");
  return url;
}


export function deployedConfigUrl(webBase) {
  const web = normalizeHttpsBase(webBase, "web");
  return new URL("/governance/config.js", web.origin);
}


export function governanceRouteUrl(webBase, routeTemplate) {
  if (!ROUTE_TEMPLATES.includes(routeTemplate)) throw new ProbeError("unsupported_route_template");
  const web = normalizeHttpsBase(webBase, "web");
  const url = new URL("/governance/", web.origin);
  url.hash = `#${routeTemplate}`;
  return url;
}


export function parseDeployedConfig(source, expectedSha, expectedApiBase, expectedWebBase) {
  const text = String(source || "");
  if (text.length < CONFIG_PREFIX.length + 4 || text.length > 8192 || !text.startsWith(CONFIG_PREFIX)) {
    throw new ProbeError("invalid_deployed_config");
  }
  const suffix = ");";
  const trimmed = text.trim();
  if (!trimmed.endsWith(suffix) || trimmed.includes("\n") || trimmed.includes("\r")) {
    throw new ProbeError("invalid_deployed_config");
  }
  let payload;
  try {
    payload = JSON.parse(trimmed.slice(CONFIG_PREFIX.length, -suffix.length));
  } catch (_error) {
    throw new ProbeError("invalid_deployed_config");
  }
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new ProbeError("invalid_deployed_config");
  }
  const keys = Object.keys(payload).sort();
  if (keys.join(",") !== "apiBase,buildSha,releaseChannel,webBase") {
    throw new ProbeError("invalid_deployed_config_fields");
  }
  if (payload.releaseChannel !== EXPECTED_RELEASE_CHANNEL) {
    throw new ProbeError("deployed_release_channel_mismatch");
  }

  const revision = requiredText(expectedSha, "missing_expected_build_sha").toLowerCase();
  const deployedRevision = requiredText(payload.buildSha, "missing_deployed_build_sha").toLowerCase();
  if (!SHA_RE.test(revision) || !SHA_RE.test(deployedRevision) || deployedRevision !== revision) {
    throw new ProbeError("deployed_build_sha_mismatch");
  }

  const configuredApi = normalizeHttpsBase(payload.apiBase, "deployed_api");
  const expectedApi = normalizeHttpsBase(expectedApiBase, "api");
  if (configuredApi.href !== expectedApi.href) throw new ProbeError("deployed_api_base_mismatch");
  const configuredWeb = normalizeHttpsBase(payload.webBase, "deployed_web");
  const expectedWeb = normalizeHttpsBase(expectedWebBase, "web");
  if (configuredWeb.href !== expectedWeb.href) throw new ProbeError("deployed_web_base_mismatch");
  return Object.freeze({
    apiBase: configuredApi.href,
    buildSha: deployedRevision,
    releaseChannel: EXPECTED_RELEASE_CHANNEL,
    webBase: requiredText(payload.webBase, "missing_deployed_web_base"),
  });
}


export function kstDate(value) {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) throw new ProbeError("invalid_measurement_time");
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Seoul",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(date);
  const byType = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${byType.year}-${byType.month}-${byType.day}`;
}


export function chunkObservations(observations, size = MAX_API_BATCH) {
  if (!Number.isInteger(size) || size < 1 || size > MAX_API_BATCH) throw new ProbeError("invalid_batch_size");
  const chunks = [];
  for (let offset = 0; offset < observations.length; offset += size) {
    chunks.push(observations.slice(offset, offset + size));
  }
  return chunks;
}


export function assertObservationMatrix(observations, expectedSha, expectedKstDate) {
  if (!Array.isArray(observations)) throw new ProbeError("invalid_observation_matrix");
  const expectedCount = ROUTE_TEMPLATES.length * RUNS_PER_ROUTE * METRIC_NAMES.length;
  if (observations.length !== expectedCount) throw new ProbeError("incomplete_observation_matrix");
  const revision = String(expectedSha || "").toLowerCase();
  const counts = new Map();
  for (const observation of observations) {
    if (!observation || typeof observation !== "object") throw new ProbeError("invalid_observation");
    if (!ROUTE_TEMPLATES.includes(observation.route_template)) throw new ProbeError("invalid_observation_route");
    if (!METRIC_NAMES.includes(observation.metric)) throw new ProbeError("invalid_observation_metric");
    if (observation.device_class !== "mobile" || observation.source !== "first_party") {
      throw new ProbeError("invalid_observation_provenance");
    }
    if (observation.build_sha !== revision || !SHA_RE.test(observation.build_sha)) {
      throw new ProbeError("invalid_observation_build_sha");
    }
    if (!Number.isFinite(observation.value) || observation.value < 0) throw new ProbeError("invalid_observation_value");
    if ((observation.metric === "LCP" || observation.metric === "INP") && observation.value <= 0) {
      throw new ProbeError("missing_browser_timing");
    }
    if (kstDate(observation.measured_at) !== expectedKstDate) throw new ProbeError("measurement_crossed_kst_day");
    const key = `${observation.route_template}|${observation.metric}`;
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  for (const route of ROUTE_TEMPLATES) {
    for (const metric of METRIC_NAMES) {
      if (counts.get(`${route}|${metric}`) !== RUNS_PER_ROUTE) {
        throw new ProbeError("incomplete_route_metric_samples");
      }
    }
  }
}


async function readDeployedConfig(webBase, expectedSha, expectedApiBase, fetchImpl) {
  let response;
  try {
    response = await fetchImpl(deployedConfigUrl(webBase), {
      method: "GET",
      headers: { Accept: "application/javascript,text/javascript;q=0.9" },
      cache: "no-store",
      redirect: "error",
      referrerPolicy: "no-referrer",
      signal: AbortSignal.timeout(15_000),
    });
  } catch (_error) {
    throw new ProbeError("deployed_config_request_failed");
  }
  if (!response.ok) throw new ProbeError("deployed_config_http_error");
  let source;
  try {
    source = await response.text();
  } catch (_error) {
    throw new ProbeError("deployed_config_read_failed");
  }
  return parseDeployedConfig(source, expectedSha, expectedApiBase, webBase);
}


export function observerInitScript() {
  const state = {
    lcp: null,
    cls: 0,
    inp: null,
    supported: {},
    frozen: false,
    interactions: {},
    observers: {},
  };
  const supported = new Set(PerformanceObserver.supportedEntryTypes || []);
  const install = (type, callback, options) => {
    state.supported[type] = supported.has(type);
    if (!supported.has(type)) return;
    const observer = new PerformanceObserver((list) => callback(list.getEntries()));
    observer.observe({ type, buffered: true, ...(options || {}) });
    state.observers[type] = observer;
  };
  const lcpEntries = (entries) => {
    if (state.frozen) return;
    const latest = entries[entries.length - 1];
    if (latest && Number.isFinite(latest.startTime)) state.lcp = latest.startTime;
  };
  const clsEntries = (entries) => {
    if (state.frozen) return;
    for (const entry of entries) {
      if (!entry.hadRecentInput && Number.isFinite(entry.value)) state.cls += entry.value;
    }
  };
  const eventEntries = (entries) => {
    for (const entry of entries) {
      if (entry.interactionId > 0 && Number.isFinite(entry.duration) && entry.duration > 0) {
        const key = String(entry.interactionId);
        state.interactions[key] = Math.max(state.interactions[key] || 0, entry.duration);
      }
    }
    const values = Object.values(state.interactions);
    if (values.length) state.inp = Math.max(...values);
  };
  install("largest-contentful-paint", lcpEntries);
  install("layout-shift", clsEntries);
  install("event", eventEntries, { durationThreshold: 16 });
  state.freezePaint = () => {
    if (state.observers["largest-contentful-paint"]) {
      lcpEntries(state.observers["largest-contentful-paint"].takeRecords());
      state.observers["largest-contentful-paint"].disconnect();
    }
    if (state.observers["layout-shift"]) {
      clsEntries(state.observers["layout-shift"].takeRecords());
      state.observers["layout-shift"].disconnect();
    }
    state.frozen = true;
  };
  state.flushEvents = () => {
    if (state.observers.event) eventEntries(state.observers.event.takeRecords());
    return { lcp: state.lcp, cls: state.cls, inp: state.inp, supported: state.supported };
  };
  window.__BSIDE_PRODUCTION_VITALS_PROBE__ = state;
}


function interactionDestination(routeTemplate) {
  const destinations = {
    "/today": "events",
    "/events": "companies",
    "/issuers": "calendar",
    "/calendar": "today",
  };
  return destinations[routeTemplate];
}


async function measureRoute(browser, { webBase, previewToken, buildSha, routeTemplate, runNumber }) {
  const context = await browser.newContext({
    ...devices["Pixel 5"],
    locale: "ko-KR",
    timezoneId: "Asia/Seoul",
    serviceWorkers: "block",
  });
  try {
    const origin = normalizeHttpsBase(webBase, "web").origin;
    await context.addInitScript(
      ({ expectedOrigin, key, token }) => {
        if (window.location.origin === expectedOrigin) sessionStorage.setItem(key, token);
      },
      { expectedOrigin: origin, key: PREVIEW_SESSION_KEY, token: previewToken },
    );
    await context.addInitScript(observerInitScript);
    await context.route("**/metrics/web-vitals", (route) => route.abort("blockedbyclient"));
    const page = await context.newPage();
    const cdp = await context.newCDPSession(page);
    await cdp.send("Emulation.setCPUThrottlingRate", { rate: 4 });
    await page.goto(governanceRouteUrl(webBase, routeTemplate).href, {
      waitUntil: "domcontentloaded",
      timeout: 30_000,
    });
    await page.waitForFunction(
      (expectedRoute) => window.location.hash.split("?", 1)[0] === `#${expectedRoute}`
        && !document.querySelector("#app[aria-busy='true']")
        && Boolean(document.querySelector("#app h1")),
      routeTemplate,
      { timeout: 30_000 },
    );
    if (await page.locator("#app .error-state").count()) throw new ProbeError("route_render_failed");
    await page.waitForTimeout(750);
    const support = await page.evaluate(() => {
      const state = window.__BSIDE_PRODUCTION_VITALS_PROBE__;
      if (!state) return null;
      state.freezePaint();
      return state.supported;
    });
    if (!support || !support["largest-contentful-paint"] || !support["layout-shift"] || !support.event) {
      throw new ProbeError("browser_vitals_unsupported");
    }

    const destination = interactionDestination(routeTemplate);
    await page.locator(`[data-nav="${destination}"]`).click({ timeout: 10_000 });
    await page.waitForFunction(
      () => {
        const state = window.__BSIDE_PRODUCTION_VITALS_PROBE__;
        return Boolean(state && state.flushEvents().inp > 0);
      },
      null,
      { timeout: 10_000 },
    );
    const values = await page.evaluate(() => window.__BSIDE_PRODUCTION_VITALS_PROBE__.flushEvents());
    if (!values || !Number.isFinite(values.lcp) || values.lcp <= 0
        || !Number.isFinite(values.inp) || values.inp <= 0
        || !Number.isFinite(values.cls) || values.cls < 0) {
      throw new ProbeError("missing_browser_timing");
    }
    const measuredAt = new Date().toISOString();
    const common = {
      route_template: routeTemplate,
      measured_at: measuredAt,
      device_class: "mobile",
      build_sha: buildSha,
      source: "first_party",
    };
    return [
      { ...common, metric: "LCP", value: Number(values.lcp.toFixed(3)) },
      { ...common, metric: "INP", value: Number(values.inp.toFixed(3)) },
      { ...common, metric: "CLS", value: Number(values.cls.toFixed(6)) },
    ];
  } catch (error) {
    if (error instanceof ProbeError) throw error;
    throw new ProbeError(`route_measurement_failed_${routeTemplate.slice(1)}_${runNumber}`);
  } finally {
    await context.close().catch(() => {});
  }
}


export async function submitObservations(apiBase, previewToken, observations, fetchImpl = fetch) {
  const api = normalizeHttpsBase(apiBase, "api");
  const endpoint = new URL(`${api.pathname.replace(/\/$/, "")}/metrics/web-vitals`, api.origin);
  let accepted = 0;
  const batches = chunkObservations(observations);
  for (const batch of batches) {
    let response;
    try {
      response = await fetchImpl(endpoint, {
        method: "POST",
        headers: {
          Accept: "application/json",
          Authorization: `Bearer ${previewToken}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ observations: batch }),
        cache: "no-store",
        credentials: "omit",
        redirect: "error",
        referrerPolicy: "no-referrer",
        signal: AbortSignal.timeout(20_000),
      });
    } catch (_error) {
      throw new ProbeError("web_vitals_submit_failed");
    }
    let payload;
    try {
      payload = await response.json();
    } catch (_error) {
      throw new ProbeError("invalid_web_vitals_response");
    }
    if (response.status !== 202 || !payload || payload.ok !== true
        || payload.accepted_count !== batch.length || payload.stored_identifiers !== false) {
      throw new ProbeError("web_vitals_ack_mismatch");
    }
    accepted += payload.accepted_count;
  }
  if (accepted !== observations.length) throw new ProbeError("web_vitals_total_ack_mismatch");
  return { acceptedCount: accepted, batchSizes: batches.map((batch) => batch.length) };
}


function parseArguments(argv) {
  const values = { summaryPath: "web-vitals-run-summary.json" };
  for (let index = 0; index < argv.length; index += 1) {
    if (argv[index] === "--summary" && argv[index + 1]) {
      values.summaryPath = argv[index + 1];
      index += 1;
    } else {
      throw new ProbeError("invalid_command_argument");
    }
  }
  return values;
}

function percentile75(values) {
  const sorted = [...values].sort((left, right) => left - right);
  if (!sorted.length) throw new ProbeError("missing_metric_samples");
  return sorted[Math.max(0, Math.ceil(sorted.length * 0.75) - 1)];
}


export async function runProbe({ env = process.env, fetchImpl = fetch, browserFactory = chromium }) {
  const webBase = normalizeHttpsBase(env.BSIDE_PUBLIC_WEB_URL, "web").href;
  const apiBase = normalizeHttpsBase(env.BSIDE_API_BASE_URL, "api").href;
  const expectedSha = requiredText(env.PROBE_EXPECTED_BUILD_SHA, "missing_expected_build_sha").toLowerCase();
  if (!SHA_RE.test(expectedSha)) throw new ProbeError("invalid_expected_build_sha");
  const previewToken = requiredText(env.GOVERNANCE_PREVIEW_TOKEN, "missing_preview_token");
  if (!TOKEN_RE.test(previewToken)) throw new ProbeError("invalid_preview_token");
  if (String(env.ENABLE_TELEGRAM_DELIVERY || "").toLowerCase() !== "false"
      || String(env.ENABLE_GOVERNANCE_DELIVERY || "").toLowerCase() !== "false") {
    throw new ProbeError("outbound_delivery_must_be_disabled");
  }

  const deployed = await readDeployedConfig(webBase, expectedSha, apiBase, fetchImpl);
  const startKstDate = kstDate(new Date());
  const browser = await browserFactory.launch({ headless: true });
  const observations = [];
  try {
    for (const routeTemplate of ROUTE_TEMPLATES) {
      for (let runNumber = 1; runNumber <= RUNS_PER_ROUTE; runNumber += 1) {
        const measured = await measureRoute(browser, {
          webBase,
          previewToken,
          buildSha: deployed.buildSha,
          routeTemplate,
          runNumber,
        });
        observations.push(...measured);
        process.stdout.write(`measured ${routeTemplate} ${runNumber}/${RUNS_PER_ROUTE}\n`);
      }
    }
  } finally {
    await browser.close().catch(() => {});
  }
  assertObservationMatrix(observations, deployed.buildSha, startKstDate);
  const submission = await submitObservations(deployed.apiBase, previewToken, observations, fetchImpl);
  assert.equal(submission.acceptedCount, observations.length);
  const metricValues = Object.fromEntries(
    METRIC_NAMES.map((metric) => [
      metric,
      observations
        .filter((record) => record.metric === metric)
        .map((record) => record.value),
    ]),
  );
  return {
    schema_version: 1,
    observation_date_kst: startKstDate,
    build_sha: deployed.buildSha,
    source: "first_party",
    device_class: "mobile",
    route_templates: [...ROUTE_TEMPLATES],
    runs_per_route: RUNS_PER_ROUTE,
    metric_names: [...METRIC_NAMES],
    observation_count: observations.length,
    accepted_count: submission.acceptedCount,
    api_batch_sizes: submission.batchSizes,
    measured_metrics: {
      lcp: {
        p75_seconds: Number((percentile75(metricValues.LCP) / 1000).toFixed(6)),
        sample_count: metricValues.LCP.length,
      },
      inp: {
        p75_ms: Number(percentile75(metricValues.INP).toFixed(3)),
        sample_count: metricValues.INP.length,
      },
      cls: {
        p75: Number(percentile75(metricValues.CLS).toFixed(6)),
        sample_count: metricValues.CLS.length,
      },
    },
    token_transport: "sessionStorage",
  };
}


async function main() {
  const args = parseArguments(process.argv.slice(2));
  try {
    const summary = await runProbe({});
    await writeFile(args.summaryPath, `${JSON.stringify(summary, null, 2)}\n`, { encoding: "utf8", mode: 0o600 });
    process.stdout.write(`accepted ${summary.accepted_count} privacy-minimal Web Vitals observations\n`);
  } catch (error) {
    const code = error instanceof ProbeError ? error.code : "unexpected_probe_failure";
    process.stderr.write(`web-vitals probe failed: ${code}\n`);
    process.exitCode = 1;
  }
}


if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await main();
}
