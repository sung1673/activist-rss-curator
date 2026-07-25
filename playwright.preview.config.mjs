import { defineConfig } from "@playwright/test";

function requiredHttpsBase(name, value) {
  const raw = String(value || "").trim();
  if (!raw) throw new Error(`${name} is required`);
  let parsed;
  try {
    parsed = new URL(raw);
  } catch (_error) {
    throw new Error(`${name} must be a valid URL`);
  }
  if (
    parsed.protocol !== "https:"
    || parsed.username
    || parsed.password
    || parsed.search
    || parsed.hash
  ) {
    throw new Error(`${name} must be a credential-free, query-free, fragment-free HTTPS URL`);
  }
  parsed.pathname = parsed.pathname.replace(/\/+$/, "") || "/";
  return parsed;
}

const publicWeb = requiredHttpsBase(
  "BSIDE_PUBLIC_WEB_URL",
  process.env.BSIDE_PUBLIC_WEB_URL || "https://news.bside.ai"
);
const configuredPreview = String(process.env.BSIDE_ALPHA_PREVIEW_WEB_URL || "").trim();
const previewWeb = configuredPreview
  ? requiredHttpsBase("BSIDE_ALPHA_PREVIEW_WEB_URL", configuredPreview)
  : new URL("/governance", publicWeb.origin);
if (previewWeb.pathname === "/") previewWeb.pathname = "/governance";
previewWeb.pathname = `${previewWeb.pathname.replace(/\/+$/, "")}/`;

export default defineConfig({
  testDir: "./tests/e2e",
  testMatch: "governance-preview-remote.spec.mjs",
  fullyParallel: true,
  forbidOnly: true,
  retries: 0,
  timeout: 120_000,
  expect: { timeout: 20_000 },
  reporter: "line",
  preserveOutput: "always",
  outputDir: "test-results/preview-remote",
  use: {
    baseURL: previewWeb.toString(),
    browserName: "chromium",
    serviceWorkers: "block",
    ignoreHTTPSErrors: false,
    screenshot: "off",
    trace: "off",
    video: "off"
  },
  projects: [
    { name: "desktop-chromium", use: { viewport: { width: 1440, height: 900 } } },
    { name: "tablet-chromium", use: { viewport: { width: 768, height: 1024 } } },
    { name: "mobile-chromium", use: { viewport: { width: 390, height: 844 } } }
  ]
});
