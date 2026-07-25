import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/e2e",
  fullyParallel: true,
  forbidOnly: true,
  retries: 1,
  timeout: 30_000,
  reporter: "line",
  use: {
    baseURL: "http://127.0.0.1:8765/governance/",
    browserName: "chromium",
    trace: "retain-on-failure"
  },
  projects: [
    { name: "desktop-chromium", use: { viewport: { width: 1440, height: 900 } } },
    { name: "tablet-chromium", use: { viewport: { width: 768, height: 1024 } } },
    { name: "mobile-chromium", use: { viewport: { width: 390, height: 844 } } }
  ],
  webServer: {
    command: "python -m http.server 8765 --bind 127.0.0.1 --directory public",
    url: "http://127.0.0.1:8765/governance/",
    reuseExistingServer: true,
    timeout: 15_000
  }
});
