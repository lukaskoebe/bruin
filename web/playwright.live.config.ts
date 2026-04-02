import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/e2e",
  testMatch: "**/*.live.spec.ts",
  fullyParallel: true,
  retries: process.env.CI ? 1 : 0,
  use: {
    trace: "on-first-retry",
  },
  projects: [
    {
      name: "chromium-live",
      use: { ...devices["Desktop Chrome"] },
    },
    {
      name: "mobile-chrome-live",
      use: { ...devices["Pixel 7"] },
    },
  ],
});
