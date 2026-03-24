import { test, expect, Page } from "@playwright/test";

test.beforeEach(async ({ page }) => {
  await mockWorkspaceEndpoints(page);
});

test("shows the empty workspace state", async ({ page }) => {
  await page.goto("/");

  const emptyStateCreateButton = page
    .getByRole("button", { name: "Create pipeline" })
    .last();

  await expect(page.getByRole("heading", { name: "Create your first pipeline" })).toBeVisible();
  await expect(emptyStateCreateButton).toBeVisible();
});

test("opens the create pipeline dialog from the empty state", async ({ page }) => {
  await page.goto("/");

  await page.getByRole("button", { name: "Create pipeline" }).last().click();

  await expect(page.getByRole("dialog")).toBeVisible();
  await expect(page.getByLabel("Pipeline path")).toBeVisible();
});

async function mockWorkspaceEndpoints(page: Page) {
  await page.route("**/api/workspace", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        pipelines: [],
        connections: {},
        selected_environment: "default",
        errors: [],
        updated_at: new Date().toISOString(),
        revision: 1,
      }),
    });
  });

  await page.route("**/api/config", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        status: "ok",
        path: ".bruin.yml",
        default_environment: "default",
        selected_environment: "default",
        environments: [],
        connection_types: [],
      }),
    });
  });

  await page.route("**/api/events", async (route) => {
    await route.fulfill({
      contentType: "text/event-stream",
      body: "",
    });
  });
}
