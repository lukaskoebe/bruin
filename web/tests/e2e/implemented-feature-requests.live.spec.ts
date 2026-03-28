import { expect } from "@playwright/test";
import { liveTest as test } from "./live-app-fixture";

test.describe("implemented feature requests live", () => {
  test.use({ fixtureName: "configured-workspace" });

  test("opens the environment editor for a configured workspace", async ({ liveApp, page }) => {
    await page.goto(`${liveApp.baseURL}/settings/environments`);

    await expect(page.getByText("Environment Editor", { exact: true })).toBeVisible();
    await expect(page.getByText(".bruin.yml", { exact: true }).last()).toBeVisible();
    await expect(page.locator('input[placeholder="staging"]')).toHaveValue("default");
    await expect(page.locator('input[placeholder="staging_"]')).toHaveValue("dev_");
  });

  test("opens the connection editor with an explicit type selector", async ({
    liveApp,
    page,
  }) => {
    await page.goto(`${liveApp.baseURL}/settings/connections`);

    await expect(page.getByText("Connection Editor", { exact: true })).toBeVisible();
    await expect(page.getByRole("textbox", { name: "MY_CONNECTION" })).toHaveValue(
      "duckdb-default"
    );
    await expect(page.getByRole("combobox").nth(1)).toContainText("duckdb");
    await expect(page.locator('input[value="duckdb-files/local.db"]')).toBeVisible();
  });

  test("renames an asset inline from the editor header with Enter", async ({
    liveApp,
    page,
  }) => {
    await page.goto(`${liveApp.baseURL}/`);

    await page.getByRole("link", { name: "analytics.customers" }).click();
    await page.getByRole("button", { name: "Rename asset" }).click();
    const renameInput = page.locator("main").getByRole("textbox").first();
    await renameInput.fill("analytics.customers_live");
    await renameInput.press("Enter");

    await expect(page.getByRole("link", { name: "analytics.customers_live" })).toBeVisible();
    await expect(page.getByTestId("editor-asset-name")).toHaveText("analytics.customers_live");
  });

  test("shows the already-saved message on Ctrl+S", async ({ liveApp, page }) => {
    await page.goto(`${liveApp.baseURL}/`);

    await page.getByRole("link", { name: "analytics.customers" }).click();
    await page.locator(".monaco-editor").click();
    await page.keyboard.press("ControlOrMeta+S");

    await expect(page.getByText("Already saved.", { exact: true })).toBeVisible();
  });

  test("runs inspect from the SQL editor with Ctrl+Enter", async ({ liveApp, page }) => {
    await page.goto(`${liveApp.baseURL}/`);

    await page.getByRole("link", { name: "analytics.customers" }).click();
    await page.locator(".monaco-editor").click();
    await page.keyboard.press("ControlOrMeta+Enter");

    await expect(page.getByRole("tab", { name: "Inspect" })).toBeVisible();
    await expect(page.getByText("2 rows", { exact: true })).toBeVisible({ timeout: 15000 });
    await expect(page.getByRole("cell", { name: "Ada", exact: true })).toBeVisible();
  });
});
