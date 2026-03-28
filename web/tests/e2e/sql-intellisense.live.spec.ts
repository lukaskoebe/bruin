import { expect, Page } from "@playwright/test";

import { liveTest as test } from "./live-app-fixture";

test.describe("sql intellisense live", () => {
  test.use({ fixtureName: "configured-workspace" });

  test("requests parser-backed intellisense context from the live server", async ({
    liveApp,
    page,
  }) => {
    await page.goto(`${liveApp.baseURL}/`);

    await openCustomersEditor(page);
    const parseContextResponse = page.waitForResponse(
      async (response) =>
        response.url().includes("/api/sql/parse-context") &&
        response.request().method() === "POST" &&
        (response.request().postData() ?? "").includes("analytics.orders as o")
    );
    await replaceEditorContent(
      page,
      "select o.order_id\nfrom analytics.orders as o"
    );

    const response = await parseContextResponse;
    const body = await response.json();

    expect(body.status).toBe("ok");
    expect(body.tables).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "analytics.orders", alias: "o" }),
      ])
    );
    expect(body.columns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ qualifier: "o", name: "o.order_id" }),
      ])
    );
  });

  test("shows resolved upstream columns in the SQL debug panel", async ({ liveApp, page }) => {
    await page.goto(`${liveApp.baseURL}/`);

    await openCustomersEditor(page);
    await replaceEditorContent(page, "select *\nfrom analytics.orders");
    await page.getByText("SQL column debug", { exact: true }).click();
    const debugPanel = page.locator("details").last();

    await expect(debugPanel.getByText("Parsed upstreams (1)")).toBeVisible();
    await expect(debugPanel.getByText("analytics.orders", { exact: true }).last()).toBeVisible();
    await expect(
      debugPanel.getByText("analytics.orders -> analytics.orders · resolved-without-columns", {
        exact: true,
      })
    ).toBeVisible();
    await expect(debugPanel.getByText("(resolved, but no columns)", { exact: true })).toBeVisible();
  });

  test("navigates to the referenced asset on Ctrl+click", async ({ liveApp, page }) => {
    await page.goto(`${liveApp.baseURL}/`);

    await openCustomersEditor(page);
    await replaceEditorContent(page, "select * from analytics.orders\n");

    const modifier = process.platform === "darwin" ? "Meta" : "Control";
    await page.keyboard.down(modifier);
    await clickEditorText(page, "analytics.orders");
    await page.keyboard.up(modifier);

    await expect(page.getByTestId("editor-asset-name")).toHaveText("analytics.orders");
    await expect(page.getByTestId("editor-asset-path")).toHaveText(
      "analytics/assets/orders.sql"
    );
  });
});

async function openCustomersEditor(page: Page) {
  await page.getByRole("link", { name: "analytics.customers" }).click();
  await expect(page.getByTestId("editor-asset-name")).toHaveText("analytics.customers");
}

async function replaceEditorContent(
  page: Page,
  content: string
) {
  const editor = page.locator(".monaco-editor").first();
  await editor.click();
  await page.keyboard.press("ControlOrMeta+A");
  await page.keyboard.type(content);
}

async function clickEditorLine(page: Page, text: string) {
  const line = page.locator(".view-line").filter({ hasText: text }).first();
  await line.click();
}

async function clickEditorText(page: Page, text: string) {
  const line = page.locator(".view-line").filter({ hasText: text }).first();
  await line.click();
}
