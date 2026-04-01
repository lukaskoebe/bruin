import { expect } from "@playwright/test";

import { liveTest as test } from "./live-app-fixture";

test.describe("workspace live basic flows", () => {
  test.use({ fixtureName: "basic-workspace" });

  test("loads the fixture workspace and opens an asset in the editor", async ({
    liveApp,
    page,
  }) => {
    await page.goto(`${liveApp.baseURL}/`);

    await expect(page.getByRole("link", { name: "analytics", exact: true })).toBeVisible();
    await expect(page.getByRole("link", { name: "analytics.customers" })).toBeVisible();

    await openCustomersEditor(page);

    await expect(
      page.getByText("analytics.customers", { exact: true }).last()
    ).toBeVisible();
    await expect(
      page.getByText("analytics/assets/customers.sql", { exact: true })
    ).toBeVisible();
  });

  test("switches assets from the sidebar against the real server", async ({
    liveApp,
    page,
  }) => {
    await page.goto(`${liveApp.baseURL}/`);

    await openCustomersEditor(page);
    await page.getByRole("link", { name: "analytics.orders" }).click();

    await expect(
      page.getByText("analytics.orders", { exact: true }).last()
    ).toBeVisible();
    await expect(
      page.getByText("analytics/assets/orders.sql", { exact: true })
    ).toBeVisible();
  });

  test("runs inspect for the selected asset", async ({ liveApp, page }) => {
    await page.goto(`${liveApp.baseURL}/`);

    await openCustomersEditor(page);
    await page.getByRole("button", { name: "Inspect" }).click();

    await expect(page.getByRole("tab", { name: "Inspect" })).toBeVisible();
    await expect(page.getByText("2 rows", { exact: true })).toBeVisible({
      timeout: 15000,
    });
    await expect(
      page.getByRole("columnheader", { name: "customer_id", exact: true })
    ).toBeVisible();
    await expect(page.getByRole("cell", { name: "Ada", exact: true })).toBeVisible();
  });

  test("reveals nested command palette matches from root search", async ({
    liveApp,
    page,
  }) => {
    await page.goto(`${liveApp.baseURL}/`);

    await openCommandPalette(page);

    const commandInput = page.locator('[data-slot="command-input"]');
    await commandInput.fill("orders");

    await expect(page.getByRole("option", { name: "analytics.orders analytics" })).toBeVisible();
    await page.getByRole("option", { name: "analytics.orders analytics" }).click();

    await expect(page.getByTestId("editor-asset-name")).toHaveText("analytics.orders");
    await expect(page.getByTestId("editor-asset-path")).toHaveText(
      "analytics/assets/orders.sql"
    );
  });

  test("clears the command palette search when entering a nested page", async ({
    liveApp,
    page,
  }) => {
    await page.goto(`${liveApp.baseURL}/`);

    await openCommandPalette(page);

    const commandInput = page.locator('[data-slot="command-input"]');
    await commandInput.fill("asset");
    await page.getByRole("option", { name: /Go to asset/i }).click();

    await expect(commandInput).toHaveValue("");
    await expect(page.getByRole("option", { name: "analytics.customers analytics" })).toBeVisible();
    await expect(page.getByRole("option", { name: "analytics.orders analytics" })).toBeVisible();
  });

  test("opens the rename pipeline dialog from the live sidebar context menu", async ({
    liveApp,
    page,
  }) => {
    await page.goto(`${liveApp.baseURL}/`);

    await page
      .getByRole("link", { name: "analytics", exact: true })
      .click({ button: "right" });
    await page.getByRole("menuitem", { name: "Rename Pipeline" }).click();

    await expect(page.getByLabel("Pipeline name")).toHaveValue("analytics");
  });

  test("materializes the selected asset and records a history entry", async ({
    liveApp,
    page,
  }) => {
    await page.goto(`${liveApp.baseURL}/`);

    await openCustomersEditor(page);
    const emptyHistoryMessage = page.getByText("No materialize runs yet.");

    await page.getByRole("tab", { name: "Materialize" }).click();
    await expect(emptyHistoryMessage).toBeVisible();
    await page.getByRole("button", { name: "Materialize", exact: true }).click();

    await expect(page.getByRole("tab", { name: "Materialize" })).toBeVisible();
    await expect(emptyHistoryMessage).toHaveCount(0);
    await expect(
      page.getByText("Asset: analytics.customers", { exact: true })
    ).toBeVisible({ timeout: 15000 });
  });

  test("creates, renames, and deletes an asset in an isolated workspace", async ({
    liveApp,
    page,
  }) => {
    await page.goto(`${liveApp.baseURL}/`);

    await openCustomersEditor(page);

    const canvas = page.locator(".react-flow__pane").first();
    const box = await canvas.boundingBox();
    if (!box) {
      throw new Error("Could not locate the React Flow pane for asset creation.");
    }

    await canvas.click({
      position: {
        x: Math.round(box.width * 0.35),
        y: Math.round(box.height * 0.35),
      },
    });
    await page.getByPlaceholder("Asset name").fill("analytics.new_asset");
    await page
      .getByTestId("rf__node-__new_asset__")
      .getByRole("button", { name: "Create", exact: true })
      .click();

    await expect(page.getByRole("link", { name: "analytics.new_asset" })).toBeVisible();

    await page.getByRole("link", { name: "analytics.new_asset" }).click();
    await page.getByRole("button", { name: "Rename asset" }).click();
    await page.locator('input[value="analytics.new_asset"]').fill("analytics.renamed_asset");
    await page.getByRole("button", { name: "Save" }).first().click();

    await expect(page.getByRole("link", { name: "analytics.renamed_asset" })).toBeVisible();

    await page.getByRole("button", { name: "Delete" }).click();
    await page.getByRole("button", { name: "Delete" }).last().click();

    await expect(
      page.getByRole("link", { name: "analytics.renamed_asset" })
    ).toHaveCount(0);
  });

  test.describe("empty workspace live flows", () => {
    test.use({ fixtureName: "empty-workspace" });

    test("creates, renames, and deletes a pipeline in an isolated workspace", async ({
      liveApp,
      page,
    }) => {
      await page.goto(`${liveApp.baseURL}/`);

      await expect(
        page.getByRole("heading", { name: "Create your first pipeline" })
      ).toBeVisible();
      await page.getByRole("button", { name: "Create pipeline" }).last().click();
      await page.getByLabel("Pipeline path").fill("experiments");
      await page.getByRole("button", { name: "Create Pipeline", exact: true }).click();

      await expect(page.getByRole("link", { name: "experiments", exact: true })).toBeVisible();

      await page
        .getByRole("link", { name: "experiments", exact: true })
        .click({ button: "right" });
      await page.getByRole("menuitem", { name: "Rename Pipeline" }).click();
      await page.getByLabel("Pipeline name").fill("experiments_renamed");
      await page.getByRole("button", { name: "Save" }).click();

      await expect(
        page.getByRole("link", { name: "experiments_renamed", exact: true })
      ).toBeVisible();

      await page.reload();

      await expect(
        page.getByRole("link", { name: "experiments_renamed", exact: true })
      ).toBeVisible();

      await page
        .getByRole("link", { name: "experiments_renamed", exact: true })
        .click({ button: "right" });
      await page.getByRole("menuitem", { name: "Delete Pipeline" }).click();
      await page.getByRole("button", { name: "Delete Pipeline" }).click();

      await expect(
        page.getByRole("link", { name: "experiments_renamed", exact: true })
      ).toHaveCount(0);
    });
  });
});

async function openCustomersEditor(page: import("@playwright/test").Page) {
  const editorAssetName = page.getByTestId("editor-asset-name");

  await expect(page.getByRole("link", { name: "analytics.customers" })).toBeVisible();

  if ((await editorAssetName.textContent())?.trim() !== "analytics.customers") {
    await page.getByRole("link", { name: "analytics.customers" }).click();
  }

  await expect(editorAssetName).toHaveText("analytics.customers");
}

async function openCommandPalette(page: import("@playwright/test").Page) {
  await page.getByRole("button", { name: "Open search" }).click();
  await expect(page.locator('[data-slot="command-input"]')).toBeVisible();
}
