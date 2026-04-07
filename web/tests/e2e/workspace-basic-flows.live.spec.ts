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

    await expect(page).toHaveTitle("analytics.customers · analytics · Bruin Web");
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

    await expect(page).toHaveTitle("analytics.orders · analytics · Bruin Web");
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

  test("removes stale Bruin Web inferred dependencies but preserves manual ones", async ({
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
        x: Math.round(box.width * 0.55),
        y: Math.round(box.height * 0.3),
      },
    });
    await page.getByPlaceholder("Asset name").fill("analytics.manual_seed");
    await page
      .getByTestId("rf__node-__new_asset__")
      .getByRole("button", { name: "Create", exact: true })
      .click();

    await expect(page.getByRole("link", { name: "analytics.manual_seed" })).toBeVisible();

    await openCustomersEditor(page);

    await page.getByRole("tab", { name: "Dependencies" }).click();
    const dependencyInput = page.getByPlaceholder("Add dependency");
    const manualDependenciesSection = page.getByText("Manual dependencies").locator("..");
    await dependencyInput.fill("analytics.manual_seed");
    await page.getByRole("option", { name: "analytics.manual_seed" }).click();

    await expect(
      manualDependenciesSection.getByText("analytics.manual_seed", { exact: true })
    ).toBeVisible();

    await replaceEditorContent(page, "select *\nfrom analytics.orders");
    await expect
      .poll(async () => {
        await page.getByRole("tab", { name: "Dependencies" }).click();
        const inferredPanel = page.getByText("Automatically inferred").locator("..");
        return await inferredPanel.getByText("analytics.orders", { exact: true }).count();
      }, {
        timeout: 15000,
      })
      .toBe(1);

    const inferredPanel = page.getByText("Automatically inferred").locator("..");
    await expect(inferredPanel.getByText("analytics.orders", { exact: true })).toBeVisible();
    await expect(
      manualDependenciesSection.getByText("analytics.manual_seed", { exact: true })
    ).toBeVisible();

    await replaceEditorContent(page, "select 1 as customer_id");

    await expect
      .poll(async () => {
        await page.getByRole("tab", { name: "Dependencies" }).click();
        return await inferredPanel.getByText("analytics.orders", { exact: true }).count();
      }, {
        timeout: 15000,
      })
      .toBe(0);

    await expect(
      manualDependenciesSection.getByText("analytics.manual_seed", { exact: true })
    ).toBeVisible();
    await expect(page.getByText("No automatically inferred dependencies for this asset.")).toBeVisible();
  });

  test("selects a dependency option by tapping it on mobile", async ({
    liveApp,
    page,
  }) => {
    test.skip(!test.info().project.name.includes("mobile"), "Mobile-only repro.");

    await page.goto(`${liveApp.baseURL}/`);
    await expect(page.getByRole("dialog", { name: "Asset Editor" })).toBeVisible();
    await page.getByRole("tab", { name: "Dependencies" }).click();

    const dependencyInput = page.getByPlaceholder("Add dependency");
    await dependencyInput.fill("analytics.orders");

    const option = page.getByRole("option", { name: "analytics.orders" });
    await expect(option).toBeVisible();
    await option.dispatchEvent("pointerdown", {
      bubbles: true,
      pointerType: "touch",
      isPrimary: true,
      button: 0,
      buttons: 1,
    });
    await option.dispatchEvent("pointerup", {
      bubbles: true,
      pointerType: "touch",
      isPrimary: true,
      button: 0,
      buttons: 0,
    });
    await option.dispatchEvent("click", { bubbles: true });

    await expect(
      page.getByText("Manual dependencies").locator("..").getByText("analytics.orders", {
        exact: true,
      })
    ).toBeVisible();
  });

  test("selects visualization combobox options by tapping them on mobile", async ({
    liveApp,
    page,
  }) => {
    test.skip(!test.info().project.name.includes("mobile"), "Mobile-only repro.");

    await page.goto(`${liveApp.baseURL}/`);
    await expect(page.getByRole("dialog", { name: "Asset Editor" })).toBeVisible();

    await page.getByRole("tab", { name: "Visualization" }).click();

    const viewCombobox = page.getByRole("combobox").filter({ hasText: /none/i }).first();
    await viewCombobox.click();
    await page.getByRole("option", { name: "Chart" }).click();

    const xAxisInput = page.getByPlaceholder("Select x-axis column");
    await xAxisInput.fill("customer_id");
    await page.getByRole("option", { name: "customer_id" }).click();

    await expect(xAxisInput).toHaveValue("customer_id");
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
      await expect(page).toHaveTitle("Workspace · Bruin Web");
      await page.getByRole("button", { name: "Create pipeline" }).last().click();
      await page.getByLabel("Pipeline path").fill("experiments");
      await page.getByRole("button", { name: "Create Pipeline", exact: true }).click();

      await expect(page.getByRole("link", { name: "experiments", exact: true })).toBeVisible();
      await expect(page).toHaveTitle("experiments · Bruin Web");

      await page
        .getByRole("link", { name: "experiments", exact: true })
        .click({ button: "right" });
      await page.getByRole("menuitem", { name: "Rename Pipeline" }).click();
      await page.getByLabel("Pipeline name").fill("experiments_renamed");
      await page.getByRole("button", { name: "Save" }).click();

      await expect(
        page.getByRole("link", { name: "experiments_renamed", exact: true })
      ).toBeVisible();
      await expect(page).toHaveTitle("experiments_renamed · Bruin Web");

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

async function replaceEditorContent(
  page: import("@playwright/test").Page,
  content: string
) {
  const editor = page.locator(".monaco-editor").first();
  await editor.click();
  await page.keyboard.press("ControlOrMeta+A");
  await page.keyboard.type(content);
}
