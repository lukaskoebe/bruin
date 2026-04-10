import { expect, Page } from "@playwright/test";
import { writeFile } from "node:fs/promises";
import { join } from "node:path";

import { liveTest as test } from "./live-app-fixture";

test.describe("sql intellisense live", () => {
  test.use({ fixtureName: "configured-workspace" });

  test("requests parser-backed intellisense context from the live server", async ({
    liveApp,
    page,
  }) => {
    await page.goto(`${liveApp.baseURL}/`);

    await openCustomersEditor(page);
    await replaceEditorContent(
      page,
      "select o.order_id\nfrom analytics.orders as o"
    );

    let body: unknown = null;
    await expect
      .poll(async () => {
        body = await page.evaluate(async () => {
          const response = await fetch("/api/sql/parse-context", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              asset_id: "YW5hbHl0aWNzL2Fzc2V0cy9jdXN0b21lcnMuc3Fs",
              content: "select o.order_id\nfrom analytics.orders as o",
              schema: [],
            }),
          });
          return await response.json();
        });

        return (body as { status?: string } | null)?.status ?? null;
      })
      .toBe("ok");

    const parseContext = body as {
      status: string;
      tables?: Array<{ name?: string; alias?: string }>;
      columns?: Array<{ qualifier?: string; name?: string }>;
    };

    expect(parseContext.tables).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "analytics.orders", alias: "o" }),
      ])
    );
    expect(parseContext.columns).toEqual(
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

  test("shows quoted workspace path suggestions for DuckDB SQL", async ({
    liveApp,
    page,
  }) => {
    await writeFile(
      join(liveApp.workspaceDir, "duckdb-files", "customers.csv"),
      "customer_id,customer_name\n1,Ada\n",
      "utf8"
    );

    await page.goto(`${liveApp.baseURL}/`);
    await openCustomersEditor(page);

    const pathSuggestionsResponse = page.waitForResponse(
      (response) =>
        response.url().includes("/sql-path-suggestions") &&
        response.request().method() === "GET" &&
        response.url().includes(`prefix=${encodeURIComponent("./duckdb-files/cu")}`)
    );

    await replaceEditorContent(page, 'select * from "./duckdb-files/cu');

    const response = await pathSuggestionsResponse;
    const body = await response.json();

    expect(body.status).toBe("ok");
    expect(body.suggestions).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          value: "./duckdb-files/customers.csv",
          kind: "file",
        }),
      ])
    );

    await page.keyboard.press("ControlOrMeta+Space");

    const suggestWidget = page.locator(".suggest-widget.visible").first();
    await expect(suggestWidget).toBeVisible();
    await expect(suggestWidget.getByText("./duckdb-files/customers.csv", { exact: true })).toBeVisible();
  });

  test("does not report DuckDB file paths as unresolved tables", async ({
    liveApp,
    page,
  }) => {
    await writeFile(
      join(liveApp.workspaceDir, "duckdb-files", "customers.csv"),
      "customer_id,customer_name\n1,Ada\n",
      "utf8"
    );

    await page.goto(`${liveApp.baseURL}/`);
    await openCustomersEditor(page);

    await replaceEditorContent(page, 'select * from "./duckdb-files/customers.csv"');

    let body: unknown = null;
    await expect
      .poll(async () => {
        body = await page.evaluate(async () => {
          const response = await fetch("/api/sql/parse-context", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              asset_id: "YW5hbHl0aWNzL2Fzc2V0cy9jdXN0b21lcnMuc3Fs",
              content: 'select * from "./duckdb-files/customers.csv"',
              schema: [],
            }),
          });
          return await response.json();
        });

        return (body as { status?: string } | null)?.status ?? null;
      })
      .toBe("ok");

    const parseContext = body as {
      diagnostics?: Array<{ message?: string }>;
    };

    expect(parseContext.diagnostics ?? []).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          message: "Unresolved table: ./duckdb-files/customers.csv",
        }),
      ])
    );
  });

  test("shows latest inspect SQL error as Monaco diagnostics while content is unchanged", async ({
    liveApp,
    page,
  }) => {
    await page.goto(`${liveApp.baseURL}/`);
    await openCustomersEditor(page);

    await replaceEditorContent(
      page,
      'select * from finances.raw_downstream_downstream_downstream'
    );
    const saveResponse = page.waitForResponse(
      (response) =>
        response.url().includes("/api/pipelines/") &&
        response.url().includes("/assets/") &&
        response.request().method() === "PUT"
    );
    await page.keyboard.press("ControlOrMeta+S");
    await saveResponse;
    const inspectResponse = page.waitForResponse(async (response) => {
      if (
        !response.url().includes("/api/assets/") ||
        !response.url().includes("/inspect") ||
        response.request().method() !== "GET"
      ) {
        return false;
      }

      try {
        const body = await response.json();
        return body.status === "error";
      } catch {
        return false;
      }
    });
    await page.keyboard.press("ControlOrMeta+Enter");

    const response = await inspectResponse;
    const body = await response.json();

    expect(body.status).toBe("error");
    expect(body.raw_output).toContain("Catalog Error");
    expect(body.raw_output).toContain("LINE 1:");

    await expect
      .poll(async () => {
        return await page.evaluate(() => {
          const monaco = (window as typeof window & {
            monaco?: {
              editor: {
                getModels(): Array<{
                  uri: { toString(): string };
                }>;
                getModelMarkers(args: { resource?: { toString(): string } }): Array<{ message: string }>;
              };
            };
          }).monaco;

          if (!monaco) {
            return [];
          }

          const models = monaco.editor.getModels();
          for (const model of models) {
            const markers = monaco.editor.getModelMarkers({ resource: model.uri });
            if (markers.length > 0) {
              return markers.map((marker) => marker.message);
            }
          }

          return [];
        });
      }, { timeout: 15000 })
      .toEqual(expect.arrayContaining([expect.stringContaining("Catalog Error")]));
  });
});

test.describe("sql intellisense ranking live", () => {
  test.use({ fixtureName: "sql-intellisense-ranking-workspace" });

  test("collapses matching table and asset suggestions into one combined entry", async ({
    liveApp,
    page,
  }) => {
    await page.goto(`${liveApp.baseURL}/?pipeline=YW5hbHl0aWNz`);

    await page.getByRole("link", { name: "analytics.dependencies" }).click();
    await page.getByRole("button", { name: "Materialize", exact: true }).click();
    await expect(page.getByText("Asset: analytics.dependencies", { exact: true })).toBeVisible({
      timeout: 15000,
    });

    await page.getByRole("link", { name: "marts" }).click();
    await page.getByRole("link", { name: "marts.dependencies" }).click();
    await page.getByRole("button", { name: "Materialize", exact: true }).click();
    await expect(page.getByText("Asset: marts.dependencies", { exact: true })).toBeVisible({
      timeout: 15000,
    });

    await page.getByRole("link", { name: "analytics.customers" }).click();
    await openCustomersEditor(page);
    await replaceEditorContent(page, "select * from dependen");
    await page.keyboard.press("ControlOrMeta+Space");

    const suggestWidget = page.locator(".suggest-widget.visible").first();
    await expect(suggestWidget).toBeVisible();

    await expect(suggestWidget.getByText("analytics.dependencies", { exact: true })).toHaveCount(1);
    await expect(
      suggestWidget.getByRole("listitem", {
        name: /analytics\.dependencies, Table \+ Asset \(analytics\.dependencies\), Class/,
      }),
    ).toBeVisible();
    await expect(
      suggestWidget.getByRole("listitem", {
        name: /marts\.dependencies, Table \+ Asset \(marts\.dependencies\), Module/,
      }),
    ).toBeVisible();
  });
});

async function openCustomersEditor(page: Page) {
  const isMobile = test.info().project.name.includes("mobile");
  if (isMobile) {
    await page.goto(`${page.url().split("?")[0]}?pipeline=YW5hbHl0aWNz&asset=YW5hbHl0aWNzL2Fzc2V0cy9jdXN0b21lcnMuc3Fs`);
    await expect(page).toHaveTitle("analytics.customers · analytics · Bruin Web");
    const editorDialog = page.getByRole("dialog", { name: "Asset Editor" });
    if (!(await editorDialog.isVisible().catch(() => false))) {
      const editButton = page.getByRole("button", { name: "Edit asset" });
      if (await editButton.isVisible().catch(() => false)) {
        await editButton.click();
      }
    }
    await expect(editorDialog).toBeVisible();
    await expect(page.getByTestId("editor-asset-name")).toHaveText("analytics.customers");
  } else {
    const editorAssetName = page.getByTestId("editor-asset-name");
    const customersLink = page.getByRole("link", { name: "analytics.customers" });
    await expect(customersLink).toBeVisible();

    if ((await editorAssetName.textContent())?.trim() !== "analytics.customers") {
      await customersLink.click();
    }

    await expect(editorAssetName).toHaveText("analytics.customers");
  }
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
