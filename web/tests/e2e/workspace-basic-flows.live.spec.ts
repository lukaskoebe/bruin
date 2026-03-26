import { expect, test as base } from "@playwright/test";
import { spawn } from "node:child_process";
import { cpSync, existsSync, mkdtempSync, rmSync, mkdirSync } from "node:fs";
import http from "node:http";
import net from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

type LiveApp = {
  baseURL: string;
  workspaceDir: string;
};

const webDir = resolve(__dirname, "..", "..");
const repoRoot = resolve(webDir, "..");
const binaryPath =
  process.env.BRUIN_E2E_BINARY || resolve(repoRoot, "bin", "bruin");
const host = process.env.BRUIN_E2E_HOST || "127.0.0.1";
const staticDir = resolve(webDir, "dist");

const test = base.extend<{ liveApp: LiveApp }>({
  liveApp: async ({}, use, testInfo) => {
    if (!existsSync(binaryPath)) {
      throw new Error(
        `Bruin binary not found at ${binaryPath}. Build it first or set BRUIN_E2E_BINARY.`
      );
    }

    const fixtureName =
      testInfo.title.includes("empty workspace") ||
      testInfo.title.includes("create pipeline") ||
      testInfo.title.includes("creates, renames, and deletes a pipeline")
        ? "empty-workspace"
        : "basic-workspace";

    const fixtureRoot = resolve(webDir, "tests", "fixtures", fixtureName);
    const workspaceDir = mkdtempSync(resolve(tmpdir(), "bruin-web-e2e-"));
    cpSync(fixtureRoot, workspaceDir, { recursive: true });
    mkdirSync(join(workspaceDir, ".git"));
    mkdirSync(join(workspaceDir, "duckdb-files"));

    const port = await getAvailablePort();
    const baseURL = `http://${host}:${port}`;
    const child = spawn(
      binaryPath,
      [
        "web",
        "--host",
        `${host}`,
        "--port",
        String(port),
        "--static-dir",
        staticDir,
        "--watch-mode",
        "poll",
        workspaceDir,
      ],
      {
        cwd: repoRoot,
        env: process.env,
        stdio: "inherit",
      }
    );

    try {
      await waitForServer(baseURL);
      await use({ baseURL, workspaceDir });
    } finally {
      child.kill("SIGTERM");
      await waitForExit(child);
      rmSync(workspaceDir, { recursive: true, force: true });
    }
  },
});

test.describe("workspace live basic flows", () => {
  test("loads the fixture workspace and opens an asset in the editor", async ({
    liveApp,
    page,
  }) => {
    await page.goto(`${liveApp.baseURL}/`);

    await expect(page.getByRole("link", { name: "analytics", exact: true })).toBeVisible();
    await expect(page.getByRole("link", { name: "analytics.customers" })).toBeVisible();

    await page.getByRole("link", { name: "analytics.customers" }).click();

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

    await page.getByRole("link", { name: "analytics.customers" }).click();
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

    await page.getByRole("link", { name: "analytics.customers" }).click();
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

    await page.getByRole("link", { name: "analytics.customers" }).click();
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

  test("creates, renames, and deletes an asset in an isolated workspace", async ({
    liveApp,
    page,
  }) => {
    await page.goto(`${liveApp.baseURL}/`);

    await page.getByRole("link", { name: "analytics.customers" }).click();

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
});

function waitForServer(baseURL: string) {
  const deadline = Date.now() + 30000;

  return new Promise<void>((resolveReady, reject) => {
    const attempt = () => {
      const request = http.get(baseURL, (response) => {
        response.resume();
        if ((response.statusCode ?? 500) < 500) {
          resolveReady();
          return;
        }

        if (Date.now() > deadline) {
          reject(new Error(`Timed out waiting for Bruin Web at ${baseURL}`));
          return;
        }

        setTimeout(attempt, 250);
      });

      request.on("error", () => {
        if (Date.now() > deadline) {
          reject(new Error(`Timed out waiting for Bruin Web at ${baseURL}`));
          return;
        }

        setTimeout(attempt, 250);
      });
    };

    attempt();
  });
}

function waitForExit(child: ReturnType<typeof spawn>) {
  return new Promise<void>((resolveDone) => {
    if (child.exitCode !== null || child.killed) {
      resolveDone();
      return;
    }

    const timer = setTimeout(() => {
      if (child.exitCode === null) {
        child.kill("SIGKILL");
      }
    }, 5000);

    child.once("exit", () => {
      clearTimeout(timer);
      resolveDone();
    });
  });
}

function getAvailablePort() {
  return new Promise<number>((resolvePort, reject) => {
    const server = net.createServer();
    server.listen(0, `${host}`, () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close();
        reject(new Error("Could not allocate a port for live E2E tests."));
        return;
      }

      const { port } = address;
      server.close(() => resolvePort(port));
    });
    server.on("error", reject);
  });
}
