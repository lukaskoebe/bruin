import { test as base } from "@playwright/test";
import { spawn } from "node:child_process";
import {
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import http from "node:http";
import net from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { randomUUID } from "node:crypto";

export type LiveApp = {
  baseURL: string;
  workspaceDir: string;
};

export type LivePostgres = {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
};

const webDir = resolve(__dirname, "..", "..");
const repoRoot = resolve(webDir, "..");
const binaryPath = process.env.BRUIN_E2E_BINARY || resolve(repoRoot, "bruin");
const host = process.env.BRUIN_E2E_HOST || "127.0.0.1";
const staticDir = resolve(webDir, "dist");

export const liveTest = base.extend<{
  fixtureName: string;
  liveApp: LiveApp;
  livePostgres: LivePostgres | null;
}>({
  fixtureName: ["basic-workspace", { option: true }],
  livePostgres: async ({ fixtureName }, use) => {
    if (fixtureName !== "empty-workspace-postgres") {
      await use(null);
      return;
    }

    const postgres = await createLivePostgres();
    try {
      await use(postgres.connection);
    } finally {
      await postgres.dispose();
    }
  },
  page: async ({ page }, use, testInfo) => {
    const networkEvents: Array<Record<string, unknown>> = [];
    const requestStartedAt = new WeakMap<object, number>();

    const recordEvent = (event: Record<string, unknown>) => {
      networkEvents.push({
        timestamp: new Date().toISOString(),
        ...event,
      });
    };

    const onRequest = (request: Parameters<typeof page.on>[1] extends never ? never : any) => {
      requestStartedAt.set(request, Date.now());
      recordEvent({
        type: "request",
        method: request.method(),
        url: request.url(),
        resourceType: request.resourceType(),
        headers: request.headers(),
        postData: request.postData() ?? undefined,
      });
    };

    const onResponse = (response: Parameters<typeof page.on>[1] extends never ? never : any) => {
      const request = response.request();
      const startedAt = requestStartedAt.get(request);
      recordEvent({
        type: "response",
        method: request.method(),
        url: response.url(),
        resourceType: request.resourceType(),
        status: response.status(),
        statusText: response.statusText(),
        ok: response.ok(),
        durationMs: startedAt ? Date.now() - startedAt : undefined,
        headers: response.headers(),
      });
    };

    const onRequestFailed = (request: Parameters<typeof page.on>[1] extends never ? never : any) => {
      const startedAt = requestStartedAt.get(request);
      recordEvent({
        type: "requestfailed",
        method: request.method(),
        url: request.url(),
        resourceType: request.resourceType(),
        durationMs: startedAt ? Date.now() - startedAt : undefined,
        failure: request.failure()?.errorText ?? "unknown",
      });
    };

    page.on("request", onRequest);
    page.on("response", onResponse);
    page.on("requestfailed", onRequestFailed);

    try {
      await use(page);
    } finally {
      page.off("request", onRequest);
      page.off("response", onResponse);
      page.off("requestfailed", onRequestFailed);

      if (testInfo.status !== testInfo.expectedStatus) {
        const networkLogPath = testInfo.outputPath("network-requests.json");
        writeFileSync(
          networkLogPath,
          JSON.stringify(networkEvents, null, 2),
          "utf8"
        );
        await testInfo.attach("network-requests", {
          path: networkLogPath,
          contentType: "application/json",
        });
      }
    }
  },
  liveApp: async ({ fixtureName, livePostgres }, use) => {
    if (!existsSync(binaryPath)) {
      throw new Error(
        `Bruin binary not found at ${binaryPath}. Build it first or set BRUIN_E2E_BINARY.`
      );
    }

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

export async function createLivePostgres() {
  const hostPort = await getAvailablePort();
  const containerName = `bruin-web-e2e-pg-${randomUUID().slice(0, 8)}`;
  const database = "bruin";
  const user = "postgres";
  const password = "postgres";

  await runCommand([
    "docker",
    "run",
    "--rm",
    "-d",
    "--name",
    containerName,
    "-e",
    `POSTGRES_DB=${database}`,
    "-e",
    `POSTGRES_USER=${user}`,
    "-e",
    `POSTGRES_PASSWORD=${password}`,
    "-p",
    `${hostPort}:5432`,
    "postgres:16-alpine",
  ]);

  try {
    await waitForPostgres(hostPort, user, database);
    await runCommand([
      "docker",
      "exec",
      containerName,
      "psql",
      "-U",
      user,
      "-d",
      database,
      "-c",
      [
        "create schema if not exists analytics;",
        "create table if not exists analytics.orders (order_id int primary key, order_total numeric);",
        "create table if not exists analytics.customers (customer_id int primary key, customer_name text);",
        "insert into analytics.orders (order_id, order_total) values (1, 10.5), (2, 22.0) on conflict do nothing;",
        "insert into analytics.customers (customer_id, customer_name) values (1, 'Ada'), (2, 'Grace') on conflict do nothing;",
      ].join(" "),
    ]);

    return {
      connection: {
        host,
        port: hostPort,
        user,
        password,
        database,
      } satisfies LivePostgres,
      async dispose() {
        await runCommand(["docker", "rm", "-f", containerName], { allowFailure: true });
      },
    };
  } catch (error) {
    await runCommand(["docker", "rm", "-f", containerName], { allowFailure: true });
    throw error;
  }
}

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

async function waitForPostgres(port: number, user: string, database: string) {
  const deadline = Date.now() + 30000;

  while (Date.now() < deadline) {
    try {
      await runCommand([
        "docker",
        "run",
        "--rm",
        "--network",
        "host",
        "postgres:16-alpine",
        "pg_isready",
        "-h",
        host,
        "-p",
        String(port),
        "-U",
        user,
        "-d",
        database,
      ]);
      return;
    } catch {
      await new Promise((resolveDelay) => setTimeout(resolveDelay, 500));
    }
  }

  throw new Error(`Timed out waiting for Postgres on ${host}:${port}`);
}

function runCommand(args: string[], options?: { allowFailure?: boolean }) {
  return new Promise<void>((resolveRun, rejectRun) => {
    const child = spawn(args[0], args.slice(1), {
      cwd: repoRoot,
      env: process.env,
      stdio: "pipe",
    });

    let stderr = "";
    child.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });

    child.on("exit", (code) => {
      if (code === 0 || options?.allowFailure) {
        resolveRun();
        return;
      }
      rejectRun(new Error(stderr || `${args.join(" ")} failed with exit code ${code}`));
    });
    child.on("error", rejectRun);
  });
}
