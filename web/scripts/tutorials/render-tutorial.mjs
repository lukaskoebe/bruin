import { spawn } from "node:child_process";
import { cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import http from "node:http";
import net from "node:net";
import { chromium } from "playwright";

const tutorialPathArg = process.argv[2];

if (!tutorialPathArg) {
  console.error("usage: node render-tutorial.mjs <tutorial.json>");
  process.exit(1);
}

const tutorialPath = resolve(process.cwd(), tutorialPathArg);
const tutorial = JSON.parse(readFileSync(tutorialPath, "utf8"));
const webDir = resolve(dirname(tutorialPath), "..", "..");
const repoRoot = resolve(webDir, "..");
const binaryPath = resolveBinaryPath();
const host = process.env.BRUIN_E2E_HOST || "127.0.0.1";
const staticDir = resolve(webDir, "dist");
const fixtureRoot = resolve(webDir, "tests", "fixtures", tutorial.workspaceFixture);
const outputRoot = resolve(webDir, ".tmp", "tutorials", tutorial.id);
const audioRoot = resolve(outputRoot, "audio");
const videoRoot = resolve(outputRoot, "video");
const introDurationMs = tutorial.intro?.durationMs ?? 0;
const thumbnailPath = resolve(outputRoot, `${tutorial.id}-thumbnail.png`);
const defaultActionDelayMs = Number(tutorial.actionDelayMs ?? 600);

if (!existsSync(binaryPath)) {
  throw new Error(`Bruin binary not found at ${binaryPath}.`);
}

if (!existsSync(staticDir)) {
  throw new Error(`Static frontend not found at ${staticDir}. Run pnpm build first.`);
}

mkdirSync(audioRoot, { recursive: true });
mkdirSync(videoRoot, { recursive: true });

  await generateAudio(tutorialPath, audioRoot);

  const timings = JSON.parse(readFileSync(resolve(audioRoot, "timings.json"), "utf8"));
const workspaceDir = mkdtempSync(resolve(tmpdir(), "bruin-web-tutorial-"));
cpSync(fixtureRoot, workspaceDir, { recursive: true });
mkdirSync(join(workspaceDir, ".git"));
mkdirSync(join(workspaceDir, "duckdb-files"));

const port = await getAvailablePort();
const baseURL = `http://127.0.0.1:${port}`;
const server = spawn(
  binaryPath,
  [
    "web",
    "--host",
    host,
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
  const rawVideoPath = await recordTutorial({ baseURL, tutorial, timings, videoRoot });
  const finalVideoPath = await buildVideoWithIntro(rawVideoPath, thumbnailPath, tutorial);
  await muxVideoAndAudio(finalVideoPath, audioRoot, tutorial.id);
} finally {
  server.kill("SIGTERM");
  await waitForExit(server);
  rmSync(workspaceDir, { recursive: true, force: true });
}

async function generateAudio(tutorialFile, outputDir) {
  await runCommand("node", [
    resolve(webDir, "scripts", "tutorials", "generate-tutorial-audio.mjs"),
    tutorialFile,
    outputDir,
  ]);
}

async function recordTutorial({ baseURL, tutorial, timings, videoRoot }) {
  const browser = await chromium.launch({ headless: true, slowMo: 250 });
  const thumbnailContext = await browser.newContext({
    viewport: tutorial.viewport,
  });
  const thumbnailPage = await thumbnailContext.newPage();
  await showIntroScene(thumbnailPage, tutorial);
  await thumbnailPage.screenshot({ path: thumbnailPath });
  await thumbnailContext.close();

  const context = await browser.newContext({
    viewport: tutorial.viewport,
    recordVideo: {
      dir: videoRoot,
      size: tutorial.viewport,
    },
  });
  const page = await context.newPage();
  await page.goto(baseURL);
  await installTutorialCursor(page);
  await page.waitForTimeout(1200);

  for (const [index, segment] of tutorial.segments.entries()) {
    const timing = timings[index];
    const actionDelayMs = Number(segment.actionDelayMs ?? defaultActionDelayMs);
    const segmentDurationMs = (timing?.duration_ms ?? 1500) + (timing?.padding_ms ?? 0);
    const segmentStartedAt = Date.now();

    if (actionDelayMs > 0) {
      await page.waitForTimeout(actionDelayMs);
    }

    await runTutorialAction(page, segment.action);
    const elapsedMs = Date.now() - segmentStartedAt;
    const remainingMs = Math.max(0, segmentDurationMs - elapsedMs);
    await page.waitForTimeout(remainingMs);
  }

  await page.waitForTimeout(1000);
  const video = page.video();
  await context.close();
  await browser.close();

  if (!video) {
    throw new Error("Playwright did not produce a video recording.");
  }

  return await video.path();
}

async function buildVideoWithIntro(rawVideoPath, thumbnailPath, tutorial) {
  if (introDurationMs <= 0) {
    return rawVideoPath;
  }

  const introPath = resolve(videoRoot, `${tutorial.id}-intro.mp4`);
  const concatPath = resolve(videoRoot, `${tutorial.id}-with-intro.mp4`);

  await runCommand("ffmpeg", [
    "-y",
    "-loop",
    "1",
    "-i",
    thumbnailPath,
    "-t",
    (introDurationMs / 1000).toFixed(3),
    "-vf",
    "format=yuv420p",
    "-c:v",
    "libx264",
    "-pix_fmt",
    "yuv420p",
    introPath,
  ]);

  await runCommand("ffmpeg", [
    "-y",
    "-i",
    introPath,
    "-i",
    rawVideoPath,
    "-filter_complex",
    "[0:v][1:v]concat=n=2:v=1:a=0[v]",
    "-map",
    "[v]",
    "-c:v",
    "libx264",
    "-pix_fmt",
    "yuv420p",
    concatPath,
  ]);

  return concatPath;
}

async function runTutorialAction(page, action) {
  switch (action.type) {
    case "clickAssetLink":
      await clickLocator(page, page.getByRole("link", { name: action.assetName }));
      break;
    case "clickPipelineLink":
      await clickLocator(
        page,
        page.getByRole("link", { name: action.pipelineName, exact: true })
      );
      break;
    case "createPipeline":
      await clickLocator(page, page.getByRole("button", { name: "Create pipeline" }).last());
      await fillInput(page, page.getByLabel("Pipeline path"), action.path, action);
      await clickLocator(
        page,
        page.getByRole("button", { name: "Create Pipeline", exact: true })
      );
      await page
        .getByRole("link", { name: action.path, exact: true })
        .waitFor({ timeout: 15000 });
      break;
    case "createAssetOnCanvas":
      await createAssetOnCanvas(page, action);
      break;
    case "typeInEditor":
      await typeInEditor(page, action);
      break;
    case "inspectAsset":
      await clickLocator(
        page,
        page.getByRole("button", { name: "Inspect Data", exact: true })
      );
      await page.getByText("2 rows", { exact: true }).waitFor({ timeout: 15000 });
      break;
    case "materializeAsset":
      await clickLocator(page, page.getByRole("tab", { name: "Materialize" }));
      await clickLocator(
        page,
        page.getByRole("button", { name: "Materialize", exact: true })
      );
      await page
        .getByText("Asset: analytics.customers", { exact: true })
        .waitFor({ timeout: 15000 });
      break;
    case "renameAsset":
      await clickLocator(page, page.getByRole("button", { name: "Rename asset" }));
      const input = page.locator(`input[value="${action.from}"]`);
      await clickLocator(page, input);
      await input.press("Control+A");
      await input.press("Backspace");
      await page.keyboard.type(action.to, {
        delay: Number(action.typingDelayMs ?? 70),
      });
      await clickLocator(page, page.getByRole("button", { name: "Save", exact: true }));
      await page.getByRole("link", { name: action.to }).waitFor({ timeout: 15000 });
      break;
    case "reloadAndVerifyAsset":
      await page.reload();
      await installTutorialCursor(page);
      await clickLocator(page, page.getByRole("link", { name: action.assetName }));
      await page.getByRole("link", { name: action.assetName }).waitFor({ timeout: 15000 });
      break;
    case "circleElement":
      await circleLocator(page, resolveTutorialLocator(page, action.target), action);
      break;
    default:
      throw new Error(`Unknown tutorial action: ${action.type}`);
  }
}

async function muxVideoAndAudio(rawVideo, audioRoot, tutorialId) {
  const narrationPath = resolve(audioRoot, "narration.wav");
  const outputPath = resolve(outputRoot, `${tutorialId}.mp4`);
  const delayedNarrationFilter =
    introDurationMs > 0
      ? `[1:a]adelay=${introDurationMs}:all=1,apad[a]`
      : "[1:a]apad[a]";

  await runCommand("ffmpeg", [
    "-y",
    "-i",
    rawVideo,
    "-i",
    narrationPath,
    "-filter_complex",
    delayedNarrationFilter,
    "-map",
    "0:v",
    "-map",
    "[a]",
    "-c:v",
    "copy",
    "-c:a",
    "aac",
    "-movflags",
    "+faststart",
    "-shortest",
    outputPath,
  ]);
}

async function runCommand(command, args) {
  await new Promise((resolveDone, reject) => {
    const child = spawn(command, args, {
      cwd: repoRoot,
      env: process.env,
      stdio: "inherit",
    });

    child.once("exit", (code) => {
      if (code === 0) {
        resolveDone();
        return;
      }

      reject(new Error(`${command} exited with code ${code ?? 1}`));
    });
  });
}

async function showIntroScene(page, tutorial) {
  const kicker = escapeHTML(tutorial.intro?.kicker ?? "Automated walkthrough");
  const title = escapeHTML(tutorial.title);
  const subtitle = escapeHTML(tutorial.intro?.subtitle ?? "");

  await page.setContent(`<!doctype html>
  <html lang="en">
    <head>
      <meta charset="utf-8" />
      <style>
        body {
          margin: 0;
          width: 100vw;
          height: 100vh;
          overflow: hidden;
          font-family: Inter, ui-sans-serif, system-ui, sans-serif;
          background:
            radial-gradient(circle at top left, rgba(59,130,246,0.28), transparent 28%),
            radial-gradient(circle at bottom right, rgba(16,185,129,0.22), transparent 34%),
            linear-gradient(135deg, #0b1220, #111827 50%, #0f172a);
          color: white;
          display: grid;
          place-items: center;
        }
        .card {
          width: min(1100px, calc(100vw - 120px));
          padding: 64px 72px;
          border-radius: 28px;
          background: rgba(15, 23, 42, 0.9);
          border: 1px solid rgba(148, 163, 184, 0.18);
          box-shadow: 0 30px 100px rgba(15, 23, 42, 0.45);
        }
        .kicker { color: #60a5fa; font-size: 26px; letter-spacing: 0.04em; margin-bottom: 20px; }
        h1 { font-size: 72px; line-height: 0.95; margin: 0 0 26px; }
        p { max-width: 920px; color: #cbd5e1; font-size: 32px; line-height: 1.35; margin: 0; }
        .footer { margin-top: 44px; color: #94a3b8; font-size: 28px; }
      </style>
    </head>
    <body>
      <main class="card">
        <div class="kicker">${kicker}</div>
        <h1>${title}</h1>
        <p>${subtitle}</p>
        <div class="footer">bruin web</div>
      </main>
    </body>
  </html>`);
}

async function installTutorialCursor(page) {
  await page.evaluate(() => {
    const existing = document.getElementById("tutorial-cursor");
    if (existing) {
      return;
    }

    const cursor = document.createElement("div");
    cursor.id = "tutorial-cursor";
    Object.assign(cursor.style, {
      position: "fixed",
      left: "0px",
      top: "0px",
      width: "22px",
      height: "22px",
      borderRadius: "999px",
      background: "rgba(59,130,246,0.92)",
      border: "2px solid white",
      boxShadow: "0 8px 24px rgba(15,23,42,0.35)",
      transform: "translate(-50%, -50%)",
      zIndex: "2147483647",
      pointerEvents: "none",
      transition: "left 220ms ease, top 220ms ease, transform 120ms ease",
    });
    document.body.appendChild(cursor);

    window.__tutorialCursorMove = ({ x, y, active }) => {
      cursor.style.left = `${x}px`;
      cursor.style.top = `${y}px`;
      cursor.style.transform = active
        ? "translate(-50%, -50%) scale(0.88)"
        : "translate(-50%, -50%) scale(1)";
    };
  });
}

async function clickLocator(page, locator) {
  const target = locator.first();
  await target.waitFor({ state: "visible", timeout: 15000 });
  const box = await target.boundingBox();
  if (!box) {
    throw new Error("Could not measure locator for tutorial cursor movement.");
  }

  const x = box.x + box.width / 2;
  const y = box.y + box.height / 2;
  await moveCursor(page, x, y);
  await page.mouse.down();
  await page.evaluate(({ cursorX, cursorY }) => {
    window.__tutorialCursorMove?.({ x: cursorX, y: cursorY, active: true });
  }, { cursorX: x, cursorY: y });
  await page.waitForTimeout(90);
  await page.mouse.up();
  await page.evaluate(({ cursorX, cursorY }) => {
    window.__tutorialCursorMove?.({ x: cursorX, y: cursorY, active: false });
  }, { cursorX: x, cursorY: y });
}

async function fillInput(page, locator, value, action = {}) {
  const target = locator.first();
  await clickLocator(page, target);
  await target.press("Control+A");
  await target.press("Backspace");
  await page.keyboard.type(value, {
    delay: Number(action.typingDelayMs ?? 55),
  });
}

async function createAssetOnCanvas(page, action) {
  const canvas = page.locator(".react-flow__pane").first();
  await canvas.waitFor({ state: "visible", timeout: 15000 });
  const box = await canvas.boundingBox();
  if (!box) {
    throw new Error("Could not locate the React Flow pane for asset creation.");
  }

  const xFactor = Number(action.xFactor ?? 0.35);
  const yFactor = Number(action.yFactor ?? 0.35);
  await moveCursor(page, box.x + box.width * xFactor, box.y + box.height * yFactor);
  await canvas.click({
    position: {
      x: Math.round(box.width * xFactor),
      y: Math.round(box.height * yFactor),
    },
  });

  const draftNode = page.locator('[data-new-asset-node="true"]').first();
  await draftNode.waitFor({ state: "visible", timeout: 15000 });

  const kind = action.kind ?? "sql";
  if (kind !== "sql") {
    await clickLocator(
      page,
      draftNode.getByRole("tab", { name: kind.toUpperCase(), exact: true })
    );
  }

  await fillInput(page, draftNode.getByPlaceholder("Asset name"), action.assetName, action);
  await clickLocator(page, draftNode.getByRole("button", { name: "Create", exact: true }));
  await page.getByRole("link", { name: action.assetName }).waitFor({ timeout: 15000 });
}

async function typeInEditor(page, action) {
  const editor = page.locator(".monaco-editor").first();
  await editor.waitFor({ state: "visible", timeout: 15000 });
  await clickLocator(page, editor);
  await page.keyboard.press("Control+A");
  await page.keyboard.press("Backspace");
  await page.keyboard.type(action.content, {
    delay: Number(action.typingDelayMs ?? 18),
  });

  if (action.saveShortcut ?? true) {
    await page.waitForTimeout(Number(action.beforeSaveDelayMs ?? 180));
    await page.keyboard.press("Control+S");
  }

  if (action.waitForEdgeCount) {
    await page.waitForFunction(
      (count) => {
        const selectorCount = document.querySelectorAll(".react-flow__edge").length;
        const fallbackCount = document.querySelectorAll('[class*="react-flow__edge"]').length;
        return Math.max(selectorCount, fallbackCount) >= count;
      },
      action.waitForEdgeCount,
      { timeout: Number(action.edgeTimeoutMs ?? 30000) }
    );
  }

  if (action.waitForText) {
    await page.getByText(action.waitForText, { exact: Boolean(action.waitForTextExact) }).waitFor({
      timeout: 15000,
    });
  }

  await page.waitForTimeout(Number(action.settleMs ?? 600));
}

async function circleLocator(page, locator, action = {}) {
  const target = locator.first();
  await target.waitFor({ state: "visible", timeout: 15000 });
  const box = await target.boundingBox();
  if (!box) {
    throw new Error("Could not measure locator for tutorial circle movement.");
  }

  const loops = Math.max(1, Number(action.loops ?? 1));
  const paddingX = Number(action.paddingX ?? 18);
  const paddingY = Number(action.paddingY ?? 18);
  const durationMs = Math.max(400, Number(action.durationMs ?? loops * 900));
  const settleMs = Math.max(0, Number(action.settleMs ?? 220));
  const viewport = page.viewportSize() ?? { width: 1440, height: 900 };
  const centerX = box.x + box.width / 2;
  const centerY = box.y + box.height / 2;
  const radiusX = Math.min(box.width / 2 + paddingX, viewport.width * 0.35);
  const radiusY = Math.min(box.height / 2 + paddingY, viewport.height * 0.35);

  await moveCursor(page, centerX + radiusX, centerY);

  await page.evaluate(
    async ({ cursorCenterX, cursorCenterY, cursorRadiusX, cursorRadiusY, cursorLoops, cursorDurationMs }) => {
      await new Promise((resolveAnimation) => {
        const startedAt = performance.now();

        const tick = (now) => {
          const elapsed = now - startedAt;
          const progress = Math.min(1, elapsed / cursorDurationMs);
          const angle = progress * Math.PI * 2 * cursorLoops;
          const x = cursorCenterX + Math.cos(angle) * cursorRadiusX;
          const y = cursorCenterY + Math.sin(angle) * cursorRadiusY;
          window.__tutorialCursorMove?.({ x, y, active: false });

          if (progress >= 1) {
            resolveAnimation();
            return;
          }

          requestAnimationFrame(tick);
        };

        requestAnimationFrame(tick);
      });
    },
    {
      cursorCenterX: centerX,
      cursorCenterY: centerY,
      cursorRadiusX: radiusX,
      cursorRadiusY: radiusY,
      cursorLoops: loops,
      cursorDurationMs: durationMs,
    }
  );

  await page.waitForTimeout(settleMs);
}

async function moveCursor(page, x, y) {
  await page.mouse.move(x, y, { steps: 18 });
  await page.evaluate(({ cursorX, cursorY }) => {
    window.__tutorialCursorMove?.({ x: cursorX, y: cursorY, active: false });
  }, { cursorX: x, cursorY: y });
  await page.waitForTimeout(120);
}

function resolveTutorialLocator(page, target) {
  if (!target || typeof target !== "object") {
    throw new Error("Tutorial action target is required.");
  }

  switch (target.type) {
    case "testId":
      return page.getByTestId(target.value);
    case "button":
      return page.getByRole("button", {
        name: target.name,
        exact: Boolean(target.exact),
      });
    case "tab":
      return page.getByRole("tab", {
        name: target.name,
        exact: Boolean(target.exact),
      });
    case "link":
      return page.getByRole("link", {
        name: target.name,
        exact: Boolean(target.exact),
      });
    case "text":
      return page.getByText(target.value, {
        exact: Boolean(target.exact),
      });
    case "css":
      return page.locator(target.value);
    default:
      throw new Error(`Unknown tutorial target type: ${target.type}`);
  }
}

function escapeHTML(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeDrawtext(value) {
  return String(value).replace(/:/g, "\\:").replace(/'/g, "\\'");
}

function resolveBinaryPath() {
  if (process.env.BRUIN_E2E_BINARY) {
    return process.env.BRUIN_E2E_BINARY;
  }

  const rootBinaryPath = resolve(repoRoot, "bruin");
  if (existsSync(rootBinaryPath)) {
    return rootBinaryPath;
  }

  return resolve(repoRoot, "bin", "bruin");
}

function waitForServer(baseURL) {
  const deadline = Date.now() + 30000;

  return new Promise((resolveReady, reject) => {
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

function waitForExit(child) {
  return new Promise((resolveDone) => {
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
  return new Promise((resolvePort, reject) => {
    const server = net.createServer();
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close();
        reject(new Error("Could not allocate a port for tutorial rendering."));
        return;
      }

      const { port } = address;
      server.close(() => resolvePort(port));
    });
    server.on("error", reject);
  });
}
