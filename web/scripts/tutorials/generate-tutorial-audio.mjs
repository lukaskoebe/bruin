import { spawn } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { basename, resolve } from "node:path";

const tutorialPath = process.argv[2];
const outputDir = process.argv[3];
const apiURL =
  process.env.CHATTERBOX_TTS_API_URL || "http://127.0.0.1:4123/v1/audio/speech";

if (!tutorialPath || !outputDir) {
  console.error("usage: node generate-tutorial-audio.mjs <tutorial.json> <output-dir>");
  process.exit(1);
}

const tutorial = JSON.parse(readFileSync(tutorialPath, "utf8"));
const segments = tutorial.segments;
const voice = tutorial.voice ?? "alba";
const ttsOptions = tutorial.tts ?? {};
const renderedSegments = [];

mkdirSync(outputDir, { recursive: true });

for (const [index, segment] of segments.entries()) {
  const segmentID = segment.id;
  const segmentPath = resolve(outputDir, `${String(index + 1).padStart(2, "0")}-${segmentID}.wav`);

  const response = await fetch(apiURL, {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify({
      input: segment.text,
      voice,
      response_format: "wav",
      ...ttsOptions,
    }),
  });

  if (!response.ok) {
    throw new Error(`Chatterbox TTS request failed with ${response.status}: ${await response.text()}`);
  }

  const audioBuffer = Buffer.from(await response.arrayBuffer());
  writeFileSync(segmentPath, audioBuffer);

  const durationMs = await probeDurationMs(segmentPath);
  const paddingMs = Number(segment.paddingMs ?? 0);

  renderedSegments.push({
    id: segmentID,
    text: segment.text,
    audio_path: segmentPath,
    duration_ms: durationMs,
    padding_ms: paddingMs,
  });

  if (paddingMs > 0) {
    const silencePath = resolve(
      outputDir,
      `${String(index + 1).padStart(2, "0")}-${segmentID}-padding.wav`
    );
    await createSilence(silencePath, paddingMs);
    renderedSegments[renderedSegments.length - 1].padding_path = silencePath;
  }
}

const concatManifestPath = resolve(outputDir, "segments.txt");
const concatLines = [];

for (const segment of renderedSegments) {
  concatLines.push(`file '${basename(segment.audio_path)}'`);
  if (segment.padding_path) {
    concatLines.push(`file '${basename(segment.padding_path)}'`);
  }
}

writeFileSync(concatManifestPath, `${concatLines.join("\n")}\n`);

const combinedPath = resolve(outputDir, "narration.wav");
await runCommand("ffmpeg", [
  "-y",
  "-f",
  "concat",
  "-safe",
  "0",
  "-i",
  concatManifestPath,
  combinedPath,
], outputDir);

writeFileSync(resolve(outputDir, "timings.json"), JSON.stringify(renderedSegments, null, 2));
console.log(combinedPath);

async function probeDurationMs(filePath) {
  const stdout = await captureCommand("ffprobe", [
    "-v",
    "error",
    "-show_entries",
    "format=duration",
    "-of",
    "default=noprint_wrappers=1:nokey=1",
    filePath,
  ]);

  return Math.round(Number.parseFloat(stdout.trim()) * 1000);
}

async function createSilence(filePath, durationMs) {
  await runCommand("ffmpeg", [
    "-y",
    "-f",
    "lavfi",
    "-i",
    "anullsrc=r=24000:cl=mono",
    "-t",
    (durationMs / 1000).toFixed(3),
    filePath,
  ]);
}

async function runCommand(command, args, cwd = process.cwd()) {
  await new Promise((resolveDone, reject) => {
    const child = spawn(command, args, {
      cwd,
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

async function captureCommand(command, args) {
  return await new Promise((resolveDone, reject) => {
    const child = spawn(command, args, {
      cwd: process.cwd(),
      env: process.env,
      stdio: ["ignore", "pipe", "inherit"],
    });

    let stdout = "";
    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });

    child.once("exit", (code) => {
      if (code === 0) {
        resolveDone(stdout);
        return;
      }

      reject(new Error(`${command} exited with code ${code ?? 1}`));
    });
  });
}
