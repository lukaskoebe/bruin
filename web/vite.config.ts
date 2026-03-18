import fs from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { defineConfig, type Plugin } from "rolldown-vite";
import react from "@vitejs/plugin-react";

const require = createRequire(import.meta.url);

function prepareMonacoAssetsPlugin(): Plugin {
  let hasPreparedAssets = false;

  const prepareAssets = () => {
    if (hasPreparedAssets) {
      return;
    }

    const loaderPath = require.resolve("monaco-editor/min/vs/loader.js");
    const sourceDir = path.dirname(loaderPath);
    const targetDir = path.resolve(__dirname, "public/monaco/vs");

    fs.rmSync(targetDir, { recursive: true, force: true });
    fs.mkdirSync(path.dirname(targetDir), { recursive: true });
    fs.cpSync(sourceDir, targetDir, { recursive: true, force: true });

    hasPreparedAssets = true;
  };

  return {
    name: "prepare-monaco-assets",
    configResolved() {
      prepareAssets();
    },
  };
}

export default defineConfig({
  plugins: [react(), prepareMonacoAssetsPlugin()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname),
    },
  },
  server: {
    port: 5173,
    proxy: {
      "^/api/pipelines/.*/materialize/stream$": {
        target: "http://127.0.0.1:3000",
        changeOrigin: true,
        timeout: 0,
        proxyTimeout: 0,
      },
      "^/api/assets/.*/materialize/stream$": {
        target: "http://127.0.0.1:3000",
        changeOrigin: true,
        timeout: 0,
        proxyTimeout: 0,
      },
      "/api": {
        target: "http://127.0.0.1:3000",
        changeOrigin: true,
        ws: true,
      },
    },
  },
  preview: {
    port: 5173,
  },
});
