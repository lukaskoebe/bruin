import path from "node:path";
import { defineConfig } from "rolldown-vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
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
