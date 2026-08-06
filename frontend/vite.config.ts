import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import fs from "node:fs";
import path from "node:path";

function resolveApiToken(): string {
  const envToken = (process.env.NCC_API_TOKEN || "").trim();
  if (envToken) return envToken;
  const candidates = [
    path.resolve(process.cwd(), ".ncc-api-token"),
    path.resolve(process.cwd(), "..", ".ncc-api-token"),
  ];
  for (const tokenPath of candidates) {
    if (!fs.existsSync(tokenPath)) continue;
    const tok = fs.readFileSync(tokenPath, "utf8").trim();
    if (tok) return tok;
  }
  return "";
}

const apiToken = resolveApiToken();
if (!apiToken) {
  // Helpful during local dev if proxy requests still return 401.
  // eslint-disable-next-line no-console
  console.warn("[vite] NCC API token not found; /api proxy requests may get 401.");
}

export default defineConfig({
  plugins: [react()],
  server: {
    port: 8080,
    proxy: {
      "/api": {
        target: "http://localhost:8081",
        changeOrigin: true,
        configure: (proxy) => {
          proxy.on("proxyReq", (proxyReq) => {
            if (apiToken) {
              proxyReq.setHeader("X-API-Token", apiToken);
            }
          });
        },
      },
    },
  },
  // Heavy editor (Monaco) is already split into its own chunk via React.lazy.
  // We additionally split the large framework vendors into their own
  // long-cached chunks so the browser can download them in parallel (the
  // ui-server is HTTP/1.1, ~6 connections) instead of fetching one ~370 kB
  // gzip blob serially — this is the dominant cost of first paint under
  // Lighthouse's throttled mobile profile. Splitting also means a UI-only
  // deploy doesn't bust the (rarely-changing) vendor caches.
  build: {
    chunkSizeWarningLimit: 900,
    rolldownOptions: {
      output: {
        codeSplitting: {
          groups: [
            // Only extract the framework runtime that every route needs anyway
            // (so it never adds unused bytes to a route) into a stable,
            // long-cached chunk. We deliberately do NOT group all of antd here:
            // that would force route-only antd components (e.g. Settings-only
            // widgets) to load eagerly on the dashboard. Letting Rolldown
            // auto-split antd keeps each route's antd surface with its chunk.
            {
              name: "react-vendor",
              test: /[\\/]node_modules[\\/](react|react-dom|scheduler|react-router)[\\/]/,
            },
          ],
        },
      },
    },
  },
});
