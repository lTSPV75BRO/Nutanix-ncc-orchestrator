import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import fs from "node:fs";
import path from "node:path";
function resolveApiToken() {
    const envToken = (process.env.NCC_API_TOKEN || "").trim();
    if (envToken)
        return envToken;
    const candidates = [
        path.resolve(process.cwd(), ".ncc-api-token"),
        path.resolve(process.cwd(), "..", ".ncc-api-token"),
    ];
    for (const tokenPath of candidates) {
        if (!fs.existsSync(tokenPath))
            continue;
        const tok = fs.readFileSync(tokenPath, "utf8").trim();
        if (tok)
            return tok;
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
});
