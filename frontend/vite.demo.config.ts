// Read-only demo build for the public website (site/, deployed by
// .github/workflows/pages.yml). Same app and bundle layout as vite.config.ts,
// with these differences, all applied here so no app source changes:
//   1. src/demo/install.ts is imported first and answers /api/* from sample
//      fixtures (no backend; every change is refused as read-only).
//   2. HashRouter instead of BrowserRouter, so deep links survive a reload on
//      a static host that serves the demo from a sub-folder (/<repo>/demo/).
//   3. Relative asset paths (base "./", and "./logo.svg" in components) for
//      the same reason.
//
// Build: vite build --config vite.demo.config.ts [--outDir <dir>]
import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";

function demoMode(): Plugin {
  return {
    name: "ncc-demo-mode",
    enforce: "pre",
    transform(code, id) {
      // public/ files are referenced as "/logo.svg"; the demo is served from
      // a sub-folder, and with HashRouter the page URL never leaves it.
      if (/[\\/]src[\\/].*\.tsx$/.test(id) && code.includes('"/logo.svg"') && !/[\\/]main\.tsx$/.test(id)) {
        return { code: code.replaceAll('"/logo.svg"', '"./logo.svg"'), map: null };
      }
      if (!/[\\/]src[\\/]main\.tsx$/.test(id)) return null;
      if (!code.includes("BrowserRouter")) {
        this.error("demo build: src/main.tsx no longer uses BrowserRouter; update vite.demo.config.ts");
      }
      return {
        code: `import "./demo/install";\n${code.replace(/\bBrowserRouter\b/g, "HashRouter")}`,
        map: null,
      };
    },
    transformIndexHtml(html) {
      return html
        .replace(/<title>[^<]*<\/title>/, "<title>NCC Orchestrator · Live demo</title>")
        .replace(
          /<meta name="description"[^>]*>/,
          '<meta name="description" content="Read-only demo of the NCC Orchestrator dashboard with sample data." />',
        );
    },
  };
}

export default defineConfig({
  base: "./",
  plugins: [demoMode(), react()],
  build: {
    outDir: "dist-demo",
    emptyOutDir: true,
    chunkSizeWarningLimit: 900,
    rolldownOptions: {
      output: {
        codeSplitting: {
          groups: [
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
