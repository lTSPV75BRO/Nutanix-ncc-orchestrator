package main

import (
	"fmt"
	"html"
	"io"
	"net/http"
	"strings"
)

func (s *apiServer) handleAPIDocsHome(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if r.URL.Path == "/docs/ui" {
		s.handleSwaggerUIPage(w, r)
		return
	}
	if r.URL.Path != "/" && r.URL.Path != "/docs" {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "not found"})
		return
	}
	authMode := html.EscapeString(strings.TrimSpace(s.authMode))
	if authMode == "" {
		authMode = "token"
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NCC API Docs</title>
  <style>
    :root { color-scheme: light dark; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 1.5rem auto; max-width: 1100px; line-height: 1.4; padding: 0 1rem; }
    h1, h2 { margin: 0.5rem 0; }
    a { color: #1677ff; text-decoration: none; }
    .muted { color: #666; }
    .row { display: grid; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); gap: 0.75rem; margin: 1rem 0; }
    .card { border: 1px solid #ddd; border-radius: 10px; padding: 0.8rem; background: rgba(127,127,127,0.05); }
    .label { font-size: 0.85rem; color: #666; margin-bottom: 0.25rem; }
    .value { font-size: 1.1rem; font-weight: 600; }
    .toolbar { display: flex; gap: 0.5rem; flex-wrap: wrap; margin: 0.75rem 0; }
    input { padding: 0.45rem 0.6rem; border: 1px solid #bbb; border-radius: 6px; min-width: 230px; }
    button { padding: 0.45rem 0.7rem; border: 1px solid #bbb; border-radius: 6px; background: transparent; cursor: pointer; }
    table { width: 100%%; border-collapse: collapse; margin-top: 0.6rem; }
    th, td { border: 1px solid #ddd; padding: 0.5rem; vertical-align: top; text-align: left; }
    th { background: rgba(127,127,127,0.09); }
    code, pre { background: rgba(127,127,127,0.12); border-radius: 6px; }
    code { padding: 0.1rem 0.35rem; }
    pre { margin: 0.4rem 0 0 0; padding: 0.6rem; white-space: pre-wrap; word-break: break-all; }
  </style>
</head>
<body>
  <h1>NCC API Docs + Live Status</h1>
  <p class="muted">Auth mode: <code>%s</code> | OpenAPI: <a href="/api/v1/openapi.json">/api/v1/openapi.json</a> | Swagger UI: <a href="/docs/ui">/docs/ui</a></p>

  <div class="row">
    <div class="card"><div class="label">Server status</div><div class="value" id="st-health">Loading...</div></div>
    <div class="card"><div class="label">API auth mode</div><div class="value" id="st-auth">-</div></div>
    <div class="card"><div class="label">Rate limit blocked total</div><div class="value" id="st-blocked">-</div></div>
    <div class="card"><div class="label">Active limiter buckets</div><div class="value" id="st-buckets">-</div></div>
  </div>

  <h2>Endpoint Explorer</h2>
  <div class="toolbar">
    <input id="route-filter" placeholder="Filter endpoints (path, method, description)">
    <input id="api-token" placeholder="Optional token for protected curl examples">
    <button id="refresh-btn" type="button">Refresh</button>
  </div>
  <table>
    <thead><tr><th>Path</th><th>Methods</th><th>Description</th><th>Try-it curl</th></tr></thead>
    <tbody id="routes-body"><tr><td colspan="4">Loading routes...</td></tr></tbody>
  </table>

  <script>
    const state = { routes: [], health: null, metrics: null, deniedToken: null };
    function esc(s) { return String(s ?? "").replace(/[&<>"']/g, c => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", "\"":"&quot;", "'":"&#39;" }[c])); }
    function tokenHeader(token) {
      const t = String(token || "").trim();
      return t ? '-H "X-API-Token: ' + t + '" ' : "";
    }
    function curlForRoute(route, token) {
      const method = (route.methods && route.methods[0]) || "GET";
      const base = window.location.origin + route.path;
      const hdr = tokenHeader(token);
      if (route.sample_body && (method === "POST" || method === "PUT")) {
        return "curl -X " + method + " " + hdr + '-H "Content-Type: application/json" ' + base + " -d '" + String(route.sample_body) + "'";
      }
      return "curl -X " + method + " " + hdr + base;
    }
    function renderRoutes() {
      const body = document.getElementById("routes-body");
      const q = document.getElementById("route-filter").value.toLowerCase().trim();
      const token = document.getElementById("api-token").value;
      const rows = state.routes.filter(r => {
        if (!q) return true;
        const hay = ((r.path || "").toLowerCase() + " " + (r.description || "").toLowerCase() + " " + (r.methods || []).join(" ").toLowerCase());
        return hay.includes(q);
      });
      if (rows.length === 0) {
        body.innerHTML = "<tr><td colspan='4'>No routes match current filter.</td></tr>";
        return;
      }
      body.innerHTML = rows.map(r => {
        const curl = curlForRoute(r, token);
        const methods = (r.methods || []).map(m => "<code>" + esc(m) + "</code>").join(" ");
        return "<tr>" +
          "<td><code>" + esc(r.path) + "</code></td>" +
          "<td>" + methods + "</td>" +
          "<td>" + esc(r.description || "") + "</td>" +
          "<td><pre>" + esc(curl) + "</pre></td>" +
        "</tr>";
      }).join("");
    }
    async function refreshStatus() {
      try {
        const healthRes = await fetch("/api/v1/health");
        const healthJson = await healthRes.json();
        state.health = healthJson.data || {};
      } catch (_) {
        state.health = null;
      }
      // The counters need a session or a token; without one ask once, not on
      // every poll.
      const token = document.getElementById("api-token").value.trim();
      if (token !== state.deniedToken) {
        try {
          const metricsRes = await fetch("/api/v1/metrics/rate-limit", token ? { headers: { "X-API-Token": token } } : {});
          if (metricsRes.status === 401 || metricsRes.status === 403) {
            state.deniedToken = token;
          }
          const metricsJson = await metricsRes.json();
          state.metrics = metricsJson.data || null;
        } catch (_) {
          state.metrics = null;
        }
      }
      document.getElementById("st-health").textContent = (state.health && state.health.status) || "unavailable";
      document.getElementById("st-auth").textContent = (state.health && state.health.auth_mode) || "-";
      document.getElementById("st-blocked").textContent = ((state.metrics && state.metrics.metrics && state.metrics.metrics.blocked_total) ?? "-");
      document.getElementById("st-buckets").textContent = ((state.metrics && state.metrics.metrics && state.metrics.metrics.active_buckets) ?? "-");
    }
    async function refreshRoutes() {
      const res = await fetch("/api/v1/meta/routes");
      const json = await res.json();
      state.routes = (json.data && json.data.routes) || [];
      renderRoutes();
    }
    async function refreshAll() {
      await Promise.all([refreshStatus(), refreshRoutes()]);
    }
    document.getElementById("route-filter").addEventListener("input", renderRoutes);
    document.getElementById("api-token").addEventListener("input", renderRoutes);
    document.getElementById("refresh-btn").addEventListener("click", () => { state.deniedToken = null; refreshAll(); });
    refreshAll();
    setInterval(refreshStatus, 5000);
  </script>
</body>
</html>
`, authMode)
}

func (s *apiServer) handleSwaggerUIPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, `<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NCC API Swagger UI</title>
  <link rel="stylesheet" href="/docs/assets/swagger-ui.css">
</head>
<body>
  <div style="margin:10px 16px;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif">
    <a href="/">Back to API docs</a>
  </div>
  <div id="swagger-ui"></div>
  <script src="/docs/assets/swagger-ui-bundle.js"></script>
  <script>
    window.ui = SwaggerUIBundle({
      url: "/api/v1/openapi.json",
      dom_id: "#swagger-ui",
      deepLinking: true,
      displayRequestDuration: true,
      persistAuthorization: true
    });
  </script>
</body>
</html>`)
}
