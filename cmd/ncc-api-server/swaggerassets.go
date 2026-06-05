package main

import (
	"embed"
	"net/http"
	"strings"
	"time"
)

// swaggerFS embeds the Swagger UI assets (vendored from swagger-ui-dist) so the
// docs page is fully self-hosted. This removes the runtime dependency on the
// public unpkg.com CDN and lets the Content-Security-Policy drop the external
// origin entirely.
//
//go:embed swaggerassets/swagger-ui.css swaggerassets/swagger-ui-bundle.js
var swaggerFS embed.FS

// swaggerUIVersion records the vendored swagger-ui-dist version for provenance.
const swaggerUIVersion = "5.32.6"

// swaggerAssetNames lists the embedded asset suffixes (exposed for tests).
var swaggerAssetNames = []string{"swagger-ui.css", "swagger-ui-bundle.js"}

// handleSwaggerAsset serves a single embedded Swagger UI asset by suffix. Paths
// are matched explicitly (no path traversal) and served with a long cache TTL
// since the files are versioned with the binary.
func (s *apiServer) handleSwaggerAsset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	var name, ctype string
	switch {
	case strings.HasSuffix(r.URL.Path, "/swagger-ui.css"):
		name, ctype = "swaggerassets/swagger-ui.css", "text/css; charset=utf-8"
	case strings.HasSuffix(r.URL.Path, "/swagger-ui-bundle.js"):
		name, ctype = "swaggerassets/swagger-ui-bundle.js", "application/javascript; charset=utf-8"
	default:
		http.NotFound(w, r)
		return
	}
	data, err := swaggerFS.ReadFile(name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", ctype)
	w.Header().Set("Cache-Control", "public, max-age=86400, immutable")
	http.ServeContent(w, r, name, time.Time{}, strings.NewReader(string(data)))
}
