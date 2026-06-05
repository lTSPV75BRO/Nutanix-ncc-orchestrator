package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestSwaggerAssetsSelfHosted verifies the vendored Swagger UI assets are served
// locally (no unpkg dependency), reachable without auth, and that the UI page
// references the local paths.
func TestSwaggerAssetsSelfHosted(t *testing.T) {
	s := newDBServer(t)
	s.corsOrigin = "http://localhost:8080"
	ts := httptest.NewServer(s.buildHandler())
	defer ts.Close()

	for _, name := range swaggerAssetNames {
		resp, err := http.Get(ts.URL + "/docs/assets/" + name)
		if err != nil {
			t.Fatalf("GET %s: %v", name, err)
		}
		body := make([]byte, 64)
		n, _ := resp.Body.Read(body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status %d (want 200, must bypass auth)", name, resp.StatusCode)
		}
		if n == 0 {
			t.Fatalf("%s: empty asset", name)
		}
		ct := resp.Header.Get("Content-Type")
		if strings.HasSuffix(name, ".css") && !strings.Contains(ct, "text/css") {
			t.Fatalf("%s: content-type %q", name, ct)
		}
		if strings.HasSuffix(name, ".js") && !strings.Contains(ct, "javascript") {
			t.Fatalf("%s: content-type %q", name, ct)
		}
	}

	// The UI page references the self-hosted assets, not unpkg.
	resp, err := http.Get(ts.URL + "/docs/ui")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	page := string(raw)
	if !strings.Contains(page, "/docs/assets/swagger-ui-bundle.js") {
		t.Fatal("swagger UI page should reference the local bundle")
	}
	if strings.Contains(page, "unpkg.com") {
		t.Fatal("swagger UI page should not reference unpkg.com")
	}
}
