package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestClusterGroupsCRUDEndToEnd exercises the admin cluster-group endpoints
// through the full handler stack using the static admin token: PUT a group set,
// GET it back, and confirm a non-admin path is rejected.
func TestClusterGroupsCRUDEndToEnd(t *testing.T) {
	dir := t.TempDir()
	db, err := openUserDB(filepath.Join(dir, "users.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.bootstrapAdminIfEmpty("admin"); err != nil {
		t.Fatal(err)
	}
	s := &apiServer{
		authMode:      "hybrid",
		authToken:     "admin-static-token",
		viewerToken:   "viewer-static-token",
		sessionSecret: "test-session-secret-value",
		sessionTTL:    10 * time.Minute,
		sessionIssuer: "ncc-api-server",
		users:         db,
		usersDBPath:   db.path,
		startedAt:     time.Now().UTC(),
	}
	ts := httptest.NewServer(s.buildHandler())
	defer ts.Close()

	do := func(method, path, token, body string) (int, map[string]any) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, _ := http.NewRequest(method, ts.URL+path, rdr)
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		if token != "" {
			req.Header.Set("X-API-Token", token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		defer resp.Body.Close()
		var env map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&env)
		return resp.StatusCode, env
	}

	// Viewer token may not read the admin endpoint.
	if code, _ := do(http.MethodGet, "/api/v1/settings/cluster-groups", "viewer-static-token", ""); code != http.StatusForbidden {
		t.Fatalf("viewer GET cluster-groups: got %d, want 403", code)
	}

	// Admin PUT a group set.
	body := `{"groups":[{"name":"Platform","clusters":["pc-east","pc-east"],"local_users":["alice"],"ad_groups":["CN=NCC-Platform,DC=corp"]}]}`
	if code, env := do(http.MethodPut, "/api/v1/settings/cluster-groups", "admin-static-token", body); code != http.StatusOK {
		t.Fatalf("admin PUT cluster-groups: got %d (%v)", code, env)
	}

	// GET back: duplicate cluster should have been de-duplicated.
	code, env := do(http.MethodGet, "/api/v1/settings/cluster-groups", "admin-static-token", "")
	if code != http.StatusOK {
		t.Fatalf("admin GET cluster-groups: got %d", code)
	}
	d, _ := env["data"].(map[string]any)
	groups, _ := d["groups"].([]any)
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	g0, _ := groups[0].(map[string]any)
	clusters, _ := g0["clusters"].([]any)
	if len(clusters) != 1 {
		t.Fatalf("expected deduped clusters len 1, got %d", len(clusters))
	}

	// A group without a name is rejected.
	if code, _ := do(http.MethodPut, "/api/v1/settings/cluster-groups", "admin-static-token", `{"groups":[{"name":"  "}]}`); code != http.StatusBadRequest {
		t.Fatalf("PUT nameless group: got %d, want 400", code)
	}
}
