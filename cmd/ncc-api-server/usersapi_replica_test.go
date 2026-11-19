package main

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

// Regression (environment test ENV-01 / ENV-02): with two API replicas, an
// admin who asked replica B for the user list right after replica A created a
// user got a list without it, a PUT for that user returned 404, and a POST of
// the same name could end in HTTP 500. Account-management requests now reload
// the shared store first, and store conflicts map to 409.
func TestUserAPIOnAStaleReplicaSeesTheOtherReplicasWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "users.json")
	open := func() userStoreBackend { return &fileStoreBackend{path: path} }
	a, b := twoReplicas(t, open)
	hash, _ := hashPassword("pw-carol-1234")
	if err := a.upsertUser("carol", hash, RoleViewer, false); err != nil {
		t.Fatal(err)
	}
	// B has not reloaded: its memory does not have carol yet.
	if _, ok := b.lookup("carol"); ok {
		t.Fatal("test setup: replica B should still be stale")
	}
	sb := &apiServer{users: b}

	rec := httptest.NewRecorder()
	sb.handleUsers(rec, httptest.NewRequest(http.MethodGet, "/api/v1/settings/users", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"carol"`) {
		t.Fatalf("list on the stale replica: %d %s", rec.Code, rec.Body.String())
	}
}

func TestUserAPIPutOnAStaleReplicaIsNot404(t *testing.T) {
	path := filepath.Join(t.TempDir(), "users.json")
	open := func() userStoreBackend { return &fileStoreBackend{path: path} }
	a, b := twoReplicas(t, open)
	hash, _ := hashPassword("pw-dave-1234")
	if err := a.upsertUser("dave", hash, RoleViewer, false); err != nil {
		t.Fatal(err)
	}
	sb := &apiServer{users: b}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/api/v1/settings/users/dave", strings.NewReader(`{"role":"operator"}`))
	sb.handleUserByName(rec, req)
	if rec.Code == http.StatusNotFound {
		t.Fatalf("PUT for a user created on another replica returned 404: %s", rec.Body.String())
	}
	if role := func() string { acc, _ := a.lookup("dave"); return acc.Role }(); role != "viewer" && role != "operator" {
		t.Fatalf("unexpected role %q", role)
	}
}

func TestUserAPICreateExistingOnAStaleReplicaIs409(t *testing.T) {
	path := filepath.Join(t.TempDir(), "users.json")
	open := func() userStoreBackend { return &fileStoreBackend{path: path} }
	a, b := twoReplicas(t, open)
	hash, _ := hashPassword("pw-erin-1234")
	if err := a.upsertUser("erin", hash, RoleViewer, false); err != nil {
		t.Fatal(err)
	}
	sb := &apiServer{users: b}
	rec := httptest.NewRecorder()
	body := `{"username":"erin","password":"Another-Long-Passw0rd-77","role":"viewer"}`
	sb.handleUsers(rec, httptest.NewRequest(http.MethodPost, "/api/v1/settings/users", strings.NewReader(body)))
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 for an existing user, got %d: %s", rec.Code, rec.Body.String())
	}
}
