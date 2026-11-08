package main

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/crewjam/saml/samlsp"
)

// An assertion that names no user (no NameID, no username attribute) must not
// sign anyone in as a shared account.
func TestSAMLCompleteRefusesAssertionWithoutUser(t *testing.T) {
	buf := captureServerLog(t, slog.LevelDebug)
	s := &apiServer{}
	s.saml = &samlProvider{roleAtt: "groups", defaultRole: RoleViewer}

	sess := samlsp.JWTSessionClaims{Attributes: samlsp.Attributes{"groups": {"ncc-admins"}}}
	r := httptest.NewRequest(http.MethodGet, "/saml/complete", nil)
	r = r.WithContext(samlsp.ContextWithSession(r.Context(), sess))
	rr := httptest.NewRecorder()
	s.handleSAMLComplete(rr, r)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status %d, want 403: %s", rr.Code, rr.Body)
	}
	for _, c := range rr.Result().Cookies() {
		if c.Value != "" {
			t.Fatalf("set cookie %s on a refused login", c.Name)
		}
	}
	if !strings.Contains(buf.String(), "saml login refused") {
		t.Fatalf("refusal not logged: %s", buf)
	}
}

// A SAML user with the same name as a local account must not get a session
// under that name (its personal access tokens would re-resolve to the local
// account's role).
func TestSAMLCompleteRefusesLocalAccountName(t *testing.T) {
	buf := captureServerLog(t, slog.LevelDebug)
	db, err := openUserDB(filepath.Join(t.TempDir(), "users.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := db.bootstrapAdminIfEmpty("admin"); err != nil || !created {
		t.Fatalf("bootstrap admin: created=%v err=%v", created, err)
	}
	s := &apiServer{users: db, sessionSecret: "test-session-secret-value", sessionTTL: time.Minute}
	s.saml = &samlProvider{roleAtt: "groups", defaultRole: RoleViewer}

	sess := samlsp.JWTSessionClaims{Attributes: samlsp.Attributes{"uid": {"ADMIN"}, "groups": {"ncc-viewers"}}}
	r := httptest.NewRequest(http.MethodGet, "/saml/complete", nil)
	r = r.WithContext(samlsp.ContextWithSession(r.Context(), sess))
	rr := httptest.NewRecorder()
	s.handleSAMLComplete(rr, r)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status %d, want 403: %s", rr.Code, rr.Body)
	}
	for _, c := range rr.Result().Cookies() {
		if c.Value != "" {
			t.Fatalf("set cookie %s on a refused login", c.Name)
		}
	}
	if !strings.Contains(buf.String(), "matches a local account") {
		t.Fatalf("refusal not logged: %s", buf)
	}
}
