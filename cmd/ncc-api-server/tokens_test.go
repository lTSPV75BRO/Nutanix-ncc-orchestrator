package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

func newTokenTestServer(t *testing.T) *apiServer {
	t.Helper()
	dir := t.TempDir()
	db, err := openUserDB(filepath.Join(dir, "users.json"))
	if err != nil {
		t.Fatalf("openUserDB: %v", err)
	}
	return &apiServer{users: db, usersDBPath: db.path}
}

func addLocalAccount(t *testing.T, db *userDB, name string, role Role) {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte("pw-"+name), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	if err := db.upsertUser(name, string(hash), role, false); err != nil {
		t.Fatalf("upsertUser: %v", err)
	}
}

// mintToken stores a PAT for owner and returns the plaintext secret.
func mintToken(t *testing.T, s *apiServer, owner string, ownerLocal bool, role Role, expires string) (string, string) {
	t.Helper()
	secret := patPrefix + "secret-" + owner + "-" + role.String()
	id := "id-" + owner + "-" + role.String()
	pt := personalToken{
		ID:         id,
		Name:       "test",
		Owner:      owner,
		OwnerLocal: ownerLocal,
		Role:       role.String(),
		Hash:       patHash(secret),
		CreatedAt:  time.Now().UTC().Format(time.RFC3339),
		ExpiresAt:  expires,
	}
	if err := s.users.addToken(pt); err != nil {
		t.Fatalf("addToken: %v", err)
	}
	return secret, id
}

func reqWithToken(secret string) *http.Request {
	r, _ := http.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	r.Header.Set("X-API-Token", secret)
	return r
}

func TestPATResolvesLocalOwnerLiveRole(t *testing.T) {
	s := newTokenTestServer(t)
	addLocalAccount(t, s.users, "alice", RoleViewer)
	secret, _ := mintToken(t, s, "alice", true, RoleViewer, "")

	p, ok := s.principalFromPAT(reqWithToken(secret))
	if !ok || p.method != authPAT || p.subject != "alice" || p.role != RoleViewer {
		t.Fatalf("expected viewer alice via PAT, got ok=%v subject=%q role=%v", ok, p.subject, p.role)
	}

	// Promote alice; the live role must be re-resolved on the next request even
	// though the token snapshot still says viewer.
	addLocalAccount(t, s.users, "alice", RoleOperator)
	p, ok = s.principalFromPAT(reqWithToken(secret))
	if !ok || p.role != RoleOperator {
		t.Fatalf("expected re-resolved operator role, got ok=%v role=%v", ok, p.role)
	}
}

func TestPATRejectedWhenLocalOwnerDeleted(t *testing.T) {
	s := newTokenTestServer(t)
	addLocalAccount(t, s.users, "bob", RoleOperator)
	secret, _ := mintToken(t, s, "bob", true, RoleOperator, "")

	if _, ok := s.principalFromPAT(reqWithToken(secret)); !ok {
		t.Fatal("token should work before deletion")
	}
	if err := s.users.deleteUser("bob"); err != nil {
		t.Fatalf("deleteUser: %v", err)
	}
	if _, ok := s.principalFromPAT(reqWithToken(secret)); ok {
		t.Fatal("token must be rejected after owner account is deleted")
	}
}

func TestPATRejectedWhenExpired(t *testing.T) {
	s := newTokenTestServer(t)
	addLocalAccount(t, s.users, "carol", RoleAdmin)
	past := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	secret, _ := mintToken(t, s, "carol", true, RoleAdmin, past)

	if _, ok := s.principalFromPAT(reqWithToken(secret)); ok {
		t.Fatal("expired token must be rejected")
	}
}

func TestPATNonLocalOwnerUsesSnapshotRole(t *testing.T) {
	s := newTokenTestServer(t)
	// No local account: an AD/SAML-minted token relies on its snapshot role.
	secret, _ := mintToken(t, s, "ad\\dave", false, RoleOperator, "")
	p, ok := s.principalFromPAT(reqWithToken(secret))
	if !ok || p.role != RoleOperator || p.subject != "ad\\dave" {
		t.Fatalf("expected snapshot operator for AD owner, got ok=%v role=%v subject=%q", ok, p.role, p.subject)
	}
}

func TestPATBearerHeaderAccepted(t *testing.T) {
	s := newTokenTestServer(t)
	addLocalAccount(t, s.users, "erin", RoleViewer)
	secret, _ := mintToken(t, s, "erin", true, RoleViewer, "")
	r, _ := http.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	r.Header.Set("Authorization", "Bearer "+secret)
	if _, ok := s.principalFromPAT(r); !ok {
		t.Fatal("PAT presented via Authorization: Bearer must be accepted")
	}
}

func TestPATDeleteOwnershipRules(t *testing.T) {
	s := newTokenTestServer(t)
	addLocalAccount(t, s.users, "frank", RoleViewer)
	_, id := mintToken(t, s, "frank", true, RoleViewer, "")

	// A different non-admin user cannot revoke frank's token.
	if _, _, err := s.users.deleteToken(id, "mallory", false); err != errTokenForbidden {
		t.Fatalf("expected errTokenForbidden, got %v", err)
	}
	// An admin can revoke any token.
	if _, removed, err := s.users.deleteToken(id, "admin", true); err != nil || !removed {
		t.Fatalf("admin revoke failed: removed=%v err=%v", removed, err)
	}
	if _, ok := s.users.findTokenByHash(patHash(patPrefix + "secret-frank-viewer")); ok {
		t.Fatal("token should be gone after revoke")
	}
}

func TestPATPerOwnerCap(t *testing.T) {
	s := newTokenTestServer(t)
	addLocalAccount(t, s.users, "grace", RoleViewer)
	for i := 0; i < maxTokensPerOwner; i++ {
		pt := personalToken{ID: randID(t, i), Name: "t", Owner: "grace", OwnerLocal: true, Role: "viewer", Hash: patHash("h" + randID(t, i))}
		if err := s.users.addToken(pt); err != nil {
			t.Fatalf("addToken %d: %v", i, err)
		}
	}
	over := personalToken{ID: "over", Name: "t", Owner: "grace", OwnerLocal: true, Role: "viewer", Hash: patHash("over")}
	if err := s.users.addToken(over); err == nil {
		t.Fatal("expected per-owner cap to reject the extra token")
	}
}

func TestPATResolvesNeverExpiringToken(t *testing.T) {
	s := newTokenTestServer(t)
	addLocalAccount(t, s.users, "heidi", RoleOperator)
	// Empty ExpiresAt means the token never expires; it must still resolve far
	// into the future.
	secret, _ := mintToken(t, s, "heidi", true, RoleOperator, "")
	if _, ok := s.principalFromPAT(reqWithToken(secret)); !ok {
		t.Fatal("a never-expiring token must always resolve")
	}
}

func TestCreateTokenNeverExpiryViaHandler(t *testing.T) {
	s := newTokenTestServer(t)
	addLocalAccount(t, s.users, "ivan", RoleViewer)

	body := strings.NewReader(`{"name":"forever","expires_in_days":0}`)
	r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/tokens", body)
	r.Header.Set("Content-Type", "application/json")
	r = withPrincipal(r, principal{subject: "ivan", role: RoleViewer, method: authSessionCookie})
	rr := httptest.NewRecorder()
	s.handleAuthTokens(rr, r)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Data struct {
			Token     string `json:"token"`
			ExpiresAt string `json:"expires_at"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Data.ExpiresAt != "" {
		t.Fatalf("expires_at should be empty for a never-expiring token, got %q", resp.Data.ExpiresAt)
	}
	if _, ok := s.principalFromPAT(reqWithToken(resp.Data.Token)); !ok {
		t.Fatal("minted never-expiring token should authenticate")
	}
}

func TestCookieSecureDefaultsInsecure(t *testing.T) {
	s := newTokenTestServer(t)
	if s.cookieSecure() {
		t.Fatal("session cookies must be insecure by default (plain HTTP works out of the box)")
	}
	// Enabling HTTPS flips cookies to Secure.
	if err := s.users.setTLSPolicy(&tlsPolicy{HTTPSEnabled: true}); err != nil {
		t.Fatalf("setTLSPolicy: %v", err)
	}
	if !s.cookieSecure() {
		t.Fatal("session cookies must be Secure once HTTPS is enabled")
	}
	// --cookie-insecure forces them off even with HTTPS on (TLS-terminating proxy).
	s.cookieInsecure = true
	if s.cookieSecure() {
		t.Fatal("--cookie-insecure must force Secure off")
	}
}

func TestPatchV2StartStateUITLS(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, v2StartStateFileName)
	// Pre-existing state with unrelated keys that must be preserved.
	if err := os.WriteFile(statePath, []byte(`{"api_listen":"127.0.0.1:8081","ui_listen":"0.0.0.0:8080"}`), 0o600); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	ok, err := patchV2StartStateUITLS(dir, "/x/ui.crt", "/x/ui.key")
	if err != nil || !ok {
		t.Fatalf("patch set: ok=%v err=%v", ok, err)
	}
	got := readState(t, statePath)
	if got["ui_tls_cert_file"] != "/x/ui.crt" || got["ui_tls_key_file"] != "/x/ui.key" {
		t.Fatalf("expected TLS files set, got %#v", got)
	}
	if got["api_listen"] != "127.0.0.1:8081" {
		t.Fatalf("unrelated keys must be preserved, got %#v", got)
	}
	// Clearing removes the keys.
	if ok, err := patchV2StartStateUITLS(dir, "", ""); err != nil || !ok {
		t.Fatalf("patch clear: ok=%v err=%v", ok, err)
	}
	got = readState(t, statePath)
	if _, present := got["ui_tls_cert_file"]; present {
		t.Fatalf("ui_tls_cert_file should be removed, got %#v", got)
	}
	// Missing file => ok=false, no error (caller declines auto-restart).
	if ok, err := patchV2StartStateUITLS(t.TempDir(), "/x/ui.crt", "/x/ui.key"); err != nil || ok {
		t.Fatalf("missing state should yield ok=false,nil; got ok=%v err=%v", ok, err)
	}
}

func readState(t *testing.T, path string) map[string]string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	m := map[string]string{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal state: %v", err)
	}
	return m
}

func randID(t *testing.T, i int) string {
	t.Helper()
	return "id-" + time.Now().Format("150405.000000000") + "-" + string(rune('a'+i%26)) + string(rune('0'+i%10))
}
