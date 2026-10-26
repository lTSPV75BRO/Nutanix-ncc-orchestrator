package main

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"goncc/internal/auth"
)

// statusFor runs one GET /api/v1/runs through withAuth with the given request
// decoration and returns the HTTP status.
func statusFor(s *apiServer, decorate func(*http.Request)) int {
	next := s.withAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	decorate(req)
	rr := httptest.NewRecorder()
	next.ServeHTTP(rr, req)
	return rr.Code
}

// Regression: deleting a local account must end its existing sessions. The
// subject of a deleted account is no longer in the store, which used to make
// its still-valid JWT/HMAC session look like an SSO user's and keep the role
// it was minted with until expiry (up to 24h).
func TestDeletedAccountSessionsAreRejected(t *testing.T) {
	s := newLoginServer(t)
	s.jwtSecret = []byte("0123456789abcdef0123456789abcdef")

	jwtTok, err := auth.GenerateSessionJWT("op1", "operator", time.Hour, s.jwtSecret)
	if err != nil {
		t.Fatal(err)
	}
	hmacTok, _, err := s.issueRoleSessionToken("192.0.2.1", "op1", RoleOperator)
	if err != nil {
		t.Fatal(err)
	}
	withJWT := func(r *http.Request) { r.AddCookie(&http.Cookie{Name: auth.CookieName, Value: jwtTok}) }
	withHMAC := func(r *http.Request) {
		r.RemoteAddr = "192.0.2.1:1234"
		r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: hmacTok})
	}
	if code := statusFor(s, withJWT); code != http.StatusOK {
		t.Fatalf("JWT before delete: %d", code)
	}
	if code := statusFor(s, withHMAC); code != http.StatusOK {
		t.Fatalf("HMAC session before delete: %d", code)
	}

	if err := s.users.deleteUser("op1"); err != nil {
		t.Fatal(err)
	}
	if code := statusFor(s, withJWT); code != http.StatusUnauthorized {
		t.Fatalf("JWT after delete: want 401, got %d", code)
	}
	if code := statusFor(s, withHMAC); code != http.StatusUnauthorized {
		t.Fatalf("HMAC session after delete: want 401, got %d", code)
	}

	// A session minted after the deletion (an LDAP/SSO user who happens to
	// share the name) is not affected by the tombstone.
	s.users.mu.Lock()
	tomb := s.users.deleted["op1"]
	tomb.DeletedAt = time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	s.users.deleted["op1"] = tomb
	s.users.mu.Unlock()
	fresh, err := auth.GenerateSessionJWT("op1", "operator", time.Hour, s.jwtSecret)
	if err != nil {
		t.Fatal(err)
	}
	if code := statusFor(s, func(r *http.Request) { r.AddCookie(&http.Cookie{Name: auth.CookieName, Value: fresh}) }); code != http.StatusOK {
		t.Fatalf("session issued after the deletion: want 200, got %d", code)
	}
}

// Regression: re-creating a deleted username must not revive the old
// account's sessions. Generations used to restart at 0, so any gen-0 token of
// the old account validated again against the new account.
func TestRecreatedAccountDoesNotAcceptOldSessions(t *testing.T) {
	s := newLoginServer(t)
	s.jwtSecret = []byte("0123456789abcdef0123456789abcdef")
	old, err := auth.GenerateSessionJWT("op1", "operator", time.Hour, s.jwtSecret)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.users.deleteUser("op1"); err != nil {
		t.Fatal(err)
	}
	hash, err := hashPassword("pw-op-new")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.users.upsertUser("op1", hash, RoleAdmin, false); err != nil {
		t.Fatal(err)
	}
	acct, ok := s.users.lookup("op1")
	if !ok || acct.TokenGen == 0 {
		t.Fatalf("re-created account should continue the generation, got %+v", acct)
	}
	if code := statusFor(s, func(r *http.Request) { r.AddCookie(&http.Cookie{Name: auth.CookieName, Value: old}) }); code != http.StatusUnauthorized {
		t.Fatalf("old account's JWT against re-created account: want 401, got %d", code)
	}
	if _, dead := s.users.deleted["op1"]; dead {
		t.Fatal("tombstone should be cleared once the name is re-created")
	}
}

func TestDeletedAccountTombstonePersistsAndPrunes(t *testing.T) {
	db, err := openUserDB(filepath.Join(t.TempDir(), "users.json"))
	if err != nil {
		t.Fatal(err)
	}
	hash, err := hashPassword("pw-someone")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"admin", "carol"} {
		if err := db.upsertUser(name, hash, RoleAdmin, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.deleteUser("carol"); err != nil {
		t.Fatal(err)
	}
	reloaded, err := openUserDB(db.path)
	if err != nil {
		t.Fatal(err)
	}
	if !reloaded.sessionPredatesDeletion("Carol", time.Now().Add(-time.Hour)) {
		t.Fatal("tombstone must survive a reload (other replicas read the same store)")
	}
	if reloaded.sessionPredatesDeletion("carol", time.Now().Add(time.Hour)) {
		t.Fatal("a session issued after the deletion must not be rejected")
	}
	if !reloaded.sessionPredatesDeletion("carol", time.Time{}) {
		t.Fatal("a session without iat must fail closed")
	}

	// Expired tombstones are dropped on the next write.
	reloaded.mu.Lock()
	reloaded.deleted["carol"] = deletedAccount{Username: "carol", TokenGen: 1,
		DeletedAt: time.Now().UTC().Add(-deletedAccountRetention - time.Hour).Format(time.RFC3339)}
	_ = reloaded.saveLocked()
	_, still := reloaded.deleted["carol"]
	reloaded.mu.Unlock()
	if still {
		t.Fatal("tombstone older than the retention window should be pruned")
	}
}

// Regression (COR-07): a validly signed session with an unknown role used to
// be treated as admin. It must be rejected instead.
func TestSessionWithUnknownRoleIsRejected(t *testing.T) {
	s := newLoginServer(t)
	s.jwtSecret = []byte("0123456789abcdef0123456789abcdef")
	jwtTok, err := auth.GenerateSessionJWT("sso-user", "superuser", time.Hour, s.jwtSecret)
	if err != nil {
		t.Fatal(err)
	}
	if code := statusFor(s, func(r *http.Request) { r.AddCookie(&http.Cookie{Name: auth.CookieName, Value: jwtTok}) }); code != http.StatusUnauthorized {
		t.Fatalf("JWT with unknown role: want 401, got %d", code)
	}
	hmacTok, _, err := s.issueRoleSessionToken("192.0.2.1", "sso-user", RoleNone)
	if err != nil {
		t.Fatal(err)
	}
	if code := statusFor(s, func(r *http.Request) {
		r.RemoteAddr = "192.0.2.1:1234"
		r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: hmacTok})
	}); code != http.StatusUnauthorized {
		t.Fatalf("HMAC session without a role: want 401, got %d", code)
	}
}
