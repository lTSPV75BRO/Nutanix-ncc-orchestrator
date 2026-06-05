package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestUserDBResetPassword exercises the offline recovery primitive used by
// --reset-password: it resets an existing account, invalidates its sessions,
// forces a change, recreates a wiped admin, and refuses unknown users.
func TestUserDBResetPassword(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "users.json")
	db, err := openUserDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.bootstrapAdminIfEmpty("admin"); err != nil {
		t.Fatal(err)
	}

	// Existing account: reset returns a usable temp password, forces change,
	// invalidates prior sessions (token gen bump), and the old password dies.
	hash, err := hashPassword("carol-original-pw")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.upsertUser("carol", hash, RoleOperator, false); err != nil {
		t.Fatal(err)
	}
	before, _ := db.lookup("carol")
	newPW, err := db.adminResetPassword("carol")
	if err != nil {
		t.Fatalf("reset carol: %v", err)
	}
	if _, ok, _ := db.verify("carol", "carol-original-pw"); ok {
		t.Fatal("old password should no longer verify after reset")
	}
	role, ok, mustChange := db.verify("carol", newPW)
	if !ok {
		t.Fatal("new temp password should verify")
	}
	if role != RoleOperator {
		t.Fatalf("role changed by reset: got %v, want operator", role)
	}
	if !mustChange {
		t.Fatal("reset must force a password change")
	}
	after, _ := db.lookup("carol")
	if after.TokenGen <= before.TokenGen {
		t.Fatalf("token gen not bumped: before=%d after=%d", before.TokenGen, after.TokenGen)
	}

	// Unknown non-admin account: errUserNotFound (no silent creation).
	if _, err := db.adminResetPassword("ghost"); err == nil {
		t.Fatal("resetting an unknown account should fail")
	}

	// A wiped admin can be recreated from a store with no admin present.
	empty, err := openUserDB(filepath.Join(dir, "empty.json"))
	if err != nil {
		t.Fatal(err)
	}
	adminPW, err := empty.adminResetPassword("admin")
	if err != nil {
		t.Fatalf("recreate admin: %v", err)
	}
	acct, ok := empty.lookup("admin")
	if !ok {
		t.Fatal("admin should have been recreated")
	}
	if r, _ := parseRole(acct.Role); r != RoleAdmin {
		t.Fatalf("recreated admin role = %q, want admin", acct.Role)
	}
	if _, ok, _ := empty.verify("admin", adminPW); !ok {
		t.Fatal("recreated admin temp password should verify")
	}
}

// TestForgotPasswordQueueAndResolve covers the self-service request queue end
// to end: a public request is recorded only for an existing account (no
// enumeration), an admin can list/dismiss it, and an admin password reset
// auto-clears the request.
func TestForgotPasswordQueueAndResolve(t *testing.T) {
	s := newDBServer(t)
	if _, _, err := s.users.bootstrapAdminIfEmpty("admin"); err != nil {
		t.Fatal(err)
	}
	hash, err := hashPassword("dave-original-pw")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.users.upsertUser("dave", hash, RoleViewer, false); err != nil {
		t.Fatal(err)
	}

	forgot := func(username string) int {
		rr := httptest.NewRecorder()
		body := strings.NewReader(`{"username":"` + username + `"}`)
		s.handleForgotPassword(rr, httptest.NewRequest(http.MethodPost, "/api/v1/auth/forgot-password", body))
		return rr.Code
	}

	// A request for a real local account is recorded; the response is generic.
	if code := forgot("dave"); code != http.StatusOK {
		t.Fatalf("forgot dave: %d", code)
	}
	// A request for a nonexistent account also returns 200 (no enumeration) but
	// records nothing.
	if code := forgot("nobody"); code != http.StatusOK {
		t.Fatalf("forgot nobody: %d", code)
	}
	reqs := s.users.listResetRequests()
	if len(reqs) != 1 || !strings.EqualFold(reqs[0].Username, "dave") {
		t.Fatalf("unexpected queued requests: %+v", reqs)
	}

	// Admin can list the pending requests.
	rrList := httptest.NewRecorder()
	s.handlePasswordResets(rrList, httptest.NewRequest(http.MethodGet, "/api/v1/settings/password-resets", nil))
	if rrList.Code != http.StatusOK || !strings.Contains(rrList.Body.String(), "dave") {
		t.Fatalf("list resets: %d (%s)", rrList.Code, rrList.Body.String())
	}
	if strings.Contains(rrList.Body.String(), "nobody") {
		t.Fatal("nonexistent account must not be enumerated in the queue")
	}

	// An admin password reset (via the users API) auto-clears the request.
	rrReset := httptest.NewRecorder()
	s.handleUserByName(rrReset, httptest.NewRequest(http.MethodPut, "/api/v1/settings/users/dave", strings.NewReader(`{"password":"dave-new-temp-pw"}`)))
	if rrReset.Code != http.StatusOK {
		t.Fatalf("reset dave: %d (%s)", rrReset.Code, rrReset.Body.String())
	}
	if got := s.users.listResetRequests(); len(got) != 0 {
		t.Fatalf("request should auto-clear after reset, still have %+v", got)
	}

	// Re-queue, then dismiss without resetting.
	s.users.addResetRequest("dave", "203.0.113.7")
	if len(s.users.listResetRequests()) != 1 {
		t.Fatal("expected one queued request after re-add")
	}
	rrDismiss := httptest.NewRecorder()
	s.handlePasswordResetByName(rrDismiss, httptest.NewRequest(http.MethodDelete, "/api/v1/settings/password-resets/dave", nil))
	if rrDismiss.Code != http.StatusOK {
		t.Fatalf("dismiss: %d (%s)", rrDismiss.Code, rrDismiss.Body.String())
	}
	if got := s.users.listResetRequests(); len(got) != 0 {
		t.Fatalf("request should be gone after dismiss, still have %+v", got)
	}
}

// TestRevokeSessionsBumpsGeneration covers session revocation: both the
// self-service path and the admin force-sign-out bump the account token
// generation so previously issued sessions stop validating.
func TestRevokeSessionsBumpsGeneration(t *testing.T) {
	s := newDBServer(t)
	hash, err := hashPassword("erin-password-1234")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.users.upsertUser("erin", hash, RoleOperator, false); err != nil {
		t.Fatal(err)
	}
	before, _ := s.users.lookup("erin")

	// Admin force-sign-out via the users API.
	rr := httptest.NewRecorder()
	s.handleUserByName(rr, httptest.NewRequest(http.MethodPut, "/api/v1/settings/users/erin", strings.NewReader(`{"revoke_sessions":true}`)))
	if rr.Code != http.StatusOK {
		t.Fatalf("revoke via admin: %d (%s)", rr.Code, rr.Body.String())
	}
	after, _ := s.users.lookup("erin")
	if after.TokenGen <= before.TokenGen {
		t.Fatalf("admin revoke did not bump token gen: before=%d after=%d", before.TokenGen, after.TokenGen)
	}

	// Self-service logout-all (principal injected into context) bumps again.
	gen2 := after.TokenGen
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout-all", nil)
	req = withPrincipal(req, principal{subject: "erin", role: RoleOperator, method: authSessionCookie})
	rr2 := httptest.NewRecorder()
	s.handleLogoutAll(rr2, req)
	if rr2.Code != http.StatusOK {
		t.Fatalf("logout-all: %d (%s)", rr2.Code, rr2.Body.String())
	}
	again, _ := s.users.lookup("erin")
	if again.TokenGen <= gen2 {
		t.Fatalf("logout-all did not bump token gen: %d -> %d", gen2, again.TokenGen)
	}
}

// TestForgotPasswordAdminSelfReset verifies that asking to reset the built-in
// admin via forgot-password regenerates a random bootstrap-style password
// (first-run workflow): the old password stops verifying, a change is forced,
// sessions are invalidated, the .ncc-initial-admin-password file is rewritten,
// and the response advertises the self-reset rather than queuing a request.
func TestForgotPasswordAdminSelfReset(t *testing.T) {
	s := newDBServer(t)
	oldPW, _, err := s.users.bootstrapAdminIfEmpty("admin")
	if err != nil {
		t.Fatal(err)
	}
	// Clear the initial bootstrap file so we can prove the self-reset rewrites it.
	s.users.clearInitialPassword()
	before, _ := s.users.lookup("admin")

	rr := httptest.NewRecorder()
	body := strings.NewReader(`{"username":"admin"}`)
	s.handleForgotPassword(rr, httptest.NewRequest(http.MethodPost, "/api/v1/auth/forgot-password", body))
	if rr.Code != http.StatusOK {
		t.Fatalf("forgot admin: %d (%s)", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"admin_reset":true`) {
		t.Fatalf("response should flag admin_reset: %s", rr.Body.String())
	}

	// The original bootstrap password must no longer verify.
	if _, ok, _ := s.users.verify("admin", oldPW); ok {
		t.Fatal("old admin password should no longer verify after self-reset")
	}
	after, _ := s.users.lookup("admin")
	if after.TokenGen <= before.TokenGen {
		t.Fatalf("token gen not bumped: before=%d after=%d", before.TokenGen, after.TokenGen)
	}
	if !after.MustChange {
		t.Fatal("admin self-reset must force a password change")
	}

	// No reset request should be queued for the admin self-service path.
	if got := s.users.listResetRequests(); len(got) != 0 {
		t.Fatalf("admin self-reset must not queue a request, got %+v", got)
	}

	// The new temporary password is surfaced through the sibling password file.
	pwFile := filepath.Join(filepath.Dir(s.usersDBPath), ".ncc-initial-admin-password")
	raw, err := os.ReadFile(pwFile)
	if err != nil {
		t.Fatalf("initial password file not rewritten: %v", err)
	}
	if !strings.Contains(string(raw), "password:") {
		t.Fatalf("initial password file missing password line: %s", string(raw))
	}
}
