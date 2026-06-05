package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestLoginGuardLockAndExpiry covers the core state machine: failures below the
// threshold don't lock, the threshold trips a lock, the lock expires after the
// configured duration, a successful reset clears state, and a nil guard is a
// no-op.
func TestLoginGuardLockAndExpiry(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	g := newLoginGuard(3, 15*time.Minute, 10*time.Minute)

	if locked, _ := g.locked("alice", now); locked {
		t.Fatal("fresh account should not be locked")
	}
	// Two failures: still allowed.
	g.recordFailure("alice", now)
	if tripped := g.recordFailure("Alice", now); tripped { // case-insensitive key
		t.Fatal("second failure should not trip a lock")
	}
	// Third failure trips the lock.
	if tripped := g.recordFailure("alice", now); !tripped {
		t.Fatal("third failure should trip the lock")
	}
	if locked, retry := g.locked("alice", now); !locked || retry <= 0 {
		t.Fatalf("account should be locked with positive retry, got locked=%v retry=%v", locked, retry)
	}
	// A different account is unaffected.
	if locked, _ := g.locked("bob", now); locked {
		t.Fatal("unrelated account must not be locked")
	}
	// After the lockout window elapses, access is allowed again.
	if locked, _ := g.locked("alice", now.Add(11*time.Minute)); locked {
		t.Fatal("lock should expire after the lockout duration")
	}
	// reset clears state immediately.
	g.recordFailure("carol", now)
	g.reset("carol")
	if locked, _ := g.locked("carol", now); locked {
		t.Fatal("reset should clear failure state")
	}

	// A disabled guard (threshold 0) is nil and never locks.
	var disabled *loginGuard = newLoginGuard(0, time.Minute, time.Minute)
	if disabled != nil {
		t.Fatal("threshold 0 must disable the guard (nil)")
	}
	for i := 0; i < 100; i++ {
		disabled.recordFailure("x", now)
	}
	if locked, _ := disabled.locked("x", now); locked {
		t.Fatal("nil guard must never lock")
	}
}

// TestHandleLoginLockoutEndToEnd drives the lockout through the real login
// handler: repeated bad passwords lock the account (429 NCC_API_ACCOUNT_LOCKED),
// and even the correct password is then rejected until an admin reset clears it.
func TestHandleLoginLockoutEndToEnd(t *testing.T) {
	s := newDBServer(t)
	s.loginGuard = newLoginGuard(3, 15*time.Minute, 15*time.Minute)
	hash, err := hashPassword("correct-horse-battery")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.users.upsertUser("dana", hash, RoleOperator, false); err != nil {
		t.Fatal(err)
	}

	login := func(pw string) (int, string) {
		rr := httptest.NewRecorder()
		body := strings.NewReader(`{"username":"dana","password":"` + pw + `"}`)
		s.handleLogin(rr, httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", body))
		return rr.Code, rr.Body.String()
	}

	for i := 0; i < 3; i++ {
		if code, _ := login("wrong"); code != http.StatusUnauthorized {
			t.Fatalf("bad attempt %d: want 401, got %d", i+1, code)
		}
	}
	// Now locked: even the right password is rejected with 429.
	code, bodyStr := login("correct-horse-battery")
	if code != http.StatusTooManyRequests || !strings.Contains(bodyStr, "NCC_API_ACCOUNT_LOCKED") {
		t.Fatalf("expected lockout 429, got %d (%s)", code, bodyStr)
	}

	// An admin reset clears the lockout; the new password works immediately.
	rr := httptest.NewRecorder()
	s.handleUserByName(rr, httptest.NewRequest(http.MethodPut, "/api/v1/settings/users/dana", strings.NewReader(`{"password":"a-fresh-strong-password"}`)))
	if rr.Code != http.StatusOK {
		t.Fatalf("admin reset: %d (%s)", rr.Code, rr.Body.String())
	}
	if code, body := login("a-fresh-strong-password"); code != http.StatusOK {
		t.Fatalf("login after reset should succeed, got %d (%s)", code, body)
	}
}
