package main

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoginGuardCredentialStuffingLocksSource(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	g := newLoginGuard(5, 15*time.Minute, 10*time.Minute) // source threshold 20
	// One client sprays a different username on every attempt.
	for i := 0; i < 19; i++ {
		g.recordFailureFrom("user"+string(rune('a'+i)), "203.0.113.9", now)
	}
	if locked, _ := g.check("brand-new-user", "203.0.113.9", now); locked {
		t.Fatal("source should not be locked below its threshold")
	}
	if !g.recordFailureFrom("usert", "203.0.113.9", now) {
		t.Fatal("20th failure from one source should trip the source lock")
	}
	if locked, _ := g.check("never-tried", "203.0.113.9", now); !locked {
		t.Fatal("a locked source must be blocked for every username")
	}
	if locked, _ := g.check("never-tried", "198.51.100.7", now); locked {
		t.Fatal("other sources must not be affected")
	}
}

func TestLoginGuardLimitsLockoutDenialOfService(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	g := newLoginGuard(5, 15*time.Minute, 10*time.Minute)
	for i := 0; i < 5; i++ {
		g.recordFailureFrom("admin", "203.0.113.9", now)
	}
	if locked, _ := g.check("admin", "203.0.113.9", now); !locked {
		t.Fatal("attacking source must be locked for the account")
	}
	if locked, _ := g.check("admin", "10.1.2.3", now); locked {
		t.Fatal("the real admin from another source must still be able to sign in (no trivial lockout DoS)")
	}
	// A distributed attack still trips the account-wide threshold (4x).
	for i := 0; i < 15; i++ {
		g.recordFailureFrom("admin", "198.51.100."+string(rune('0'+i%10)), now)
	}
	if locked, _ := g.check("admin", "10.1.2.3", now); !locked {
		t.Fatal("account-wide threshold should lock the account against distributed guessing")
	}
}

func TestLoginGuardProgressiveLockoutAndDecay(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	g := newLoginGuard(2, 15*time.Minute, time.Minute)
	trip := func() time.Duration {
		g.recordFailureFrom("eve", "203.0.113.1", now)
		g.recordFailureFrom("eve", "203.0.113.1", now)
		_, d := g.check("eve", "203.0.113.1", now)
		return d
	}
	first := trip()
	now = now.Add(first + time.Second)
	second := trip()
	if second <= first {
		t.Fatalf("repeated lockouts should grow: %v then %v", first, second)
	}
	for i := 0; i < 10; i++ {
		now = now.Add(second + time.Second)
		second = trip()
	}
	if second > g.maxLockout {
		t.Fatalf("lockout must be capped at %v, got %v", g.maxLockout, second)
	}
}

func TestLoginGuardSuccessResetsAccountNotSource(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	g := newLoginGuard(3, 15*time.Minute, 10*time.Minute)
	g.sourceThreshold = 4
	for i := 0; i < 3; i++ {
		g.recordFailureFrom("u"+string(rune('a'+i)), "203.0.113.5", now)
	}
	g.reset("owned-account") // attacker logs into an account it controls
	if !g.recordFailureFrom("ux", "203.0.113.5", now) {
		t.Fatal("a successful login must not clear the source's failure count")
	}
}

// Two replicas sharing a state file enforce one combined limit.
func TestLoginGuardSharedStateAcrossReplicas(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".ncc-login-guard.json")
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	a := newLoginGuard(4, 15*time.Minute, 10*time.Minute)
	b := newLoginGuard(4, 15*time.Minute, 10*time.Minute)
	a.configureSharedState(path)
	b.configureSharedState(path)
	a.recordFailureFrom("dana", "203.0.113.2", now)
	b.recordFailureFrom("dana", "203.0.113.2", now)
	a.recordFailureFrom("dana", "203.0.113.2", now)
	if !b.recordFailureFrom("dana", "203.0.113.2", now) {
		t.Fatal("4th failure across replicas should lock (counters must be shared)")
	}
	if locked, _ := a.check("dana", "203.0.113.2", now); !locked {
		t.Fatal("replica A must see the lock set via replica B")
	}
	b.reset("dana")
	if locked, _ := a.check("dana", "203.0.113.2", now); locked {
		t.Fatal("reset on one replica must clear the lock everywhere")
	}
}

func TestClientIPIgnoresSpoofedForwardedFor(t *testing.T) {
	t.Cleanup(func() { _ = configureTrustedProxies("private,loopback") })
	if err := configureTrustedProxies("private,loopback"); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil)
	req.RemoteAddr = "10.0.0.5:4321" // the UI proxy pod
	// Client forged a left-most hop; the proxy appended the real address.
	req.Header.Set("X-Forwarded-For", "1.2.3.4, 198.51.100.20")
	if got := cleanClientIP(req); got != "198.51.100.20" {
		t.Fatalf("client IP = %s, want the hop appended by the trusted proxy", got)
	}
	// Direct (untrusted) peers cannot use X-Forwarded-For at all.
	direct := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil)
	direct.RemoteAddr = "198.51.100.30:1111"
	direct.Header.Set("X-Forwarded-For", "1.2.3.4")
	if got := cleanClientIP(direct); got != "198.51.100.30" {
		t.Fatalf("untrusted peer spoofed its IP: %s", got)
	}
	// With an explicit proxy CIDR, a private-network client is no longer trusted.
	if err := configureTrustedProxies("10.0.0.0/24"); err != nil {
		t.Fatal(err)
	}
	inside := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil)
	inside.RemoteAddr = "10.0.0.5:1"
	inside.Header.Set("X-Forwarded-For", "6.6.6.6, 192.168.7.7")
	if got := cleanClientIP(inside); got != "192.168.7.7" {
		t.Fatalf("private client behind the proxy = %s, want 192.168.7.7", got)
	}
	if err := configureTrustedProxies("not-a-cidr"); err == nil {
		t.Fatal("invalid CIDR accepted")
	}
	if err := configureTrustedProxies("none"); err != nil || trustForwardedHeaders(parseRemoteIP("127.0.0.1:1")) {
		t.Fatal("'none' must trust no forwarded headers")
	}
}

func TestLoginLockoutResponseIsGeneric(t *testing.T) {
	s := newDBServer(t)
	s.loginGuard = newLoginGuard(2, 15*time.Minute, 15*time.Minute)
	hash, err := hashPassword("correct-horse-battery")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.users.upsertUser("admin", hash, RoleAdmin, false); err != nil {
		t.Fatal(err)
	}
	try := func(user string) (int, string) {
		rr := httptest.NewRecorder()
		s.handleLogin(rr, httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(`{"username":"`+user+`","password":"nope"}`)))
		return rr.Code, rr.Body.String()
	}
	for i := 0; i < 2; i++ {
		try("ghost-user-does-not-exist")
		try("admin")
	}
	c1, b1 := try("ghost-user-does-not-exist")
	c2, b2 := try("admin")
	if c1 != http.StatusTooManyRequests || c2 != http.StatusTooManyRequests {
		t.Fatalf("both should be throttled: %d %d", c1, c2)
	}
	strip := func(s string) string { return strings.Split(s, "about")[0] }
	if strip(b1) != strip(b2) || strings.Contains(b1, "account is") {
		t.Fatalf("lockout responses must not reveal whether an account exists:\n%s\n%s", b1, b2)
	}
}

func TestRateLimitCoversAlertMutationsAsOneBucket(t *testing.T) {
	s := &apiServer{rateLimiter: newFixedWindowRateLimiter(2, time.Minute)}
	h := s.withRateLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	codes := []int{}
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/alerts/ncc/"+[]string{"a", "b", "c"}[i]+"/acknowledge", nil)
		req.RemoteAddr = "198.51.100.9:1"
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		codes = append(codes, rec.Code)
	}
	if codes[2] != http.StatusTooManyRequests {
		t.Fatalf("alert mutations on different ids must share one limit: %v", codes)
	}
	get := httptest.NewRequest(http.MethodGet, "/api/v1/alerts/ncc", nil)
	get.RemoteAddr = "198.51.100.9:1"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, get)
	if rec.Code != http.StatusOK {
		t.Fatalf("reads (integration polling) must not be throttled by this limiter: %d", rec.Code)
	}
}
