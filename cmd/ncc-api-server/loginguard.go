package main

import (
	"strings"
	"sync"
	"time"
)

// loginGuard provides per-account brute-force protection: after a configurable
// number of failed login attempts within a rolling window, the account is
// temporarily locked regardless of the source IP. It complements the global
// per-IP rate limiter, which alone cannot stop an attacker who rotates IPs
// while grinding a single username.
//
// State is in-memory (resets on restart) and keyed by the lowercased username.
// Expired records are swept opportunistically so the map cannot grow without
// bound under a username-spraying attack. All methods are safe on a nil
// receiver so deployments that disable the feature (threshold 0) pay nothing.
type loginGuard struct {
	mu        sync.Mutex
	records   map[string]*loginAttempt
	threshold int           // failures within window before locking (>0)
	window    time.Duration // rolling window for accumulating failures
	lockout   time.Duration // how long an account stays locked
	lastSweep time.Time
}

type loginAttempt struct {
	failures    int
	firstFailAt time.Time
	lockedUntil time.Time
}

// newLoginGuard builds a guard. A threshold <= 0 disables locking entirely
// (every method becomes a no-op), letting operators opt out via flag.
func newLoginGuard(threshold int, window, lockout time.Duration) *loginGuard {
	if threshold <= 0 {
		return nil
	}
	if window <= 0 {
		window = 15 * time.Minute
	}
	if lockout <= 0 {
		lockout = 15 * time.Minute
	}
	return &loginGuard{
		records:   make(map[string]*loginAttempt),
		threshold: threshold,
		window:    window,
		lockout:   lockout,
	}
}

func loginKey(username string) string { return strings.ToLower(strings.TrimSpace(username)) }

// locked reports whether the account is currently locked and, if so, how much
// longer. A nil guard never locks.
func (g *loginGuard) locked(username string, now time.Time) (bool, time.Duration) {
	if g == nil {
		return false, 0
	}
	key := loginKey(username)
	if key == "" {
		return false, 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	rec, ok := g.records[key]
	if !ok {
		return false, 0
	}
	if now.Before(rec.lockedUntil) {
		return true, rec.lockedUntil.Sub(now)
	}
	return false, 0
}

// recordFailure registers a failed attempt and locks the account once the
// threshold is reached within the window. Returns true if this failure tripped
// a new lock.
func (g *loginGuard) recordFailure(username string, now time.Time) bool {
	if g == nil {
		return false
	}
	key := loginKey(username)
	if key == "" {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.sweepLocked(now)
	rec, ok := g.records[key]
	if !ok || now.Sub(rec.firstFailAt) > g.window {
		rec = &loginAttempt{firstFailAt: now}
		g.records[key] = rec
	}
	rec.failures++
	if rec.failures >= g.threshold {
		rec.lockedUntil = now.Add(g.lockout)
		rec.failures = 0
		rec.firstFailAt = now
		return true
	}
	return false
}

// reset clears all failure/lock state for an account (e.g. on a successful
// login or an admin password reset).
func (g *loginGuard) reset(username string) {
	if g == nil {
		return
	}
	key := loginKey(username)
	if key == "" {
		return
	}
	g.mu.Lock()
	delete(g.records, key)
	g.mu.Unlock()
}

// sweepLocked drops records that are neither currently locked nor inside an
// active failure window. Runs at most once per window to keep the hot path
// cheap; callers must hold g.mu.
func (g *loginGuard) sweepLocked(now time.Time) {
	if len(g.records) < 256 && now.Sub(g.lastSweep) < g.window {
		return
	}
	for k, rec := range g.records {
		if now.Before(rec.lockedUntil) {
			continue
		}
		if now.Sub(rec.firstFailAt) <= g.window && rec.failures > 0 {
			continue
		}
		delete(g.records, k)
	}
	g.lastSweep = now
}
