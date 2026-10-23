package main

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// loginGuard provides brute-force and credential-stuffing protection for
// interactive logins. It tracks failed attempts on three keys:
//
//   - account+source ("p|user|ip"): the classic lockout. After `threshold`
//     failures within `window`, that account is locked *for that source*.
//     Keying on the pair means an attacker cannot lock a real user out from
//     every location just by typing their username wrong a few times
//     (trivial lockout denial of service), yet a single source grinding one
//     account is stopped quickly.
//   - account ("a|user"): a higher account-wide threshold (default 4x) that
//     catches distributed guessing against one account from many sources.
//   - source ("s|ip"): a per-source threshold across all usernames (default
//     4x), which stops password spraying / credential stuffing from one
//     client regardless of how many usernames it tries.
//
// Each repeated lockout of the same key doubles the lock duration (progressive
// delay), capped at maxLockout. Expired records decay away after the window.
//
// State is in memory by default. When statePath is set (the default in
// Kubernetes, where several API replicas serve logins behind one Service) the
// counters live in a JSON file on the shared volume, updated under a
// cross-process file lock, so the limits hold across replicas instead of being
// multiplied by the replica count.
//
// All methods are safe on a nil receiver so deployments that disable the
// feature (threshold 0) pay nothing.
type loginGuard struct {
	mu               sync.Mutex
	records          map[string]*loginAttempt
	threshold        int           // failures per account+source before locking (>0)
	accountThreshold int           // failures per account from any source
	sourceThreshold  int           // failures per source across all accounts
	window           time.Duration // rolling window for accumulating failures
	lockout          time.Duration // base lock duration
	maxLockout       time.Duration // cap for progressive lock durations
	lastSweep        time.Time
	statePath        string // optional shared state file (multi-replica)
}

type loginAttempt struct {
	Failures    int       `json:"failures"`
	FirstFailAt time.Time `json:"first_fail_at"`
	LockedUntil time.Time `json:"locked_until,omitempty"`
	LockCount   int       `json:"lock_count,omitempty"`
	LastLockAt  time.Time `json:"last_lock_at,omitempty"`
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
		records:          make(map[string]*loginAttempt),
		threshold:        threshold,
		accountThreshold: threshold * 4,
		sourceThreshold:  threshold * 4,
		window:           window,
		lockout:          lockout,
		maxLockout:       lockout * 8,
	}
}

func loginKey(username string) string { return strings.ToLower(strings.TrimSpace(username)) }

func (g *loginGuard) keys(username, source string) (account, pair, src string) {
	u := loginKey(username)
	s := strings.TrimSpace(source)
	if u != "" {
		account = "a|" + u
		pair = "p|" + u + "|" + s
	}
	if s != "" {
		src = "s|" + s
	}
	return account, pair, src
}

// withState runs fn with the guard's records loaded (and, when shared, the
// cross-process lock held), persisting afterwards when fn reports a change.
func (g *loginGuard) withState(fn func() bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.statePath == "" {
		fn()
		return
	}
	release, err := lockStateFile(g.statePath+".lock", 2*time.Second)
	if err != nil {
		// Fail safe: still enforce with this replica's in-memory view.
		log.Printf("login guard: shared state unavailable (%v); enforcing per-replica", err)
		fn()
		return
	}
	defer release()
	if b, err := os.ReadFile(g.statePath); err == nil && len(b) > 0 {
		var recs map[string]*loginAttempt
		if json.Unmarshal(b, &recs) == nil && recs != nil {
			g.records = recs
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Printf("login guard: read shared state: %v", err)
	}
	if fn() {
		if b, err := json.Marshal(g.records); err == nil {
			if err := writeFileAtomic(g.statePath, b, 0o600); err != nil {
				log.Printf("login guard: write shared state: %v", err)
			}
		}
	}
}

// check reports whether a login for username from source is currently
// blocked, and for how long. source may be empty (unknown client).
func (g *loginGuard) check(username, source string, now time.Time) (bool, time.Duration) {
	if g == nil {
		return false, 0
	}
	account, pair, src := g.keys(username, source)
	var locked bool
	var retry time.Duration
	g.withState(func() bool {
		for _, k := range []string{pair, account, src} {
			if k == "" {
				continue
			}
			if rec, ok := g.records[k]; ok && now.Before(rec.LockedUntil) {
				locked = true
				if d := rec.LockedUntil.Sub(now); d > retry {
					retry = d
				}
			}
		}
		return false
	})
	return locked, retry
}

// recordFailureFrom registers a failed attempt for username from source and
// returns true if this failure tripped a new lock on any key.
func (g *loginGuard) recordFailureFrom(username, source string, now time.Time) bool {
	if g == nil {
		return false
	}
	account, pair, src := g.keys(username, source)
	tripped := false
	g.withState(func() bool {
		g.sweepLocked(now)
		bump := func(key string, threshold int) {
			if key == "" || threshold <= 0 {
				return
			}
			rec, ok := g.records[key]
			if !ok {
				rec = &loginAttempt{FirstFailAt: now}
				g.records[key] = rec
			}
			if now.Sub(rec.FirstFailAt) > g.window {
				rec.Failures = 0
				rec.FirstFailAt = now
			}
			// Progressive lockout memory decays after a quiet period.
			if !rec.LastLockAt.IsZero() && now.Sub(rec.LastLockAt) > g.window+g.maxLockout {
				rec.LockCount = 0
			}
			rec.Failures++
			if rec.Failures >= threshold {
				d := g.lockout
				for i := 0; i < rec.LockCount && d < g.maxLockout; i++ {
					d *= 2
				}
				if d > g.maxLockout {
					d = g.maxLockout
				}
				rec.LockedUntil = now.Add(d)
				rec.LockCount++
				rec.LastLockAt = now
				rec.Failures = 0
				rec.FirstFailAt = now
				tripped = true
			}
		}
		bump(pair, g.threshold)
		bump(account, g.accountThreshold)
		bump(src, g.sourceThreshold)
		return true
	})
	return tripped
}

// locked is the account-only view kept for existing callers (unknown source).
func (g *loginGuard) locked(username string, now time.Time) (bool, time.Duration) {
	return g.check(username, "", now)
}

// recordFailure is the account-only variant kept for existing callers.
func (g *loginGuard) recordFailure(username string, now time.Time) bool {
	return g.recordFailureFrom(username, "", now)
}

// reset clears all failure/lock state for an account across every source
// (e.g. on a successful login or an admin password reset). Per-source
// counters are intentionally kept: one successful login must not let a
// credential-stuffing client clear its own failures.
func (g *loginGuard) reset(username string) {
	if g == nil {
		return
	}
	u := loginKey(username)
	if u == "" {
		return
	}
	g.withState(func() bool {
		changed := false
		for k := range g.records {
			if k == "a|"+u || strings.HasPrefix(k, "p|"+u+"|") {
				delete(g.records, k)
				changed = true
			}
		}
		return changed
	})
}

// sweepLocked drops records that are neither currently locked nor inside an
// active failure window, nor remembered for progressive lockout. Callers must
// hold g.mu.
func (g *loginGuard) sweepLocked(now time.Time) {
	if len(g.records) < 256 && now.Sub(g.lastSweep) < g.window {
		return
	}
	for k, rec := range g.records {
		if now.Before(rec.LockedUntil) {
			continue
		}
		if now.Sub(rec.FirstFailAt) <= g.window && rec.Failures > 0 {
			continue
		}
		if rec.LockCount > 0 && now.Sub(rec.LastLockAt) <= g.window+g.maxLockout {
			continue
		}
		delete(g.records, k)
	}
	g.lastSweep = now
}

// configureLoginGuardSharedState points the guard at a shared state file so
// limits are enforced across API replicas.
func (g *loginGuard) configureSharedState(path string) {
	if g == nil || strings.TrimSpace(path) == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		log.Printf("login guard: cannot use shared state %s: %v", filepath.Base(path), err)
		return
	}
	g.statePath = path
}
