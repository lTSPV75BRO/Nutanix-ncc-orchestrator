package main

import (
	"sync"
	"time"
)

// absPathCache remembers what absPath resolved a path to. Resolving a path walks
// it and follows symlinks (several stat calls, and on Windows a case-normalising
// directory lookup per component), and the dashboard's busiest endpoints do it for
// the same few configured directories many times per request: in a profile of
// /report/data about three quarters of the CPU went there. The answer only changes
// if someone swaps a directory for a symlink, so it is reused for a short time.
//
// absPath is not the security check (it falls back to the cleaned path); the
// confinement gate for caller-supplied paths is normalizeAndConfinePath, which is
// not cached.
type absPathCache struct {
	mu      sync.Mutex
	entries map[string]absPathEntry
	now     func() time.Time // replaced in tests
}

type absPathEntry struct {
	value string
	at    time.Time
}

const (
	absPathTTL      = 2 * time.Second
	absPathMaxItems = 512 // paths come from configuration; the bound only guards odd callers
)

func (c *absPathCache) get(p string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[p]
	if !ok {
		return "", false
	}
	if c.clock().Sub(e.at) >= absPathTTL {
		delete(c.entries, p)
		return "", false
	}
	return e.value, true
}

func (c *absPathCache) put(p, v string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil || len(c.entries) >= absPathMaxItems {
		c.entries = make(map[string]absPathEntry, 16)
	}
	c.entries[p] = absPathEntry{value: v, at: c.clock()}
}

func (c *absPathCache) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}
