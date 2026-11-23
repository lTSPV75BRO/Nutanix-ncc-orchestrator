package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAbsPathCacheMatchesUncachedAndExpires(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "outputfiles"), 0o755); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	s := &apiServer{repoRoot: root}
	s.absCache.now = func() time.Time { return now }

	want := s.resolveAbsPath("outputfiles")
	if got := s.absPath("outputfiles"); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	// Within the TTL a second call is answered from the cache: prove it by
	// planting a different value under the key.
	s.absCache.put(s.repoRoot+"\x00outputfiles", "planted")
	if got := s.absPath("outputfiles"); got != "planted" {
		t.Fatalf("expected the cached value, got %q", got)
	}
	now = now.Add(absPathTTL + time.Millisecond)
	if got := s.absPath("outputfiles"); got != want {
		t.Fatalf("after the TTL the path must be resolved again: got %q want %q", got, want)
	}
}

func TestAbsPathCacheIsBounded(t *testing.T) {
	var c absPathCache
	for i := 0; i < absPathMaxItems*3; i++ {
		c.put(filepath.Join("p", string(rune('a'+i%26)), time.Duration(i).String()), "v")
	}
	if len(c.entries) > absPathMaxItems {
		t.Fatalf("cache grew to %d entries", len(c.entries))
	}
}

// A path outside the root still falls back exactly as before (absPath is not the
// confinement gate), and the confinement check itself is not cached.
func TestConfinementIsStillEnforcedEachTime(t *testing.T) {
	root := t.TempDir()
	s := &apiServer{repoRoot: root}
	outside := filepath.Join(filepath.Dir(root), "elsewhere")
	for i := 0; i < 2; i++ {
		if _, err := s.normalizeAndConfinePath(outside); err == nil {
			t.Fatalf("call %d: a path outside the repo root must be refused", i)
		}
	}
}
