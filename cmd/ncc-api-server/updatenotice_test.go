package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// noticeServer returns a server whose orchestrator binary exists, so the
// update lookup counts as supported, and a stubbed lookup that counts calls.
func noticeServer(t *testing.T, res updateNoticeResult) (*apiServer, *int32) {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "ncc-orchestrator")
	if err := os.WriteFile(bin, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	var calls int32
	prev := updateNoticeCheck
	updateNoticeCheck = func(*apiServer) updateNoticeResult {
		atomic.AddInt32(&calls, 1)
		r := res
		r.CheckedAt = time.Now().UTC()
		r.Current = Version
		return r
	}
	t.Cleanup(func() { updateNoticeCheck = prev })
	return &apiServer{repoRoot: dir, orchestratorBin: bin}, &calls
}

func getNotice(t *testing.T, s *apiServer) map[string]interface{} {
	t.Helper()
	rec := httptest.NewRecorder()
	s.handleUpdateNotice(rec, httptest.NewRequest(http.MethodGet, "/api/v1/settings/update/notice", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	var env struct {
		Data map[string]interface{} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	return env.Data
}

func waitRefreshDone(t *testing.T, s *apiServer) {
	t.Helper()
	for i := 0; i < 200; i++ {
		s.updateNotice.mu.Lock()
		busy := s.updateNotice.refreshing
		s.updateNotice.mu.Unlock()
		if !busy {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("background update lookup did not finish")
}

func TestUpdateNoticeChecksOnceADayAndPersists(t *testing.T) {
	s, calls := noticeServer(t, updateNoticeResult{Latest: "v9.9.9", Available: true})

	// Nothing cached yet: answers at once, looks up in the background.
	if got := getNotice(t, s); got["update_available"] != false {
		t.Fatalf("first answer = %v", got)
	}
	waitRefreshDone(t, s)
	got := getNotice(t, s)
	if got["update_available"] != true || got["latest_version"] != "9.9.9" {
		t.Fatalf("after lookup = %v", got)
	}
	if n := atomic.LoadInt32(calls); n != 1 {
		t.Fatalf("lookups = %d, want 1", n)
	}

	// A restart reads the saved result and does not look up again.
	restarted := &apiServer{repoRoot: s.repoRoot, orchestratorBin: s.orchestratorBin}
	if got := getNotice(t, restarted); got["latest_version"] != "9.9.9" {
		t.Fatalf("after restart = %v", got)
	}
	waitRefreshDone(t, restarted)
	if n := atomic.LoadInt32(calls); n != 1 {
		t.Fatalf("lookups after restart = %d, want 1", n)
	}
}

func TestUpdateNoticeRefreshesAfterADay(t *testing.T) {
	s, calls := noticeServer(t, updateNoticeResult{Latest: "9.9.9", Available: true})
	old := updateNoticeResult{CheckedAt: time.Now().Add(-25 * time.Hour), Current: Version, Latest: "1.0.0"}
	s.recordUpdateNotice(old)
	getNotice(t, s)
	waitRefreshDone(t, s)
	if n := atomic.LoadInt32(calls); n != 1 {
		t.Fatalf("lookups = %d, want 1", n)
	}
	if got := getNotice(t, s); got["latest_version"] != "9.9.9" {
		t.Fatalf("after refresh = %v", got)
	}
}

func TestUpdateNoticeIgnoresResultFromAnotherVersion(t *testing.T) {
	s, _ := noticeServer(t, updateNoticeResult{})
	s.recordUpdateNotice(updateNoticeResult{CheckedAt: time.Now(), Current: "0.0.1-old", Latest: "9.9.9", Available: true})
	if got := getNotice(t, s); got["update_available"] != false {
		t.Fatalf("stale version result shown: %v", got)
	}
	waitRefreshDone(t, s)
}

func TestUpdateNoticeOffWhenUpdateChecksDisabled(t *testing.T) {
	s, calls := noticeServer(t, updateNoticeResult{Latest: "9.9.9", Available: true})
	flags := defaultFeatureFlags()
	flags.UpdateChecks = false
	if err := s.saveFeatureFlags(flags); err != nil {
		t.Fatal(err)
	}
	if got := getNotice(t, s); got["disabled"] != true || got["update_available"] != false {
		t.Fatalf("disabled answer = %v", got)
	}
	if n := atomic.LoadInt32(calls); n != 0 {
		t.Fatalf("lookups = %d, want 0", n)
	}
}

func TestUpdateNoticeStaleRules(t *testing.T) {
	now := time.Now()
	ok := &updateNoticeResult{CheckedAt: now.Add(-2 * time.Hour), Current: Version}
	if updateNoticeStale(ok, now) {
		t.Fatal("a 2 h old result is not stale")
	}
	failed := &updateNoticeResult{CheckedAt: now.Add(-2 * time.Hour), Current: Version, Error: "offline"}
	if !updateNoticeStale(failed, now) {
		t.Fatal("a failed lookup is retried after an hour")
	}
	if !updateNoticeStale(nil, now) {
		t.Fatal("nothing cached is stale")
	}
}
