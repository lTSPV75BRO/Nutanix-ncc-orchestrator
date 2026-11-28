package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// The "a new version is available" banner. The release lookup (`update
// --check`, which asks GitHub) runs at most once a day per server: the result
// is kept in memory and in updateNoticeFile, so a restart does not ask again.
// A check that failed is retried after an hour. A manual check on Updates &
// backups refreshes the same result.

const (
	updateNoticeFile     = ".ncc-update-check.json"
	updateNoticeMaxAge   = 24 * time.Hour
	updateNoticeRetryAge = time.Hour
)

// updateNoticeResult is one release lookup, as cached.
type updateNoticeResult struct {
	CheckedAt time.Time `json:"checked_at"`
	// Current is the version that did the lookup: after an upgrade the result
	// is stale.
	Current   string `json:"current_version"`
	Latest    string `json:"latest_version,omitempty"`
	Available bool   `json:"update_available"`
	Error     string `json:"error,omitempty"`
}

type updateNoticeState struct {
	mu         sync.Mutex
	loaded     bool
	last       *updateNoticeResult
	refreshing bool
}

// updateNoticeCheck runs the release lookup. Tests replace it.
var updateNoticeCheck = func(s *apiServer) updateNoticeResult {
	data := map[string]interface{}{}
	out, err := s.runOrchestrator([]string{"update", "--check", "--json"}, 40*time.Second)
	if !parseUpdateCheckJSON(out, data) {
		parseUpdateCheck(out, data)
	}
	res := updateNoticeFromCheck(data)
	if err != nil {
		res.Error = "could not check for updates: " + firstNonEmptyLine(out, err.Error())
	}
	return res
}

// updateNoticeFromCheck turns the fields of an `update --check` result into a
// cached result.
func updateNoticeFromCheck(data map[string]interface{}) updateNoticeResult {
	res := updateNoticeResult{CheckedAt: time.Now().UTC(), Current: Version}
	res.Latest, _ = data["latest_version"].(string)
	res.Available, _ = data["update_available"].(bool)
	if e, _ := data["check_error"].(string); e != "" {
		res.Error = e
	}
	return res
}

func (s *apiServer) updateNoticePath() string {
	return filepath.Join(s.repoRoot, updateNoticeFile)
}

// loadUpdateNoticeLocked reads the saved result once. Caller holds mu.
func (s *apiServer) loadUpdateNoticeLocked() {
	if s.updateNotice.loaded {
		return
	}
	s.updateNotice.loaded = true
	body, err := os.ReadFile(s.updateNoticePath())
	if err != nil {
		return
	}
	var res updateNoticeResult
	if json.Unmarshal(body, &res) == nil && !res.CheckedAt.IsZero() {
		s.updateNotice.last = &res
	}
}

// recordUpdateNotice keeps a lookup's result in memory and on disk.
func (s *apiServer) recordUpdateNotice(res updateNoticeResult) {
	s.updateNotice.mu.Lock()
	s.updateNotice.loaded = true
	s.updateNotice.last = &res
	s.updateNotice.mu.Unlock()
	if body, err := json.Marshal(res); err == nil {
		_ = writeFileAtomic(s.updateNoticePath(), body, 0o600)
	}
	if res.Error == "" {
		s.setUpdateCheck(res.Available, res.Latest)
	}
}

// updateNoticeStale reports whether res is missing, from another version, or
// older than a day (an hour for a failed lookup).
func updateNoticeStale(res *updateNoticeResult, now time.Time) bool {
	if res == nil || res.Current != Version {
		return true
	}
	age := updateNoticeMaxAge
	if res.Error != "" {
		age = updateNoticeRetryAge
	}
	return now.Sub(res.CheckedAt) >= age
}

// updateBusy reports an in-app update or upgrade in progress.
func (s *apiServer) updateBusy() bool {
	updateJob.mu.Lock()
	busy := updateJob.inProgress
	updateJob.mu.Unlock()
	if up := s.readUpgradeJob(); up != nil && upgradeJobActive(up) {
		busy = true
	}
	return busy
}

// updateNoticeSnapshot returns the cached result and, when it is stale, starts
// one lookup in the background (never more than one at a time). The caller
// gets the cached answer right away; the next poll sees the new one.
func (s *apiServer) updateNoticeSnapshot(now time.Time, canCheck bool) *updateNoticeResult {
	s.updateNotice.mu.Lock()
	s.loadUpdateNoticeLocked()
	last := s.updateNotice.last
	start := canCheck && !s.updateNotice.refreshing && updateNoticeStale(last, now)
	if start {
		s.updateNotice.refreshing = true
	}
	s.updateNotice.mu.Unlock()
	if start {
		go func() {
			defer func() {
				s.updateNotice.mu.Lock()
				s.updateNotice.refreshing = false
				s.updateNotice.mu.Unlock()
			}()
			s.recordUpdateNotice(updateNoticeCheck(s))
		}()
	}
	if last == nil || last.Current != Version {
		return nil
	}
	cp := *last
	return &cp
}

// handleUpdateNotice (GET /api/v1/settings/update/notice, admin) answers the
// update banner from the daily cached release lookup. It never waits for
// GitHub.
func (s *apiServer) handleUpdateNotice(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	data := map[string]interface{}{"current_version": Version, "update_available": false}
	if !s.loadFeatureFlags().UpdateChecks {
		data["disabled"] = true
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
		return
	}
	supported := true
	if base := s.orchestratorBaseCommand(); len(base) == 0 || base[0] == "go" {
		supported = false
	}
	res := s.updateNoticeSnapshot(time.Now().UTC(), supported && !s.updateBusy())
	if res != nil {
		data["checked_at"] = res.CheckedAt.Format(time.RFC3339)
		if res.Error == "" && res.Available && strings.TrimSpace(res.Latest) != "" {
			data["update_available"] = true
			data["latest_version"] = strings.TrimPrefix(strings.TrimSpace(res.Latest), "v")
		}
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
}
