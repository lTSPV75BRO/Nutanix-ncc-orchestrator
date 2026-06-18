package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// This file lets the api-server surface orchestrator runs it did NOT spawn —
// chiefly systemd-timer / cron *scheduled* runs — in its Active Runs view.
// Those runs are launched directly by the OS scheduler, bypassing the run
// manager, so without this they were invisible while running (and only
// appeared in the Runs table afterward, once run-history archived them).
//
// The orchestrator writes a tiny per-pid heartbeat file
// (.ncc-run-active-<pid>.json) into the output dir for the lifetime of every
// run. We read those, drop any whose pid is dead (cleaning up the stale file)
// or that belong to a run the manager already tracks, and present the rest as
// synthetic "running" entries tagged with their source.

type runHeartbeat struct {
	PID       int      `json:"pid"`
	StartedAt string   `json:"started_at"`
	Clusters  []string `json:"clusters"`
	Source    string   `json:"source"`
}

// processAlive reports whether pid refers to a live process. Uses signal 0
// (no-op probe) on POSIX; a permission error still means the process exists.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = p.Signal(syscall.Signal(0))
	return err == nil || err == os.ErrPermission || strings.Contains(strings.ToLower(safeErr(err)), "permission")
}

func safeErr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// externalActiveRuns returns synthetic active-run entries for orchestrator runs
// not managed by this api-server (managedPIDs are the pids it owns). Entry shape
// matches activeRunsSnapshot so the UI renders them uniformly.
func (s *apiServer) externalActiveRuns(managedPIDs map[int]bool) []map[string]interface{} {
	dir := s.absPath(s.outputDir)
	matches, _ := filepath.Glob(filepath.Join(dir, ".ncc-run-active-*.json"))
	var out []map[string]interface{}
	for _, m := range matches {
		b, err := os.ReadFile(m)
		if err != nil {
			continue
		}
		var hb runHeartbeat
		if err := json.Unmarshal(b, &hb); err != nil || hb.PID <= 0 {
			continue
		}
		if !processAlive(hb.PID) {
			_ = os.Remove(m) // stale heartbeat from a crashed/finished run
			continue
		}
		if managedPIDs[hb.PID] {
			continue // already shown as a manager-owned run
		}
		started, _ := time.Parse(time.RFC3339, hb.StartedAt)
		elapsed := 0
		if !started.IsZero() {
			elapsed = int(time.Since(started).Round(time.Second).Seconds())
		}
		source := strings.TrimSpace(hb.Source)
		if source == "" {
			source = "external"
		}
		stamp := hb.StartedAt
		if !started.IsZero() {
			stamp = started.UTC().Format("20060102T150405Z")
		}
		entry := map[string]interface{}{
			"id":           source + "-" + stamp,
			"status":       "running",
			"source":       source,
			"started_at":   hb.StartedAt,
			"elapsed_sec":  elapsed,
			"clusters":     hb.Clusters,
			"all_clusters": len(hb.Clusters) == 0,
			"live_output":  s.schedulerLogTail(),
			"external":     true,
		}
		out = append(out, entry)
	}
	return out
}

// schedulerLogTail returns the last few KB of the scheduler log so the Active
// Runs view can show recent output for an in-progress scheduled run (which has
// no live stdout pipe into the api-server).
func (s *apiServer) schedulerLogTail() string {
	logPath := ""
	if st, err := s.loadSchedule(); err == nil {
		logPath = strings.TrimSpace(st.LogPath)
	}
	if logPath == "" {
		logPath = filepath.Join("logs", "ncc-scheduler.log")
	}
	abs := s.absPath(logPath)
	f, err := os.Open(abs)
	if err != nil {
		return "Scheduled run in progress (no scheduler log yet at " + abs + ")."
	}
	defer f.Close()
	const tailBytes = 8000
	info, err := f.Stat()
	if err != nil {
		return "Scheduled run in progress."
	}
	start := int64(0)
	if info.Size() > tailBytes {
		start = info.Size() - tailBytes
	}
	buf := make([]byte, info.Size()-start)
	if _, err := f.ReadAt(buf, start); err != nil && len(buf) == 0 {
		return "Scheduled run in progress."
	}
	text := string(buf)
	// Drop a partial first line when we seeked into the middle of the file.
	if start > 0 {
		if i := strings.IndexByte(text, '\n'); i >= 0 {
			text = text[i+1:]
		}
	}
	return strings.TrimSpace(text)
}
