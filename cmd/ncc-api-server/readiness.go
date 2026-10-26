package main

import (
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"goncc/internal/v2layout"
)

// Readiness (GET /api/v1/health/ready).
//
// Kubernetes used /api/v1/health for both liveness and readiness, but that
// endpoint always answers "ok": a pod whose user store could not be read,
// whose runner binary was missing, or whose output volume was read-only still
// received traffic and failed every login or run. This endpoint checks those
// dependencies. It is unauthenticated (probes carry no credentials), so it
// reports only check names and pass/fail — never paths or error text.

type readinessCheck struct {
	Name string `json:"name"`
	OK   bool   `json:"ok"`
}

type readinessResult struct {
	Ready  bool             `json:"ready"`
	Checks []readinessCheck `json:"checks"`
	At     string           `json:"checked_at"`
}

// readinessCacheTTL bounds how often the checks touch the store and disk;
// probes typically arrive every few seconds from each kubelet.
const readinessCacheTTL = 5 * time.Second

type readinessCache struct {
	mu     sync.Mutex
	result readinessResult
	at     time.Time
}

func (s *apiServer) readiness() readinessResult {
	s.readyCache.mu.Lock()
	defer s.readyCache.mu.Unlock()
	if !s.readyCache.at.IsZero() && time.Since(s.readyCache.at) < readinessCacheTTL {
		return s.readyCache.result
	}
	checks := []readinessCheck{
		{Name: "user_store", OK: s.userStoreReadable()},
		{Name: "runner_binary", OK: v2layout.IsExecutable(s.absPath(s.orchestratorBin))},
		{Name: "output_dir_writable", OK: dirWritable(s.absPath(s.outputDir))},
	}
	res := readinessResult{Ready: true, Checks: checks, At: time.Now().UTC().Format(time.RFC3339)}
	for _, c := range checks {
		if !c.OK {
			res.Ready = false
		}
	}
	s.readyCache.result = res
	s.readyCache.at = time.Now()
	return res
}

// userStoreReadable reports whether the persisted user store can be loaded.
// Installs without local accounts (pure token mode) have nothing to read.
func (s *apiServer) userStoreReadable() bool {
	if s.users == nil || s.users.backend == nil {
		return true
	}
	_, _, err := backendLoad(s.users.backend)
	return err == nil
}

// dirWritable creates and removes a probe file in dir (creating dir first,
// as the run manager would).
func dirWritable(dir string) bool {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false
	}
	f, err := os.CreateTemp(dir, ".ncc-ready-*")
	if err != nil {
		return false
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(filepath.Clean(name))
	return true
}

func (s *apiServer) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	res := s.readiness()
	status := http.StatusOK
	if !res.Ready {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, envelope{Success: res.Ready, Data: res})
}
