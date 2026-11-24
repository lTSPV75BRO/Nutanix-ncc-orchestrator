package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"goncc/internal/procutil"
	"goncc/internal/runtimecaps"
)

// v2StatusOptions wires the `status` subcommand's flags to runV2Status.
// Mirrors v2StartOptions where overlap exists so an operator who
// recalls v2-start's flag names can reuse them on status without
// re-reading the help.
type v2StatusOptions struct {
	InstallDir string
	APIListen  string
	UIListen   string
	JSON       bool
}

// v2StatusEntry is the per-service row populated by runV2Status. It's
// also the JSON-array element when --json is set, so its field tags
// are part of the public output contract: monitoring tooling can rely
// on them. Add new fields rather than renaming existing ones.
type v2StatusEntry struct {
	Service  string `json:"service"`
	PIDPath  string `json:"pid_path"`
	PID      int    `json:"pid,omitempty"`
	State    string `json:"state"` // "alive", "dead-stale-pid", "missing-pid", "unreadable-pid"
	Listen   string `json:"listen,omitempty"`
	Health   string `json:"health,omitempty"` // "ok", "unhealthy", "unreachable", "n/a"
	HealthMS int64  `json:"health_response_ms,omitempty"`
	LogPath  string `json:"log_path,omitempty"`
	Error    string `json:"error,omitempty"`
}

// runV2Status reports the live state of a v2 stack. Reads the PID
// files written by v2-start --detach, probes each PID with signal 0,
// and (for the api-server) hits /api/v1/health to verify the
// listener is actually accepting requests.
//
// Designed to be run from anywhere — does NOT require the orchestrator
// to be the same binary that started the stack, and does NOT modify
// any state. Always exits 0 unless --json failed to marshal; the
// caller can grep "state=alive" / "health=ok" if they want a
// strict liveness check.
func runV2Status(opts v2StatusOptions) error {
	installDir := strings.TrimSpace(opts.InstallDir)
	if installDir == "" {
		installDir = defaultV2InstallDir()
	}
	runDir := filepath.Join(installDir, "run")
	logDir := filepath.Join(installDir, "logs")

	apiListen := strings.TrimSpace(opts.APIListen)
	if apiListen == "" {
		apiListen = ":8081"
	}
	uiListen := strings.TrimSpace(opts.UIListen)
	if uiListen == "" {
		uiListen = ":8080"
	}

	tokenPath := filepath.Join(installDir, ".ncc-api-token")

	type svc struct {
		name     string
		pidFile  string
		listen   string
		logFile  string
		probe    bool
		identity []string
	}
	services := []svc{
		// Native foreground supervisor (v2-supervise); owns both children.
		{"supervisor", filepath.Join(runDir, "v2-supervisor.pid"), "", filepath.Join(logDir, "v2-supervisor.log"), false, []string{"ncc-orchestrator", "v2-supervise"}},
		// Legacy per-service sh supervisors (v2-start --detach --self-heal).
		{"api-supervisor", filepath.Join(runDir, "v2-api-supervisor.pid"), "", filepath.Join(logDir, "v2-api-supervisor.log"), false, []string{"v2-api-supervisor", "ncc-api-server"}},
		{"ncc-api-server", filepath.Join(runDir, "v2-api.pid"), apiListen, filepath.Join(logDir, "v2-api.log"), true, []string{"ncc-api-server"}},
		{"ui-supervisor", filepath.Join(runDir, "v2-ui-supervisor.pid"), "", filepath.Join(logDir, "v2-ui-supervisor.log"), false, []string{"v2-ui-supervisor", "ncc-ui-server"}},
		{"ncc-ui-server", filepath.Join(runDir, "v2-ui.pid"), uiListen, filepath.Join(logDir, "v2-ui.log"), false, []string{"ncc-ui-server"}},
	}

	out := make([]v2StatusEntry, 0, len(services))
	httpClient := &http.Client{Timeout: 3 * time.Second}
	apiToken := ""
	if b, err := os.ReadFile(tokenPath); err == nil {
		apiToken = strings.TrimSpace(string(b))
	}

	for _, s := range services {
		entry := v2StatusEntry{
			Service: s.name,
			PIDPath: s.pidFile,
			Listen:  s.listen,
			LogPath: s.logFile,
			Health:  "n/a",
		}
		pid, err := readPIDFromFile(s.pidFile)
		switch {
		case errors.Is(err, os.ErrNotExist):
			entry.State = "missing-pid"
		case err != nil:
			entry.State = "unreadable-pid"
			entry.Error = err.Error()
		default:
			entry.PID = pid
			if processIsExpected(pid, s.identity...) {
				entry.State = "alive"
			} else {
				entry.State = "dead-stale-pid"
				entry.Error = fmt.Sprintf("pid %d is not an NCC %s process", pid, s.name)
			}
		}

		if s.probe && entry.State == "alive" {
			url := localHTTPURLFromListen(s.listen, "8081") + "/api/v1/health"
			req, _ := http.NewRequest(http.MethodGet, url, nil)
			if apiToken != "" {
				req.Header.Set("X-Api-Token", apiToken)
			}
			start := time.Now()
			resp, err := httpClient.Do(req)
			entry.HealthMS = time.Since(start).Milliseconds()
			if err != nil {
				entry.Health = "unreachable"
				entry.Error = err.Error()
			} else {
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					entry.Health = "ok"
				} else {
					entry.Health = fmt.Sprintf("http-%d", resp.StatusCode)
				}
			}
		}
		out = append(out, entry)
	}

	if opts.JSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(map[string]any{
			"install_dir": installDir,
			"services":    out,
		})
	}

	fmt.Printf("ncc-orchestrator v2 stack status\n")
	fmt.Printf("--------------------------------\n")
	fmt.Printf("install-dir: %s\n\n", installDir)
	fmt.Printf("%-18s %-7s %-15s %-22s %-12s %s\n",
		"SERVICE", "PID", "STATE", "LISTEN", "HEALTH", "LOG")
	for _, e := range out {
		pidStr := "-"
		if e.PID > 0 {
			pidStr = strconv.Itoa(e.PID)
		}
		listen := e.Listen
		if listen == "" {
			listen = "-"
		}
		health := e.Health
		if e.HealthMS > 0 && (health == "ok" || strings.HasPrefix(health, "http-")) {
			health = fmt.Sprintf("%s (%dms)", health, e.HealthMS)
		}
		fmt.Printf("%-18s %-7s %-15s %-22s %-12s %s\n",
			e.Service, pidStr, e.State, listen, health, e.LogPath)
		if e.Error != "" {
			fmt.Printf("%-18s   error: %s\n", "", e.Error)
		}
	}
	return nil
}

// processIsAlive returns true when a process with the given pid exists
// and signal 0 reports "yes" (not "no such process" / EPERM-on-others).
// On Windows, Process.Signal supports only Kill, so a signal-0 probe there
// reported every live process as dead (restore could overwrite a running
// stack, a second supervisor could start); procutil queries the process
// object instead.
func processIsAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	if runtime.GOOS == "windows" {
		return procutil.Exists(pid)
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	if err := p.Signal(syscall.Signal(0)); err != nil {
		return false
	}
	return true
}

func waitForPIDExit(pid int, timeout time.Duration) bool {
	if pid <= 0 {
		return true
	}
	// Windows used to sleep the whole timeout and report "still running", so
	// every v2-stop there waited stopTimeout and then force-killed.
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !processIsAlive(pid) {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func runV2Stop(opts v2StopOptions) error {
	if err := runtimecaps.Detect().RejectHostOperation("local stack stop/restart"); err != nil {
		return err
	}
	installDir := strings.TrimSpace(opts.InstallDir)
	if installDir == "" {
		installDir = defaultV2InstallDir()
	}
	stopTimeout := opts.StopTimeout
	if stopTimeout <= 0 {
		stopTimeout = 5 * time.Second
	}
	runDir := filepath.Join(installDir, "run")
	apiPIDPath := strings.TrimSpace(opts.APIPIDFile)
	if apiPIDPath == "" {
		apiPIDPath = filepath.Join(runDir, "v2-api.pid")
	}
	uiPIDPath := strings.TrimSpace(opts.UIPIDFile)
	if uiPIDPath == "" {
		uiPIDPath = filepath.Join(runDir, "v2-ui.pid")
	}
	targets := []struct {
		name     string
		pidPath  string
		identity []string
	}{
		// Stop the native foreground supervisor first: on SIGTERM it gracefully
		// stops its own children and clears their pid files, so the api/ui
		// targets below become no-ops (or clean up anything it missed).
		{name: "supervisor", pidPath: filepath.Join(runDir, "v2-supervisor.pid"), identity: []string{"ncc-orchestrator", "v2-supervise"}},
		{name: "api-supervisor", pidPath: filepath.Join(runDir, "v2-api-supervisor.pid"), identity: []string{"v2-api-supervisor", "ncc-api-server"}},
		{name: "ui-supervisor", pidPath: filepath.Join(runDir, "v2-ui-supervisor.pid"), identity: []string{"v2-ui-supervisor", "ncc-ui-server"}},
		{name: "api", pidPath: apiPIDPath, identity: []string{"ncc-api-server"}},
		{name: "ui", pidPath: uiPIDPath, identity: []string{"ncc-ui-server"}},
	}

	foundAny := false
	stoppedAny := false
	cleanedStaleAny := false
	for _, t := range targets {
		pid, err := readPIDFromFile(t.pidPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			fmt.Fprintf(os.Stderr, "skip %s: %v\n", t.name, err)
			continue
		}
		foundAny = true
		if processIsAlive(pid) {
			known, matches := processIdentityMatches(pid, t.identity...)
			if known && !matches {
				_ = os.Remove(t.pidPath)
				cleanedStaleAny = true
				fmt.Fprintf(os.Stderr, "removed stale %s pid file (pid=%d belongs to an unrelated process)\n", t.name, pid)
				continue
			}
		}
		if err := signalPIDStop(pid, opts.Force); err != nil {
			if errors.Is(err, os.ErrProcessDone) {
				_ = os.Remove(t.pidPath)
				cleanedStaleAny = true
				fmt.Fprintf(os.Stderr, "removed stale %s pid file (pid=%d already exited)\n", t.name, pid)
				continue
			}
			fmt.Fprintf(os.Stderr, "failed to stop %s (pid=%d): %v\n", t.name, pid, err)
			continue
		}
		if !opts.Force && !waitForPIDExit(pid, stopTimeout) {
			if err := signalPIDStop(pid, true); err != nil {
				fmt.Fprintf(os.Stderr, "failed to force-kill %s (pid=%d): %v\n", t.name, pid, err)
				continue
			}
		}
		stoppedAny = true
		_ = os.Remove(t.pidPath)
		fmt.Fprintf(os.Stderr, "stopped %s (pid=%d)\n", t.name, pid)
	}
	if !foundAny {
		return fmt.Errorf("no detached pid files found under %s", runDir)
	}
	if !stoppedAny && !cleanedStaleAny {
		return errors.New("found detached pid files but failed to stop services")
	}
	if !stoppedAny && cleanedStaleAny {
		fmt.Fprintln(os.Stderr, "detached services were already stopped; stale pid files cleaned.")
	}
	return nil
}
