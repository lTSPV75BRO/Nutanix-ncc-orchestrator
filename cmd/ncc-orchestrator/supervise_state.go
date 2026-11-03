package main

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// The supervisor publishes what it is doing to run/v2-supervisor-state.json so
// doctor, the api-server and Health & repair can show it without parsing logs:
// each child's state, uptime, restarts, the last exit with the tail of the
// child's log, and the probe results. It also takes restart requests: a file
// run/v2-supervisor-restart-<child>.request makes it restart that child
// gracefully, outside the restart budget.

const (
	supervisorStateFile  = "v2-supervisor-state.json"
	supervisorEventLimit = 50
	exitLogTailLines     = 20
)

func supervisorStatePath(installDir string) string {
	return filepath.Join(installDir, "run", supervisorStateFile)
}

func supervisorRestartRequestPath(installDir, child string) string {
	return filepath.Join(installDir, "run", "v2-supervisor-restart-"+child+".request")
}

// superviseChildState is one child as published in the state file.
type superviseChildState struct {
	Name             string   `json:"name"`
	State            string   `json:"state"` // starting | running | backoff | cooldown | stopped
	PID              int      `json:"pid,omitempty"`
	StartedAt        string   `json:"started_at,omitempty"`
	RestartsTotal    int      `json:"restarts_total"`
	RestartsInWindow int      `json:"restarts_in_window"`
	NextStartAt      string   `json:"next_start_at,omitempty"`
	ProbeFailures    int      `json:"probe_failures"`
	LastProbeAt      string   `json:"last_probe_at,omitempty"`
	LastProbeMS      int64    `json:"last_probe_ms,omitempty"`
	LastProbeOK      *bool    `json:"last_probe_ok,omitempty"`
	LastExitAt       string   `json:"last_exit_at,omitempty"`
	LastExitReason   string   `json:"last_exit_reason,omitempty"` // crashed | unhealthy | requested | port-in-use | start-failed
	LastExitDetail   string   `json:"last_exit_detail,omitempty"`
	LastExitLogTail  []string `json:"last_exit_log_tail,omitempty"`
	Probed           bool     `json:"probed"`
}

type superviseEvent struct {
	At     string `json:"at"`
	Child  string `json:"child"`
	Event  string `json:"event"`
	Detail string `json:"detail,omitempty"`
}

type superviseState struct {
	PID       int                             `json:"pid"`
	Version   string                          `json:"version,omitempty"`
	Features  []string                        `json:"features,omitempty"`
	StartedAt string                          `json:"started_at"`
	UpdatedAt string                          `json:"updated_at"`
	Policy    map[string]interface{}          `json:"policy"`
	Children  map[string]*superviseChildState `json:"children"`
	Events    []superviseEvent                `json:"events"`
}

// superviseStateWriter serialises updates from the child loops. A nil writer
// (unit tests) ignores every call.
type superviseStateWriter struct {
	mu    sync.Mutex
	path  string
	state superviseState
}

func newSuperviseStateWriter(path string, cfg *superviseConfig) *superviseStateWriter {
	w := &superviseStateWriter{path: path}
	now := time.Now().UTC().Format(time.RFC3339)
	w.state = superviseState{
		PID:     os.Getpid(),
		Version: stripGoBuildGitSuffix(Version),
		// What the API may ask of this supervisor (restart requests, upgrades).
		Features:  []string{"restart", "upgrade"},
		StartedAt: now,
		Policy: map[string]interface{}{
			"max_restarts":        cfg.maxRestarts,
			"window":              cfg.window.String(),
			"probe_interval":      cfg.probeInterval.String(),
			"unhealthy_threshold": cfg.unhealthyThreshold,
			"start_grace":         cfg.startGrace.String(),
			"stable_after":        cfg.stableAfter.String(),
			"backoff_cap":         cfg.backoffCap.String(),
		},
		Children: map[string]*superviseChildState{},
	}
	for _, c := range cfg.children {
		w.state.Children[c.name] = &superviseChildState{Name: c.name, State: "starting", Probed: len(c.healthArgs) > 0}
	}
	w.flushLocked()
	return w
}

// update applies fn to a child's state and writes the file.
func (w *superviseStateWriter) update(child string, fn func(*superviseChildState)) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	cs := w.state.Children[child]
	if cs == nil {
		cs = &superviseChildState{Name: child}
		w.state.Children[child] = cs
	}
	fn(cs)
	w.flushLocked()
}

// child returns a copy of one child's state.
func (w *superviseStateWriter) child(name string) (superviseChildState, bool) {
	if w == nil {
		return superviseChildState{}, false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	cs := w.state.Children[name]
	if cs == nil {
		return superviseChildState{}, false
	}
	out := *cs
	out.LastExitLogTail = append([]string(nil), cs.LastExitLogTail...)
	return out, true
}

func (w *superviseStateWriter) event(child, event, detail string) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.state.Events = append(w.state.Events, superviseEvent{At: time.Now().UTC().Format(time.RFC3339), Child: child, Event: event, Detail: detail})
	if n := len(w.state.Events); n > supervisorEventLimit {
		w.state.Events = w.state.Events[n-supervisorEventLimit:]
	}
	w.flushLocked()
}

func (w *superviseStateWriter) flushLocked() {
	w.state.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	b, err := json.MarshalIndent(w.state, "", "  ")
	if err != nil {
		return
	}
	tmp := w.path + ".tmp"
	if os.WriteFile(tmp, b, 0o644) == nil {
		_ = os.Rename(tmp, w.path)
	}
}

// remove deletes the state file on a clean shutdown.
func (w *superviseStateWriter) remove() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = os.Remove(w.path)
}

// readSupervisorState loads the published state, if any.
func readSupervisorState(installDir string) (*superviseState, error) {
	b, err := os.ReadFile(supervisorStatePath(installDir))
	if err != nil {
		return nil, err
	}
	var st superviseState
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

// tailLines returns the last n non-empty lines of a file, reading at most the
// final 64 KiB.
func tailLines(path string, n int) []string {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	if st, err := f.Stat(); err == nil && st.Size() > 64<<10 {
		_, _ = f.Seek(st.Size()-64<<10, io.SeekStart)
	}
	var lines []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		if line := strings.TrimRight(sc.Text(), "\r"); strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return lines
}

func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }
