package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"goncc/internal/hostinfo"
	"goncc/internal/runtimetune"
)

// Status → Supervisor and Platform: what the native supervisor
// publishes in run/v2-supervisor-state.json, restart requests to it, and the
// host and runtime facts the platform checks are based on. Admin-only.

func (s *apiServer) supervisorStatePath() string {
	return filepath.Join(s.maintenanceInstallDir(), "run", "v2-supervisor-state.json")
}

func (s *apiServer) supervisorRestartRequestPath(child string) string {
	return filepath.Join(s.maintenanceInstallDir(), "run", "v2-supervisor-restart-"+child+".request")
}

// readSupervisorState returns the published state, or nil when there is none.
func (s *apiServer) readSupervisorState() map[string]interface{} {
	b, err := os.ReadFile(s.supervisorStatePath())
	if err != nil {
		return nil
	}
	var st map[string]interface{}
	if json.Unmarshal(b, &st) != nil {
		return nil
	}
	return st
}

// supervisorSupports reports whether a running supervisor publishes state
// and advertises feature (restart, upgrade).
func (s *apiServer) supervisorSupports(feature string) bool {
	if s.capabilities.Kubernetes {
		return false
	}
	st := s.readSupervisorState()
	pid, _ := st["pid"].(float64)
	if st == nil || int(pid) <= 0 || !processAlive(int(pid)) {
		return false
	}
	features, _ := st["features"].([]interface{})
	for _, f := range features {
		if f == feature {
			return true
		}
	}
	return false
}

// upgradeRequest and upgradeJob mirror the supervisor's upgrade files
// (cmd/ncc-orchestrator/supervise_upgrade.go).
type upgradeRequest struct {
	ID            string `json:"id"`
	TargetVersion string `json:"target_version,omitempty"`
	BackupPath    string `json:"backup_path,omitempty"`
	RequestedBy   string `json:"requested_by,omitempty"`
	RequestedAt   string `json:"requested_at,omitempty"`
}

type upgradeJob struct {
	ID                       string                   `json:"id"`
	Phase                    string                   `json:"phase"`
	Message                  string                   `json:"message"`
	Error                    string                   `json:"error,omitempty"`
	TargetVersion            string                   `json:"target_version,omitempty"`
	FromVersion              string                   `json:"from_version,omitempty"`
	ToVersion                string                   `json:"to_version,omitempty"`
	BackupPath               string                   `json:"backup_path,omitempty"`
	RequestedBy              string                   `json:"requested_by,omitempty"`
	RequestedAt              string                   `json:"requested_at,omitempty"`
	StartedAt                string                   `json:"started_at"`
	UpdatedAt                string                   `json:"updated_at"`
	FinishedAt               string                   `json:"finished_at,omitempty"`
	Resumed                  bool                     `json:"resumed,omitempty"`
	Steps                    []map[string]interface{} `json:"steps"`
	Doctor                   map[string]interface{}   `json:"doctor,omitempty"`
	SupervisorRestartPending bool                     `json:"supervisor_restart_pending,omitempty"`
	// Stale: the job says it is running but no supervisor is there to run it.
	Stale bool `json:"stale,omitempty"`
}

func upgradeJobActive(j *upgradeJob) bool {
	switch j.Phase {
	case "preflight", "snapshot", "installing", "restarting", "verifying", "rolling_back":
		return !j.Stale
	}
	return false
}

func (s *apiServer) upgradeStatePath() string {
	return filepath.Join(s.maintenanceInstallDir(), "run", "v2-upgrade-state.json")
}

// readUpgradeJob returns the supervisor's current or last upgrade, or nil.
func (s *apiServer) readUpgradeJob() *upgradeJob {
	b, err := os.ReadFile(s.upgradeStatePath())
	if err != nil {
		return nil
	}
	var j upgradeJob
	if json.Unmarshal(b, &j) != nil {
		return nil
	}
	if upgradeJobActive(&j) && !s.supervisorSupports("upgrade") {
		// It resumes when the supervisor starts again.
		j.Stale = true
	}
	return &j
}

// requestSupervisedUpgrade writes the request the supervisor picks up.
func (s *apiServer) requestSupervisedUpgrade(req upgradeRequest) error {
	b, err := json.Marshal(req)
	if err != nil {
		return err
	}
	path := filepath.Join(s.maintenanceInstallDir(), "run", "v2-supervisor-upgrade.request")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// GET /api/v1/health/supervisor
func (s *apiServer) handleSupervisor(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	data := map[string]interface{}{"mode": "manual", "running": false}
	if s.capabilities.Kubernetes {
		data["mode"] = "kubernetes"
		data["message"] = "Kubernetes restarts the API and UI pods; there is no NCC supervisor in this deployment."
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
		return
	}
	pid, alive := s.supervisorPID()
	st := s.readSupervisorState()
	if up := s.readUpgradeJob(); up != nil {
		data["upgrade"] = up
	}
	switch {
	case st != nil:
		statePID, _ := st["pid"].(float64)
		running := int(statePID) > 0 && processAlive(int(statePID))
		data["mode"] = "supervised"
		data["running"] = running
		data["pid"] = int(statePID)
		data["state"] = st
		if !running {
			data["message"] = fmt.Sprintf("The supervisor (pid %d) is no longer running; the state shown is from before it stopped.", int(statePID))
		}
	case alive:
		data["mode"] = "supervised"
		data["running"] = true
		data["pid"] = pid
		data["message"] = "The supervisor is running but publishes no state: it was started by an older release. Restart the ncc-orchestrator service to see restarts and exit reasons here."
	default:
		data["message"] = "No supervisor is running: the API and UI were started by hand (v2-start without the service) and nothing restarts them if they stop. Install the boot service with ncc-orchestrator v2-install-service --now."
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
}

// POST /api/v1/health/supervisor/restart {"child":"api"|"ui"} asks the
// supervisor to restart one server gracefully. It also ends a cooldown.
func (s *apiServer) handleSupervisorRestart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	var body struct {
		Child string `json:"child"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid JSON body"})
		return
	}
	child := strings.TrimSpace(body.Child)
	if child != "api" && child != "ui" {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: `child must be "api" or "ui"`})
		return
	}
	st := s.readSupervisorState()
	statePID, _ := st["pid"].(float64)
	if st == nil || int(statePID) <= 0 || !processAlive(int(statePID)) {
		writeJSON(w, http.StatusConflict, envelope{Success: false, Error: "no running supervisor publishes state here; restart the service instead"})
		return
	}
	if children, _ := st["children"].(map[string]interface{}); children[child] == nil {
		writeJSON(w, http.StatusConflict, envelope{Success: false, Error: "the supervisor does not manage " + child})
		return
	}
	if child == "api" && s.hasInFlightRuns() {
		writeJSON(w, http.StatusConflict, envelope{Success: false, Error: "a run is in progress; restart the API when it finishes"})
		return
	}
	err := os.WriteFile(s.supervisorRestartRequestPath(child), []byte(time.Now().UTC().Format(time.RFC3339)+"\n"), 0o644)
	s.audit(r, "health.supervisor.restart", err == nil, map[string]interface{}{"child": child})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not write the restart request: " + err.Error()})
		return
	}
	msg := "The supervisor restarts the UI server within a few seconds."
	if child == "api" {
		msg = "The supervisor restarts the API server within a few seconds; this page reconnects when it is back."
	}
	writeJSON(w, http.StatusAccepted, envelope{Success: true, Message: msg, Data: map[string]interface{}{"child": child}})
}

// openFileCount counts this process's open descriptors where the OS shows them.
func openFileCount() (int, bool) {
	if runtime.GOOS != "linux" {
		return 0, false
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, false
	}
	return len(entries), true
}

// serviceManager names what keeps the stack running across reboots.
func (s *apiServer) serviceManager() string {
	switch {
	case s.capabilities.Kubernetes:
		return "kubernetes"
	case runtime.GOOS == "linux":
		if _, err := os.Stat("/etc/systemd/system/ncc-orchestrator.service"); err == nil {
			return "systemd"
		}
	case runtime.GOOS == "darwin":
		if _, err := os.Stat("/Library/LaunchDaemons/io.github.ncc-orchestrator.ncc-orchestrator.plist"); err == nil {
			return "launchd"
		}
	case runtime.GOOS == "windows":
		if _, alive := s.supervisorPID(); alive {
			return "task-scheduler"
		}
	}
	if _, alive := s.supervisorPID(); alive {
		return "supervisor"
	}
	return "none"
}

// GET /api/v1/health/platform
func (s *apiServer) handlePlatform(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	soft, hard, limOK := hostinfo.FileLimit()
	proc := map[string]interface{}{
		"pid":              os.Getpid(),
		"uptime_seconds":   int64(time.Since(s.startedAt).Seconds()),
		"goroutines":       runtime.NumGoroutine(),
		"heap_alloc_bytes": ms.HeapAlloc,
		"sys_bytes":        ms.Sys,
		"gc_cycles":        ms.NumGC,
		"memory_limit":     debug.SetMemoryLimit(-1),
		"tuning":           runtimetune.Apply(),
	}
	if n, ok := openFileCount(); ok {
		proc["open_files"] = n
	}
	if limOK {
		proc["file_limit_soft"], proc["file_limit_hard"] = soft, hard
	}
	host := map[string]interface{}{
		"os":              runtime.GOOS,
		"arch":            hostinfo.CPUArch(),
		"cpus":            runtime.NumCPU(),
		"gomaxprocs":      runtime.GOMAXPROCS(0),
		"go_version":      runtime.Version(),
		"memory":          hostinfo.SystemMemory(),
		"cgroup":          runtimetune.ReadLimits(),
		"kubernetes":      s.capabilities.Kubernetes,
		"service_manager": s.serviceManager(),
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{"host": host, "api_process": proc}})
}

// apiRuntimeDiagnostic checks this API process: goroutine leaks, a heap near
// its limit, and open files near the descriptor limit.
func (s *apiServer) apiRuntimeDiagnostic() diagResult {
	d := diagResult{ID: "api-runtime", Title: "API server process", Category: "performance", Status: diagOK}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	g := runtime.NumGoroutine()
	parts := []string{
		fmt.Sprintf("up %s", time.Since(s.startedAt).Truncate(time.Minute)),
		fmt.Sprintf("%d goroutines", g),
		fmt.Sprintf("heap %d MiB", ms.HeapAlloc>>20),
	}
	var hints []string
	if g > 10000 {
		d.Status = diagWarn
		hints = append(hints, "an unusually high goroutine count usually means requests or streams are not finishing; restart the API from Supervisor and send a support bundle if it climbs again")
	}
	if lim := debug.SetMemoryLimit(-1); lim > 0 && lim < 1<<62 {
		parts = append(parts, fmt.Sprintf("memory limit %d MiB", lim>>20))
		if float64(ms.HeapAlloc) > 0.85*float64(lim) {
			d.Status = diagWarn
			hints = append(hints, "the heap is close to the memory limit; raise the container limit or GOMEMLIMIT")
		}
	}
	if n, ok := openFileCount(); ok {
		parts = append(parts, fmt.Sprintf("%d open files", n))
		if _, hard, ok := hostinfo.FileLimit(); ok && hard > 0 && float64(n) > 0.8*float64(hard) {
			d.Status = diagWarn
			hints = append(hints, fmt.Sprintf("open files are close to the limit (%d); raise LimitNOFILE", hard))
		}
	}
	d.Message = strings.Join(parts, ", ")
	if len(hints) > 0 {
		d.Hint = strings.ToUpper(hints[0][:1]) + strings.Join(hints, "; ")[1:] + "."
	}
	return d
}
