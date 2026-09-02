package main

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	yaml "go.yaml.in/yaml/v3"
	"goncc/internal/selfsigned"
)

// This file implements the orchestrator's active self-heal subsystem: a small
// registry of checks, each of which detects a class of latent operational fault
// and (when --fix is requested) applies a *safe* remediation. It backs the
// `ncc-orchestrator doctor` command and the api-server's diagnostics endpoint.
//
// Design rules for a check's Run:
//   - Read-only by default. Mutate the system only when hc.Fix is true.
//   - A remediation must be safe to apply unattended (idempotent, reversible or
//     non-destructive). Anything risky should warn with a Hint instead.
//   - Never panic; turn errors into a fail/warn result.

type healStatus string

const (
	healOK   healStatus = "ok"
	healWarn healStatus = "warn"
	healFail healStatus = "fail"
)

// healResult is the outcome of a single check.
type healResult struct {
	ID         string     `json:"id"`
	Title      string     `json:"title"`
	Category   string     `json:"category"`
	Status     healStatus `json:"status"`
	Message    string     `json:"message"`
	Hint       string     `json:"hint,omitempty"`
	Fixed      bool       `json:"fixed,omitempty"`
	FixMsg     string     `json:"fix_message,omitempty"`
	Disruptive bool       `json:"disruptive,omitempty"`
}

// healContext carries shared, lazily-resolved state into each check.
type healContext struct {
	InstallDir string
	ConfigPath string
	Cfg        Config
	CfgLoaded  bool
	Fix        bool
}

type healCheck struct {
	ID       string
	Title    string
	Category string
	// Disruptive checks may restart services or otherwise perturb in-flight work.
	Disruptive bool
	Run        func(hc *healContext) healResult
}

// healReport is the aggregate, JSON-serializable result of a self-heal run.
type healReport struct {
	GeneratedAt string         `json:"generated_at"`
	InstallDir  string         `json:"install_dir"`
	ConfigPath  string         `json:"config_path"`
	FixApplied  bool           `json:"fix_applied"`
	Summary     map[string]int `json:"summary"`
	Results     []healResult   `json:"results"`
}

type healRunOptions struct {
	Fix          bool
	OnlyChecks   map[string]bool
	NoDisruptive bool
}

// Worst returns the most severe status across all results, defaulting to ok.
func (r healReport) Worst() healStatus {
	worst := healOK
	for _, res := range r.Results {
		if res.Status == healFail {
			return healFail
		}
		if res.Status == healWarn {
			worst = healWarn
		}
	}
	return worst
}

// selfHealChecks is the check registry. Order here is the report order.
func selfHealChecks() []healCheck {
	return []healCheck{
		{ID: "config-schema", Title: "Canonical configuration schema", Category: "config", Run: checkConfigSchema},
		{ID: "config-valid", Title: "Configuration validity", Category: "config", Run: checkConfigValid},
		{ID: "config-output-routing", Title: "Output-dir path routing", Category: "config", Run: checkConfigOutputRouting},
		{ID: "output-dirs-writable", Title: "Output directories writable", Category: "storage", Run: checkOutputDirsWritable},
		{ID: "disk-space", Title: "Free disk space", Category: "storage", Run: checkDiskSpace},
		{ID: "secrets-perms", Title: "Secret file permissions", Category: "encryption", Run: checkSecretsPerms},
		{ID: "backup-staleness", Title: "Backup freshness", Category: "backups", Run: checkBackupStaleness},
		{ID: "backup-restorable", Title: "Newest backup restorable", Category: "backups", Run: checkBackupRestorable},
		{ID: "recent-run-health", Title: "Most recent run health", Category: "runs", Run: checkRecentRunHealth},
		{ID: "run-output-freshness", Title: "Run output freshness", Category: "runs", Run: checkRunOutputFreshness},
		{ID: "tls-cert-expiry", Title: "TLS certificate validity", Category: "tls", Run: checkTLSCertExpiry},
		{ID: "stale-pids", Title: "Stale PID files", Category: "process", Run: checkStalePIDs},
		{ID: "runtime-mode-drift", Title: "Supervisor/runtime mode alignment", Category: "process", Disruptive: true, Run: checkRuntimeModeDrift},
		{ID: "scheduler-integrity", Title: "Scheduler integrity", Category: "process", Run: checkSchedulerIntegrity},
		{ID: "selinux-exec-context", Title: "SELinux executable context", Category: "process", Run: checkSELinuxExecContext},
		{ID: "log-sizes", Title: "Log file sizes", Category: "storage", Run: checkLogSizes},
	}
}

func checkConfigSchema(hc *healContext) healResult {
	res := healResult{ID: "config-schema", Title: "Canonical configuration schema", Category: "config"}
	raw, err := os.ReadFile(hc.ConfigPath)
	if err != nil {
		res.Status = healWarn
		res.Message = "config unavailable; schema check skipped"
		return res
	}
	var doc map[string]interface{}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		res.Status = healFail
		res.Message = "YAML parse failed: " + err.Error()
		return res
	}
	version, exists := doc["schema-version"]
	if !exists {
		res.Status = healWarn
		res.Message = "configuration has no schema-version and uses legacy compatibility mode"
		if hc.Fix {
			updated := append([]byte("schema-version: 1\n"), raw...)
			if err := os.WriteFile(hc.ConfigPath, updated, 0o600); err == nil {
				res.Status = healOK
				res.Fixed = true
				res.FixMsg = "added schema-version: 1; legacy flat keys remain supported"
				res.Message = "configuration is now versioned"
			} else {
				res.Hint = "Could not write schema-version; update the file manually."
			}
		} else {
			res.Hint = "Run with --fix to add schema-version: 1, or migrate the file to the nested canonical structure."
		}
		return res
	}
	if fmt.Sprint(version) != "1" {
		res.Status = healFail
		res.Message = fmt.Sprintf("unsupported schema-version %v (supported: 1)", version)
		res.Hint = "Migrate the configuration to schema-version: 1."
		return res
	}
	res.Status = healOK
	res.Message = "schema-version 1 detected; canonical and legacy keys are supported"
	return res
}

func parseCertFile(path string) (*x509.Certificate, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("no PEM certificate data")
	}
	return x509.ParseCertificate(block.Bytes)
}

// checkTLSCertExpiry validates the UI TLS certificate's validity window and,
// for the stack-managed self-signed cert only, auto-renews it under --fix
// (regenerating with the same SANs). Operator-supplied certs are never
// rewritten — they warn so an admin can renew and re-upload.
func checkTLSCertExpiry(hc *healContext) healResult {
	res := healResult{ID: "tls-cert-expiry", Title: "TLS certificate validity", Category: "tls"}
	certPath, keyPath := "", ""
	if st, ok := loadV2StartState(hc.InstallDir); ok && strings.TrimSpace(st.UITLSCertFile) != "" {
		certPath, keyPath = st.UITLSCertFile, st.UITLSKeyFile
	}
	if certPath == "" {
		certPath = filepath.Join(hc.InstallDir, "tls", "ui-selfsigned.crt")
		keyPath = filepath.Join(hc.InstallDir, "tls", "ui-selfsigned.key")
	}
	if !fileExists(certPath) {
		res.Status = healOK
		res.Message = "no UI TLS certificate present (HTTP, or stack not started)"
		return res
	}
	managed := filepath.Base(certPath) == "ui-selfsigned.crt"
	cert, err := parseCertFile(certPath)
	if err != nil {
		res.Status = healWarn
		res.Message = fmt.Sprintf("could not parse %s: %v", filepath.Base(certPath), err)
		return res
	}
	left := time.Until(cert.NotAfter)
	res.Message = fmt.Sprintf("%s valid until %s (%s left)", filepath.Base(certPath), cert.NotAfter.UTC().Format(time.RFC3339), humanDuration(left))
	if left > 30*24*time.Hour {
		res.Status = healOK
		return res
	}
	if managed && hc.Fix {
		hosts := append([]string{}, cert.DNSNames...)
		for _, ip := range cert.IPAddresses {
			hosts = append(hosts, ip.String())
		}
		certPEM, keyPEM, gerr := selfsigned.Generate(hosts, 0)
		if gerr == nil &&
			os.WriteFile(certPath, certPEM, 0o600) == nil &&
			os.WriteFile(keyPath, keyPEM, 0o600) == nil {
			res.Status = healOK
			res.Fixed = true
			res.FixMsg = "regenerated the self-signed UI certificate (same SANs)"
			res.Message = "self-signed certificate renewed"
			res.Hint = "Restart the stack (v2-restart) to load the renewed certificate."
			return res
		}
	}
	if left <= 0 {
		res.Status = healFail
	} else {
		res.Status = healWarn
	}
	if managed {
		res.Hint = "Self-signed UI cert is expiring; re-run with --fix to regenerate it, then restart the stack."
	} else {
		res.Hint = "Operator-supplied cert is expiring; renew it and update TLS settings (cannot be auto-renewed)."
	}
	return res
}

// checkStalePIDs detects pid files whose process is dead (a crashed stack) and,
// under --fix, removes them so a restart isn't blocked by a phantom "running".
func checkStalePIDs(hc *healContext) healResult {
	res := healResult{ID: "stale-pids", Title: "Stale PID files", Category: "process"}
	runDir := filepath.Join(hc.InstallDir, "run")
	pidFiles, _ := filepath.Glob(filepath.Join(runDir, "*.pid"))
	supervisorPID, apiPID, uiPID := detectRuntimePIDs(hc.InstallDir)
	liveForFile := map[string]int{
		"v2-supervisor.pid": supervisorPID,
		"v2-api.pid":        apiPID,
		"v2-ui.pid":         uiPID,
	}
	var stale, malformed, cleaned, reconciled []string
	for _, pf := range pidFiles {
		pid, err := readPIDFromFile(pf)
		if err != nil {
			name := filepath.Base(pf)
			if hc.Fix {
				if os.Remove(pf) == nil {
					cleaned = append(cleaned, name+" (invalid contents)")
					continue
				}
			}
			malformed = append(malformed, fmt.Sprintf("%s (%v)", name, err))
			continue
		}
		known, matches := processIdentityMatchesForPIDFile(filepath.Base(pf), pid)
		if processIsAlive(pid) && (!known || matches) {
			continue
		}
		if hc.Fix {
			if live := liveForFile[filepath.Base(pf)]; live > 0 {
				if reconcilePIDFile(pf, live) {
					reconciled = append(reconciled, fmt.Sprintf("%s -> pid %d", filepath.Base(pf), live))
					continue
				}
			}
			if os.Remove(pf) == nil {
				cleaned = append(cleaned, filepath.Base(pf))
				continue
			}
		}
		reason := "dead"
		if processIsAlive(pid) && known && !matches {
			reason = "pid reused by an unrelated process"
		}
		stale = append(stale, fmt.Sprintf("%s (pid %d %s)", filepath.Base(pf), pid, reason))
	}
	switch {
	case len(stale) == 0 && len(malformed) == 0 && len(cleaned) == 0 && len(reconciled) == 0:
		res.Status = healOK
		res.Message = "no stale pid files"
	case len(stale) == 0 && len(malformed) == 0:
		res.Status = healOK
		res.Fixed = true
		parts := []string{}
		if len(cleaned) > 0 {
			parts = append(parts, "removed stale pid file(s): "+strings.Join(cleaned, ", "))
		}
		if len(reconciled) > 0 {
			parts = append(parts, "reconciled live pid file(s): "+strings.Join(reconciled, ", "))
		}
		res.FixMsg = strings.Join(parts, "; ")
		res.Message = "stale pid files cleaned/reconciled"
	default:
		res.Status = healWarn
		issues := append([]string{}, stale...)
		issues = append(issues, malformed...)
		res.Message = "stale/invalid pid file(s): " + strings.Join(issues, ", ")
		res.Hint = "Re-run with --fix to remove them so the stack can restart cleanly."
	}
	return res
}

func hasSystemdServiceUnit(name string) bool {
	if _, err := exec.LookPath("systemctl"); err != nil {
		return false
	}
	return exec.Command("systemctl", "cat", name).Run() == nil
}

func isSystemdServiceActive(name string) bool {
	if _, err := exec.LookPath("systemctl"); err != nil {
		return false
	}
	return exec.Command("systemctl", "is-active", "--quiet", name).Run() == nil
}

type runtimeDriftEval struct {
	Status     healStatus
	Message    string
	Hint       string
	CanAutoFix bool
}

func evaluateRuntimeDrift(servicePresent, serviceActive, supervisorAlive, apiAlive, uiAlive bool) runtimeDriftEval {
	switch {
	case !servicePresent:
		return runtimeDriftEval{
			Status:  healOK,
			Message: "service mode not installed (detached/shell-managed runtime)",
		}
	case serviceActive && supervisorAlive:
		return runtimeDriftEval{
			Status:  healOK,
			Message: "service mode active and supervisor process is healthy",
		}
	case serviceActive && !supervisorAlive && (apiAlive || uiAlive):
		return runtimeDriftEval{
			Status:     healWarn,
			Message:    "runtime drift: service is active but detached API/UI process(es) are running without a live supervisor",
			Hint:       "Run self-heal with --fix to restart ncc-orchestrator.service and re-align process ownership.",
			CanAutoFix: true,
		}
	case serviceActive && !supervisorAlive:
		return runtimeDriftEval{
			Status:     healFail,
			Message:    "service is active but no live supervisor process was found",
			Hint:       "Run self-heal with --fix (or `systemctl restart ncc-orchestrator.service`) to recover.",
			CanAutoFix: true,
		}
	case !serviceActive && (apiAlive || uiAlive):
		return runtimeDriftEval{
			Status:     healWarn,
			Message:    "detached API/UI process(es) are running while the systemd service is not active",
			Hint:       "Run self-heal with --fix to bring the stack back under ncc-orchestrator.service supervision.",
			CanAutoFix: true,
		}
	default:
		return runtimeDriftEval{
			Status:  healWarn,
			Message: "service mode is installed but not active",
			Hint:    "Start the stack with `systemctl start ncc-orchestrator.service`.",
		}
	}
}

func alivePIDFromFile(path string) (int, bool) {
	pid, err := readPIDFromFile(path)
	if err != nil || !processIsAlive(pid) {
		return 0, false
	}
	known, matches := processIdentityMatchesForPIDFile(filepath.Base(path), pid)
	if known && !matches {
		return 0, false
	}
	return pid, true
}

func processIdentityMatchesForPIDFile(name string, pid int) (bool, bool) {
	switch name {
	case "v2-supervisor.pid":
		return processIdentityMatches(pid, "ncc-orchestrator", "v2-supervise")
	case "v2-api.pid":
		return processIdentityMatches(pid, "ncc-api-server")
	case "v2-ui.pid":
		return processIdentityMatches(pid, "ncc-ui-server")
	default:
		return false, true
	}
}

func alivePIDByPattern(patterns ...string) (int, bool) {
	if _, err := exec.LookPath("pgrep"); err != nil {
		return 0, false
	}
	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		identity := "unknown"
		if fields := strings.Fields(pattern); len(fields) > 0 {
			identity = filepath.Base(fields[0])
		}
		out, err := exec.Command("pgrep", "-f", pattern).Output()
		if err != nil {
			continue
		}
		lines := strings.Split(strings.TrimSpace(string(out)), "\n")
		best := 0
		for _, line := range lines {
			pid, err := strconv.Atoi(strings.TrimSpace(line))
			if err != nil || pid <= 0 {
				continue
			}
			known, matches := processIdentityMatches(pid, identity)
			if processIsAlive(pid) && (!known || matches) && pid > best {
				best = pid
			}
		}
		if best > 0 {
			return best, true
		}
	}
	return 0, false
}

func detectRuntimePIDs(installDir string) (supervisorPID, apiPID, uiPID int) {
	runDir := filepath.Join(installDir, "run")
	if pid, ok := alivePIDFromFile(filepath.Join(runDir, "v2-supervisor.pid")); ok {
		supervisorPID = pid
	} else if pid, ok := alivePIDByPattern(
		filepath.Join(installDir, "bin", "ncc-orchestrator")+" v2-supervise",
		"ncc-orchestrator v2-supervise",
	); ok {
		supervisorPID = pid
	}
	if pid, ok := alivePIDFromFile(filepath.Join(runDir, "v2-api.pid")); ok {
		apiPID = pid
	} else if pid, ok := alivePIDByPattern(
		filepath.Join(installDir, "bin", "ncc-api-server"),
		"ncc-api-server",
	); ok {
		apiPID = pid
	}
	if pid, ok := alivePIDFromFile(filepath.Join(runDir, "v2-ui.pid")); ok {
		uiPID = pid
	} else if pid, ok := alivePIDByPattern(
		filepath.Join(installDir, "bin", "ncc-ui-server"),
		"ncc-ui-server",
	); ok {
		uiPID = pid
	}
	return supervisorPID, apiPID, uiPID
}

func reconcilePIDFile(path string, pid int) bool {
	if pid <= 0 {
		return false
	}
	cur, err := readPIDFromFile(path)
	if err == nil && cur == pid {
		return false
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false
	}
	return os.WriteFile(path, []byte(fmt.Sprintf("%d\n", pid)), 0o644) == nil
}

func checkRuntimeModeDrift(hc *healContext) healResult {
	res := healResult{ID: "runtime-mode-drift", Title: "Supervisor/runtime mode alignment", Category: "process"}
	supervisorPID, apiPID, uiPID := detectRuntimePIDs(hc.InstallDir)
	supervisorAlive := supervisorPID > 0
	apiAlive := apiPID > 0
	uiAlive := uiPID > 0
	serviceName := "ncc-orchestrator.service"
	servicePresent := hasSystemdServiceUnit(serviceName)
	serviceActive := isSystemdServiceActive(serviceName)

	eval := evaluateRuntimeDrift(servicePresent, serviceActive, supervisorAlive, apiAlive, uiAlive)
	res.Status = eval.Status
	res.Message = eval.Message
	res.Hint = eval.Hint

	// In service-managed mode, stale/missing pid files can survive a crash/recover
	// cycle even when the live processes are healthy. Reconcile them under --fix so
	// status/health checks converge without requiring a disruptive restart.
	if hc.Fix && servicePresent && serviceActive && supervisorAlive {
		runDir := filepath.Join(hc.InstallDir, "run")
		updated := []string{}
		if reconcilePIDFile(filepath.Join(runDir, "v2-supervisor.pid"), supervisorPID) {
			updated = append(updated, "v2-supervisor.pid")
		}
		if apiPID > 0 && reconcilePIDFile(filepath.Join(runDir, "v2-api.pid"), apiPID) {
			updated = append(updated, "v2-api.pid")
		}
		if uiPID > 0 && reconcilePIDFile(filepath.Join(runDir, "v2-ui.pid"), uiPID) {
			updated = append(updated, "v2-ui.pid")
		}
		if len(updated) > 0 {
			res.Fixed = true
			res.FixMsg = "reconciled runtime pid file(s): " + strings.Join(updated, ", ")
		}
	}

	if !hc.Fix || !eval.CanAutoFix || !servicePresent {
		return res
	}
	if out, err := exec.Command("systemctl", "restart", serviceName).CombinedOutput(); err == nil {
		res.Status = healOK
		res.Fixed = true
		res.FixMsg = "restarted ncc-orchestrator.service to recover supervisor-managed runtime"
		res.Message = "runtime ownership re-aligned under systemd supervisor"
		res.Hint = ""
		return res
	} else {
		msg := strings.TrimSpace(string(out))
		if msg == "" {
			msg = err.Error()
		}
		res.Hint = "auto-fix could not restart ncc-orchestrator.service: " + msg
		return res
	}
}

func checkSchedulerIntegrity(hc *healContext) healResult {
	res := healResult{ID: "scheduler-integrity", Title: "Scheduler integrity", Category: "process"}
	statePath := filepath.Join(hc.InstallDir, ".ncc-api-schedule.json")
	raw, err := os.ReadFile(statePath)
	if err != nil {
		res.Status = healOK
		res.Message = "no persisted scheduler state found"
		return res
	}
	var st map[string]interface{}
	if json.Unmarshal(raw, &st) != nil {
		res.Status = healWarn
		res.Message = "scheduler state is present but unreadable"
		res.Hint = "Re-save scheduler settings from Settings -> Schedule."
		return res
	}
	task := strings.TrimSpace(fmt.Sprintf("%v", st["task_name"]))
	if task == "" {
		task = "ncc-orchestrator"
	}
	typ := strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", st["type"])))
	logPath := strings.TrimSpace(fmt.Sprintf("%v", st["log_path"]))
	if logPath == "" {
		logPath = filepath.Join(hc.InstallDir, "logs", "ncc-scheduler.log")
	}
	if typ == "systemd" {
		runner := filepath.Join(filepath.Dir(logPath), "ncc-sched-"+task+".sh")
		timer := filepath.Join("/etc/systemd/system", "ncc-sched-"+task+".timer")
		service := filepath.Join("/etc/systemd/system", "ncc-sched-"+task+".service")
		missing := []string{}
		for _, p := range []string{runner, timer, service} {
			if !fileExists(p) {
				missing = append(missing, filepath.Base(p))
			}
		}
		if len(missing) > 0 {
			if hc.Fix {
				if fixed, msg := repairSchedulerArtifactsFromState(hc.InstallDir, st, typ, task, logPath); fixed {
					res.Status = healOK
					res.Fixed = true
					res.FixMsg = msg
					res.Message = "repaired missing scheduler artifacts from persisted state"
					return res
				}
			}
			res.Status = healWarn
			res.Message = "scheduler artifacts missing: " + strings.Join(missing, ", ")
			res.Hint = "Re-apply schedule (Settings -> Schedule -> Save + Apply) or run restore self-heal."
			return res
		}
		res.Status = healOK
		res.Message = "systemd scheduler state, units, and runner script are aligned"
		return res
	}
	if typ == "cron" {
		runner := filepath.Join(filepath.Dir(logPath), "ncc-scheduler.sh")
		if !fileExists(runner) {
			if hc.Fix {
				if fixed, msg := repairSchedulerArtifactsFromState(hc.InstallDir, st, typ, task, logPath); fixed {
					res.Status = healOK
					res.Fixed = true
					res.FixMsg = msg
					res.Message = "repaired missing scheduler artifacts from persisted state"
					return res
				}
			}
			res.Status = healWarn
			res.Message = "cron scheduler runner script is missing"
			res.Hint = "Re-apply schedule to regenerate the runner script."
			return res
		}
		res.Status = healOK
		res.Message = "cron scheduler state and runner script are aligned"
		return res
	}
	res.Status = healWarn
	res.Message = "unknown scheduler type in persisted state: " + typ
	res.Hint = "Re-save scheduler settings to normalize scheduler state."
	return res
}

func repairSchedulerArtifactsFromState(installDir string, st map[string]interface{}, typ, task, logPath string) (bool, string) {
	action := strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", st["action"])))
	if action != "" && action != "create" {
		return false, ""
	}
	every := strings.TrimSpace(fmt.Sprintf("%v", st["every"]))
	if every == "" {
		every = "4h"
	}
	withLock := strings.TrimSpace(fmt.Sprintf("%v", st["with_lock"]))
	if withLock == "" {
		withLock = "true"
	}
	configPath := strings.TrimSpace(fmt.Sprintf("%v", st["config"]))
	if configPath == "" {
		configPath = filepath.Join(installDir, "config.yaml")
	}
	if !filepath.IsAbs(configPath) {
		configPath = filepath.Join(installDir, configPath)
	}
	if logPath == "" {
		logPath = filepath.Join(installDir, "logs", "ncc-scheduler.log")
	}
	if !filepath.IsAbs(logPath) {
		logPath = filepath.Join(installDir, logPath)
	}

	orch := filepath.Join(installDir, "bin", "ncc-orchestrator")
	if !fileExists(orch) {
		orch = "ncc-orchestrator"
	}
	args := []string{
		"--config", configPath,
		"create-schedule",
		"--type", typ,
		"--every", every,
		"--log-path", logPath,
		"--task-name", task,
		"--with-lock", withLock,
		"--print-only=false",
	}
	out, err := exec.Command(orch, args...).CombinedOutput()
	if err != nil {
		_ = out
		return false, ""
	}
	return true, "re-applied scheduler via create-schedule using persisted state"
}

func checkSELinuxExecContext(hc *healContext) healResult {
	res := healResult{ID: "selinux-exec-context", Title: "SELinux executable context", Category: "process"}
	if runtime.GOOS != "linux" {
		res.Status = healOK
		res.Message = "not a Linux host"
		return res
	}
	if _, err := exec.LookPath("getenforce"); err != nil {
		res.Status = healOK
		res.Message = "SELinux tools not present"
		return res
	}
	out, err := exec.Command("getenforce").CombinedOutput()
	mode := strings.ToLower(strings.TrimSpace(string(out)))
	if err != nil || mode == "" || mode == "disabled" {
		res.Status = healOK
		res.Message = "SELinux disabled"
		return res
	}
	if _, err := exec.LookPath("ls"); err != nil {
		res.Status = healWarn
		res.Message = "cannot verify SELinux context (ls not found)"
		return res
	}
	bins := []string{
		filepath.Join(hc.InstallDir, "bin", "ncc-orchestrator"),
		filepath.Join(hc.InstallDir, "bin", "ncc-api-server"),
		filepath.Join(hc.InstallDir, "bin", "ncc-ui-server"),
	}
	bad := []string{}
	for _, b := range bins {
		if !fileExists(b) {
			continue
		}
		lo, lerr := exec.Command("ls", "-Z", b).CombinedOutput()
		if lerr != nil || !strings.Contains(string(lo), ":bin_t:") {
			bad = append(bad, filepath.Base(b))
		}
	}
	if len(bad) == 0 {
		res.Status = healOK
		res.Message = "SELinux executable contexts look healthy (bin_t)"
		return res
	}
	if hc.Fix {
		if _, cerr := exec.Command("chcon", append([]string{"-t", "bin_t"}, bins...)...).CombinedOutput(); cerr == nil {
			res.Status = healOK
			res.Fixed = true
			res.FixMsg = "re-labeled executable contexts for: " + strings.Join(bad, ", ")
			res.Message = "SELinux executable context repaired"
			return res
		}
	}
	res.Status = healWarn
	res.Message = "unexpected SELinux context on executable(s): " + strings.Join(bad, ", ")
	res.Hint = "Set context to bin_t (e.g. `chcon -t bin_t <binary>`), then restart the service."
	return res
}

// checkLogSizes flags oversized log files and, under --fix, rotates them
// (renaming to <name>.1) so an unbounded log can't fill the volume — the
// self-heal cooldown loop in particular can be chatty.
func checkLogSizes(hc *healContext) healResult {
	res := healResult{ID: "log-sizes", Title: "Log file sizes", Category: "storage"}
	logDir := filepath.Join(hc.InstallDir, "logs")
	logs, _ := filepath.Glob(filepath.Join(logDir, "*.log"))
	const cap = int64(50) << 20 // 50 MiB
	var big, rotated []string
	for _, lg := range logs {
		st, err := os.Stat(lg)
		if err != nil || st.Size() < cap {
			continue
		}
		if hc.Fix {
			rot := lg + ".1"
			_ = os.Remove(rot)
			if os.Rename(lg, rot) == nil {
				rotated = append(rotated, filepath.Base(lg))
				continue
			}
		}
		big = append(big, fmt.Sprintf("%s (%s)", filepath.Base(lg), humanBytes(uint64(st.Size()))))
	}
	switch {
	case len(big) == 0 && len(rotated) == 0:
		res.Status = healOK
		res.Message = "all log files under 50 MiB"
	case len(big) == 0:
		res.Status = healOK
		res.Fixed = true
		res.FixMsg = "rotated oversized log(s): " + strings.Join(rotated, ", ")
		res.Message = "oversized logs rotated"
	default:
		res.Status = healWarn
		res.Message = "oversized log file(s): " + strings.Join(big, ", ")
		res.Hint = "Re-run with --fix to rotate them (renames to <name>.1)."
	}
	return res
}

// latestRunSummary reads <output-dir-filtered>/run-summary.json, the
// machine-readable result the orchestrator writes after every run.
func latestRunSummary(hc *healContext) (RunSummaryJSON, string, bool) {
	if !hc.CfgLoaded || strings.TrimSpace(hc.Cfg.OutputDirFiltered) == "" {
		return RunSummaryJSON{}, "", false
	}
	path := filepath.Join(hc.Cfg.OutputDirFiltered, "run-summary.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return RunSummaryJSON{}, path, false
	}
	var sum RunSummaryJSON
	if json.Unmarshal(raw, &sum) != nil {
		return RunSummaryJSON{}, path, false
	}
	return sum, path, true
}

// runClassMitigation describes the recommended remediation for a failure class
// in operator-facing terms (the api-server applies these automatically on a
// bounded auto-retry; here we only advise).
func runClassMitigation(class string) string {
	switch class {
	case "auth":
		return "verify Prism credentials / secret:// source and account lock state (preflight-check)"
	case "rate_limit":
		return "lower --max-parallel and raise --retry-max-attempts (auto-retried once by the api-server)"
	case "timeout", "network":
		return "raise --request-timeout, lower --max-parallel, check routing/firewall (auto-retried once)"
	case "parser":
		return "inspect raw NCC logs under output-dir-logs for unexpected payloads"
	default:
		return "inspect the run log for the failing cluster"
	}
}

func checkRecentRunHealth(hc *healContext) healResult {
	res := healResult{ID: "recent-run-health", Title: "Most recent run health", Category: "runs"}
	sum, _, ok := latestRunSummary(hc)
	if !ok {
		res.Status = healWarn
		res.Message = "no run-summary.json found; no completed run to evaluate"
		res.Hint = "Trigger a run (or wait for the scheduled one) to populate run history."
		return res
	}
	if sum.ClustersFailed == 0 {
		res.Status = healOK
		res.Message = fmt.Sprintf("last run %s: %d cluster(s) OK, 0 failed", sum.Timestamp, sum.ClustersOK)
		return res
	}
	// Identify the dominant failure class to advise the right mitigation.
	dominant, max := "unknown", 0
	for class, n := range sum.FailureClasses {
		if n > max {
			dominant, max = class, n
		}
	}
	res.Message = fmt.Sprintf("last run %s: %d failed / %d OK; dominant cause: %s", sum.Timestamp, sum.ClustersFailed, sum.ClustersOK, dominant)
	res.Hint = runClassMitigation(dominant)
	if hc.Fix {
		res.Hint += "; this check reports run quality and does not apply an automatic fix"
	}
	if sum.ClustersOK == 0 {
		res.Status = healFail // total failure
	} else {
		res.Status = healWarn // partial failure
	}
	return res
}

func checkRunOutputFreshness(hc *healContext) healResult {
	res := healResult{ID: "run-output-freshness", Title: "Run output freshness", Category: "runs"}
	sum, _, ok := latestRunSummary(hc)
	if !ok {
		res.Status = healWarn
		res.Message = "no run-summary.json; cannot assess freshness"
		return res
	}
	ts, err := time.Parse(time.RFC3339, sum.Timestamp)
	if err != nil {
		res.Status = healWarn
		res.Message = "run-summary.json timestamp unparseable: " + sum.Timestamp
		return res
	}
	age := time.Since(ts)
	res.Message = fmt.Sprintf("last run completed %s ago (%s)", humanDuration(age), sum.Timestamp)
	// A scan that hasn't landed in over a day usually means a broken schedule —
	// e.g. cron writing output to the wrong dir (the classic relative-path bug)
	// or the scheduled job not firing at all.
	const staleAfter = 24 * time.Hour
	if age > staleAfter {
		res.Status = healWarn
		res.Hint = "Scheduled runs may not be landing here. Check the cron/Task schedule and that its output-dir-* matches the served stack (doctor --fix anchors relative paths)."
		return res
	}
	res.Status = healOK
	return res
}

// runSelfHeal executes every registered check and returns an aggregate report.
// When fix is true, checks may apply their safe remediations.
func shouldRunHealCheck(c healCheck, opts healRunOptions) bool {
	if len(opts.OnlyChecks) > 0 && !opts.OnlyChecks[c.ID] {
		return false
	}
	if opts.NoDisruptive && c.Disruptive {
		return false
	}
	return true
}

func runSelfHealWithOptions(installDir, configPath string, opts healRunOptions) healReport {
	installDir = strings.TrimSpace(installDir)
	if installDir == "" {
		installDir = defaultV2InstallDir()
	}
	if abs, err := filepath.Abs(installDir); err == nil {
		installDir = abs
	}
	configPath = strings.TrimSpace(configPath)
	if configPath == "" {
		configPath = resolveDoctorConfigPath(installDir)
	}

	hc := &healContext{InstallDir: installDir, ConfigPath: configPath, Fix: opts.Fix}
	// Best-effort config load so downstream checks can read resolved paths.
	if configPath != "" {
		if cfg, err := loadConfigForValidation(configPath); err == nil {
			hc.Cfg = cfg
			hc.CfgLoaded = true
		}
	}

	report := healReport{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		InstallDir:  installDir,
		ConfigPath:  configPath,
		FixApplied:  opts.Fix,
		Summary:     map[string]int{"ok": 0, "warn": 0, "fail": 0},
	}
	for _, c := range selfHealChecks() {
		if !shouldRunHealCheck(c, opts) {
			continue
		}
		res := c.Run(hc)
		if res.ID == "" {
			res.ID = c.ID
		}
		if res.Title == "" {
			res.Title = c.Title
		}
		if res.Category == "" {
			res.Category = c.Category
		}
		res.Disruptive = c.Disruptive
		report.Summary[string(res.Status)]++
		report.Results = append(report.Results, res)
	}
	return report
}

func runSelfHeal(installDir, configPath string, fix bool) healReport {
	return runSelfHealWithOptions(installDir, configPath, healRunOptions{Fix: fix})
}

// resolveDoctorConfigPath mirrors v2-start's config resolution: prefer
// <install-dir>/config.yaml, fall back to example_config.yaml.
func resolveDoctorConfigPath(installDir string) string {
	primary := filepath.Join(installDir, "config.yaml")
	if fileExists(primary) {
		return primary
	}
	if alt := filepath.Join(installDir, "example_config.yaml"); fileExists(alt) {
		return alt
	}
	// Also try the current working directory for a bare CLI invocation.
	if fileExists("config.yaml") {
		if abs, err := filepath.Abs("config.yaml"); err == nil {
			return abs
		}
	}
	return primary // report the expected path even if absent
}

func fileExists(p string) bool {
	if strings.TrimSpace(p) == "" {
		return false
	}
	st, err := os.Stat(p)
	return err == nil && !st.IsDir()
}

// ---- checks -------------------------------------------------------------

func checkConfigValid(hc *healContext) healResult {
	res := healResult{ID: "config-valid", Title: "Configuration validity", Category: "config"}
	if !fileExists(hc.ConfigPath) {
		res.Status = healFail
		res.Message = fmt.Sprintf("config file not found at %s", hc.ConfigPath)
		res.Hint = "Run `ncc-orchestrator quickstart` or point --config at a valid file."
		return res
	}
	if _, err := loadConfigForValidation(hc.ConfigPath); err == nil {
		res.Status = healOK
		res.Message = "config parses and validates"
		return res
	} else {
		res.Status = healFail
		res.Message = err.Error()
	}
	if hc.Fix {
		// The one repair that is always safe: strip inline-comment artifacts
		// from known duration keys (a frequent hand-edit mistake).
		if changed, ferr := repairConfigInlineCommentValues(hc.ConfigPath); ferr == nil && changed {
			if cfg, err := loadConfigForValidation(hc.ConfigPath); err == nil {
				hc.Cfg = cfg
				hc.CfgLoaded = true
				res.Status = healOK
				res.Fixed = true
				res.FixMsg = "repaired inline-comment artifacts in duration values; config now valid"
				res.Message = "config repaired and validated"
				return res
			}
		}
		res.Hint = "Automatic repair could not make the config valid; run `ncc-orchestrator validate-config --config <file>` for details."
	} else {
		res.Hint = "Re-run with --fix to attempt automatic repair, or `validate-config` for details."
	}
	return res
}

// checkConfigOutputRouting flags relative output-dir-* in the on-disk config —
// a scheduled (cron) run executes with an arbitrary cwd, so relative output
// dirs silently diverge from where the served stack reads results. The loader
// anchors them to the config dir at runtime; this rewrites the file so the
// stored config is unambiguous too.
func checkConfigOutputRouting(hc *healContext) healResult {
	res := healResult{ID: "config-output-routing", Title: "Output-dir path routing", Category: "config"}
	if !fileExists(hc.ConfigPath) {
		res.Status = healWarn
		res.Message = "config absent; skipped"
		return res
	}
	raw, err := os.ReadFile(hc.ConfigPath)
	if err != nil {
		res.Status = healWarn
		res.Message = "could not read config: " + err.Error()
		return res
	}
	rel := relativeOutputDirKeys(string(raw))
	if len(rel) == 0 {
		res.Status = healOK
		res.Message = "output directories are absolute (cron-safe)"
		return res
	}
	res.Status = healWarn
	res.Message = fmt.Sprintf("relative output dir(s) %s resolve against the working directory; scheduled runs may write elsewhere", strings.Join(rel, ", "))
	if !hc.Fix {
		res.Hint = "Re-run with --fix to rewrite them as absolute paths anchored to the config directory."
		return res
	}
	cfgDir := filepath.Dir(hc.ConfigPath)
	newText, changed := absolutizeOutputDirKeys(string(raw), cfgDir)
	if !changed {
		res.Hint = "Could not rewrite paths automatically; edit the config to use absolute output dirs."
		return res
	}
	if err := os.WriteFile(hc.ConfigPath, []byte(newText), 0o600); err != nil {
		res.Hint = "Rewrite failed: " + err.Error()
		return res
	}
	res.Status = healOK
	res.Fixed = true
	res.FixMsg = fmt.Sprintf("rewrote %s to absolute paths under %s", strings.Join(rel, ", "), cfgDir)
	res.Message = "output directories anchored to absolute paths"
	return res
}

func checkOutputDirsWritable(hc *healContext) healResult {
	res := healResult{ID: "output-dirs-writable", Title: "Output directories writable", Category: "storage"}
	if !hc.CfgLoaded {
		res.Status = healWarn
		res.Message = "config not loaded; skipped"
		return res
	}
	dirs := map[string]string{
		"output-dir-logs":     hc.Cfg.OutputDirLogs,
		"output-dir-filtered": hc.Cfg.OutputDirFiltered,
	}
	var problems, fixed []string
	for name, dir := range dirs {
		if strings.TrimSpace(dir) == "" {
			continue
		}
		err := dirWritable(dir)
		if err == nil {
			continue
		}
		if hc.Fix {
			if mkErr := os.MkdirAll(dir, 0o755); mkErr == nil && dirWritable(dir) == nil {
				fixed = append(fixed, fmt.Sprintf("%s (%s)", name, dir))
				continue
			}
		}
		problems = append(problems, fmt.Sprintf("%s (%s): %v", name, dir, err))
	}
	switch {
	case len(problems) == 0 && len(fixed) == 0:
		res.Status = healOK
		res.Message = "output directories exist and are writable"
	case len(problems) == 0:
		res.Status = healOK
		res.Fixed = true
		res.FixMsg = "created/repaired: " + strings.Join(fixed, ", ")
		res.Message = "output directories writable after repair"
	default:
		res.Status = healFail
		res.Message = strings.Join(problems, "; ")
		if !hc.Fix {
			res.Hint = "Re-run with --fix to create missing output directories."
		}
	}
	return res
}

func checkDiskSpace(hc *healContext) healResult {
	res := healResult{ID: "disk-space", Title: "Free disk space", Category: "storage"}
	target := hc.InstallDir
	if hc.CfgLoaded && strings.TrimSpace(hc.Cfg.OutputDirFiltered) != "" {
		if _, err := os.Stat(hc.Cfg.OutputDirFiltered); err == nil {
			target = hc.Cfg.OutputDirFiltered
		}
	}
	free, ok := diskFreeBytes(target)
	if !ok {
		res.Status = healWarn
		res.Message = "could not determine free space for " + target
		return res
	}
	const minFree = uint64(1) << 30 // 1 GiB
	res.Message = fmt.Sprintf("%s free on %s", humanBytes(free), target)
	if free < minFree {
		res.Status = healWarn
		res.Hint = "Low disk space can cause runs to write partial output. Prune old backups/run-history or grow the volume."
		return res
	}
	res.Status = healOK
	return res
}

// checkSecretsPerms flags secret files that are group/world-readable and (with
// --fix) tightens them to 0600. No-op on Windows where POSIX mode bits do not
// carry the same meaning.
func checkSecretsPerms(hc *healContext) healResult {
	res := healResult{ID: "secrets-perms", Title: "Secret file permissions", Category: "encryption"}
	if runtime.GOOS == "windows" {
		res.Status = healOK
		res.Message = "skipped on windows (NTFS ACLs, not POSIX mode bits)"
		return res
	}
	candidates := []string{
		filepath.Join(hc.InstallDir, ".ncc-api-users.json"),
		filepath.Join(hc.InstallDir, ".ncc-api-token"),
		filepath.Join(hc.InstallDir, ".ncc-initial-admin-password"),
	}
	if st, ok := loadV2StartState(hc.InstallDir); ok {
		if strings.TrimSpace(st.UITLSKeyFile) != "" {
			candidates = append(candidates, st.UITLSKeyFile)
		}
	}
	if hc.CfgLoaded && strings.TrimSpace(hc.Cfg.SecretsFile) != "" {
		candidates = append(candidates, hc.Cfg.SecretsFile)
	}
	var loose, fixed []string
	for _, p := range candidates {
		info, err := os.Stat(p)
		if err != nil || info.IsDir() {
			continue
		}
		if info.Mode().Perm()&0o077 == 0 {
			continue // already 0600-ish
		}
		if hc.Fix {
			if chErr := os.Chmod(p, 0o600); chErr == nil {
				fixed = append(fixed, filepath.Base(p))
				continue
			}
		}
		loose = append(loose, fmt.Sprintf("%s (%04o)", filepath.Base(p), info.Mode().Perm()))
	}
	switch {
	case len(loose) == 0 && len(fixed) == 0:
		res.Status = healOK
		res.Message = "secret files are owner-only (0600)"
	case len(loose) == 0:
		res.Status = healOK
		res.Fixed = true
		res.FixMsg = "tightened to 0600: " + strings.Join(fixed, ", ")
		res.Message = "secret file permissions repaired"
	default:
		res.Status = healWarn
		res.Message = "group/world-accessible secret files: " + strings.Join(loose, ", ")
		if !hc.Fix {
			res.Hint = "Re-run with --fix to chmod them to 0600."
		}
	}
	return res
}

func checkBackupStaleness(hc *healContext) healResult {
	res := healResult{ID: "backup-staleness", Title: "Backup freshness", Category: "backups"}
	dir := filepath.Join(hc.InstallDir, "backups")
	matches, _ := filepath.Glob(filepath.Join(dir, "*.tar.gz"))
	const staleAfter = 7 * 24 * time.Hour

	if len(matches) == 0 {
		if hc.Fix {
			if out, err := selfHealCreateBackup(hc.InstallDir); err == nil {
				res.Status = healOK
				res.Fixed = true
				res.FixMsg = "no backup existed; created and verified " + filepath.Base(out)
				res.Message = "auto-backup created"
				return res
			}
		}
		res.Status = healWarn
		res.Message = "no backups found under " + dir
		res.Hint = "Re-run with --fix to take one now, or create from Settings → Maintenance."
		return res
	}
	var newest time.Time
	var newestName string
	for _, m := range matches {
		if info, err := os.Stat(m); err == nil && info.ModTime().After(newest) {
			newest = info.ModTime()
			newestName = filepath.Base(m)
		}
	}
	age := time.Since(newest)
	res.Message = fmt.Sprintf("%d backup(s); newest %s is %s old", len(matches), newestName, humanDuration(age))
	if age > staleAfter {
		if hc.Fix {
			if out, err := selfHealCreateBackup(hc.InstallDir); err == nil {
				res.Status = healOK
				res.Fixed = true
				res.FixMsg = "newest backup was stale; created and verified " + filepath.Base(out)
				res.Message = "auto-backup created"
				return res
			}
		}
		res.Status = healWarn
		res.Hint = "Latest backup is over 7 days old; re-run with --fix to take a fresh one."
		return res
	}
	res.Status = healOK
	return res
}

// selfHealCreateBackup builds a verified backup into <installDir>/backups and
// prunes to the newest 7. It writes nothing to stdout (the doctor controls
// output) and removes any archive that fails verification.
func selfHealCreateBackup(installDir string) (string, error) {
	entries, _ := collectBackupEntries(installDir)
	if len(entries) == 0 {
		return "", fmt.Errorf("nothing to back up under %s", installDir)
	}
	dir := filepath.Join(installDir, "backups")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	out := filepath.Join(dir, fmt.Sprintf("ncc-backup-%s.tar.gz", time.Now().UTC().Format("20060102T150405Z")))
	rels := make([]string, 0, len(entries))
	for _, e := range entries {
		rels = append(rels, e.Rel)
	}
	manifest := backupManifest{
		Tool:       "ncc-orchestrator",
		Version:    Version,
		Stream:     Stream,
		BuildDate:  BuildDate,
		GoVersion:  GoVersion,
		CreatedAt:  time.Now().UTC().Format(time.RFC3339),
		InstallDir: installDir,
		Files:      rels,
		Auth:       summarizeAuthProviders(filepath.Join(installDir, ".ncc-api-users.json")),
	}
	if err := writeBackupArchive(out, manifest, entries); err != nil {
		return "", err
	}
	if _, err := verifyBackupArchive(out); err != nil {
		_ = os.Remove(out)
		return "", fmt.Errorf("verification failed: %w", err)
	}
	_, _ = pruneOldBackups(dir, 7)
	return out, nil
}

// checkBackupRestorable validates the newest backup archive end-to-end so a
// silently-corrupt backup is caught before it is needed for a restore.
func checkBackupRestorable(hc *healContext) healResult {
	res := healResult{ID: "backup-restorable", Title: "Newest backup restorable", Category: "backups"}
	dir := filepath.Join(hc.InstallDir, "backups")
	matches, _ := filepath.Glob(filepath.Join(dir, "*.tar.gz"))
	if len(matches) == 0 {
		res.Status = healWarn
		res.Message = "no backups to validate"
		return res
	}
	var newest string
	var newestMod time.Time
	for _, m := range matches {
		if info, err := os.Stat(m); err == nil && info.ModTime().After(newestMod) {
			newest, newestMod = m, info.ModTime()
		}
	}
	vr, err := verifyBackupArchive(newest)
	if err != nil {
		res.Status = healFail
		res.Message = fmt.Sprintf("%s is not restorable: %v", filepath.Base(newest), err)
		res.Hint = "This backup cannot be restored. Take a fresh backup (`v2-backup`) and investigate the storage."
		return res
	}
	res.Status = healOK
	res.Message = fmt.Sprintf("%s verified: %d file(s), created by %s at %s", filepath.Base(newest), vr.DataFiles, vr.Manifest.Version, vr.Manifest.CreatedAt)
	return res
}

// ---- helpers ------------------------------------------------------------

func dirWritable(dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}
	f, err := os.CreateTemp(dir, ".ncc-heal-probe-*")
	if err != nil {
		return err
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)
	return nil
}

var outputDirKeyNames = []string{"output-dir-logs", "output-dir-filtered", "run-history-dir"}

// relativeOutputDirKeys returns the output-dir keys whose YAML value is a
// non-empty relative path.
func relativeOutputDirKeys(yaml string) []string {
	var out []string
	for _, line := range strings.Split(yaml, "\n") {
		key, val, ok := parseSimpleYAMLKV(line)
		if !ok || !strInList(outputDirKeyNames, key) {
			continue
		}
		if val != "" && !filepath.IsAbs(val) {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

// absolutizeOutputDirKeys rewrites relative output-dir-* values to absolute
// paths anchored at base, preserving the rest of each line.
func absolutizeOutputDirKeys(yaml, base string) (string, bool) {
	lines := strings.Split(yaml, "\n")
	changed := false
	for i, line := range lines {
		key, val, ok := parseSimpleYAMLKV(line)
		if !ok || !strInList(outputDirKeyNames, key) || val == "" || filepath.IsAbs(val) {
			continue
		}
		abs := filepath.Join(base, val)
		// Replace just the value token, keeping indentation and any comment.
		idx := strings.Index(line, val)
		if idx < 0 {
			continue
		}
		lines[i] = line[:idx] + abs + line[idx+len(val):]
		changed = true
	}
	return strings.Join(lines, "\n"), changed
}

// parseSimpleYAMLKV parses a `key: value` line, stripping surrounding quotes
// and trailing inline comments from the value. Returns ok=false for blanks,
// comments, and list items.
func parseSimpleYAMLKV(line string) (key, val string, ok bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "-") {
		return "", "", false
	}
	colon := strings.Index(trimmed, ":")
	if colon < 0 {
		return "", "", false
	}
	key = strings.TrimSpace(trimmed[:colon])
	val = strings.TrimSpace(trimmed[colon+1:])
	// Strip an unquoted trailing comment.
	if !strings.HasPrefix(val, "\"") && !strings.HasPrefix(val, "'") {
		if h := strings.Index(val, " #"); h >= 0 {
			val = strings.TrimSpace(val[:h])
		}
	}
	val = strings.Trim(val, "\"'")
	return key, val, true
}

func strInList(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func humanBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func humanDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%.1fh", d.Hours())
	}
	return fmt.Sprintf("%.1fd", d.Hours()/24)
}
