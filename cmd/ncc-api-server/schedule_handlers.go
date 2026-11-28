package main

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

func (s *apiServer) handleSchedule(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		st, err := s.loadSchedule()
		if err != nil {
			writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: st})
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		var req scheduleUpdateRequest
		if err := decodeJSON(r.Body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		st := scheduleState{
			Type:      defaultIfEmpty(req.Type, "auto"),
			Action:    defaultIfEmpty(req.Action, "create"),
			Cron:      strings.TrimSpace(req.Cron),
			Every:     strings.TrimSpace(req.Every),
			Config:    defaultIfEmpty(req.Config, s.configPath),
			LogPath:   strings.TrimSpace(req.LogPath),
			TaskName:  strings.TrimSpace(req.TaskName),
			PrintOnly: true,
			WithLock:  true,
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),

			ResourceVersion: strings.TrimSpace(req.ResourceVersion),
		}
		if req.PrintOnly != nil {
			st.PrintOnly = *req.PrintOnly
		}
		if req.WithLock != nil {
			st.WithLock = *req.WithLock
		}
		if err := validateScheduleInput(st); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if s.capabilities.Kubernetes {
			s.handleKubernetesScheduleUpdate(w, r, st)
			return
		}
		// Anchor config + log paths to absolute, install-root paths before we
		// persist or apply them. Otherwise the orchestrator's create-schedule
		// resolves a relative path against ITS OWN working directory (e.g.
		// <install>/bin), producing a cron line / systemd unit whose runner
		// can't find config.yaml — the "at least one cluster must be provided"
		// failure that silently breaks scheduled runs. Resolving here makes the
		// generated schedule deterministic regardless of the api-server's cwd.
		resolvedCfg, cfgErr := s.validateConfigPath(st.Config)
		if cfgErr != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: cfgErr.Error()})
			return
		}
		st.Config = resolvedCfg
		if st.LogPath != "" {
			st.LogPath = s.absPath(st.LogPath)
		}
		// apply + print_only is a preview of the scheduler command: it must not
		// replace the saved (and possibly installed) schedule.
		preview := req.Apply && st.PrintOnly
		if !preview {
			// The version the form was loaded from (empty from older clients,
			// which keeps their saves unconditional).
			version, err := s.saveHostScheduleIfCurrent(st, st.ResourceVersion)
			if errors.Is(err, errHostScheduleConflict) {
				s.audit(r, "schedule.update", false, map[string]interface{}{"applied": false, "conflict": true})
				writeJSON(w, http.StatusConflict, envelope{Success: false, Error: err.Error(), ErrorCode: "NCC_API_SCHEDULE_CONFLICT"})
				return
			}
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
				return
			}
			st.ResourceVersion = version
		} else {
			st.ResourceVersion = ""
		}
		resp := map[string]interface{}{"schedule": st, "applied": false, "preview": preview}
		applyErr := error(nil)
		if req.Apply {
			args := []string{"create-schedule", "--type", st.Type, "--action", st.Action, "--config", st.Config, "--print-only=" + fmt.Sprintf("%t", st.PrintOnly)}
			if st.Cron != "" {
				args = append(args, "--cron", st.Cron)
			}
			if st.Every != "" {
				args = append(args, "--every", st.Every)
			}
			if st.LogPath != "" {
				args = append(args, "--log-path", st.LogPath)
			}
			args = append(args, fmt.Sprintf("--with-lock=%t", st.WithLock))
			if st.TaskName != "" {
				args = append(args, "--task-name", st.TaskName)
			}
			out, err := s.runOrchestrator(args, 60*time.Second)
			resp["applied"] = err == nil
			resp["command"] = append(s.orchestratorBaseCommand(), args...)
			resp["output"] = tailString(strings.TrimSpace(out), 4000)
			if err != nil {
				resp["apply_error"] = err.Error()
				applyErr = err
			}
		}
		s.audit(r, "schedule.update", applyErr == nil, map[string]interface{}{
			"applied": req.Apply,
			"type":    st.Type,
			"action":  st.Action,
		})
		// State was saved, but the apply step failed — surface that at the
		// envelope level instead of pretending the whole call succeeded.
		if applyErr != nil {
			writeJSON(w, http.StatusBadGateway, envelope{
				Success: false,
				Error:   fmt.Sprintf("schedule saved but apply failed: %v", applyErr),
				Data:    resp,
			})
			return
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "schedule updated", Data: resp})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

func parseScheduleHealthFromLog(path string) map[string]string {
	out := map[string]string{
		"last_run":     "",
		"last_success": "",
		"last_error":   "",
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return out
	}
	lines := strings.Split(string(b), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		if out["last_run"] == "" {
			out["last_run"] = line
		}
		lower := strings.ToLower(line)
		if out["last_success"] == "" && (strings.Contains(lower, "completed") || strings.Contains(lower, "success") || strings.Contains(lower, "report generated")) {
			out["last_success"] = line
		}
		if out["last_error"] == "" && (strings.Contains(lower, "error") || strings.Contains(lower, "failed") || strings.Contains(lower, "panic")) {
			out["last_error"] = line
		}
		if out["last_success"] != "" && out["last_error"] != "" && out["last_run"] != "" {
			break
		}
	}
	return out
}

func (s *apiServer) handleScheduleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if s.capabilities.Kubernetes {
		s.handleKubernetesScheduleHealth(w)
		return
	}
	// loadSchedule returns a default state when no file is present, so we
	// cannot infer existence from a load error alone — stat the path directly
	// to give the UI an honest "state_file_exists" signal.
	stateFileExists := false
	if st, statErr := os.Stat(s.absPath(s.scheduleStatePath)); statErr == nil && !st.IsDir() {
		stateFileExists = true
	}
	st, err := s.loadSchedule()
	if err != nil {
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"configured":        false,
			"saved":             false,
			"installed":         false,
			"state_file_exists": stateFileExists,
			"error":             err.Error(),
		}})
		return
	}
	saved := strings.TrimSpace(st.UpdatedAt) != ""
	taskName := strings.TrimSpace(st.TaskName)
	if taskName == "" {
		taskName = "ncc-orchestrator"
	}
	installed, installError := s.detectInstalledSchedule(taskName)

	logPath := strings.TrimSpace(st.LogPath)
	if logPath == "" {
		logPath = filepath.Join("logs", "ncc-scheduler.log")
	}
	logAbs := s.absPath(logPath)
	logInfo, statErr := os.Stat(logAbs)
	lockPath := filepath.Join(filepath.Dir(logAbs), ".ncc-scheduler.lock")
	parsed := parseScheduleHealthFromLog(logAbs)
	// Surface the exact config the schedule runs with (absolute) so the UI can
	// show — and operators can verify — which config.yaml the timer uses. A
	// relative/missing config here is precisely what silently breaks scheduled
	// runs ("at least one cluster must be provided").
	configDisp := strings.TrimSpace(st.Config)
	if configDisp != "" {
		configDisp = s.absPath(configDisp)
	}
	data := map[string]interface{}{
		"configured":        installed, // authoritative: schedule is actually installed in OS
		"saved":             saved,
		"installed":         installed,
		"state_file_exists": stateFileExists,
		"task_name":         taskName,
		"type":              st.Type,
		"action":            st.Action,
		"with_lock":         st.WithLock,
		"config":            configDisp,
		"every":             st.Every,
		"cron":              st.Cron,
		"log_path":          logAbs,
		"lock_path":         lockPath,
		"last_updated_at":   st.UpdatedAt,
		"last_run":          parsed["last_run"],
		"last_success":      parsed["last_success"],
		"last_error":        parsed["last_error"],
		"detector":          s.scheduleDetectorName(),
	}
	if installError != "" {
		data["install_check_error"] = installError
	}
	if statErr == nil {
		data["log_exists"] = true
		data["log_size"] = logInfo.Size()
		data["log_mod_time"] = logInfo.ModTime().UTC().Format(time.RFC3339)
	} else {
		data["log_exists"] = false
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
}

// detectInstalledSchedule returns true iff a schedule entry tagged with the
// expected ncc-orchestrator marker is present in the host's scheduler.
//
// Detection is delegated to the orchestrator binary via
//
//	create-schedule --type=auto --action=list --task-name=<name>
//
// so that the API server never touches the host scheduler directly. The
// orchestrator prints the matching schedule line (containing the marker) when
// installed and a stable "No cron entries found" / "No scheduled tasks" line
// otherwise. Returns (installed, errorMessage). errorMessage is non-empty
// only when the orchestrator could not be invoked or returned an unexpected
// failure.
func (s *apiServer) detectInstalledSchedule(taskName string) (bool, string) {
	if taskName == "" {
		return false, ""
	}
	args := []string{
		"create-schedule",
		"--type", "auto",
		"--action", "list",
		"--task-name", taskName,
	}
	out, err := s.runOrchestrator(args, 8*time.Second)
	text := string(out)
	if err != nil {
		return false, fmt.Sprintf("orchestrator schedule list failed: %v: %s", err, strings.TrimSpace(tailString(text, 240)))
	}
	marker := fmt.Sprintf("# ncc-orchestrator:%s", taskName)
	if strings.Contains(text, marker) {
		return true, ""
	}
	// Defensive: also accept the marker without the leading hash in case the
	// orchestrator output format changes for the listing line. (Used by both
	// cron and schtasks list outputs which include the task name.)
	bareMarker := fmt.Sprintf("ncc-orchestrator:%s", taskName)
	if strings.Contains(text, bareMarker) && !strings.Contains(text, "No cron entries") && !strings.Contains(text, "No scheduled tasks") {
		return true, ""
	}
	return false, ""
}

func (s *apiServer) scheduleDetectorName() string {
	if runtime.GOOS == "windows" {
		return "schtasks (via orchestrator)"
	}
	return "crontab / systemd timer (via orchestrator)"
}
