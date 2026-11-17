package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

// Retention preview: what retain-last, retain-days, artifact-retain-days,
// artifact-retain-max-files and raw-log-retain-days would delete now. The
// orchestrator owns the rules, so this runs `retention --dry-run --json`
// against the server's config and its canonical report and raw log folders.
// Nothing is deleted.

// retentionPreviewArgs is the orchestrator command line for the preview.
func (s *apiServer) retentionPreviewArgs() []string {
	args := []string{"retention", "--dry-run", "--json"}
	if cfg := strings.TrimSpace(s.configPath); cfg != "" {
		args = append(args, "--config", s.absPath(cfg))
	}
	if out := strings.TrimSpace(s.outputDir); out != "" {
		args = append(args, "--output-dir-filtered", s.absPath(out))
	}
	if logs := strings.TrimSpace(s.logDir); logs != "" {
		args = append(args, "--output-dir-logs", s.absPath(logs))
	}
	return args
}

func (s *apiServer) handleRetentionPreview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if strings.TrimSpace(s.configPath) == "" {
		writeJSON(w, http.StatusConflict, envelope{Success: false, Error: "no config file is set for this server"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	args := s.retentionPreviewArgs()
	cmd := s.makeOrchestratorCommand(ctx, args...)
	cmd.Dir = s.absPath(s.repoRoot)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	started := time.Now()
	out, err := cmd.Output()
	var plan map[string]interface{}
	if jerr := json.Unmarshal(out, &plan); jerr != nil {
		logSubprocess("retention preview", args, time.Since(started), stderr.String()+string(out), err)
		msg := strings.TrimSpace(tailString(stderr.String(), 400))
		if msg == "" {
			msg = "the retention preview did not return a result"
		}
		writeJSON(w, http.StatusBadGateway, envelope{Success: false, Error: "retention preview failed: " + msg})
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: plan})
}
