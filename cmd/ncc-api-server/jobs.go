package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"goncc/internal/ai"
)

func (s *apiServer) persistJob(rec *runRecord) {
	if rec == nil || strings.TrimSpace(rec.id) == "" {
		return
	}
	path := filepath.Join(s.absPath(s.outputDir), "jobs", rec.id+".json")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return
	}
	data, err := json.MarshalIndent(jobFromRunRecord(rec), "", "  ")
	if err == nil {
		_ = os.WriteFile(path, data, 0o600)
	}
}

// handleJobs exposes the common asynchronous job shape for API-triggered NCC
// runs. The run manager remains the source of truth, while this endpoint gives
// AI clients a stable polling contract independent of the dashboard's legacy
// /runs/active response.
func (s *apiServer) handleJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodDelete {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/jobs/")
	id = strings.Trim(id, "/")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, envelope{
			Success: false, Error: "job id is required", ErrorCode: ai.CodeInvalidArgument,
		})
		return
	}

	s.mu.Lock()
	rec := s.runs[id]
	if rec != nil && r.Method == http.MethodDelete && (rec.status == "queued" || rec.status == "running") {
		rec.cancelled = true
		if rec.cancel != nil {
			rec.cancel()
		}
	}
	var job ai.Job
	if rec != nil {
		job = jobFromRunRecord(rec)
	}
	s.mu.Unlock()
	if rec == nil {
		job, ok := s.archivedJob(id)
		if ok {
			writeJSON(w, http.StatusOK, envelope{Success: true, Data: job})
			return
		}
		writeJSON(w, http.StatusNotFound, envelope{
			Success: false, Error: "job not found", ErrorCode: ai.CodeNotFound,
		})
		return
	}
	if r.Method == http.MethodDelete {
		writeJSON(w, http.StatusAccepted, envelope{
			Success: true, Message: "job cancellation requested", Data: job,
		})
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: job})
}

func (s *apiServer) archivedJob(id string) (ai.Job, bool) {
	jobPath := filepath.Join(s.absPath(s.outputDir), "jobs", id+".json")
	if data, err := os.ReadFile(jobPath); err == nil {
		var job ai.Job
		if json.Unmarshal(data, &job) == nil {
			return job, true
		}
	}
	path := filepath.Join(s.absPath(s.outputDir), "runs", id, "run-summary.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return ai.Job{}, false
	}
	var summary struct {
		RunID     string `json:"run_id"`
		Timestamp string `json:"timestamp"`
		Status    string `json:"status"`
		ExitCode  int    `json:"exit_code"`
		IndexHTML string `json:"index_html"`
	}
	if json.Unmarshal(data, &summary) != nil {
		return ai.Job{}, false
	}
	created, err := time.Parse(time.RFC3339, summary.Timestamp)
	if err != nil {
		created = time.Now().UTC()
	}
	state := summary.Status
	if state == "" {
		state = "succeeded"
	}
	if state == "success" || state == "partial" {
		state = "succeeded"
	}
	job := ai.Job{
		ID:         id,
		Kind:       "ncc_run",
		State:      state,
		Phase:      "completed",
		CreatedAt:  created,
		UpdatedAt:  created,
		FinishedAt: &created,
		ResultURL:  summary.IndexHTML,
		RequestID:  id,
	}
	if summary.RunID != "" {
		job.ID = summary.RunID
	}
	if summary.ExitCode != 0 {
		job.Error = ai.NewError(ai.CodeInternal, "execution", "archived run completed with a non-zero exit code")
	}
	progress := 1.0
	job.Progress = &progress
	return job, true
}

func jobFromRunRecord(rec *runRecord) ai.Job {
	now := time.Now().UTC()
	job := ai.Job{
		ID:        rec.id,
		Kind:      "ncc_run",
		State:     rec.status,
		CreatedAt: rec.queuedAt,
		UpdatedAt: now,
		RequestID: rec.id,
	}
	if !rec.startedAt.IsZero() {
		started := rec.startedAt
		job.StartedAt = &started
	}
	if !rec.finished.IsZero() {
		finished := rec.finished
		job.FinishedAt = &finished
		job.UpdatedAt = finished
	}
	if rec.status == "running" {
		progress := 0.5
		job.Progress = &progress
		job.Phase = "running_checks"
	} else if rec.status == "done" {
		progress := 1.0
		job.Progress = &progress
		job.Phase = "completed"
	} else {
		progress := 0.0
		job.Progress = &progress
		job.Phase = "queued"
	}
	if rec.cancelled {
		job.State = "cancelled"
		job.Phase = "cancelled"
		job.Error = ai.NewError(ai.CodeCancelled, "execution", "run cancellation requested").WithRetryable(true)
	} else if rec.errStr != "" {
		job.State = "failed"
		job.Error = ai.NewError(ai.CodeInternal, "execution", rec.errStr)
	}
	return job
}
