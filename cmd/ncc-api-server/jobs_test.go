package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"goncc/internal/ai"
)

func jobRequest(t *testing.T, s *apiServer, method, id string) (int, ai.Job) {
	t.Helper()
	rec := httptest.NewRecorder()
	s.handleJobs(rec, httptest.NewRequest(method, "/api/v1/jobs/"+id, nil))
	var env struct {
		Data ai.Job `json:"data"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &env)
	return rec.Code, env.Data
}

func TestJobsRejectsPathLikeIDs(t *testing.T) {
	dir := t.TempDir()
	// A JSON file outside jobs/ that a traversal would reach.
	if err := os.WriteFile(filepath.Join(dir, "secret.json"), []byte(`{"job_id":"leak","state":"succeeded"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	s := &apiServer{repoRoot: dir, outputDir: filepath.Join(dir, "out")}
	s.ensureRunManager()
	for _, id := range []string{`..\..\secret`, "..%5C..%5Csecret", "a.b", ".hidden"} {
		if code, _ := jobRequest(t, s, http.MethodGet, id); code != http.StatusBadRequest {
			t.Errorf("GET %q: want 400, got %d", id, code)
		}
	}
}

// DELETE on a queued job must take it out of the queue (it used to only set
// a flag, so the run still started later) and leave a cancelled record.
func TestJobsDeleteCancelsQueuedRun(t *testing.T) {
	dir := t.TempDir()
	s := &apiServer{repoRoot: dir, outputDir: filepath.Join(dir, "out")}
	s.ensureRunManager()
	id := "20261103T081500Z-001"
	s.mu.Lock()
	s.runs[id] = &runRecord{id: id, status: "queued", queuedAt: time.Now().UTC()}
	s.runOrder = []string{id}
	s.runQueue = []string{id}
	s.mu.Unlock()

	code, job := jobRequest(t, s, http.MethodDelete, id)
	if code != http.StatusAccepted || job.State != "cancelled" {
		t.Fatalf("DELETE: %d %+v", code, job)
	}
	s.mu.Lock()
	queued, active := len(s.runQueue), s.runs[id] != nil
	s.mu.Unlock()
	if queued != 0 || active {
		t.Fatalf("run still queued=%d active=%v", queued, active)
	}

	code, job = jobRequest(t, s, http.MethodGet, id)
	if code != http.StatusOK || job.State != "cancelled" {
		t.Fatalf("GET after cancel: %d %+v", code, job)
	}
	if code, _ := jobRequest(t, s, http.MethodDelete, id); code != http.StatusConflict {
		t.Fatalf("second DELETE: want 409, got %d", code)
	}
	if code, _ := jobRequest(t, s, http.MethodDelete, "20261103T081500Z-999"); code != http.StatusNotFound {
		t.Fatalf("unknown DELETE: want 404, got %d", code)
	}
}

func TestJobStateForFinishedRuns(t *testing.T) {
	ok := jobFromRunRecord(&runRecord{id: "a", status: "done"})
	failed := jobFromRunRecord(&runRecord{id: "b", status: "done", errStr: "exit status 2"})
	if ok.State != "succeeded" || failed.State != "failed" {
		t.Fatalf("states: ok=%q failed=%q", ok.State, failed.State)
	}
}
