package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func putOnboarding(t *testing.T, s *apiServer, body string) (int, onboardingStatus) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "/api/v1/onboarding", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.handleOnboarding(rec, req)
	var env struct {
		Data onboardingStatus `json:"data"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &env)
	return rec.Code, env.Data
}

func TestOnboardingSkipAndFinish(t *testing.T) {
	s := newOnboardingServer(t, sampleCanonicalConfig)

	before := s.onboardingStatus()
	if before.Completed || before.RequiredDone != 0 {
		t.Fatalf("fresh install: %+v", before)
	}

	// Skipping a required step counts it toward progress.
	code, st := putOnboarding(t, s, `{"skip_step":"backups"}`)
	if code != http.StatusOK || !stepByID(t, st, "backups").Skipped || st.RequiredDone != 1 {
		t.Fatalf("skip: %d %+v", code, st)
	}
	// Skipping twice does not double count; unskip restores it.
	_, st = putOnboarding(t, s, `{"skip_step":"backups"}`)
	if st.RequiredDone != 1 {
		t.Fatalf("double skip counted twice: %+v", st)
	}
	_, st = putOnboarding(t, s, `{"unskip_step":"backups"}`)
	if stepByID(t, st, "backups").Skipped || st.RequiredDone != 0 {
		t.Fatalf("unskip: %+v", st)
	}

	// Skipping every required step completes setup.
	for _, id := range []string{"clusters", "first_run", "schedule", "backups"} {
		_, st = putOnboarding(t, s, `{"skip_step":"`+id+`"}`)
	}
	if !st.Completed || st.Finished {
		t.Fatalf("all skipped: %+v", st)
	}
	for _, id := range []string{"clusters", "first_run", "schedule", "backups"} {
		_, st = putOnboarding(t, s, `{"unskip_step":"`+id+`"}`)
	}

	// Finish completes setup with steps left, and can be undone.
	_, st = putOnboarding(t, s, `{"finished":true}`)
	if !st.Completed || !st.Finished || st.FinishedAt == "" {
		t.Fatalf("finish: %+v", st)
	}
	_, st = putOnboarding(t, s, `{"finished":false}`)
	if st.Completed || st.Finished {
		t.Fatalf("reopen: %+v", st)
	}

	// The old dismiss body still works and keeps other choices.
	putOnboarding(t, s, `{"skip_step":"notifications"}`)
	_, st = putOnboarding(t, s, `{"dismissed":true}`)
	if !st.Dismissed || !stepByID(t, st, "notifications").Skipped {
		t.Fatalf("dismiss: %+v", st)
	}

	for _, bad := range []string{`{}`, `{"skip_step":"../etc"}`} {
		if code, _ := putOnboarding(t, s, bad); code != http.StatusBadRequest {
			t.Errorf("%s: want 400, got %d", bad, code)
		}
	}
}
