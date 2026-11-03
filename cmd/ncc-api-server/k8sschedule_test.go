package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestKubernetesCron(t *testing.T) {
	tests := []struct {
		every string
		want  string
	}{
		{"15m", "*/15 * * * *"},
		{"30m", "*/30 * * * *"},
		{"60m", "0 * * * *"},
		{"1h", "0 * * * *"},
		{"4h", "0 */4 * * *"},
		{"12h", "0 */12 * * *"},
		{"24h", "0 0 * * *"},
		{"1d", "0 0 * * *"},
		{"7d", "0 0 * * 0"},
	}
	for _, tt := range tests {
		got, err := kubernetesCron(tt.every)
		if err != nil {
			t.Errorf("kubernetesCron(%q): %v", tt.every, err)
		} else if got != tt.want {
			t.Errorf("kubernetesCron(%q) = %q, want %q", tt.every, got, tt.want)
		}
	}
}

// Intervals that do not divide their period would fire unevenly on a
// wall-clock CronJob, so they are refused rather than silently rounded.
func TestKubernetesCronRejectsUnevenIntervals(t *testing.T) {
	for _, every := range []string{"0m", "7m", "45m", "61m", "5h", "25h", "2d", "32d", "weekly"} {
		if _, err := kubernetesCron(every); err == nil {
			t.Errorf("kubernetesCron(%q) unexpectedly succeeded", every)
		}
	}
}

func TestKubernetesEveryInvertsKubernetesCron(t *testing.T) {
	for _, every := range []string{"15m", "1h", "4h", "1d", "7d"} {
		cron, err := kubernetesCron(every)
		if err != nil {
			t.Fatal(err)
		}
		if got := kubernetesEvery(cron); got != every {
			t.Errorf("kubernetesEvery(%q) = %q, want %q", cron, got, every)
		}
	}
	for _, cron := range []string{"15 */4 * * *", "0 2 * * 1-5", "@daily", ""} {
		if got := kubernetesEvery(cron); got != "" {
			t.Errorf("kubernetesEvery(%q) = %q, want custom", cron, got)
		}
	}
}

// fakeCronJobAPI serves one CronJob and applies JSON merge patches to it the
// way the Kubernetes API server does (null deletes a key).
type fakeCronJobAPI struct {
	mu      sync.Mutex
	name    string
	obj     map[string]interface{}
	patches []map[string]interface{}
	status  int // forced response status, 0 = normal
}

func newFakeCronJobAPI(t *testing.T, name, schedule string) (*fakeCronJobAPI, *httptest.Server) {
	f := &fakeCronJobAPI{name: name, obj: map[string]interface{}{
		"metadata": map[string]interface{}{"name": name},
		"spec":     map[string]interface{}{"schedule": schedule, "suspend": false},
		"status":   map[string]interface{}{"lastScheduleTime": "2026-11-03T08:15:00Z", "lastSuccessfulTime": "2026-11-03T08:21:00Z"},
	}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.status != 0 {
			w.WriteHeader(f.status)
			_, _ = w.Write([]byte(`{"kind":"Status","message":"forced"}`))
			return
		}
		if r.URL.Path != "/apis/batch/v1/namespaces/ncc/cronjobs/"+f.name {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodGet:
		case http.MethodPatch:
			if ct := r.Header.Get("Content-Type"); ct != "application/merge-patch+json" {
				t.Errorf("patch content type %q", ct)
			}
			b, _ := io.ReadAll(r.Body)
			var patch map[string]interface{}
			if err := json.Unmarshal(b, &patch); err != nil {
				t.Errorf("bad patch: %v", err)
			}
			f.patches = append(f.patches, patch)
			mergePatch(f.obj, patch)
			if spec, _ := f.obj["spec"].(map[string]interface{}); spec["schedule"] == "" {
				w.WriteHeader(http.StatusUnprocessableEntity)
				_, _ = w.Write([]byte(`{"message":"spec.schedule: Required value"}`))
				return
			}
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		_ = json.NewEncoder(w).Encode(f.obj)
	}))
	t.Cleanup(srv.Close)
	return f, srv
}

func mergePatch(dst, patch map[string]interface{}) {
	for k, v := range patch {
		if v == nil {
			delete(dst, k)
			continue
		}
		if pm, ok := v.(map[string]interface{}); ok {
			dm, _ := dst[k].(map[string]interface{})
			if dm == nil {
				dm = map[string]interface{}{}
			}
			mergePatch(dm, pm)
			dst[k] = dm
			continue
		}
		dst[k] = v
	}
}

func newK8sScheduleServer(srv *httptest.Server, cronJob string) *apiServer {
	s := &apiServer{configPath: "/data/config/config.yaml", runnerCronJob: cronJob}
	s.capabilities.Kubernetes = true
	s.scheduleK8sClient = func() (*k8sSecretClient, error) {
		return &k8sSecretClient{base: srv.URL, namespace: "ncc", token: "t", httpClient: srv.Client()}, nil
	}
	return s
}

func putSchedule(t *testing.T, s *apiServer, body string) (int, envelope) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "/api/v1/schedule", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.handleSchedule(rec, req)
	var env envelope
	_ = json.Unmarshal(rec.Body.Bytes(), &env)
	return rec.Code, env
}

func getSchedule(t *testing.T, s *apiServer) scheduleState {
	t.Helper()
	st, err := s.loadSchedule()
	if err != nil {
		t.Fatalf("load schedule: %v", err)
	}
	return st
}

func TestKubernetesScheduleRoundTrip(t *testing.T) {
	// The Helm chart names its CronJob differently from Kustomize.
	f, srv := newFakeCronJobAPI(t, "ncc-orchestrator", "15 */4 * * *")
	s := newK8sScheduleServer(srv, "ncc-orchestrator")

	// A schedule set outside NCC is shown as custom cron, not as a stale interval.
	if st := getSchedule(t, s); st.Cron != "15 */4 * * *" || st.Every != "" || st.Action != "create" {
		t.Fatalf("initial state %+v", st)
	}

	// Simple mode: the interval becomes the cron and is remembered.
	code, env := putSchedule(t, s, `{"type":"auto","action":"create","every":"30m","config":"config.yaml","apply":true}`)
	if code != http.StatusOK || !env.Success {
		t.Fatalf("PUT interval: %d %+v", code, env)
	}
	if st := getSchedule(t, s); st.Cron != "*/30 * * * *" || st.Every != "30m" || st.UpdatedAt == "" {
		t.Fatalf("after interval %+v", st)
	}

	// Advanced mode sends a custom cron plus the form's interval; the cron
	// wins and the old interval annotation is removed.
	code, env = putSchedule(t, s, `{"type":"auto","action":"create","cron":"5 2 * * 1-5","every":"30m","config":"config.yaml"}`)
	if code != http.StatusOK {
		t.Fatalf("PUT cron: %d %+v", code, env)
	}
	st := getSchedule(t, s)
	if st.Cron != "5 2 * * 1-5" || st.Every != "" {
		t.Fatalf("after custom cron %+v", st)
	}
	meta := f.obj["metadata"].(map[string]interface{})
	if ann, _ := meta["annotations"].(map[string]interface{}); ann[kubernetesScheduleAnnotation] != nil {
		t.Fatalf("stale interval annotation kept: %v", ann)
	}

	// Pausing suspends the CronJob without clearing its schedule (an empty
	// schedule is rejected by the API server).
	code, env = putSchedule(t, s, `{"type":"auto","action":"remove","config":"config.yaml"}`)
	if code != http.StatusOK {
		t.Fatalf("PUT pause: %d %+v", code, env)
	}
	if st := getSchedule(t, s); st.Action != "remove" || st.Cron != "5 2 * * 1-5" {
		t.Fatalf("after pause %+v", st)
	}
	last := f.patches[len(f.patches)-1]["spec"].(map[string]interface{})
	if _, ok := last["schedule"]; ok || last["suspend"] != true {
		t.Fatalf("pause patch %v", last)
	}

	// Saving again resumes it.
	if code, _ := putSchedule(t, s, `{"type":"auto","action":"create","every":"4h","config":"config.yaml"}`); code != http.StatusOK {
		t.Fatalf("PUT resume: %d", code)
	}
	if st := getSchedule(t, s); st.Action != "create" || st.Cron != "0 */4 * * *" || st.Every != "4h" {
		t.Fatalf("after resume %+v", st)
	}
}

func TestKubernetesScheduleRejectsBadInputWith400(t *testing.T) {
	f, srv := newFakeCronJobAPI(t, kubernetesRunnerCronJob, "15 */4 * * *")
	s := newK8sScheduleServer(srv, "")
	for _, body := range []string{
		`{"action":"create","every":"45m","config":"config.yaml"}`,
		`{"action":"create","cron":"*/5 * * *","config":"config.yaml"}`,
		`{"action":"run-now","config":"config.yaml"}`,
	} {
		code, env := putSchedule(t, s, body)
		if code != http.StatusBadRequest || env.Success {
			t.Errorf("%s: want 400, got %d %+v", body, code, env)
		}
	}
	if len(f.patches) != 0 {
		t.Fatalf("invalid input reached the API server: %v", f.patches)
	}
}

func TestKubernetesScheduleErrorsNameTheFix(t *testing.T) {
	_, srv := newFakeCronJobAPI(t, "ncc-orchestrator", "15 */4 * * *")
	s := newK8sScheduleServer(srv, "") // default name does not exist
	_, err := s.loadSchedule()
	if err == nil || !strings.Contains(err.Error(), "NCC_RUNNER_CRONJOB") {
		t.Fatalf("missing CronJob error = %v", err)
	}

	f, srv := newFakeCronJobAPI(t, kubernetesRunnerCronJob, "15 */4 * * *")
	f.status = http.StatusForbidden
	s = newK8sScheduleServer(srv, "")
	code, env := putSchedule(t, s, `{"action":"create","every":"1h","config":"config.yaml"}`)
	if code != http.StatusBadGateway || !strings.Contains(env.Error, "patch on cronjobs.batch") {
		t.Fatalf("forbidden: %d %q", code, env.Error)
	}
}

func TestKubernetesScheduleHealthReportsCronJobStatus(t *testing.T) {
	f, srv := newFakeCronJobAPI(t, kubernetesRunnerCronJob, "0 */4 * * *")
	s := newK8sScheduleServer(srv, "")
	health := func() map[string]interface{} {
		rec := httptest.NewRecorder()
		s.handleScheduleHealth(rec, httptest.NewRequest(http.MethodGet, "/api/v1/schedule/health", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("health: %d %s", rec.Code, rec.Body.String())
		}
		var env struct {
			Data map[string]interface{} `json:"data"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &env)
		return env.Data
	}
	// A CronJob deployed with a schedule is configured even before anyone
	// saves it from NCC.
	h := health()
	if h["configured"] != true || h["every"] != "4h" || h["task_name"] != kubernetesRunnerCronJob ||
		h["last_schedule_time"] != "2026-11-03T08:15:00Z" || h["managed_by_ncc"] != false {
		t.Fatalf("health %v", h)
	}
	f.obj["spec"].(map[string]interface{})["suspend"] = true
	if h := health(); h["configured"] != false || h["suspended"] != true {
		t.Fatalf("suspended health %v", h)
	}
}
