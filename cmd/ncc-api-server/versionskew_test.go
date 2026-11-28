package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestVersionsCompatible(t *testing.T) {
	tests := []struct {
		a, b              string
		compatible, known bool
	}{
		{"2.4.0", "2.4.0", true, true},       // equal
		{"2.4.0", "v2.4.3", true, true},      // patch difference allowed
		{"2.4.0", "2.4.1-rc1", true, true},   // pre-release of the same minor
		{"2.4.0", "2.3.9", false, true},      // minor difference blocked
		{"2.4.0", "3.4.0", false, true},      // major difference blocked
		{"2.4.0", "latest", true, false},     // not a version: unknown
		{"2.4.0", "", true, false},           // missing: unknown
		{"2.4.0", "sha-1a2b3c", true, false}, // build tag: unknown
		{"dev", "2.4.0", true, false},        // API version unknown
	}
	for _, tt := range tests {
		ok, known := versionsCompatible(tt.a, tt.b)
		if ok != tt.compatible || known != tt.known {
			t.Errorf("versionsCompatible(%q, %q) = %v, %v; want %v, %v", tt.a, tt.b, ok, known, tt.compatible, tt.known)
		}
	}
}

func TestImageTag(t *testing.T) {
	tests := map[string]string{
		"docker.io/org/runner:2.4.0":               "2.4.0",
		"registry.local:5000/org/runner:2.3.1":     "2.3.1",
		"registry.local:5000/org/runner":           "",
		"org/runner:2.4.0@sha256:0123456789abcdef": "2.4.0",
		"org/runner@sha256:0123456789abcdef":       "",
		"runner":                                   "",
	}
	for image, want := range tests {
		if got := imageTag(image); got != want {
			t.Errorf("imageTag(%q) = %q, want %q", image, got, want)
		}
	}
}

// withRunnerImage serves a runner CronJob whose container uses image.
func withRunnerImage(t *testing.T, image string) *apiServer {
	t.Helper()
	f, srv := newFakeCronJobAPI(t, "ncc-v2-runner", "15 */4 * * *")
	f.obj["spec"].(map[string]interface{})["jobTemplate"] = map[string]interface{}{
		"spec": map[string]interface{}{"template": map[string]interface{}{"spec": map[string]interface{}{
			"containers": []interface{}{
				map[string]interface{}{"name": "sidecar", "image": "busybox:1.37"},
				map[string]interface{}{"name": "ncc-orchestrator", "image": image},
			},
		}}},
	}
	return newK8sScheduleServer(srv, "ncc-v2-runner")
}

func withoutCronJob() *apiServer {
	s := &apiServer{}
	s.capabilities.Kubernetes = true
	s.scheduleK8sClient = func() (*k8sSecretClient, error) { return nil, errors.New("no cluster") }
	return s
}

func TestRunnerVersionPrefersCronJobImage(t *testing.T) {
	t.Setenv("NCC_ORCHESTRATOR_IMAGE_TAG", Version)
	s := withRunnerImage(t, "docker.io/org/runner:1.9.0")
	if v, src := s.k8sRunnerVersion(); v != "1.9.0" || !strings.Contains(src, "ncc-v2-runner") {
		t.Fatalf("k8sRunnerVersion = %q, %q", v, src)
	}
	// Without a readable CronJob the configured tag is used.
	if v, src := withoutCronJob().k8sRunnerVersion(); v != Version || src != "NCC_ORCHESTRATOR_IMAGE_TAG" {
		t.Fatalf("fallback = %q, %q", v, src)
	}
}

func triggerRun(t *testing.T, s *apiServer) (int, envelope) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/trigger", bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.handleRunTrigger(rec, req)
	var env envelope
	_ = json.Unmarshal(rec.Body.Bytes(), &env)
	return rec.Code, env
}

func TestRunTriggerRefusedOnIncompatibleRunner(t *testing.T) {
	t.Setenv("NCC_ORCHESTRATOR_IMAGE_TAG", "")
	major, _ := versionMajorMinor(Version)
	if major == "" {
		t.Skip("API version is not a release version")
	}
	s := withRunnerImage(t, "docker.io/org/runner:0.1.0")
	code, env := triggerRun(t, s)
	if code != http.StatusConflict || env.ErrorCode != errorCodeVersionSkew {
		t.Fatalf("trigger = %d %+v, want 409 %s", code, env, errorCodeVersionSkew)
	}
	if !strings.Contains(env.Error, "0.1.0") || !strings.Contains(env.Error, Version) {
		t.Fatalf("error should name both versions: %q", env.Error)
	}

	// The configured tag alone is enough to refuse when the CronJob is unreadable.
	t.Setenv("NCC_ORCHESTRATOR_IMAGE_TAG", "0.1.0")
	if code, _ := triggerRun(t, withoutCronJob()); code != http.StatusConflict {
		t.Fatalf("env-only skew: trigger = %d, want 409", code)
	}
}

func TestRunTriggerAllowedWhenRunnerCompatibleOrUnknown(t *testing.T) {
	t.Setenv("NCC_ORCHESTRATOR_IMAGE_TAG", "")
	for name, s := range map[string]*apiServer{
		"same version": withRunnerImage(t, "docker.io/org/runner:"+Version),
		"unknown tag":  withRunnerImage(t, "docker.io/org/runner:latest"),
		"no cronjob":   withoutCronJob(),
	} {
		if err := s.runnerVersionSkewError(); err != nil {
			t.Errorf("%s: unexpected skew error: %v", name, err)
		}
	}
	// Host installs are never checked here.
	t.Setenv("NCC_ORCHESTRATOR_IMAGE_TAG", "0.1.0")
	if err := (&apiServer{}).runnerVersionSkewError(); err != nil {
		t.Fatalf("host install: %v", err)
	}
}

func TestComponentsReportKubernetesSkew(t *testing.T) {
	t.Setenv("NCC_IMAGE_TAG", Version)
	t.Setenv("NCC_ORCHESTRATOR_IMAGE_TAG", Version)
	t.Setenv("NCC_UI_IMAGE_TAG", "9.9.9")
	s := withRunnerImage(t, "docker.io/org/runner:0.1.0")
	rec := httptest.NewRecorder()
	s.handleComponents(rec, httptest.NewRequest(http.MethodGet, "/api/v1/components", nil))
	var body struct {
		Data struct {
			Components map[string]json.RawMessage `json:"components"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	c := body.Data.Components
	if string(c["consistent"]) != "false" {
		t.Fatalf("consistent = %s, want false", c["consistent"])
	}
	var msg string
	_ = json.Unmarshal(c["message"], &msg)
	for _, want := range []string{"runner image is 0.1.0", "UI image is 9.9.9", "NCC_ORCHESTRATOR_IMAGE_TAG is " + Version} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q missing %q", msg, want)
		}
	}
	if _, ok := c["runner_note"]; ok {
		t.Error("runner_note must not leak into the response")
	}
	var orch component
	_ = json.Unmarshal(c["orchestrator"], &orch)
	if orch.Version != "0.1.0" {
		t.Errorf("orchestrator version = %q, want the CronJob's 0.1.0", orch.Version)
	}
}

func TestComponentsConsistentHasNoMessage(t *testing.T) {
	t.Setenv("NCC_IMAGE_TAG", Version)
	t.Setenv("NCC_ORCHESTRATOR_IMAGE_TAG", Version)
	t.Setenv("NCC_UI_IMAGE_TAG", Version)
	s := withRunnerImage(t, "docker.io/org/runner:v"+Version)
	rec := httptest.NewRecorder()
	s.handleComponents(rec, httptest.NewRequest(http.MethodGet, "/api/v1/components", nil))
	if strings.Contains(rec.Body.String(), `"message"`) || !strings.Contains(rec.Body.String(), `"consistent":true`) {
		t.Fatalf("unexpected body %s", rec.Body.String())
	}
}
