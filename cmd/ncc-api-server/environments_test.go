package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	yaml "go.yaml.in/yaml/v3"
)

const testMainConfig = `schema-version: 1
runner:
  targets:
    clusters: ["10.0.0.1"]
  credentials:
    username: admin
  execution:
    timeout: 15m
storage:
  output-dir: outputfiles
notifications:
  webhook-url: https://hooks.example.com/main
api:
  listen: ":9000"
`

func envTestServer(t *testing.T) *apiServer {
	t.Helper()
	tmp := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmp, "config.yaml"), []byte(testMainConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(tmp, "configs", "environments"), 0o755); err != nil {
		t.Fatal(err)
	}
	return &apiServer{repoRoot: tmp, configPath: "config.yaml", outputDir: "outputfiles"}
}

func writeEnv(t *testing.T, s *apiServer, name, content string) {
	t.Helper()
	p, err := s.environmentFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestParseEnvironmentRefusesServerSettings(t *testing.T) {
	if _, err := parseEnvironment([]byte("runner:\n  targets:\n    clusters: [a]\n")); err != nil {
		t.Fatalf("runner-only file refused: %v", err)
	}
	_, err := parseEnvironment([]byte("runner: {}\nstorage:\n  output-dir: elsewhere\napi:\n  listen: ':1'\n"))
	if err == nil || !strings.Contains(err.Error(), "api, storage") {
		t.Fatalf("want refusal naming api, storage; got %v", err)
	}
	if _, err := parseEnvironment([]byte("runner: [1, 2]\n")); err == nil {
		t.Fatal("runner as a list should be refused")
	}
}

func TestEnvironmentNames(t *testing.T) {
	for _, ok := range []string{"prod", "staging-eu", "dev2"} {
		if validEnvironmentName(ok) != nil {
			t.Errorf("%q should be valid", ok)
		}
	}
	for _, bad := range []string{"", "Prod", "../x", "-dev", "a b", strings.Repeat("a", 41), ".effective"} {
		if validEnvironmentName(bad) == nil {
			t.Errorf("%q should be invalid", bad)
		}
	}
}

func TestMergedEnvironmentKeepsServerSettings(t *testing.T) {
	s := envTestServer(t)
	writeEnv(t, s, "prod", `description: Production
runner:
  targets:
    clusters: ["10.9.0.1", "10.9.0.2"]
  execution:
    max-parallel: 8
`)
	path, err := s.materializeEnvironment("prod")
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]interface{}{}
	if err := yaml.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got["description"]; ok {
		t.Error("description must not reach the runner config")
	}
	if asStringMap(got["storage"])["output-dir"] != "outputfiles" {
		t.Errorf("storage changed: %v", got["storage"])
	}
	if asStringMap(got["notifications"])["webhook-url"] != "https://hooks.example.com/main" {
		t.Errorf("notifications changed: %v", got["notifications"])
	}
	runner := asStringMap(got["runner"])
	if c := stringList(asStringMap(runner["targets"])["clusters"]); strings.Join(c, ",") != "10.9.0.1,10.9.0.2" {
		t.Errorf("clusters = %v", c)
	}
	exec := asStringMap(runner["execution"])
	if exec["timeout"] != "15m" || exec["max-parallel"] != 8 {
		t.Errorf("execution should merge main and environment: %v", exec)
	}
	if asStringMap(runner["credentials"])["username"] != "admin" {
		t.Error("credentials from the main config should carry over")
	}
}

func TestResolveRunConfig(t *testing.T) {
	s := envTestServer(t)
	writeEnv(t, s, "dev", "runner:\n  targets:\n    clusters: [10.1.0.1]\n")
	if err := os.WriteFile(filepath.Join(s.repoRoot, "configs", "old.yaml"), []byte(testMainConfig), 0o600); err != nil {
		t.Fatal(err)
	}

	main, err := s.resolveRunConfig(nil, "", "")
	if err != nil || main.environment != "" || !strings.HasSuffix(main.path, "config.yaml") {
		t.Fatalf("default should be the main config: %+v %v", main, err)
	}
	dev, err := s.resolveRunConfig(nil, "dev", "")
	if err != nil || dev.environment != "dev" || len(dev.clusters) != 1 {
		t.Fatalf("dev: %+v %v", dev, err)
	}
	byPath, err := s.resolveRunConfig(nil, "", "configs/environments/dev.yaml")
	if err != nil || byPath.environment != "dev" {
		t.Fatalf("an environment file path should run as that environment: %+v %v", byPath, err)
	}
	if _, err := s.resolveRunConfig(nil, "", "configs/old.yaml"); err == nil {
		t.Fatal("a full config other than the main one must be refused")
	}
	if _, err := s.resolveRunConfig(nil, "missing", ""); err == nil {
		t.Fatal("unknown environment should fail")
	}
}

func TestConfineToEnvironment(t *testing.T) {
	choice := runConfigChoice{environment: "prod", clusters: []string{"10.0.0.1", "10.0.0.2"}}
	got, err := confineToEnvironment([]string{"10.0.0.2", "10.5.5.5"}, choice, false)
	if err != nil || strings.Join(got, ",") != "10.0.0.2" {
		t.Fatalf("got %v %v", got, err)
	}
	if _, err := confineToEnvironment([]string{"10.5.5.5"}, choice, false); err == nil {
		t.Fatal("no overlap should be refused")
	}
	if got, _ := confineToEnvironment([]string{"10.5.5.5"}, choice, true); len(got) != 1 {
		t.Fatal("unrestricted callers are not narrowed")
	}
}

func TestListEnvironmentsSkipsHelpers(t *testing.T) {
	s := envTestServer(t)
	writeEnv(t, s, "prod", "description: Production\nrunner:\n  targets:\n    clusters: [a, b, c]\n")
	writeEnv(t, s, "dev", "storage: {}\n")
	if _, err := s.materializeEnvironment("prod"); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(s.environmentsDir(), "Not A Name.yaml"), []byte("runner: {}\n"), 0o600)
	items := s.listEnvironments()
	if len(items) != 2 || items[0].Name != "dev" || items[1].Name != "prod" {
		t.Fatalf("items = %+v", items)
	}
	if items[0].Error == "" {
		t.Error("dev holds server settings and should report an error")
	}
	if items[1].ClusterCount != 3 || items[1].Description != "Production" {
		t.Errorf("prod = %+v", items[1])
	}
}

func TestNextEnvironmentRun(t *testing.T) {
	loc := time.UTC
	created := time.Date(2026, 11, 8, 10, 0, 0, 0, loc)
	e := environmentSchedule{Enabled: true, Every: "4h", CreatedAt: created.Format(time.RFC3339)}
	if got := nextEnvironmentRun(e, loc); !got.Equal(created.Add(4 * time.Hour)) {
		t.Errorf("interval: %v", got)
	}
	e = environmentSchedule{Enabled: true, Every: "1d", At: "02:00", CreatedAt: created.Format(time.RFC3339)}
	if got := nextEnvironmentRun(e, loc); !got.Equal(time.Date(2026, 11, 9, 2, 0, 0, 0, loc)) {
		t.Errorf("first daily run: %v", got)
	}
	e.LastRunAt = time.Date(2026, 11, 9, 2, 3, 0, 0, loc).Format(time.RFC3339)
	if got := nextEnvironmentRun(e, loc); !got.Equal(time.Date(2026, 11, 10, 2, 0, 0, 0, loc)) {
		t.Errorf("next daily run: %v", got)
	}
	e.Enabled = false
	if !nextEnvironmentRun(e, loc).IsZero() {
		t.Error("disabled entries never run")
	}
}

func TestValidateEnvironmentSchedule(t *testing.T) {
	exists := func(n string) bool { return n == "prod" }
	ok := environmentSchedule{Environment: "prod", Every: "1d", At: "02:00"}
	if err := validateEnvironmentSchedule(ok, exists); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []environmentSchedule{
		{Environment: "nope", Every: "1d"},
		{Environment: "prod", Every: "5m"},
		{Environment: "prod", Every: "4h", At: "02:00"},
		{Environment: "prod", Every: "1d", At: "25:00"},
	} {
		if validateEnvironmentSchedule(bad, exists) == nil {
			t.Errorf("%+v should be refused", bad)
		}
	}
}

func TestResolveRunConfigStalePreferenceFallsBack(t *testing.T) {
	s := envTestServer(t)
	writeEnv(t, s, "dev", "runner:\n  targets:\n    clusters: [10.1.0.1]\n")
	if err := os.WriteFile(filepath.Join(s.repoRoot, "configs", "old.yaml"), []byte(testMainConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	s.users = newUserDB("")
	s.users.accounts["ops"] = &account{}
	req := withPrincipal(httptest.NewRequest(http.MethodPost, "/api/v1/runs/trigger", nil), principal{role: RoleOperator, subject: "ops"})

	for _, pref := range []string{"configs/old.yaml", "env:gone"} {
		s.users.accounts["ops"].RunConfigPath = pref
		got, err := s.resolveRunConfig(req, "", "")
		if err != nil || got.environment != "" || !strings.HasSuffix(got.path, "config.yaml") {
			t.Fatalf("preference %q should fall back to the main config: %+v %v", pref, got, err)
		}
	}
	s.users.accounts["ops"].RunConfigPath = "env:dev"
	if got, err := s.resolveRunConfig(req, "", ""); err != nil || got.environment != "dev" {
		t.Fatalf("a valid environment preference should be used: %+v %v", got, err)
	}
	// An explicit choice is never silently replaced.
	if _, err := s.resolveRunConfig(req, "", "configs/old.yaml"); err == nil {
		t.Fatal("an explicit non-environment config_path must still be refused")
	}
}
