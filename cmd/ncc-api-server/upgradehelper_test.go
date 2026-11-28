package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestRollbackOffer(t *testing.T) {
	s := supervisorTestServer(t)
	dir := filepath.Join(s.maintenanceInstallDir(), "run", "upgrade")
	if err := os.MkdirAll(filepath.Join(dir, "snapshot"), 0o700); err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(-time.Hour).UTC()
	write := func(at time.Time, action string) {
		b, _ := json.Marshal(map[string]string{"from": "2.3.0", "to": "2.4.0", "at": at.Format(time.RFC3339), "action": action, "kind": "systemd"})
		if err := os.WriteFile(filepath.Join(dir, "rollback.json"), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(at, "update")
	o := s.readRollbackOffer(time.Now())
	if o == nil || o.To != "2.3.0" || o.From != "2.4.0" || o.Of != "update" || o.Until != at.Add(24*time.Hour).Format(time.RFC3339) {
		t.Fatalf("%+v", o)
	}
	write(time.Now().Add(-25*time.Hour), "adopt")
	if s.readRollbackOffer(time.Now()) != nil {
		t.Error("offered after 24 hours")
	}
	write(at, "adopt")
	_ = os.RemoveAll(filepath.Join(dir, "snapshot"))
	if s.readRollbackOffer(time.Now()) != nil {
		t.Error("offered without a snapshot")
	}
}

// writeUpgradeJob writes a job as the orchestrator does.
func writeUpgradeJob(t *testing.T, s *apiServer, j upgradeJob) {
	t.Helper()
	b, err := json.Marshal(j)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.upgradeStatePath(), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// fakeOrchestrator builds a program that prints its arguments to args.txt and
// answers with out (exit code 1 when fail).
func fakeOrchestrator(t *testing.T, s *apiServer, out string, fail bool) (argsFile string) {
	t.Helper()
	argsFile = filepath.Join(t.TempDir(), "args.txt")
	src := `package main
import ("fmt";"os";"strings")
func main() {
	os.WriteFile(` + quoteGo(argsFile) + `, []byte(strings.Join(os.Args[1:], " ")), 0o644)
	fmt.Print(` + quoteGo(out) + `)
	if ` + map[bool]string{true: "true", false: "false"}[fail] + ` { os.Exit(1) }
}`
	fakeOrchestratorSource(t, s, src)
	return argsFile
}

func fakeOrchestratorSource(t *testing.T, s *apiServer, src string) {
	t.Helper()
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go tool")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(dir, "ncc-orchestrator")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	cmd := exec.Command("go", "build", "-o", bin, "main.go")
	cmd.Dir = dir
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, b)
	}
	s.orchestratorBin = bin
}

// killedOrchestrator builds a program that writes the job and ends with an
// error and no output.
func killedOrchestrator(t *testing.T, s *apiServer, job string) {
	t.Helper()
	fakeOrchestratorSource(t, s, `package main
import "os"
func main() {
	b, _ := os.ReadFile(`+quoteGo(job)+`)
	os.WriteFile(`+quoteGo(s.upgradeStatePath())+`, b, 0o644)
	os.Exit(1)
}`)
}

func quoteGo(v string) string { b, _ := json.Marshal(v); return string(b) }

func TestHelperUpgradeJobsAreActiveWhileTouched(t *testing.T) {
	s := supervisorTestServer(t)
	now := time.Now().UTC().Format(time.RFC3339)
	for phase := range helperUpgradePhases {
		writeUpgradeJob(t, s, upgradeJob{ID: "x", Phase: phase, Runner: "helper", UpdatedAt: now})
		// No supervisor runs while the helper swaps it.
		if j := s.readUpgradeJob(); j == nil || j.Stale || !upgradeJobActive(j) {
			t.Fatalf("%s: %+v", phase, j)
		}
	}
	if s.upgradeInProgress() == "" {
		t.Fatal("runs must wait")
	}
	writeUpgradeJob(t, s, upgradeJob{ID: "x", Phase: "starting", Runner: "helper", UpdatedAt: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)})
	if j := s.readUpgradeJob(); !j.Stale || upgradeJobActive(j) || s.upgradeInProgress() != "" {
		t.Fatalf("out of touch: %+v", j)
	}
	writeUpgradeJob(t, s, upgradeJob{ID: "x", Phase: "done", Runner: "helper", UpdatedAt: now})
	if j := s.readUpgradeJob(); j.Stale || upgradeJobActive(j) {
		t.Fatalf("done: %+v", j)
	}
}

func TestRunsWaitForAnUpgrade(t *testing.T) {
	s := supervisorTestServer(t)
	writeUpgradeJob(t, s, upgradeJob{ID: "x", Phase: "migrating", Runner: "helper", UpdatedAt: time.Now().UTC().Format(time.RFC3339)})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/trigger", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	s.handleRunTrigger(rec, req)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "upgrade is in progress") {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
}

func TestUpdatePlanAndRollbackRoutes(t *testing.T) {
	for _, p := range []string{"/api/v1/settings/update/plan", "/api/v1/settings/update/rollback"} {
		for _, read := range []bool{true, false} {
			if got := routeMinRoleFor(p, read); got != RoleAdmin {
				t.Errorf("%s (read=%v) = %v, want admin", p, read, got)
			}
		}
	}
}

func TestUpdatePlan(t *testing.T) {
	s := supervisorTestServer(t)
	args := fakeOrchestrator(t, s, "warning: x\n{\"current_version\":\"2.4.0\",\"helper\":\"ready\"}\n", false)
	get := func(query string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		s.handleUpdatePlan(rec, httptest.NewRequest(http.MethodGet, "/api/v1/settings/update/plan"+query, nil))
		return rec
	}
	rec := get("?target_version=2.4.1")
	if d := decodeData(t, rec); rec.Code != http.StatusOK || d["helper"] != "ready" {
		t.Fatalf("%d %v", rec.Code, d)
	}
	b, _ := os.ReadFile(args)
	if !strings.HasPrefix(string(b), "upgrade --plan --json --install-dir ") || !strings.HasSuffix(string(b), " --to 2.4.1") {
		t.Fatalf("args = %s", b)
	}
	if rec := get("?target_version=--rollback"); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad version = %d", rec.Code)
	}
	fakeOrchestrator(t, s, "boom", true)
	if rec := get(""); rec.Code != http.StatusBadGateway {
		t.Fatalf("broken program = %d", rec.Code)
	}
	s.capabilities.Kubernetes, s.capabilities.ImmutableUpdates = true, true
	if rec := get(""); rec.Code != http.StatusConflict {
		t.Fatalf("kubernetes = %d", rec.Code)
	}
}

func TestUpdateRollback(t *testing.T) {
	s := supervisorTestServer(t)
	post := func() *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		s.handleUpdateRollback(rec, httptest.NewRequest(http.MethodPost, "/api/v1/settings/update/rollback", bytes.NewReader([]byte("{}"))))
		return rec
	}
	args := fakeOrchestrator(t, s, "ok", false)
	if rec := post(); rec.Code != http.StatusAccepted {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	if b, _ := os.ReadFile(args); !strings.HasPrefix(string(b), "upgrade --rollback --no-wait --install-dir ") {
		t.Fatalf("args = %s", b)
	}
	fakeOrchestrator(t, s, "Error: there is no version to go back to\n", true)
	if rec := post(); rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "no version to go back to") {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	// The helper stops the service, and the program with it.
	bin := s.orchestratorBin
	job := filepath.Join(filepath.Dir(bin), "job.json")
	b, _ := json.Marshal(upgradeJob{ID: "r", Phase: "restoring", Action: "rollback", Runner: "helper", UpdatedAt: time.Now().UTC().Format(time.RFC3339)})
	if err := os.WriteFile(job, b, 0o644); err != nil {
		t.Fatal(err)
	}
	killedOrchestrator(t, s, job)
	if rec := post(); rec.Code != http.StatusAccepted {
		t.Fatalf("started, then stopped with the service: %d %s", rec.Code, rec.Body)
	}
	s.orchestratorBin = bin
	writeUpgradeJob(t, s, upgradeJob{ID: "x", Phase: "saving", Runner: "helper", UpdatedAt: time.Now().UTC().Format(time.RFC3339)})
	if rec := post(); rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "in progress") {
		t.Fatalf("while upgrading: %d %s", rec.Code, rec.Body)
	}
}

func TestHandStartedStackIsUpgradedByTheHelper(t *testing.T) {
	s := supervisorTestServer(t)
	args := fakeOrchestrator(t, s, "", false)
	if s.handStartedStack() {
		t.Fatal("not started with v2-start")
	}
	if err := os.WriteFile(filepath.Join(s.maintenanceInstallDir(), "run", "v2-api.pid"), []byte(itoa(os.Getpid())+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !s.handStartedStack() {
		t.Fatal("started with v2-start")
	}
	t.Setenv("NCC_UPGRADE_HELPER", "0")
	if s.handStartedStack() {
		t.Fatal("helper turned off")
	}
	t.Setenv("NCC_UPGRADE_HELPER", "")
	if err := s.startHandStartedUpgrade(upgradeRequest{ID: "id1", TargetVersion: "2.4.1", BackupPath: "/b.tar.gz", RequestedBy: "ann"}); err != nil {
		t.Fatal(err)
	}
	var got string
	for i := 0; i < 100 && got == ""; i++ {
		b, _ := os.ReadFile(args)
		got = string(b)
		time.Sleep(50 * time.Millisecond)
	}
	for _, want := range []string{"upgrade --no-wait --install-dir ", "--id id1", "--backup-path /b.tar.gz", "--requested-by ann", "--to 2.4.1"} {
		if !strings.Contains(got, want) {
			t.Fatalf("args = %q, want %q", got, want)
		}
	}
	// With a supervisor, the supervisor is asked instead.
	if err := os.WriteFile(filepath.Join(s.maintenanceInstallDir(), "run", "v2-supervisor.pid"), []byte(itoa(os.Getpid())), 0o644); err != nil {
		t.Fatal(err)
	}
	if s.handStartedStack() {
		t.Fatal("supervised")
	}
}

func TestAdoptionRunsOnceNothingElseGoesOn(t *testing.T) {
	first, retry := adoptFirstWait, adoptRetryWait
	adoptFirstWait, adoptRetryWait = time.Millisecond, 10*time.Millisecond
	defer func() { adoptFirstWait, adoptRetryWait = first, retry }()
	s := supervisorTestServer(t)
	args := fakeOrchestrator(t, s, "", false)
	writeUpgradeJob(t, s, upgradeJob{ID: "x", Phase: "checking", Runner: "helper", UpdatedAt: time.Now().UTC().Format(time.RFC3339)})
	done := make(chan struct{})
	go func() { s.adoptInstallation(); close(done) }()
	time.Sleep(30 * time.Millisecond)
	if _, err := os.Stat(args); err == nil {
		t.Fatal("adopted during an upgrade")
	}
	writeUpgradeJob(t, s, upgradeJob{ID: "x", Phase: "done", Runner: "helper", UpdatedAt: time.Now().UTC().Format(time.RFC3339)})
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("not adopted")
	}
	if b, _ := os.ReadFile(args); !strings.HasPrefix(string(b), "upgrade-adopt --auto --install-dir ") {
		t.Fatalf("args = %s", b)
	}
}

func TestUpgradeResultIsAudited(t *testing.T) {
	s := supervisorTestServer(t)
	s.auditLogPath = filepath.Join(t.TempDir(), "audit.log")
	now := time.Now().UTC().Format(time.RFC3339)
	writeUpgradeJob(t, s, upgradeJob{ID: "x", Phase: "done", Action: "upgrade", Runner: "helper", ToVersion: "2.4.1", UpdatedAt: now, FinishedAt: now})
	s.auditUpgradeResult("x")
	if b, _ := os.ReadFile(s.auditLogPath); s.updateAppliedTotal.Load() != 1 || !strings.Contains(string(b), "settings.update.result") {
		t.Fatalf("audit = %s", b)
	}
	writeUpgradeJob(t, s, upgradeJob{ID: "y", Phase: "rolled_back", Action: "upgrade", Runner: "helper", UpdatedAt: now, FinishedAt: now})
	s.auditUpgradeResult("y")
	if s.updateFailedTotal.Load() != 1 {
		t.Fatal("failure not counted")
	}
}
