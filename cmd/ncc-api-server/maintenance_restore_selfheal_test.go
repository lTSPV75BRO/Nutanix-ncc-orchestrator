package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestHealMissingTLSPairFromSelfSignedFallback(t *testing.T) {
	dir := t.TempDir()
	tlsDir := filepath.Join(dir, "tls")
	if err := os.MkdirAll(tlsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	fallbackCert := filepath.Join(tlsDir, "ui-selfsigned.crt")
	fallbackKey := filepath.Join(tlsDir, "ui-selfsigned.key")
	if err := os.WriteFile(fallbackCert, []byte("CERT"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fallbackKey, []byte("KEY"), 0o600); err != nil {
		t.Fatal(err)
	}

	targetCert := filepath.Join(tlsDir, "ui.crt")
	targetKey := filepath.Join(tlsDir, "ui.key")
	if !healMissingTLSPair(targetCert, targetKey, dir) {
		t.Fatal("expected TLS pair to be healed from fallback")
	}
	if _, err := os.Stat(targetCert); err != nil {
		t.Fatalf("expected cert created: %v", err)
	}
	if _, err := os.Stat(targetKey); err != nil {
		t.Fatalf("expected key created: %v", err)
	}
}

func TestNormalizeScheduleStatePaths(t *testing.T) {
	installDir := t.TempDir()
	statePath := filepath.Join(installDir, ".ncc-api-schedule.json")
	if err := os.WriteFile(statePath, []byte(`{"config":"/root/config.yaml","log_path":"/root/logs/ncc-scheduler.log"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if !normalizeScheduleStatePaths(statePath, installDir) {
		t.Fatal("expected schedule state paths to be normalized")
	}
	raw, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got["config"] != filepath.Join(installDir, "config.yaml") {
		t.Fatalf("unexpected config path: %q", got["config"])
	}
	if got["log_path"] != filepath.Join(installDir, "logs", "ncc-scheduler.log") {
		t.Fatalf("unexpected log path: %q", got["log_path"])
	}
}

func TestNormalizeConfigOutputDirPaths(t *testing.T) {
	installDir := t.TempDir()
	configPath := filepath.Join(installDir, "config.yaml")
	original := "clusters: 10.0.0.1\n" +
		"output-dir-logs: /root/ncc-orchestrator/nccfiles\n" +
		"output-dir-filtered: /root/ncc-orchestrator/outputfiles   # generated HTML/CSV\n" +
		"run-history-dir: /root/ncc-orchestrator/outputfiles/runs\n" +
		"log-file: logs/ncc-runner.log\n"
	if err := os.WriteFile(configPath, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	if !normalizeConfigOutputDirPaths(configPath, installDir) {
		t.Fatal("expected config output-dir paths to be normalized")
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	got := string(raw)
	wantLogs := filepath.Join(installDir, "nccfiles")
	wantFiltered := filepath.Join(installDir, "outputfiles")
	wantHistory := filepath.Join(installDir, "outputfiles", "runs")
	if !strings.Contains(got, "output-dir-logs: "+wantLogs) {
		t.Errorf("output-dir-logs not re-anchored, got:\n%s", got)
	}
	if !strings.Contains(got, "output-dir-filtered: "+wantFiltered+"   # generated HTML/CSV") {
		t.Errorf("output-dir-filtered not re-anchored (or comment lost), got:\n%s", got)
	}
	if !strings.Contains(got, "run-history-dir: "+wantHistory) {
		t.Errorf("run-history-dir not re-anchored, got:\n%s", got)
	}
	// A relative path (log-file) is untouched — that's the root package's
	// relative-path self-heal's job, not this restore-specific fixer's.
	if !strings.Contains(got, "log-file: logs/ncc-runner.log") {
		t.Errorf("unrelated relative key was unexpectedly touched, got:\n%s", got)
	}

	// Re-running against already-correct paths is a no-op.
	if normalizeConfigOutputDirPaths(configPath, installDir) {
		t.Fatal("expected no further changes once paths are already anchored to installDir")
	}
}

func TestNormalizeConfigOutputDirPathsLeavesCustomExternalPathAlone(t *testing.T) {
	installDir := t.TempDir()
	configPath := filepath.Join(installDir, "config.yaml")
	original := "output-dir-filtered: /data/ncc-reports\n"
	if err := os.WriteFile(configPath, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	if normalizeConfigOutputDirPaths(configPath, installDir) {
		t.Fatal("a deliberately external (non-/root/) path should not be rewritten")
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != original {
		t.Fatalf("file unexpectedly modified: %q", string(raw))
	}
}

func TestPostRestoreSelfHealIncludesConfigOutputDirNormalizationNote(t *testing.T) {
	installDir := t.TempDir()
	logsDir := filepath.Join(installDir, "logs")
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(logsDir, "ncc-sched-ncc-orchestrator.sh"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(installDir, "config.yaml"), []byte(
		"output-dir-filtered: /root/ncc-orchestrator/outputfiles\n"+
			"output-dir-logs: /root/ncc-orchestrator/nccfiles\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := &apiServer{
		repoRoot:   installDir,
		configPath: filepath.Join(installDir, "config.yaml"),
	}
	notes := s.postRestoreValidateAndSelfHeal(installDir)
	joined := strings.Join(notes, " | ")
	if !strings.Contains(joined, "re-anchored config.yaml output-dir-filtered/output-dir-logs/run-history-dir to install-root") {
		t.Fatalf("expected config output-dir normalization note, got: %v", notes)
	}
	raw, err := os.ReadFile(filepath.Join(installDir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "output-dir-filtered: "+filepath.Join(installDir, "outputfiles")) {
		t.Fatalf("config.yaml not re-anchored via postRestoreValidateAndSelfHeal: %s", string(raw))
	}
}

func TestPostRestoreSelfHealIncludesSchedulerNormalizationNote(t *testing.T) {
	installDir := t.TempDir()
	logsDir := filepath.Join(installDir, "logs")
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Keep artifact healer from invoking orchestrator by presenting an existing runner.
	if err := os.WriteFile(filepath.Join(logsDir, "ncc-sched-ncc-orchestrator.sh"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(installDir, ".ncc-api-schedule.json"), []byte(`{
  "type":"systemd",
  "action":"create",
  "task_name":"ncc-orchestrator",
  "config":"/root/config.yaml",
  "log_path":"/root/logs/ncc-scheduler.log"
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	s := &apiServer{
		repoRoot:   installDir,
		configPath: filepath.Join(installDir, "config.yaml"),
	}
	notes := s.postRestoreValidateAndSelfHeal(installDir)
	joined := strings.Join(notes, " | ")
	if !strings.Contains(joined, "normalized scheduler state paths to install-root defaults") {
		t.Fatalf("expected normalization note, got: %v", notes)
	}
}

// A start state without TLS paths (plain HTTP, or the self-signed default)
// must not be "repaired" into files named "<nil>".
func TestPostRestoreSelfHealIgnoresMissingTLSPaths(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.MkdirAll(filepath.Join(dir, "tls"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		startStateFileName:      `{"ui_listen":":8080"}`,
		"tls/ui-selfsigned.crt": "CERT",
		"tls/ui-selfsigned.key": "KEY",
	} {
		if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(name)), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	s := &apiServer{configPath: filepath.Join(dir, "config.yaml")}
	if notes := s.postRestoreValidateAndSelfHeal(dir); len(notes) != 0 {
		t.Errorf("notes = %v", notes)
	}
	if _, err := os.Stat(filepath.Join(dir, "<nil>")); err == nil {
		t.Error(`wrote a file named "<nil>"`)
	}
}

// A restore re-created a "15 2 * * *" schedule as every 4 hours, and turned the
// lock on: the cron expression was dropped, and "--with-lock false" as two
// arguments parses as true.
func TestHealMissingScheduleArtifactsKeepsCronAndLock(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("needs a POSIX shell stub")
	}
	dir := t.TempDir()
	bin, calls := filepath.Join(dir, "orch"), filepath.Join(dir, "calls")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nprintf '[%s]' \"$@\" > '"+calls+"'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	// The API leaves with_lock out when it is off.
	state := `{"type":"cron","action":"create","cron":"15 2 * * *"}`
	if err := os.WriteFile(filepath.Join(dir, scheduleStateFileName), []byte(state), 0o600); err != nil {
		t.Fatal(err)
	}
	s := &apiServer{repoRoot: dir, orchestratorBin: bin}
	if healed, _ := s.healMissingScheduleArtifacts(dir); !healed {
		t.Fatal("not healed")
	}
	got, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"[--cron][15 2 * * *]", "[--with-lock=false]"} {
		if !strings.Contains(string(got), want) {
			t.Errorf("args lack %s: %s", want, got)
		}
	}
}
