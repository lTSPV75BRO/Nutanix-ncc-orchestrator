package main

import (
	"encoding/json"
	"os"
	"path/filepath"
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
