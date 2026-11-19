package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// In a container the API binary sits in /usr/local/bin next to the
// orchestrator, which the stack-aware defaults mistake for an extracted v2
// stack and then point log-dir, token-file-path and users-db at /usr/local
// (not writable by uid 1000). Kubernetes `args` replace the image's CMD, so
// every deployment must pass these flags itself. The Helm chart once passed
// only a subset and the API pod crash-looped on first start.
func TestKubernetesAPIArgsPinWritablePaths(t *testing.T) {
	required := []string{
		`"--repo-root"`, `"--log-dir"`, `"--token-file-path"`, `"--audit-log-path"`,
		`"--runner-log-path"`, `"--users-db"`, `"--users-db-secret"`,
	}
	for _, file := range []string{
		filepath.Join("..", "..", "helm", "ncc-orchestrator", "templates", "api.yaml"),
		filepath.Join("..", "..", "k8s", "api-deployment.yaml"),
	} {
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		text := string(b)
		for _, flag := range required {
			if !strings.Contains(text, flag) {
				t.Errorf("%s does not pass %s explicitly", file, flag)
			}
		}
	}
}

// The runner image's WORKDIR (/workspace) is not writable by uid 1000, so a
// relative logging.file in the config failed with "mkdir logs: permission
// denied" and the Helm CronJob never completed a run. Both CronJob manifests
// must pass explicit writable paths (and skip the secret preflight like the
// k8s/ one), and the chart must run from the PVC.
func TestRunnerCronJobPinsWritablePaths(t *testing.T) {
	required := []string{`"--log-file"`, `"--output-dir-filtered"`, `"--output-dir-logs"`, `"--run-history-dir"`, `"--skip-preflight-check"`}
	for _, file := range []string{
		filepath.Join("..", "..", "helm", "ncc-orchestrator", "templates", "cronjob.yaml"),
		filepath.Join("..", "..", "k8s", "runner-cronjob.yaml"),
	} {
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, flag := range required {
			if !strings.Contains(string(b), flag) {
				t.Errorf("%s does not pass %s", file, flag)
			}
		}
	}
	b, err := os.ReadFile(filepath.Join("..", "..", "helm", "ncc-orchestrator", "templates", "cronjob.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "workingDir: /data") {
		t.Error("the Helm CronJob must run from /data, not the image WORKDIR")
	}
}

// The chart-seeded config is what the API uses for Settings → Run now. Without
// skip-preflight-check and secrets.provider=env, that path failed
// NCC_PREFLIGHT_VALIDATE_SECRETS even though the CronJob CLI skipped it.
func TestHelmSeedConfigSkipsSecretPreflight(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "helm", "ncc-orchestrator", "templates", "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	if !strings.Contains(text, "skip-preflight-check: true") {
		t.Error("helm seed config must skip preflight so Run now matches the CronJob")
	}
	if !strings.Contains(text, "provider: env") {
		t.Error("helm seed config must use secrets.provider env (NCC_PASSWORD from the Secret)")
	}
}
