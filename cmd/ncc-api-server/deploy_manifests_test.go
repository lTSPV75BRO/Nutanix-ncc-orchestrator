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

// Every container of the API, UI and runner runs with a read-only root
// filesystem (HARD-01), so each needs a writable /tmp next to /data. One
// securityContext per container: init containers count too.
func TestManifestsUseReadOnlyRootFilesystem(t *testing.T) {
	helm := filepath.Join("..", "..", "helm", "ncc-orchestrator", "templates")
	k8s := filepath.Join("..", "..", "k8s")
	for file, containers := range map[string]int{
		filepath.Join(helm, "api.yaml"):           2,
		filepath.Join(helm, "ui.yaml"):            1,
		filepath.Join(helm, "cronjob.yaml"):       2,
		filepath.Join(k8s, "api-deployment.yaml"): 3,
		filepath.Join(k8s, "ui-deployment.yaml"):  1,
		filepath.Join(k8s, "runner-cronjob.yaml"): 2,
	} {
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		text := string(b)
		for _, want := range []string{"readOnlyRootFilesystem: ", "allowPrivilegeEscalation: false"} {
			if got := strings.Count(text, want); got != containers {
				t.Errorf("%s: %q on %d of %d containers", file, want, got, containers)
			}
		}
		if !strings.Contains(text, "mountPath: /tmp") {
			t.Errorf("%s: no writable /tmp", file)
		}
	}
	values, err := os.ReadFile(filepath.Join("..", "..", "helm", "ncc-orchestrator", "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(values), "\nreadOnlyRootFilesystem: true") {
		t.Error("values.yaml: readOnlyRootFilesystem is not true by default")
	}
}

// Neither installer may delete an existing install (user database, token,
// backups), and both look for the release under its tag name (WIN-03).
func TestInstallersKeepExistingData(t *testing.T) {
	for file, wants := range map[string][]string{
		filepath.Join("..", "..", "deploy", "cloud-init", "ncc-v2.yaml"): {"NCC_FORCE_CLEAN", "gh attestation verify", `for tag in "v$version" "$version"`},
		filepath.Join("..", "..", "deploy", "windows", "setup-ncc.ps1"):  {"NCC_FORCE_CLEAN", `"attestation", "verify"`, `@("v$NccVersion", $NccVersion)`},
	} {
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range wants {
			if !strings.Contains(string(b), want) {
				t.Errorf("%s: %q is missing", file, want)
			}
		}
	}
}
