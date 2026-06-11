package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func TestParseSimpleYAMLKV(t *testing.T) {
	cases := []struct {
		line string
		key  string
		val  string
		ok   bool
	}{
		{`output-dir-logs: "nccfiles"`, "output-dir-logs", "nccfiles", true},
		{`output-dir-filtered: outputfiles   # generated HTML/CSV`, "output-dir-filtered", "outputfiles", true},
		{`username: admin`, "username", "admin", true},
		{`# a comment`, "", "", false},
		{`   `, "", "", false},
		{`  - listitem`, "", "", false},
		{`novalue`, "", "", false},
	}
	for _, c := range cases {
		k, v, ok := parseSimpleYAMLKV(c.line)
		if ok != c.ok || k != c.key || v != c.val {
			t.Errorf("parseSimpleYAMLKV(%q) = (%q,%q,%v), want (%q,%q,%v)", c.line, k, v, ok, c.key, c.val, c.ok)
		}
	}
}

func TestRelativeAndAbsolutizeOutputDirKeys(t *testing.T) {
	yaml := "clusters: \"10.0.0.1\"\n" +
		"output-dir-logs: \"nccfiles\"\n" +
		"output-dir-filtered: outputfiles   # comment kept\n" +
		"run-history-dir: /abs/runs\n"

	rel := relativeOutputDirKeys(yaml)
	if len(rel) != 2 || !strInList(rel, "output-dir-logs") || !strInList(rel, "output-dir-filtered") {
		t.Fatalf("relativeOutputDirKeys = %v, want output-dir-logs + output-dir-filtered (absolute run-history-dir excluded)", rel)
	}

	base := filepath.Join(string(filepath.Separator), "opt", "ncc")
	out, changed := absolutizeOutputDirKeys(yaml, base)
	if !changed {
		t.Fatal("expected absolutizeOutputDirKeys to change the document")
	}
	if !strings.Contains(out, filepath.Join(base, "nccfiles")) || !strings.Contains(out, filepath.Join(base, "outputfiles")) {
		t.Errorf("rewritten doc missing anchored paths:\n%s", out)
	}
	// The inline comment and the already-absolute key must be preserved.
	if !strings.Contains(out, "# comment kept") {
		t.Error("inline comment was dropped during rewrite")
	}
	if !strings.Contains(out, "run-history-dir: /abs/runs") {
		t.Error("already-absolute key must be left untouched")
	}
}

// TestRunSelfHealFixesRoutingAndDirs exercises the end-to-end self-heal run in
// --fix mode against a temp install dir whose config has relative output dirs.
func TestRunSelfHealFixesRoutingAndDirs(t *testing.T) {
	viper.Reset()
	defer viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	body := "clusters: \"10.0.0.1\"\n" +
		"username: \"admin\"\n" +
		"output-dir-logs: \"nccfiles\"\n" +
		"output-dir-filtered: \"outputfiles\"\n"
	if err := os.WriteFile(cfgPath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	hr := runSelfHeal(dir, cfgPath, true)

	byID := map[string]healResult{}
	for _, r := range hr.Results {
		byID[r.ID] = r
	}

	routing := byID["config-output-routing"]
	if routing.Status != healOK || !routing.Fixed {
		t.Errorf("config-output-routing: got status=%s fixed=%v msg=%q", routing.Status, routing.Fixed, routing.Message)
	}
	// The on-disk config must now hold absolute paths anchored to the config dir.
	rewritten, _ := os.ReadFile(cfgPath)
	if !strings.Contains(string(rewritten), filepath.Join(dir, "outputfiles")) {
		t.Errorf("config not rewritten to absolute path:\n%s", rewritten)
	}

	dirs := byID["output-dirs-writable"]
	if dirs.Status != healOK {
		t.Errorf("output-dirs-writable: got status=%s msg=%q", dirs.Status, dirs.Message)
	}
	if _, err := os.Stat(filepath.Join(dir, "outputfiles")); err != nil {
		t.Errorf("expected outputfiles dir to be created: %v", err)
	}
}

func TestVerifyBackupArchiveAndRetention(t *testing.T) {
	dir := t.TempDir()
	installDir := filepath.Join(dir, "install")
	if err := os.MkdirAll(installDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// A minimal but backup-worthy install dir (config.yaml is collected).
	if err := os.WriteFile(filepath.Join(installDir, "config.yaml"), []byte("clusters: \"10.0.0.1\"\nusername: admin\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	out, err := selfHealCreateBackup(installDir)
	if err != nil {
		t.Fatalf("selfHealCreateBackup: %v", err)
	}
	vr, err := verifyBackupArchive(out)
	if err != nil {
		t.Fatalf("verifyBackupArchive: %v", err)
	}
	if vr.DataFiles == 0 || vr.Manifest.Tool != "ncc-orchestrator" {
		t.Fatalf("unexpected verify result: %+v", vr)
	}

	// A truncated/garbage file must fail verification, not pass silently.
	bad := filepath.Join(dir, "bad.tar.gz")
	if err := os.WriteFile(bad, []byte("not a real gzip archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyBackupArchive(bad); err == nil {
		t.Fatal("expected verification to fail on a non-archive file")
	}

	// Retention keeps only the newest N.
	bdir := filepath.Join(installDir, "backups")
	for i := 0; i < 4; i++ {
		f := filepath.Join(bdir, fmt.Sprintf("ncc-backup-2026010%dT000000Z.tar.gz", i+1))
		if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		// Stagger mtimes so ordering is deterministic.
		mt := time.Now().Add(time.Duration(i) * time.Minute)
		_ = os.Chtimes(f, mt, mt)
	}
	pruned, err := pruneOldBackups(bdir, 2)
	if err != nil {
		t.Fatalf("pruneOldBackups: %v", err)
	}
	remaining, _ := filepath.Glob(filepath.Join(bdir, "*.tar.gz"))
	if len(remaining) != 2 {
		t.Fatalf("expected 2 backups after prune, got %d (pruned %v)", len(remaining), pruned)
	}
}

func TestCheckStalePIDs(t *testing.T) {
	dir := t.TempDir()
	runDir := filepath.Join(dir, "run")
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// A pid that is essentially never alive.
	deadPID := filepath.Join(runDir, "v2-api.pid")
	if err := os.WriteFile(deadPID, []byte("999999\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Read-only: warns, leaves the file.
	res := checkStalePIDs(&healContext{InstallDir: dir, Fix: false})
	if res.Status != healWarn {
		t.Errorf("read-only stale pid: status=%s, want warn", res.Status)
	}
	if !fileExists(deadPID) {
		t.Error("read-only run must not delete the pid file")
	}

	// --fix: cleans it.
	res = checkStalePIDs(&healContext{InstallDir: dir, Fix: true})
	if res.Status != healOK || !res.Fixed {
		t.Errorf("--fix stale pid: status=%s fixed=%v", res.Status, res.Fixed)
	}
	if fileExists(deadPID) {
		t.Error("--fix should have removed the stale pid file")
	}
}

func TestCheckLogSizes(t *testing.T) {
	dir := t.TempDir()
	logDir := filepath.Join(dir, "logs")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatal(err)
	}
	big := filepath.Join(logDir, "v2-api.log")
	// Sparse 51 MiB file (>50 MiB cap) so Stat reports an oversized log.
	if err := os.Truncate(big, 51<<20); err != nil {
		// Truncate requires the file to exist on some platforms.
		_ = os.WriteFile(big, nil, 0o644)
		if err := os.Truncate(big, 51<<20); err != nil {
			t.Fatal(err)
		}
	}
	res := checkLogSizes(&healContext{InstallDir: dir, Fix: true})
	if res.Status != healOK || !res.Fixed {
		t.Errorf("--fix log-sizes: status=%s fixed=%v msg=%q", res.Status, res.Fixed, res.Message)
	}
	if !fileExists(big + ".1") {
		t.Error("oversized log should have been rotated to .1")
	}
}

func TestBuildUIHealthProbeCmd(t *testing.T) {
	if got := buildUIHealthProbeCmd("", ":8080", false); got != "" {
		t.Errorf("empty bin must yield empty cmd, got %q", got)
	}
	httpCmd := buildUIHealthProbeCmd("/opt/ncc/ncc-ui-server", ":8080", false)
	if !strings.Contains(httpCmd, "--health-check") || !strings.Contains(httpCmd, "--listen") || strings.Contains(httpCmd, "--tls-cert-file") {
		t.Errorf("unexpected http ui probe cmd: %q", httpCmd)
	}
	httpsCmd := buildUIHealthProbeCmd("/opt/ncc/ncc-ui-server", ":8443", true)
	if !strings.Contains(httpsCmd, "--tls-cert-file") || !strings.Contains(httpsCmd, "--tls-key-file") {
		t.Errorf("https ui probe cmd should carry tls flags for scheme selection: %q", httpsCmd)
	}
}

func TestCheckSecretsPermsChmod(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX mode bits not meaningful on windows")
	}
	dir := t.TempDir()
	secret := filepath.Join(dir, ".ncc-api-token")
	if err := os.WriteFile(secret, []byte("tok"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Read-only run should warn, not modify.
	hc := &healContext{InstallDir: dir, Fix: false}
	res := checkSecretsPerms(hc)
	if res.Status != healWarn {
		t.Errorf("read-only: got status=%s, want warn", res.Status)
	}
	if info, _ := os.Stat(secret); info.Mode().Perm() != 0o644 {
		t.Errorf("read-only run must not change perms; got %04o", info.Mode().Perm())
	}

	// --fix run should tighten to 0600.
	hcFix := &healContext{InstallDir: dir, Fix: true}
	res = checkSecretsPerms(hcFix)
	if res.Status != healOK || !res.Fixed {
		t.Errorf("--fix: got status=%s fixed=%v", res.Status, res.Fixed)
	}
	if info, _ := os.Stat(secret); info.Mode().Perm() != 0o600 {
		t.Errorf("--fix should chmod to 0600; got %04o", info.Mode().Perm())
	}
}
