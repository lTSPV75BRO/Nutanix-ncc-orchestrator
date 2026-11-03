package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func TestMigrateConfigToNestedKeepsRunnerSettings(t *testing.T) {
	raw := []byte(`# NCC config
clusters: "10.0.0.1,10.0.0.2"   # targets
timeout: 20m
outputs: html,csv
email-enabled: true
api:
  listen: ":8081"
runner:
  execution:
    timeout: 15m
  policy:
    flaky-lookback-runs: 6
  filtering:
    flaky-lookback-runs: 9
`)
	out, res, err := migrateConfigToNested(raw)
	if err != nil {
		t.Fatal(err)
	}
	if res.Layout != "mixed" || !res.Changed {
		t.Fatalf("layout=%q changed=%v", res.Layout, res.Changed)
	}
	got := string(out)
	for _, want := range []string{
		"schema-version: 1",
		"# NCC config",
		"timeout: 20m", // the flat value wins, as at run time
		"clusters: \"10.0.0.1,10.0.0.2\" # targets", // quoting and comments survive
		"listen: \":8081\"",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("migrated file lacks %q:\n%s", want, got)
		}
	}
	if strings.HasPrefix(strings.TrimSpace(got), "runner") || !strings.HasPrefix(got, "# NCC config") {
		t.Errorf("file header should stay first:\n%s", got)
	}
	for _, gone := range []string{"15m", "flaky-lookback-runs: 9", "\ntimeout:", "\nclusters:"} {
		if strings.Contains(got, gone) {
			t.Errorf("migrated file still has %q:\n%s", gone, got)
		}
	}
	before, _ := effectiveConfigSettings(raw)
	after, _ := effectiveConfigSettings(out)
	if d := diffSettings(before, after); d != "" {
		t.Fatalf("settings changed: %s", d)
	}
	// A second pass has nothing left to do.
	if _, again, err := migrateConfigToNested(out); err != nil || again.Changed {
		t.Fatalf("second pass changed=%v err=%v", again.Changed, err)
	}
}

func TestMigrateConfigReportsDuplicates(t *testing.T) {
	raw := []byte("schema-version: 1\ntimeout: 5m\nrunner:\n  execution:\n    timeout: 5m\n  filtering:\n    flaky-lookback-runs: 3\n  policy:\n    flaky-lookback-runs: 4\n")
	dups, err := findConfigDuplicates(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(dups) != 2 {
		t.Fatalf("dups = %+v", dups)
	}
	flaky, timeout := dups[0], dups[1]
	if flaky.Flat != "flaky-lookback-runs" || flaky.Winner != "runner.policy.flaky-lookback-runs" || !flaky.Differs {
		t.Fatalf("flaky = %+v", flaky)
	}
	if timeout.Winner != "timeout" || timeout.Differs {
		t.Fatalf("timeout = %+v", timeout)
	}
}

func TestMigrateConfigRefusesUnsupportedSchema(t *testing.T) {
	if _, _, err := migrateConfigToNested([]byte("schema-version: 2\ntimeout: 5m\n")); err == nil {
		t.Fatal("expected an error for schema-version 2")
	}
	if _, _, err := migrateConfigToNested([]byte("timeout: 5m\nrunner: nope\n")); err == nil {
		t.Fatal("expected an error when runner is not a mapping")
	}
}

func TestMigrateConfigFileWritesBackup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	orig := "clusters: 10.0.0.1\nusername: admin\n"
	if err := os.WriteFile(path, []byte(orig), 0o600); err != nil {
		t.Fatal(err)
	}
	dry, err := migrateConfigFile(path, true)
	if err != nil || dry.Applied || dry.Content == "" {
		t.Fatalf("dry run: %+v %v", dry, err)
	}
	if b, _ := os.ReadFile(path); string(b) != orig {
		t.Fatal("dry run wrote the file")
	}
	res, err := migrateConfigFile(path, false)
	if err != nil || !res.Applied {
		t.Fatalf("apply: %+v %v", res, err)
	}
	if b, _ := os.ReadFile(res.Backup); string(b) != orig {
		t.Fatalf("backup = %q", b)
	}
	b, _ := os.ReadFile(path)
	if !strings.Contains(string(b), "runner:") || !strings.Contains(string(b), "username: admin") {
		t.Fatalf("migrated = %s", b)
	}
	// The runner reads the migrated file the same way.
	viper.Reset()
	defer viper.Reset()
	viper.SetConfigFile(path)
	if err := viper.ReadInConfig(); err != nil {
		t.Fatal(err)
	}
	normalizeCanonicalConfig()
	if viper.GetString("clusters") != "10.0.0.1" || viper.GetString("username") != "admin" {
		t.Fatalf("clusters=%q username=%q", viper.GetString("clusters"), viper.GetString("username"))
	}
}

// With both nested spellings set, the runner used whichever Go's map order
// visited last. The preferred spelling now always wins.
func TestNormalizeCanonicalConfigPrefersOneSpelling(t *testing.T) {
	for i := 0; i < 20; i++ {
		viper.Reset()
		path := filepath.Join(t.TempDir(), "config.yaml")
		raw := "schema-version: 1\nrunner:\n  filtering:\n    flaky-lookback-runs: 3\n  policy:\n    flaky-lookback-runs: 4\n"
		if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
			t.Fatal(err)
		}
		viper.SetConfigFile(path)
		if err := viper.ReadInConfig(); err != nil {
			t.Fatal(err)
		}
		normalizeCanonicalConfig()
		if got := viper.GetInt("flaky-lookback-runs"); got != 4 {
			t.Fatalf("pass %d: flaky-lookback-runs = %d, want 4 (runner.policy)", i, got)
		}
	}
	viper.Reset()
}
