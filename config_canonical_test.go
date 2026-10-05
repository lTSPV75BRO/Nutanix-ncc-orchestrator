package main

import (
	"os"
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func TestNormalizeCanonicalConfig(t *testing.T) {
	viper.Reset()
	defer viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()
	viper.Set("schema-version", 1)
	viper.Set("runner.execution.request-timeout", "45s")
	viper.Set("runner.retry.circuit-breaker", 7)
	viper.Set("runner.targets.pcs", []string{"pc-a", "pc-b"})
	viper.Set("storage.logs-dir", "/var/lib/ncc/logs")

	normalizeCanonicalConfig()

	if got := viper.GetString("request-timeout"); got != "45s" {
		t.Fatalf("request-timeout = %q, want 45s", got)
	}
	if got := viper.GetInt("retry-circuit-breaker"); got != 7 {
		t.Fatalf("retry-circuit-breaker = %d, want 7", got)
	}
	if got := viper.GetString("output-dir-logs"); got != "/var/lib/ncc/logs" {
		t.Fatalf("output-dir-logs = %q", got)
	}
	if got := viper.GetString("pcs"); got != "pc-a,pc-b" {
		t.Fatalf("pcs = %q, want pc-a,pc-b", got)
	}
}

func TestExampleConfigIsSafeSample(t *testing.T) {
	b, err := os.ReadFile("example_config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	if !strings.Contains(text, "REPLACE_WITH_CLUSTER_IP") {
		t.Fatal("example config must keep the sample cluster placeholder")
	}
	if strings.Contains(text, "10.38.") || strings.Contains(text, "10.2.XX") {
		t.Fatal("example config must not contain a real-looking cluster address")
	}
}

func TestRunnerEnvKeysHaveFlags(t *testing.T) {
	cmd := newRootCmd()
	noFlag := map[string]bool{
		"SCHEMA_VERSION": true,
		"WEBHOOK_SECRET": true,
	}
	if cmd.Flags().Lookup("webhook-secret") != nil {
		t.Fatal("webhook-secret must stay env/config only")
	}
	for _, key := range runnerEnvKeys {
		if noFlag[key] {
			continue
		}
		flat := strings.ToLower(strings.ReplaceAll(key, "_", "-"))
		if cmd.Flags().Lookup(flat) == nil && cmd.PersistentFlags().Lookup(flat) == nil {
			t.Errorf("NCC_%s has no flag --%s", key, flat)
		}
	}
	for _, name := range []string{"max-idle-conns", "pc-alerts-cache-ttl", "email-subject-template", "webhook-template"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Errorf("missing flag --%s", name)
		}
	}
}
