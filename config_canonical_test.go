package main

import (
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
}
