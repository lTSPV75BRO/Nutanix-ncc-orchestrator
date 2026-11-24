package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func configJSONSchema() map[string]interface{} {
	props := map[string]interface{}{
		"cluster-source-mode":       map[string]interface{}{"type": "string", "enum": []string{"clusters", "pc"}},
		"pc-alerts-cache-ttl":       map[string]interface{}{"type": "string", "description": "Duration. 0 disables the Prism Central alert cache"},
		"email-subject-template":    map[string]interface{}{"type": "string"},
		"email-body-template":       map[string]interface{}{"type": "string"},
		"webhook-template":          map[string]interface{}{"type": "string"},
		"skip-preflight-check":      map[string]interface{}{"type": "boolean"},
		"clusters":                  map[string]interface{}{"type": "string", "description": "Comma-separated cluster IPs/FQDNs"},
		"clusters-file":             map[string]interface{}{"type": "string"},
		"pcs":                       map[string]interface{}{"type": "string", "description": "Comma-separated Prism Central IPs/FQDNs/URLs for pc mode"},
		"pcs-file":                  map[string]interface{}{"type": "string"},
		"update":                    map[string]interface{}{"type": "boolean"},
		"username":                  map[string]interface{}{"type": "string"},
		"password":                  map[string]interface{}{"type": "string"},
		"ncc-api-version":           map[string]interface{}{"type": "string", "enum": []string{"v4", "Legacy", "v1"}},
		"nutanix-v4-api-version":    map[string]interface{}{"type": "string"},
		"insecure-skip-verify":      map[string]interface{}{"type": "boolean"},
		"timeout":                   map[string]interface{}{"type": "string"},
		"request-timeout":           map[string]interface{}{"type": "string"},
		"poll-interval":             map[string]interface{}{"type": "string"},
		"poll-jitter":               map[string]interface{}{"type": "string"},
		"max-parallel":              map[string]interface{}{"type": "integer", "minimum": 0, "maximum": maxParallelLimit},
		"outputs":                   map[string]interface{}{"type": "string", "description": "Comma-separated html,csv,json,markdown,sarif"},
		"output-dir-logs":           map[string]interface{}{"type": "string"},
		"output-dir-filtered":       map[string]interface{}{"type": "string"},
		"single-report":             map[string]interface{}{"type": "boolean"},
		"gen-test-agg":              map[string]interface{}{"type": "integer", "minimum": 0},
		"severity-filter":           map[string]interface{}{"type": "string"},
		"exclude-alert-titles":      map[string]interface{}{"type": "string"},
		"exclude-alert-titles-file": map[string]interface{}{"type": "string"},
		"exclude-alert-match-mode":  map[string]interface{}{"type": "string", "enum": []string{"exact", "contains", "regex"}},
		"dry-run":                   map[string]interface{}{"type": "boolean"},
		"replay":                    map[string]interface{}{"type": "boolean"},
		"log-file":                  map[string]interface{}{"type": "string"},
		"log-level":                 map[string]interface{}{"type": "string", "enum": []string{"trace", "debug", "info", "warn", "warning", "error", "fatal", "0", "1", "2", "3", "4", "5"}},
		"log-http":                  map[string]interface{}{"type": "boolean"},
		"retry-max-attempts":        map[string]interface{}{"type": "integer", "minimum": 1},
		"retry-base-delay":          map[string]interface{}{"type": "string"},
		"retry-max-delay":           map[string]interface{}{"type": "string"},
		"retry-circuit-breaker":     map[string]interface{}{"type": "integer", "minimum": 1},
		"prom-enabled":              map[string]interface{}{"type": "boolean"},
		"prom-dir":                  map[string]interface{}{"type": "string"},
		"run-history":               map[string]interface{}{"type": "boolean"},
		"run-history-dir":           map[string]interface{}{"type": "string"},
		"retain-last":               map[string]interface{}{"type": "integer", "minimum": 0},
		"retain-days":               map[string]interface{}{"type": "integer", "minimum": 0},
		"artifact-retain-days":      map[string]interface{}{"type": "integer", "minimum": 0},
		"artifact-retain-max-files": map[string]interface{}{"type": "integer", "minimum": 0},
		"raw-log-retain-days":       map[string]interface{}{"type": "integer", "minimum": 0},
		"notify-on-regression":      map[string]interface{}{"type": "boolean"},
		"adaptive-parallelism":      map[string]interface{}{"type": "boolean"},
		"retry-failed-clusters":     map[string]interface{}{"type": "integer", "minimum": 0, "maximum": 5},
		"resume":                    map[string]interface{}{"type": "boolean"},
		"policy-gates":              map[string]interface{}{"type": "string"},
		"quiet-hours":               map[string]interface{}{"type": "string", "description": "HH:MM-HH:MM local time"},
		"maintenance-windows":       map[string]interface{}{"type": "string", "description": "comma-separated start/end RFC3339 windows"},
		"flaky-lookback-runs":       map[string]interface{}{"type": "integer", "minimum": 2},
		"flaky-min-transitions":     map[string]interface{}{"type": "integer", "minimum": 1},
		"email-enabled":             map[string]interface{}{"type": "boolean"},
		"email-attach-html":         map[string]interface{}{"type": "boolean"},
		"notify-digest":             map[string]interface{}{"type": "boolean"},
		"smtp-server":               map[string]interface{}{"type": "string"},
		"smtp-port":                 map[string]interface{}{"type": "integer"},
		"smtp-user":                 map[string]interface{}{"type": "string"},
		"smtp-password":             map[string]interface{}{"type": "string"},
		"email-from":                map[string]interface{}{"type": "string"},
		"email-to":                  map[string]interface{}{"type": "string"},
		"email-use-tls":             map[string]interface{}{"type": "boolean"},
		"webhook-enabled":           map[string]interface{}{"type": "boolean"},
		"webhook-include-html":      map[string]interface{}{"type": "boolean"},
		"webhook-url":               map[string]interface{}{"type": "string"},
		"webhook-headers": map[string]interface{}{
			"type":                 "object",
			"additionalProperties": map[string]interface{}{"type": "string"},
		},
		"slack-enabled":               map[string]interface{}{"type": "boolean"},
		"slack-webhook-url":           map[string]interface{}{"type": "string"},
		"slack-channel":               map[string]interface{}{"type": "string"},
		"secrets-provider":            map[string]interface{}{"type": "string", "enum": []string{"", "env", "file"}},
		"secrets-file":                map[string]interface{}{"type": "string"},
		"prism-central-url":           map[string]interface{}{"type": "string"},
		"discover-api-version":        map[string]interface{}{"type": "string", "enum": []string{"v3", "v4"}},
		"max-idle-conns":              map[string]interface{}{"type": "integer"},
		"max-idle-conns-per-host":     map[string]interface{}{"type": "integer"},
		"max-conns-per-host":          map[string]interface{}{"type": "integer"},
		"idle-conn-timeout":           map[string]interface{}{"type": "string"},
		"ca-bundle":                   map[string]interface{}{"type": "string"},
		"pin-sha256":                  map[string]interface{}{"type": "string"},
		"smtp-insecure-skip-verify":   map[string]interface{}{"type": "boolean"},
		"webhook-secret":              map[string]interface{}{"type": "string"},
		"notification-deadletter-dir": map[string]interface{}{"type": "string"},
	}
	return map[string]interface{}{
		"$schema":              "https://json-schema.org/draft/2020-12/schema",
		"title":                "NCC Orchestrator Config",
		"type":                 "object",
		"additionalProperties": false,
		"properties":           props,
	}
}

func runConfigSchema(cmd *cobra.Command, args []string) error {
	schema := configJSONSchema()
	data, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return err
	}
	outPath, _ := cmd.Flags().GetString("output")
	if strings.TrimSpace(outPath) == "" {
		fmt.Println(string(data))
		return nil
	}
	return os.WriteFile(outPath, data, 0644)
}

func runValidateConfigCommand(cmd *cobra.Command, args []string) error {
	cfgPath, _ := cmd.Flags().GetString("config")
	if strings.TrimSpace(cfgPath) == "" {
		return exitConfig(errors.New("--config is required"))
	}
	if _, err := loadConfigForValidation(cfgPath); err != nil {
		return exitConfig(err)
	}
	fmt.Printf("Config is valid: %s\n", cfgPath)
	return nil
}

func runValidateSecretsCommand(cmd *cobra.Command, args []string) error {
	cfgPath, _ := cmd.Flags().GetString("config")
	if strings.TrimSpace(cfgPath) == "" {
		return exitConfig(errors.New("--config is required"))
	}
	secretRefs, provider, err := validateSecretsForPath(cfgPath)
	if err != nil {
		return exitConfig(err)
	}
	if secretRefs == 0 {
		fmt.Printf("No secret:// references found in config: %s\n", cfgPath)
		return nil
	}
	fmt.Printf("Secrets validation passed: refs=%d provider=%s config=%s\n", secretRefs, provider, cfgPath)
	return nil
}

func loadConfigForValidation(cfgPath string) (Config, error) {
	if _, err := os.Stat(cfgPath); err != nil {
		return Config{}, fmt.Errorf("config path %s: %w", cfgPath, err)
	}
	// validate-* commands are standalone subcommands and do not share root --config binding.
	viper.Set("config", cfgPath)
	cfg, err := bindConfig()
	if err != nil {
		return Config{}, fmt.Errorf("configuration: %w", err)
	}
	if err := validateConfig(cfg); err != nil {
		return Config{}, fmt.Errorf("validation: %w", err)
	}
	return cfg, nil
}

func validateSecretsForPath(cfgPath string) (int, string, error) {
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		return 0, "", fmt.Errorf("config path %s: %w", cfgPath, err)
	}
	secretRefs := bytes.Count(raw, []byte("secret://"))
	viper.Set("config", cfgPath)
	cfg, err := bindConfig()
	if err != nil {
		return secretRefs, "", fmt.Errorf("secret validation failed: %w", err)
	}
	provider := strings.TrimSpace(cfg.SecretsProvider)
	if secretRefs > 0 && provider == "" {
		return secretRefs, provider, errors.New("secret:// references found but secrets-provider is empty")
	}
	return secretRefs, provider, nil
}

func validateSecretsWithConfig(cfgPath string, cfg Config) (int, string, error) {
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		return 0, "", fmt.Errorf("config path %s: %w", cfgPath, err)
	}
	secretRefs := bytes.Count(raw, []byte("secret://"))
	provider := strings.TrimSpace(cfg.SecretsProvider)
	if secretRefs > 0 && provider == "" {
		return secretRefs, provider, errors.New("secret:// references found but secrets-provider is empty")
	}
	return secretRefs, provider, nil
}
