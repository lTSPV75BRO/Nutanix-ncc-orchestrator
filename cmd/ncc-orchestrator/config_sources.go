package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"
)

func validateSecretsFileHardening(path string) error {
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if st.Mode()&os.ModeSymlink != 0 {
		return errors.New("secrets-file must not be a symlink")
	}
	if !st.Mode().IsRegular() {
		return errors.New("secrets-file must be a regular file")
	}
	if st.Size() > maxSecretsFileBytes {
		return fmt.Errorf("secrets-file exceeds max size (%d bytes)", maxSecretsFileBytes)
	}
	// Windows has no POSIX mode bits: Go reports every writable file as 0666,
	// so this check rejected every secrets file there and the file provider
	// was unusable. Access is governed by NTFS ACLs (see checkSecretsPerms,
	// which likewise skips Windows).
	if runtime.GOOS != "windows" && st.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("secrets-file permissions are too open (%#o); expected owner-only (e.g. 0600)", st.Mode().Perm())
	}
	return nil
}

func loadSecretMapFile(path string) (map[string]string, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("secrets-file is required for provider=file")
	}
	if err := validateSecretsFileHardening(path); err != nil {
		return nil, fmt.Errorf("secrets-file hardening check failed: %w", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out map[string]string
	if err := json.Unmarshal(b, &out); err == nil {
		return out, nil
	}
	v := viper.New()
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("parse secrets-file: %w", err)
	}
	raw := v.AllSettings()
	out = make(map[string]string, len(raw))
	for k, val := range raw {
		if s, ok := val.(string); ok {
			out[k] = s
		}
	}
	return out, nil
}

func resolveSecretRef(ref string, provider string, fileSecrets map[string]string) (string, error) {
	v := strings.TrimSpace(ref)
	if !strings.HasPrefix(v, "secret://") {
		return v, nil
	}
	name := strings.TrimPrefix(v, "secret://")
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("empty secret reference")
	}
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "env":
		if val := os.Getenv(name); val != "" {
			return val, nil
		}
		if val := os.Getenv("NCC_SECRET_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_"))); val != "" {
			return val, nil
		}
		return "", fmt.Errorf("secret %q not found in env", name)
	case "file":
		if val, ok := fileSecrets[name]; ok {
			return val, nil
		}
		return "", fmt.Errorf("secret %q not found in secrets-file", name)
	default:
		return "", fmt.Errorf("secrets-provider must be env or file when secret:// refs are used")
	}
}

func applySecretsToConfig(cfg *Config) error {
	provider := strings.ToLower(strings.TrimSpace(cfg.SecretsProvider))
	var fileSecrets map[string]string
	var err error
	if provider == "file" {
		fileSecrets, err = loadSecretMapFile(cfg.SecretsFile)
		if err != nil {
			return fmt.Errorf("load secrets-file: %w", err)
		}
	}
	cfg.Password, err = resolveSecretRef(cfg.Password, provider, fileSecrets)
	if err != nil {
		return fmt.Errorf("password: %w", err)
	}
	cfg.SMTPPassword, err = resolveSecretRef(cfg.SMTPPassword, provider, fileSecrets)
	if err != nil {
		return fmt.Errorf("smtp-password: %w", err)
	}
	cfg.WebhookURL, err = resolveSecretRef(cfg.WebhookURL, provider, fileSecrets)
	if err != nil {
		return fmt.Errorf("webhook-url: %w", err)
	}
	cfg.SlackWebhookURL, err = resolveSecretRef(cfg.SlackWebhookURL, provider, fileSecrets)
	if err != nil {
		return fmt.Errorf("slack-webhook-url: %w", err)
	}
	for cluster, cred := range cfg.ClusterCredentials {
		if strings.TrimSpace(cred.Username) != "" {
			cred.Username, err = resolveSecretRef(cred.Username, provider, fileSecrets)
			if err != nil {
				return fmt.Errorf("clusters-file username for cluster %s: %w", cluster, err)
			}
		}
		if strings.TrimSpace(cred.Password) != "" {
			cred.Password, err = resolveSecretRef(cred.Password, provider, fileSecrets)
			if err != nil {
				return fmt.Errorf("clusters-file password for cluster %s: %w", cluster, err)
			}
		}
		cfg.ClusterCredentials[cluster] = cred
	}
	return nil
}

// readClusterFile reads cluster targets from file.
// Supported formats per non-comment line:
//  1. cluster
//  2. cluster,username
//  3. cluster,username,password
//
// Blank and # lines are ignored.
func readClusterFile(path string) ([]string, map[string]ClusterCredential, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	var out []string
	creds := make(map[string]ClusterCredential)
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cluster, user, pass, err := parseClusterFileLine(line)
		if err != nil {
			return nil, nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		out = append(out, cluster)
		if user != "" || pass != "" {
			creds[cluster] = ClusterCredential{Username: user, Password: pass}
		}
	}
	return out, creds, nil
}

func parseClusterFileLine(line string) (cluster string, username string, password string, err error) {
	r := csv.NewReader(strings.NewReader(line))
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1
	rec, err := r.Read()
	if err != nil {
		return "", "", "", fmt.Errorf("invalid cluster entry: %w", err)
	}
	for i := range rec {
		rec[i] = strings.TrimSpace(rec[i])
	}
	switch len(rec) {
	case 1:
		if rec[0] == "" {
			return "", "", "", errors.New("cluster value is empty")
		}
		return rec[0], "", "", nil
	case 2:
		if rec[0] == "" {
			return "", "", "", errors.New("cluster value is empty")
		}
		if rec[1] == "" {
			return "", "", "", errors.New("username is empty")
		}
		return rec[0], rec[1], "", nil
	case 3:
		if rec[0] == "" {
			return "", "", "", errors.New("cluster value is empty")
		}
		if rec[1] == "" {
			return "", "", "", errors.New("username is empty")
		}
		return rec[0], rec[1], rec[2], nil
	default:
		return "", "", "", fmt.Errorf("expected 1, 2, or 3 CSV fields (cluster[,username[,password]]), got %d", len(rec))
	}
}

func normalizeClusterAddress(raw string) (string, error) {
	cluster := strings.TrimSpace(raw)
	if cluster == "" {
		return "", errors.New("cluster address cannot be empty")
	}
	if strings.Contains(cluster, "://") {
		parsed, err := url.Parse(cluster)
		if err != nil {
			return "", fmt.Errorf("invalid cluster URL: %w", err)
		}
		host := strings.TrimSpace(parsed.Hostname())
		if host == "" {
			return "", errors.New("cluster URL must include a host")
		}
		cluster = host
	}
	// Accept host:port/IP:port shorthand; NCC always targets 9440 internally.
	if host, port, err := net.SplitHostPort(cluster); err == nil {
		if port == "" {
			return "", errors.New("cluster port is empty")
		}
		cluster = strings.Trim(host, "[]")
	} else {
		if last := strings.LastIndex(cluster, ":"); last > 0 && strings.Count(cluster, ":") == 1 {
			port := strings.TrimSpace(cluster[last+1:])
			if _, pErr := strconv.Atoi(port); pErr == nil {
				cluster = strings.TrimSpace(cluster[:last])
			}
		}
	}
	cluster = strings.Trim(cluster, "[]")
	if cluster == "" {
		return "", errors.New("cluster address cannot be empty")
	}
	return cluster, nil
}

func normalizeClusters(clusters []string) ([]string, error) {
	out := make([]string, 0, len(clusters))
	for i, cluster := range clusters {
		norm, err := normalizeClusterAddress(cluster)
		if err != nil {
			return nil, fmt.Errorf("cluster %d (%s): %w", i+1, cluster, err)
		}
		out = append(out, norm)
	}
	return out, nil
}

func credentialsForCluster(cfg Config, cluster string) (string, string) {
	user := strings.TrimSpace(cfg.Username)
	pass := cfg.Password
	if cred, ok := cfg.ClusterCredentials[cluster]; ok {
		if strings.TrimSpace(cred.Username) != "" {
			user = strings.TrimSpace(cred.Username)
		}
		if strings.TrimSpace(cred.Password) != "" {
			pass = cred.Password
		}
	}
	return user, pass
}

func validateClusterCredentialCoverage(cfg Config) error {
	for _, cluster := range cfg.Clusters {
		user, _ := credentialsForCluster(cfg, cluster)
		if strings.TrimSpace(user) == "" {
			return fmt.Errorf("missing username for cluster %s (set global username or provide cluster,username[,password] in clusters-file)", cluster)
		}
		if len(user) > 255 {
			return fmt.Errorf("username too long for cluster %s (max 255 characters)", cluster)
		}
	}
	return nil
}

func needsPasswordPrompt(cfg Config) (bool, string) {
	if strings.TrimSpace(cfg.Password) != "" {
		return false, ""
	}
	for _, cluster := range cfg.Clusters {
		user, pass := credentialsForCluster(cfg, cluster)
		if strings.TrimSpace(pass) == "" {
			return true, user
		}
	}
	return false, ""
}

func mustParseDur(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}
	return d
}
