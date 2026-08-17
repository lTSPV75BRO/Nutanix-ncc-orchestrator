package main

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"html/template"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"text/tabwriter"
	"time"

	"goncc/internal/httpclient"
	"goncc/internal/model"
	"goncc/internal/nccparse"
	"goncc/internal/notify"
	"goncc/internal/promtext"
	"goncc/internal/retryutil"
	"goncc/internal/selfsigned"
	"goncc/internal/trace"
	"goncc/internal/v2layout"

	"github.com/Masterminds/semver/v3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
	"golang.org/x/term"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

// ==================== Configuration ====================

// The foundational shared types now live in goncc/internal/model so the
// extracted subsystem packages (promtext, notify, parser) can depend on them
// without importing package main. These aliases keep the thousands of
// existing references in this package compiling unchanged.
type (
	Config              = model.Config
	ClusterCredential   = model.ClusterCredential
	NotificationSummary = model.NotificationSummary
	ParsedBlock         = model.ParsedBlock
	FS                  = model.FS
	HTTPClient          = model.HTTPClient
)

type HTMLMeta struct {
	ClusterName    string
	ClusterVersion string
	NCCVersion     string
}

type HTMLData struct {
	Rows    []Row
	Now     string
	Meta    HTMLMeta
	Summary SummaryCounts
}

// SummaryCounts holds per-severity counts for the report header.
type SummaryCounts struct {
	FAIL, WARN, ERR, INFO int
}

const termsText = `
Nutanix NCC Orchestrator - Terms, Support, and Usage Notice

Project:
- Repository: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator
- Maintainer: Prajwal Vernekar

What this tool does:
- Runs NCC checks across multiple clusters.
- Aggregates and filters results.
- Produces reports and automation artifacts.
- Optionally exposes v2 API and UI services.

Your responsibilities:
- Review and validate configuration before production use.
- Protect credentials, API tokens, and generated artifacts.
- Use secure defaults (TLS verification, least privilege, explicit allowlists).
- Test changes in non-production first, especially update and scheduling workflows.

Update and version policy:
- Use "ncc-orchestrator update --check" to check availability before replacing binaries.
- By default, updates stay on your current major track (v1.x stays on v1.x).
- Major upgrades (for example v1 -> v2) require explicit opt-in via "--allow-major-upgrade".
- Review v1 to v2 migration guidance before major upgrades:
  https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/blob/main/docs/V2_BACKEND_FRONTEND_MVP.md

Support and documentation:
- Start with: ncc-orchestrator --help
- Full usage and flags: README.md and docs/FEATURES_AND_CONFIG_FLAGS.md
- MVP backend/frontend design: docs/V2_BACKEND_FRONTEND_MVP.md

Disclaimer:
- This software is provided "as is", without warranties of any kind.
- Use at your own risk; you are responsible for operational, security, and compliance outcomes.
- Contributors and affiliated organizations are not liable for direct or indirect damages.
`

// ==================== Constants ====================

const (
	// Default values
	defaultTimeout             = 15 * time.Minute
	defaultRequestTimeout      = 20 * time.Second
	defaultPollInterval        = 15 * time.Second
	defaultPollJitter          = 2 * time.Second
	defaultMaxParallel         = 4
	defaultRetryAttempts       = 6
	defaultRetryBaseDelay      = 400 * time.Millisecond
	defaultRetryMaxDelay       = 8 * time.Second
	defaultRetryCircuitBreaker = 3
	defaultOutputDirLogs       = "nccfiles"
	defaultOutputDirFiltered   = "outputfiles"
	defaultPromDir             = "promfiles"
	defaultLogFile             = "logs/ncc-runner.log"
	defaultOutputFormat        = "html"
	defaultExcludeMatchMode    = "exact"
	maxSecretsFileBytes        = 1 << 20 // 1 MiB

	// HTTP connection pooling defaults live in goncc/internal/httpclient
	// (DefaultMaxIdleConns / DefaultMaxIdleConnsPerHost / DefaultIdleConnTimeout).

	// Graceful shutdown
	shutdownTimeout = 30 * time.Second

	// Security
	minPasswordLength = 1
	maxClusterNameLen = 255
	maxURLLength      = 2048

	// Prism Gateway port
	prismGatewayPort = 9440

	// defaultNutanixV4APIVersion is the default Nutanix v4 API path segment (clustermgmt / monitoring).
	defaultNutanixV4APIVersion = "v4.2"
	defaultDiscoverAPIVersion  = "v4"
	defaultClusterSourceMode   = "clusters"
	defaultFlakyLookbackRuns   = 6
	defaultFlakyTransitions    = 2
)

// ==================== Exit Codes ====================
// Exit code 0 = success, 1 = run/execution error, 2 = config/validation error (see README).

type exitCodeError struct {
	code int
	err  error
}

func (e *exitCodeError) Error() string { return e.err.Error() }
func (e *exitCodeError) Unwrap() error { return e.err }

func exitConfig(err error) error { return &exitCodeError{code: 2, err: err} }

// exitPartial indicates some clusters succeeded and some failed (exit code 3).
func exitPartial(err error) error { return &exitCodeError{code: 3, err: err} }

// ==================== Utility Functions ====================

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func normalizeClusterSourceMode(s string) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(s))
	if mode == "" {
		return defaultClusterSourceMode, nil
	}
	switch mode {
	case "clusters", "pc":
		return mode, nil
	default:
		return "", fmt.Errorf("cluster-source-mode must be one of clusters or pc")
	}
}

func splitCSVTrimLower(s string) []string {
	in := splitCSV(s)
	out := make([]string, 0, len(in))
	for _, v := range in {
		out = append(out, strings.ToLower(strings.TrimSpace(v)))
	}
	return out
}

func levenshteinDistance(a, b string) int {
	if a == b {
		return 0
	}
	if len(a) == 0 {
		return len(b)
	}
	if len(b) == 0 {
		return len(a)
	}
	prev := make([]int, len(b)+1)
	for j := 0; j <= len(b); j++ {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		curr := make([]int, len(b)+1)
		curr[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			del := prev[j] + 1
			ins := curr[j-1] + 1
			sub := prev[j-1] + cost
			curr[j] = minInt(del, minInt(ins, sub))
		}
		prev = curr
	}
	return prev[len(b)]
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func closestToken(input string, candidates []string) string {
	in := strings.ToLower(strings.TrimSpace(input))
	if in == "" || len(candidates) == 0 {
		return ""
	}
	best := ""
	bestDist := 1 << 30
	for _, c := range candidates {
		clean := strings.ToLower(strings.TrimSpace(c))
		if clean == "" {
			continue
		}
		if clean == in {
			return c
		}
		d := levenshteinDistance(in, clean)
		if d < bestDist {
			bestDist = d
			best = c
		}
	}
	// Keep suggestions conservative to avoid noisy/wrong hints.
	if bestDist > 3 && !strings.HasPrefix(strings.ToLower(best), in) {
		return ""
	}
	return best
}

func knownCommandNames(cmd *cobra.Command) []string {
	names := []string{}
	for _, c := range cmd.Commands() {
		if !c.IsAvailableCommand() || c.Hidden {
			continue
		}
		names = append(names, c.Name())
	}
	return names
}

func knownFlagNames(cmd *cobra.Command) []string {
	set := map[string]struct{}{}
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		set["--"+f.Name] = struct{}{}
	})
	cmd.InheritedFlags().VisitAll(func(f *pflag.Flag) {
		set["--"+f.Name] = struct{}{}
	})
	cmd.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		set["--"+f.Name] = struct{}{}
	})
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func extractQuotedValue(msg string) string {
	start := strings.Index(msg, "\"")
	if start < 0 {
		return ""
	}
	rest := msg[start+1:]
	end := strings.Index(rest, "\"")
	if end < 0 {
		return ""
	}
	return rest[:end]
}

func humanizeCLIError(root *cobra.Command, argv []string, err error) string {
	msg := strings.TrimSpace(err.Error())
	lower := strings.ToLower(msg)
	active := root
	if len(argv) > 0 {
		if found, _, findErr := root.Find(argv); findErr == nil && found != nil {
			active = found
		}
	}
	helpCmd := "ncc-orchestrator --help"
	if active != nil && active != root {
		helpCmd = active.CommandPath() + " --help"
	}
	if strings.Contains(lower, "unknown command") {
		bad := extractQuotedValue(msg)
		suggestion := closestToken(bad, knownCommandNames(root))
		if suggestion != "" {
			return fmt.Sprintf("unknown command %q. Did you mean `%s`?\nHint: run `%s` for available commands.", bad, suggestion, helpCmd)
		}
		return fmt.Sprintf("%s\nHint: run `%s` for available commands.", msg, helpCmd)
	}
	if strings.Contains(lower, "unknown flag") {
		bad := extractQuotedValue(msg)
		if bad == "" {
			if idx := strings.Index(lower, "unknown flag:"); idx >= 0 {
				raw := strings.TrimSpace(msg[idx+len("unknown flag:"):])
				if raw != "" {
					bad = strings.Fields(raw)[0]
				}
			}
		}
		suggestion := closestToken(bad, knownFlagNames(active))
		if strings.EqualFold(strings.TrimSpace(suggestion), strings.TrimSpace(bad)) {
			suggestion = ""
		}
		if suggestion != "" {
			return fmt.Sprintf("%s\nDid you mean `%s`?\nHint: run `%s` to view supported flags.", msg, suggestion, helpCmd)
		}
		return fmt.Sprintf("%s\nHint: run `%s` to view supported flags.", msg, helpCmd)
	}
	return msg
}

func readLineValuesFile(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []string
	seen := make(map[string]bool)
	for i, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if err := validateClusterAddress(trimmed); err != nil {
			return nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		if seen[trimmed] {
			continue
		}
		seen[trimmed] = true
		out = append(out, trimmed)
	}
	return out, nil
}

func loadExcludeAlertTitlesFromFile(path string) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(b), "\n")
	out := make([]string, 0, len(lines))
	for _, ln := range lines {
		v := strings.TrimSpace(ln)
		if v == "" || strings.HasPrefix(v, "#") {
			continue
		}
		out = append(out, v)
	}
	return out, nil
}

func mergeUniqueStrings(items ...[]string) []string {
	if len(items) == 0 {
		return nil
	}
	seen := map[string]bool{}
	out := make([]string, 0)
	for _, arr := range items {
		for _, raw := range arr {
			v := strings.TrimSpace(raw)
			if v == "" {
				continue
			}
			k := strings.ToLower(v)
			if seen[k] {
				continue
			}
			seen[k] = true
			out = append(out, v)
		}
	}
	return out
}

func parseHHMM(s string) (int, int, error) {
	parts := strings.Split(strings.TrimSpace(s), ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("expected HH:MM")
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, err
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, err
	}
	if h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, 0, fmt.Errorf("time out of range")
	}
	return h, m, nil
}

func inQuietHours(now time.Time, quietHours string) (bool, error) {
	q := strings.TrimSpace(quietHours)
	if q == "" {
		return false, nil
	}
	parts := strings.Split(q, "-")
	if len(parts) != 2 {
		return false, fmt.Errorf("quiet-hours must be HH:MM-HH:MM")
	}
	sh, sm, err := parseHHMM(parts[0])
	if err != nil {
		return false, fmt.Errorf("quiet-hours start: %w", err)
	}
	eh, em, err := parseHHMM(parts[1])
	if err != nil {
		return false, fmt.Errorf("quiet-hours end: %w", err)
	}
	curMins := now.Hour()*60 + now.Minute()
	startMins := sh*60 + sm
	endMins := eh*60 + em
	if startMins == endMins {
		return true, nil // full-day quiet window
	}
	if startMins < endMins {
		return curMins >= startMins && curMins < endMins, nil
	}
	return curMins >= startMins || curMins < endMins, nil
}

func inMaintenanceWindow(now time.Time, windows []string) (bool, error) {
	for _, w := range windows {
		w = strings.TrimSpace(w)
		if w == "" {
			continue
		}
		parts := strings.Split(w, "/")
		if len(parts) != 2 {
			return false, fmt.Errorf("maintenance window %q must be start/end RFC3339", w)
		}
		start, err := time.Parse(time.RFC3339, strings.TrimSpace(parts[0]))
		if err != nil {
			return false, fmt.Errorf("maintenance window start %q: %w", parts[0], err)
		}
		end, err := time.Parse(time.RFC3339, strings.TrimSpace(parts[1]))
		if err != nil {
			return false, fmt.Errorf("maintenance window end %q: %w", parts[1], err)
		}
		if !end.After(start) {
			return false, fmt.Errorf("maintenance window %q end must be after start", w)
		}
		if (now.Equal(start) || now.After(start)) && now.Before(end) {
			return true, nil
		}
	}
	return false, nil
}

func notificationsSuppressedNow(cfg Config, now time.Time) (bool, string) {
	if ok, err := inQuietHours(now, cfg.QuietHours); err == nil && ok {
		return true, "quiet-hours"
	}
	if ok, err := inMaintenanceWindow(now, cfg.MaintenanceWindows); err == nil && ok {
		return true, "maintenance-window"
	}
	return false, ""
}

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
	if st.Mode().Perm()&0o077 != 0 {
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

// ==================== Validation Functions ====================

// validateClusterAddress validates cluster IP or hostname
func validateClusterAddress(cluster string) error {
	cluster, err := normalizeClusterAddress(cluster)
	if err != nil {
		return err
	}
	if len(cluster) > maxClusterNameLen {
		return fmt.Errorf("cluster address too long (max %d chars)", maxClusterNameLen)
	}

	// Check if it's a valid IP or hostname
	if net.ParseIP(cluster) != nil {
		return nil // Valid IP
	}

	// Validate hostname format (basic check)
	if strings.Contains(cluster, "..") || strings.HasPrefix(cluster, ".") || strings.HasSuffix(cluster, ".") {
		return errors.New("invalid cluster hostname format")
	}

	// Check for valid characters in hostname
	for _, r := range cluster {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') ||
			r == '.' || r == '-' || r == '_') {
			return fmt.Errorf("invalid character in cluster address: %c", r)
		}
	}

	return nil
}

// validateClusters validates all cluster addresses
func validateClusters(clusters []string) error {
	if len(clusters) == 0 {
		return errors.New("at least one cluster must be provided")
	}

	seen := make(map[string]bool)
	for i, cluster := range clusters {
		norm, err := normalizeClusterAddress(cluster)
		if err != nil {
			return fmt.Errorf("cluster %d (%s): %w", i+1, cluster, err)
		}
		if err := validateClusterAddress(norm); err != nil {
			return fmt.Errorf("cluster %d (%s): %w", i+1, cluster, err)
		}
		if seen[norm] {
			return fmt.Errorf("duplicate cluster address: %s", norm)
		}
		seen[norm] = true
	}

	return nil
}

// validateURL validates webhook/Slack URLs
func validateURL(urlStr string) error {
	if urlStr == "" {
		return errors.New("URL cannot be empty")
	}
	if len(urlStr) > maxURLLength {
		return fmt.Errorf("URL too long (max %d chars)", maxURLLength)
	}

	parsed, err := url.Parse(urlStr)
	if err != nil {
		return fmt.Errorf("invalid URL format: %w", err)
	}

	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("URL must use http or https scheme, got: %s", parsed.Scheme)
	}

	if parsed.Host == "" {
		return errors.New("URL must have a host")
	}

	return nil
}

// validateEmailAddress validates email address format
func validateEmailAddress(email string) error {
	if email == "" {
		return errors.New("email address cannot be empty")
	}

	parts := strings.Split(email, "@")
	if len(parts) != 2 {
		return errors.New("invalid email format: must contain exactly one @")
	}

	local, domain := parts[0], parts[1]
	if len(local) == 0 || len(domain) == 0 {
		return errors.New("invalid email format: local or domain part is empty")
	}

	if !strings.Contains(domain, ".") {
		return errors.New("invalid email format: domain must contain a dot")
	}

	return nil
}

// nutanixV4PathSegment returns the API path segment for Nutanix v4 routes (clustermgmt / monitoring), defaulting to v4.2.
func nutanixV4PathSegment(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return defaultNutanixV4APIVersion
	}
	return s
}

// normalizeNCCAPIVersion maps config values to internal "v4" or "v1" (Legacy). Accepts v4, Legacy (any case), or v1 as alias for Legacy.
func normalizeNCCAPIVersion(s string) (string, error) {
	t := strings.ToLower(strings.TrimSpace(s))
	if t == "" {
		return "v4", nil
	}
	switch t {
	case "v4":
		return "v4", nil
	case "v1", "legacy":
		return "v1", nil
	default:
		return "", fmt.Errorf("must be v4 or Legacy (v1 is accepted as an alias for Legacy), got %q", s)
	}
}

// validateNutanixV4APIVersion checks the path revision string (e.g. v4.2, v4.0.a1) used in /api/clustermgmt/{ver}/ and /api/monitoring/{ver}/.
func validateNutanixV4APIVersion(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	s = strings.ToLower(s)
	if len(s) > 48 {
		return errors.New("nutanix-v4-api-version must be at most 48 characters")
	}
	if strings.ContainsAny(s, `/\`) || strings.Contains(s, "..") {
		return errors.New("nutanix-v4-api-version must not contain path separators or '..'")
	}
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '_' {
			continue
		}
		return fmt.Errorf("nutanix-v4-api-version: invalid character %q (examples: v4.2, v4.1, v4.0.a1)", r)
	}
	if !strings.HasPrefix(s, "v") {
		return errors.New("nutanix-v4-api-version must start with v (e.g. v4.2, v4.1, v4.0.a1)")
	}
	return nil
}

func normalizeExcludeAlertMatchMode(s string) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(s))
	if mode == "" {
		return defaultExcludeMatchMode, nil
	}
	switch mode {
	case "exact", "contains", "regex":
		return mode, nil
	default:
		return "", fmt.Errorf("exclude-alert-match-mode must be one of exact, contains, regex")
	}
}

// validateConfig performs comprehensive configuration validation
func validateConfig(cfg Config) error {
	sourceMode, err := normalizeClusterSourceMode(cfg.ClusterSourceMode)
	if err != nil {
		return err
	}
	if sourceMode == "pc" {
		if len(cfg.PCs) == 0 && strings.TrimSpace(cfg.PrismCentralURL) == "" {
			return errors.New("pc mode requires pcs, pcs-file, or prism-central-url")
		}
		if strings.TrimSpace(cfg.Username) == "" {
			return errors.New("username is required in pc mode")
		}
		for i, pc := range cfg.PCs {
			if _, err := normalizePCBaseURL(pc, cfg.InsecureSkipVerify); err != nil {
				return fmt.Errorf("pc target %d (%s): %w", i+1, pc, err)
			}
		}
	} else {
		// Validate direct clusters input.
		if err := validateClusters(cfg.Clusters); err != nil {
			return fmt.Errorf("cluster validation failed: %w", err)
		}
		// Validate effective credentials (global or per-cluster from clusters-file).
		if err := validateClusterCredentialCoverage(cfg); err != nil {
			return err
		}
	}

	if _, err := normalizeNCCAPIVersion(cfg.NCCAPIVersion); err != nil {
		return fmt.Errorf("ncc-api-version: %w", err)
	}
	if err := validateNutanixV4APIVersion(cfg.NutanixV4APIVersion); err != nil {
		return fmt.Errorf("nutanix-v4-api-version: %w", err)
	}
	mode, err := normalizeExcludeAlertMatchMode(cfg.ExcludeAlertMatchMode)
	if err != nil {
		return err
	}
	if mode == "regex" {
		for _, pattern := range cfg.ExcludeAlertTitles {
			p := strings.TrimSpace(pattern)
			if p == "" {
				continue
			}
			if _, err := regexp.Compile(p); err != nil {
				return fmt.Errorf("exclude-alert-titles regex %q: %w", p, err)
			}
		}
	}

	// Validate timeouts
	if cfg.Timeout <= 0 {
		return errors.New("timeout must be greater than 0")
	}
	if cfg.RequestTimeout <= 0 {
		return errors.New("request-timeout must be greater than 0")
	}
	if cfg.RequestTimeout > cfg.Timeout {
		return errors.New("request-timeout cannot be greater than overall timeout")
	}

	// Validate concurrency
	if cfg.MaxParallel <= 0 {
		return errors.New("max-parallel must be greater than 0")
	}
	if cfg.MaxParallel > 100 {
		return errors.New("max-parallel cannot exceed 100 (safety limit)")
	}

	// Validate output formats
	validFormats := map[string]bool{"html": true, "csv": true, "json": true, "markdown": true, "sarif": true}
	for _, format := range cfg.OutputFormats {
		if !validFormats[strings.ToLower(format)] {
			return fmt.Errorf("invalid output format: %s (valid: html, csv, json, markdown, sarif)", format)
		}
	}

	// Validate severity filter
	validSeverities := map[string]bool{"FAIL": true, "WARN": true, "ERR": true, "INFO": true}
	for _, sev := range cfg.SeverityFilter {
		if !validSeverities[strings.ToUpper(sev)] {
			return fmt.Errorf("invalid severity: %s (valid: FAIL, WARN, ERR, INFO)", sev)
		}
	}

	// Validate email settings if enabled
	if cfg.EmailEnabled {
		if cfg.SMTPServer == "" {
			return errors.New("smtp-server is required when email is enabled")
		}
		if cfg.EmailFrom == "" {
			return errors.New("email-from is required when email is enabled")
		}
		if len(cfg.EmailTo) == 0 {
			return errors.New("email-to is required when email is enabled")
		}
		if cfg.SMTPPort <= 0 || cfg.SMTPPort > 65535 {
			return errors.New("smtp-port must be between 1 and 65535")
		}

		if err := validateEmailAddress(cfg.EmailFrom); err != nil {
			return fmt.Errorf("invalid email-from: %w", err)
		}
		for i, to := range cfg.EmailTo {
			if err := validateEmailAddress(to); err != nil {
				return fmt.Errorf("invalid email-to[%d]: %w", i, err)
			}
		}
	}

	// Validate webhook if enabled
	if cfg.WebhookEnabled {
		if err := validateURL(cfg.WebhookURL); err != nil {
			return fmt.Errorf("invalid webhook-url: %w", err)
		}
	}

	// Validate Slack if enabled
	if cfg.SlackEnabled {
		if err := validateURL(cfg.SlackWebhookURL); err != nil {
			return fmt.Errorf("invalid slack-webhook-url: %w", err)
		}
	}

	// Validate output paths (non-empty)
	if strings.TrimSpace(cfg.OutputDirLogs) == "" {
		return errors.New("output-dir-logs cannot be empty")
	}
	if strings.TrimSpace(cfg.OutputDirFiltered) == "" {
		return errors.New("output-dir-filtered cannot be empty")
	}
	if strings.TrimSpace(cfg.LogFile) == "" {
		return errors.New("log-file cannot be empty")
	}
	if cfg.PromEnabled && strings.TrimSpace(cfg.PromDir) == "" {
		return errors.New("prom-dir cannot be empty when prom-enabled=true")
	}
	if cfg.RetainLastRuns < 0 {
		return errors.New("retain-last must be >= 0")
	}
	if cfg.RetainDays < 0 {
		return errors.New("retain-days must be >= 0")
	}
	if cfg.ArtifactRetainDays < 0 {
		return errors.New("artifact-retain-days must be >= 0")
	}
	if cfg.ArtifactRetainMaxFiles < 0 {
		return errors.New("artifact-retain-max-files must be >= 0")
	}
	if cfg.RunHistoryEnabled && strings.TrimSpace(cfg.RunHistoryDir) == "" {
		return errors.New("run-history-dir cannot be empty when run-history is enabled")
	}
	if err := validateLogLevelStrict(cfg.LogLevel); err != nil {
		return err
	}
	if cfg.FlakyLookbackRuns < 0 {
		return errors.New("flaky-lookback-runs must be >= 0")
	}
	if cfg.FlakyLookbackRuns > 0 && cfg.FlakyLookbackRuns < 2 {
		return errors.New("flaky-lookback-runs must be >= 2")
	}
	if cfg.FlakyLookbackRuns > 200 {
		return errors.New("flaky-lookback-runs must be <= 200")
	}
	if cfg.FlakyMinTransitions < 0 {
		return errors.New("flaky-min-transitions must be >= 0")
	}
	if cfg.FlakyMinTransitions > 0 && cfg.FlakyMinTransitions < 1 {
		return errors.New("flaky-min-transitions must be >= 1")
	}
	if cfg.FlakyMinTransitions > 20 {
		return errors.New("flaky-min-transitions must be <= 20")
	}
	if _, err := inQuietHours(time.Now(), cfg.QuietHours); err != nil {
		return fmt.Errorf("quiet-hours: %w", err)
	}
	if _, err := inMaintenanceWindow(time.Now(), cfg.MaintenanceWindows); err != nil {
		return fmt.Errorf("maintenance-windows: %w", err)
	}
	switch strings.ToLower(strings.TrimSpace(cfg.SecretsProvider)) {
	case "", "env", "file":
	default:
		return errors.New("secrets-provider must be one of: env, file")
	}
	if strings.EqualFold(strings.TrimSpace(cfg.SecretsProvider), "file") && strings.TrimSpace(cfg.SecretsFile) == "" {
		return errors.New("secrets-file is required when secrets-provider=file")
	}

	// Validate retry settings
	if cfg.RetryMaxAttempts <= 0 {
		return errors.New("retry-max-attempts must be greater than 0")
	}
	if cfg.RetryMaxAttempts > 50 {
		return errors.New("retry-max-attempts cannot exceed 50 (safety limit)")
	}
	if cfg.RetryBaseDelay <= 0 {
		return errors.New("retry-base-delay must be greater than 0")
	}
	if cfg.RetryMaxDelay <= 0 {
		return errors.New("retry-max-delay must be greater than 0")
	}
	if cfg.RetryBaseDelay > cfg.RetryMaxDelay {
		return errors.New("retry-base-delay cannot be greater than retry-max-delay")
	}
	if cfg.RetryCircuitBreaker < 0 {
		return errors.New("retry-circuit-breaker must be >= 0")
	}
	if cfg.RetryCircuitBreaker > 0 && cfg.RetryCircuitBreaker > cfg.RetryMaxAttempts {
		return errors.New("retry-circuit-breaker cannot exceed retry-max-attempts")
	}

	// Security warning for insecure skip verify
	if cfg.InsecureSkipVerify {
		log.Warn().Msg("WARNING: TLS verification is disabled. This should only be used in trusted lab environments")
	}

	return nil
}

// maskPassword returns a masked version of password for logging
func maskPassword(pwd string) string {
	if pwd == "" {
		return "(empty)"
	}
	if len(pwd) <= 4 {
		return "****"
	}
	return pwd[:2] + "****" + pwd[len(pwd)-2:]
}

func writeDummyConfig(path string) error {
	ext := strings.ToLower(filepath.Ext(path))
	dummy := ""
	switch ext {
	case ".yaml", ".yml":
		dummy = `# NCC Runner configuration (dummy values)

# Required
clusters: "10.2.XX.XX,10.0.XX.XX"      	  # Comma-separated list of Prism Element cluster IPs/cluster FQDNs
# clusters-file: ""                        # Optional: cluster or cluster,username[,password] per line; overrides clusters when set
username: "admin"                         # Prism element username
password: ""                              # Prefer env NCC_PASSWORD in CLI; leave empty here if using env
ncc-api-version: v4                       # v4 (default) or Legacy (Prism Gateway v1 start-checks only; v1 accepted as alias)
nutanix-v4-api-version: v4.2              # v4 path revision (v4.2 default; e.g. v4.1, v4.0.a1)

# TLS and timeouts
insecure-skip-verify: false               # Set true only for lab/self-signed
timeout: "15m"                            # Per-cluster overall timeout  
request-timeout: "30s"                    # Per HTTP request timeout  
poll-interval: "15s"                      # Polling interval for task status  
poll-jitter: "2s"                         # Random jitter to avoid herd behavior  

# Concurrency and outputs
max-parallel: 4                           # Parallel clusters processed  
outputs: "html,csv"                       # One or more: html,csv,json,markdown,sarif
output-dir-logs: "nccfiles"               # Directory for raw NCC summary text  
output-dir-filtered: "outputfiles"        # Directory for generated HTML/CSV  
single-report: false                      # Also write ncc-report-single.html
run-history: false                        # Save each run snapshot under run-history-dir
run-history-dir: "outputfiles/runs"       # History base directory
retain-last: 0                            # Keep last N snapshots (0 = unlimited)
retain-days: 0                            # Keep snapshots newer than N days (0 = unlimited)
artifact-retain-days: 0                   # Remove generated artifacts older than N days (0 = unlimited)
artifact-retain-max-files: 0              # Keep only N newest generated artifacts (0 = unlimited)
notify-on-regression: false               # Notify only when FAIL count increases
adaptive-parallelism: true                # Reduce/increase effective concurrency on 429s
policy-gates: ""                          # e.g. new-fails>0,fail-rate>2,min-health-score<90,flaky-checks>0,timeout-clusters>0,auth-failures>0
quiet-hours: ""                           # Local HH:MM-HH:MM notification quiet window
maintenance-windows: ""                   # RFC3339 windows start/end[,start/end...]
flaky-lookback-runs: 6                    # Runs to inspect for flaky checks
flaky-min-transitions: 2                  # Minimum severity transitions to mark flaky
exclude-alert-titles: ""                  # Comma-separated NCC alert titles to exclude from final reports/HTML
exclude-alert-titles-file: ""             # Optional file with one alert title per line
exclude-alert-match-mode: "exact"         # exact (default), contains, regex

# Logging
log-file: "logs/ncc-runner.log"           # Rotated JSON logs path  
log-level: "2"                            # 0 trace, 1 debug, 2 info, 3 warn, 4 error  
log-http: false                           # Set true only for debugging; logs request/response dumps  

# Retry behavior
retry-max-attempts: 6                     # Max attempts per request  
retry-base-delay: "400ms"                 # Base backoff delay  
retry-max-delay: "8s"                     # Max jittered backoff delay  
retry-circuit-breaker: 3                  # Fail fast after N consecutive retryable failures

# Email notifications
email-enabled: false
email-attach-html: false
notify-digest: false
smtp-server: "smtp.example.com"
smtp-port: 587
smtp-user: "ncc@example.com"
smtp-password: ""
email-from: "ncc@example.com"
email-to: "ops@example.com,sre@example.com"
email-use-tls: true
# email-subject-template: ""              # Optional Go text/template, e.g. "NCC {{.Cluster}}: {{.FailCount}} FAIL"
# email-body-template: ""                 # Optional Go text/template for the email body
# Webhook notifications
webhook-enabled: false
webhook-include-html: false
webhook-url: "https://hooks.example.com/ncc"
webhook-headers:
  X-Auth-Token: "changeme"
# webhook-template: ""                     # Optional Go text/template for the webhook body (must render valid JSON)

# Slack notifications
slack-enabled: false
slack-webhook-url: ""
slack-channel: ""
secrets-provider: ""                      # env or file
secrets-file: ""                          # YAML/JSON key-value map when secrets-provider=file

`
	case ".json":
		dummy = `{
  "clusters": "10.0.0.1,10.0.0.2",
  "clusters-file": "",
  "username": "admin",
  "password": "",
  "ncc-api-version": "v4",
  "nutanix-v4-api-version": "v4.2",
  "insecure-skip-verify": false,
  "timeout": "15m",
  "request-timeout": "30s",
  "poll-interval": "15s",
  "poll-jitter": "2s",
  "max-parallel": 4,
  "outputs": "html,csv",
  "output-dir-logs": "nccfiles",
  "output-dir-filtered": "outputfiles",
  "single-report": false,
  "run-history": false,
  "run-history-dir": "outputfiles/runs",
  "retain-last": 0,
  "retain-days": 0,
  "artifact-retain-days": 0,
  "artifact-retain-max-files": 0,
  "notify-on-regression": false,
  "adaptive-parallelism": true,
  "policy-gates": "",
  "quiet-hours": "",
  "maintenance-windows": "",
  "flaky-lookback-runs": 6,
  "flaky-min-transitions": 2,
  "exclude-alert-titles": "",
  "exclude-alert-titles-file": "",
  "exclude-alert-match-mode": "exact",
  "log-file": "logs/ncc-runner.log",
  "log-level": "2",
  "log-http": false,
  "retry-max-attempts": 6,
  "retry-base-delay": "400ms",
  "retry-max-delay": "8s",
  "retry-circuit-breaker": 3,
  "email-enabled": false,
  "email-attach-html": false,
  "notify-digest": false,
  "smtp-server": "smtp.example.com",
  "smtp-port": 587,
  "smtp-user": "ncc@example.com",
  "smtp-password": "",
  "email-from": "ncc@example.com",
  "email-to": "ops@example.com,sre@example.com",
  "email-use-tls": true,
  "webhook-enabled": false,
  "webhook-include-html": false,
  "webhook-url": "https://hooks.example.com/ncc",
  "webhook-headers": {
    "X-Auth-Token": "changeme"
  },
  "slack-enabled": false,
  "slack-webhook-url": "",
  "slack-channel": "",
  "secrets-provider": "",
  "secrets-file": ""
}
`
	default:
		dummy = `# NCC Runner configuration (dummy values)

# Required
clusters: "10.2.XX.XX,10.0.XX.XX"      	  # Comma-separated list of Prism Element cluster IPs/cluster FQDNs
# clusters-file: ""                        # Optional: cluster or cluster,username[,password] per line; overrides clusters when set
username: "admin"                         # Prism element username
password: ""                              # Prefer env NCC_PASSWORD in CLI; leave empty here if using env
ncc-api-version: v4                       # v4 (default) or Legacy (Prism Gateway v1 start-checks only; v1 accepted as alias)
nutanix-v4-api-version: v4.2              # v4 path revision (v4.2 default; e.g. v4.1, v4.0.a1)

# TLS and timeouts
insecure-skip-verify: false               # Set true only for lab/self-signed
timeout: "15m"                            # Per-cluster overall timeout  
request-timeout: "30s"                    # Per HTTP request timeout  
poll-interval: "15s"                      # Polling interval for task status  
poll-jitter: "2s"                         # Random jitter to avoid herd behavior  

# Concurrency and outputs
max-parallel: 4                           # Parallel clusters processed  
outputs: "html,csv"                       # One or more: html,csv,json,markdown,sarif
output-dir-logs: "nccfiles"               # Directory for raw NCC summary text  
output-dir-filtered: "outputfiles"        # Directory for generated HTML/CSV  
single-report: false                      # Also write ncc-report-single.html
run-history: false                        # Save each run snapshot under run-history-dir
run-history-dir: "outputfiles/runs"       # History base directory
retain-last: 0                            # Keep last N snapshots (0 = unlimited)
retain-days: 0                            # Keep snapshots newer than N days (0 = unlimited)
notify-on-regression: false               # Notify only when FAIL count increases
adaptive-parallelism: true                # Reduce/increase effective concurrency on 429s

# Logging
log-file: "logs/ncc-runner.log"           # Rotated JSON logs path  
log-level: "2"                            # 0 trace, 1 debug, 2 info, 3 warn, 4 error  
log-http: false                           # Set true only for debugging; logs request/response dumps  

# Retry behavior
retry-max-attempts: 6                     # Max attempts per request  
retry-base-delay: "400ms"                 # Base backoff delay  
retry-max-delay: "8s"                     # Max jittered backoff delay  
retry-circuit-breaker: 3                  # Fail fast after N consecutive retryable failures

# Email notifications
email-enabled: false
email-attach-html: false
notify-digest: false
smtp-server: "smtp.example.com"
smtp-port: 587
smtp-user: "ncc@example.com"
smtp-password: ""
email-from: "ncc@example.com"
email-to: "ops@example.com,sre@example.com"
email-use-tls: true
# email-subject-template: ""              # Optional Go text/template, e.g. "NCC {{.Cluster}}: {{.FailCount}} FAIL"
# email-body-template: ""                 # Optional Go text/template for the email body
# Webhook notifications
webhook-enabled: false
webhook-include-html: false
webhook-url: "https://hooks.example.com/ncc"
webhook-headers:
  X-Auth-Token: "changeme"
# webhook-template: ""                     # Optional Go text/template for the webhook body (must render valid JSON)

# Slack notifications
slack-enabled: false
slack-webhook-url: ""
slack-channel: ""
`
	}
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, []byte(dummy), 0644)
}

func parseLogLevel(s string) zerolog.Level {
	if s == "" {
		if env := os.Getenv("LOG_LEVEL"); env != "" {
			s = env
		} else {
			return zerolog.InfoLevel
		}
	}
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "trace", "0":
		return zerolog.TraceLevel
	case "debug", "1":
		return zerolog.DebugLevel
	case "info", "2":
		return zerolog.InfoLevel
	case "warn", "warning", "3":
		return zerolog.WarnLevel
	case "error", "4":
		return zerolog.ErrorLevel
	case "fatal", "5":
		return zerolog.FatalLevel
	default:
		if n, err := strconv.Atoi(s); err == nil {
			if n >= math.MinInt8 && n <= math.MaxInt8 {
				return zerolog.Level(n)
			}
		}
		return zerolog.InfoLevel
	}
}

func parseBoolStrict(v interface{}) (bool, error) {
	switch b := v.(type) {
	case bool:
		return b, nil
	case string:
		p, err := strconv.ParseBool(strings.TrimSpace(b))
		if err != nil {
			return false, err
		}
		return p, nil
	default:
		return false, fmt.Errorf("expected bool/string, got %T", v)
	}
}

func parseIntStrict(v interface{}) (int, error) {
	switch n := v.(type) {
	case int:
		return n, nil
	case int64:
		return int(n), nil
	case float64:
		if math.Trunc(n) != n {
			return 0, fmt.Errorf("expected integer value, got %v", n)
		}
		return int(n), nil
	case string:
		i, err := strconv.Atoi(strings.TrimSpace(n))
		if err != nil {
			return 0, err
		}
		return i, nil
	default:
		return 0, fmt.Errorf("expected int/string, got %T", v)
	}
}

func parseStringStrict(v interface{}) (string, error) {
	switch s := v.(type) {
	case string:
		return s, nil
	default:
		return "", fmt.Errorf("expected string, got %T", v)
	}
}

func parseMapStringStringStrict(v interface{}) error {
	switch m := v.(type) {
	case map[string]interface{}:
		for k, vv := range m {
			if _, ok := vv.(string); !ok {
				return fmt.Errorf("key %q must be string value, got %T", k, vv)
			}
		}
		return nil
	case map[string]string:
		return nil
	default:
		return fmt.Errorf("expected object/map, got %T", v)
	}
}

func validateLogLevelStrict(s string) error {
	v := strings.ToLower(strings.TrimSpace(s))
	if v == "" {
		return nil
	}
	switch v {
	case "trace", "debug", "info", "warn", "warning", "error", "fatal",
		"0", "1", "2", "3", "4", "5":
		return nil
	default:
		return fmt.Errorf("invalid log-level %q (valid: trace/debug/info/warn/error/fatal or 0..5)", s)
	}
}

// validateConfigFileRawTypes validates values as written in the config file (before env/flag overrides)
// so typos like insecure-skip-verify: flse are surfaced clearly.
func validateConfigFileRawTypes() error {
	if viper.ConfigFileUsed() == "" {
		return nil
	}
	allowedTopKeys := map[string]bool{
		"config":               true,
		"update":               true,
		"skip-preflight-check": true,
		"clusters":             true, "clusters-file": true, "cluster-source-mode": true, "pcs": true, "pcs-file": true, "prism-central-url": true, "discover-api-version": true,
		"username": true, "password": true, "ncc-api-version": true, "nutanix-v4-api-version": true,
		"insecure-skip-verify": true, "ca-bundle": true, "pin-sha256": true, "timeout": true, "request-timeout": true, "poll-interval": true, "poll-jitter": true,
		"max-parallel": true, "outputs": true, "output-dir-logs": true, "output-dir-filtered": true,
		"single-report": true, "run-history": true, "run-history-dir": true, "retain-last": true, "retain-days": true, "artifact-retain-days": true, "artifact-retain-max-files": true,
		"notify-on-regression": true, "adaptive-parallelism": true,
		"policy-gates": true, "quiet-hours": true, "maintenance-windows": true,
		"flaky-lookback-runs": true, "flaky-min-transitions": true,
		"log-file": true, "log-level": true, "log-http": true,
		"retry-max-attempts": true, "retry-base-delay": true, "retry-max-delay": true, "retry-circuit-breaker": true,
		"prom-enabled": true, "prom-dir": true, "severity-filter": true, "exclude-alert-titles": true, "exclude-alert-titles-file": true, "exclude-alert-match-mode": true, "dry-run": true, "replay": true,
		"max-idle-conns": true, "max-idle-conns-per-host": true, "max-conns-per-host": true, "idle-conn-timeout": true,
		"gen-test-agg":  true,
		"email-enabled": true, "email-attach-html": true, "notify-digest": true,
		"smtp-server": true, "smtp-port": true, "smtp-user": true, "smtp-password": true,
		"email-from": true, "email-to": true, "email-use-tls": true, "smtp-insecure-skip-verify": true,
		"email-subject-template": true, "email-body-template": true,
		"webhook-enabled": true, "webhook-include-html": true, "webhook-url": true, "webhook-headers": true, "webhook-template": true, "webhook-secret": true,
		"slack-enabled": true, "slack-webhook-url": true, "slack-channel": true,
		"notification-deadletter-dir": true,
		"secrets-provider":            true, "secrets-file": true,
	}
	for key := range viper.AllSettings() {
		// Only enforce unknown-key checks for keys that actually come from config file.
		// Viper may contain extra keys set from flags/env/programmatic overrides.
		if !viper.InConfig(key) {
			continue
		}
		if !allowedTopKeys[key] {
			return fmt.Errorf("unknown config key %q", key)
		}
	}

	stringKeys := []string{
		"clusters", "clusters-file", "cluster-source-mode", "pcs", "pcs-file", "prism-central-url", "discover-api-version",
		"username", "password", "ncc-api-version", "nutanix-v4-api-version",
		"timeout", "request-timeout", "poll-interval", "poll-jitter",
		"outputs", "output-dir-logs", "output-dir-filtered", "run-history-dir",
		"log-file", "log-level", "retry-base-delay", "retry-max-delay", "prom-dir",
		"severity-filter", "exclude-alert-titles", "exclude-alert-titles-file", "exclude-alert-match-mode", "idle-conn-timeout", "policy-gates", "quiet-hours", "maintenance-windows",
		"ca-bundle", "pin-sha256",
		"smtp-server", "smtp-user", "smtp-password", "email-from", "email-to",
		"email-subject-template", "email-body-template",
		"webhook-url", "webhook-template", "webhook-secret", "notification-deadletter-dir", "slack-webhook-url", "slack-channel", "secrets-provider", "secrets-file",
	}
	for _, key := range stringKeys {
		if !viper.InConfig(key) {
			continue
		}
		if _, err := parseStringStrict(viper.Get(key)); err != nil {
			return fmt.Errorf("%s: invalid string value (%v)", key, err)
		}
	}

	boolKeys := []string{
		"update",
		"skip-preflight-check",
		"insecure-skip-verify", "dry-run", "replay", "log-http",
		"prom-enabled",
		"run-history", "single-report", "notify-on-regression", "adaptive-parallelism",
		"email-enabled", "email-attach-html", "notify-digest", "email-use-tls", "smtp-insecure-skip-verify",
		"webhook-enabled", "webhook-include-html", "slack-enabled",
	}
	for _, key := range boolKeys {
		if !viper.InConfig(key) {
			continue
		}
		if _, err := parseBoolStrict(viper.Get(key)); err != nil {
			return fmt.Errorf("%s: invalid boolean value (%v)", key, err)
		}
	}
	intKeys := []string{
		"max-parallel", "retry-max-attempts", "retry-circuit-breaker", "max-idle-conns", "max-idle-conns-per-host",
		"max-conns-per-host", "smtp-port", "retain-last", "retain-days", "artifact-retain-days", "artifact-retain-max-files", "gen-test-agg",
		"flaky-lookback-runs", "flaky-min-transitions",
	}
	for _, key := range intKeys {
		if !viper.InConfig(key) {
			continue
		}
		if _, err := parseIntStrict(viper.Get(key)); err != nil {
			return fmt.Errorf("%s: invalid integer value (%v)", key, err)
		}
	}
	durationKeys := []string{
		"timeout", "request-timeout", "poll-interval", "poll-jitter",
		"retry-base-delay", "retry-max-delay", "idle-conn-timeout",
	}
	for _, key := range durationKeys {
		if !viper.InConfig(key) {
			continue
		}
		raw := viper.Get(key)
		s, ok := raw.(string)
		if !ok {
			return fmt.Errorf("%s: expected duration string, got %T", key, raw)
		}
		if _, err := time.ParseDuration(strings.TrimSpace(s)); err != nil {
			return fmt.Errorf("%s: invalid duration %q (%v)", key, s, err)
		}
	}
	if viper.InConfig("webhook-headers") {
		if err := parseMapStringStringStrict(viper.Get("webhook-headers")); err != nil {
			return fmt.Errorf("webhook-headers: invalid map value (%v)", err)
		}
	}
	if viper.InConfig("log-level") {
		if err := validateLogLevelStrict(viper.GetString("log-level")); err != nil {
			return err
		}
	}
	return nil
}

// configBaseDir returns the absolute directory of the loaded config file. It is
// used to anchor relative output paths so they resolve consistently regardless
// of the process working directory. Returns "" when no config file is in use
// (flag-only runs), in which case the legacy cwd-relative behavior is kept.
func configBaseDir(cfgFile string) string {
	p := strings.TrimSpace(cfgFile)
	if p == "" {
		p = strings.TrimSpace(viper.ConfigFileUsed())
	}
	if p == "" {
		return ""
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return ""
	}
	return filepath.Dir(abs)
}

// resolveUnderBase makes a relative path absolute by anchoring it to base. An
// already-absolute path, an empty path, or an empty base is returned unchanged.
// This is the core of the "self-healing" output-path resolution: a scheduled
// (cron) run executes with an arbitrary working directory (often the invoking
// user's home), so a relative output-dir like "outputfiles" would otherwise be
// created beside that cwd and silently diverge from where the API/UI reads
// results. Anchoring to the config file's directory keeps scheduled runs and
// interactive runs writing to the same place.
func resolveUnderBase(p, base string) string {
	p = strings.TrimSpace(p)
	if p == "" || base == "" || filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(base, p)
}

func bindConfig() (Config, error) {
	cfgFile := viper.GetString("config")
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		if _, err := os.Stat(cfgFile); errors.Is(err, os.ErrNotExist) {
			if err := writeDummyConfig(cfgFile); err != nil {
				return Config{}, fmt.Errorf("failed to create dummy config at %s: %w", cfgFile, err)
			}
			fmt.Printf("Created dummy config at %s. Please edit it according to your Nutanix environment and re-run.\n", cfgFile)
			return Config{}, errors.New("dummy config created; edit and re-run")
		}
		if err := viper.ReadInConfig(); err != nil {
			var nf viper.ConfigFileNotFoundError
			if !errors.As(err, &nf) {
				return Config{}, fmt.Errorf("read config: %w", err)
			}
		}
		if err := validateConfigFileRawTypes(); err != nil {
			return Config{}, fmt.Errorf("invalid config file values: %w", err)
		}
	}

	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	clustersFromFlag := splitCSV(viper.GetString("clusters"))
	clustersFile := strings.TrimSpace(viper.GetString("clusters-file"))
	pcsFromFlag := splitCSV(viper.GetString("pcs"))
	pcsFile := strings.TrimSpace(viper.GetString("pcs-file"))
	clusterSourceMode := strings.TrimSpace(viper.GetString("cluster-source-mode"))
	clusterCreds := map[string]ClusterCredential{}
	if clustersFile != "" {
		lines, fileCreds, err := readClusterFile(clustersFile)
		if err != nil {
			return Config{}, fmt.Errorf("clusters-file %s: %w", clustersFile, err)
		}
		if len(lines) > 0 {
			clustersFromFlag = lines
		}
		clusterCreds = fileCreds
	}
	if len(clustersFromFlag) > 0 {
		normalizedClusters, err := normalizeClusters(clustersFromFlag)
		if err != nil {
			return Config{}, err
		}
		clustersFromFlag = normalizedClusters
	}
	if len(clusterCreds) > 0 {
		normalizedCreds := make(map[string]ClusterCredential, len(clusterCreds))
		for cluster, cred := range clusterCreds {
			norm, err := normalizeClusterAddress(cluster)
			if err != nil {
				return Config{}, fmt.Errorf("clusters-file credential key %s: %w", cluster, err)
			}
			if existing, ok := normalizedCreds[norm]; ok {
				if (strings.TrimSpace(existing.Username) != "" && strings.TrimSpace(cred.Username) != "" && existing.Username != cred.Username) ||
					(strings.TrimSpace(existing.Password) != "" && strings.TrimSpace(cred.Password) != "" && existing.Password != cred.Password) {
					return Config{}, fmt.Errorf("clusters-file has conflicting credentials for %s after normalization", norm)
				}
			}
			normalizedCreds[norm] = cred
		}
		clusterCreds = normalizedCreds
	}
	if pcsFile != "" {
		lines, err := readLineValuesFile(pcsFile)
		if err != nil {
			return Config{}, fmt.Errorf("pcs-file %s: %w", pcsFile, err)
		}
		if len(lines) > 0 {
			pcsFromFlag = lines
		}
	}
	nccAPIVer, err := normalizeNCCAPIVersion(viper.GetString("ncc-api-version"))
	if err != nil {
		return Config{}, fmt.Errorf("ncc-api-version: %w", err)
	}
	cfg := Config{
		Clusters:                  clustersFromFlag,
		ClustersFile:              clustersFile,
		ClusterCredentials:        clusterCreds,
		ClusterSourceMode:         clusterSourceMode,
		PCs:                       pcsFromFlag,
		PCsFile:                   pcsFile,
		PrismCentralURL:           strings.TrimSpace(viper.GetString("prism-central-url")),
		DiscoverAPIVersion:        strings.TrimSpace(viper.GetString("discover-api-version")),
		Username:                  viper.GetString("username"),
		Password:                  viper.GetString("password"),
		InsecureSkipVerify:        viper.GetBool("insecure-skip-verify"),
		CABundle:                  strings.TrimSpace(viper.GetString("ca-bundle")),
		PinSHA256:                 splitCSV(viper.GetString("pin-sha256")),
		Timeout:                   mustParseDur(viper.GetString("timeout"), defaultTimeout),
		RequestTimeout:            mustParseDur(viper.GetString("request-timeout"), defaultRequestTimeout),
		PollInterval:              mustParseDur(viper.GetString("poll-interval"), defaultPollInterval),
		PollJitter:                mustParseDur(viper.GetString("poll-jitter"), defaultPollJitter),
		OutputDirLogs:             viper.GetString("output-dir-logs"),
		OutputDirFiltered:         viper.GetString("output-dir-filtered"),
		OutputFormats:             splitCSV(viper.GetString("outputs")),
		MaxParallel:               viper.GetInt("max-parallel"),
		TLSMinVersion:             tls.VersionTLS12,
		LogFile:                   viper.GetString("log-file"),
		LogLevel:                  viper.GetString("log-level"),
		LogHTTP:                   viper.GetBool("log-http"),
		RetryMaxAttempts:          viper.GetInt("retry-max-attempts"),
		RetryBaseDelay:            mustParseDur(viper.GetString("retry-base-delay"), defaultRetryBaseDelay),
		RetryMaxDelay:             mustParseDur(viper.GetString("retry-max-delay"), defaultRetryMaxDelay),
		RetryCircuitBreaker:       viper.GetInt("retry-circuit-breaker"),
		MaxIdleConns:              viper.GetInt("max-idle-conns"),
		MaxIdleConnsPerHost:       viper.GetInt("max-idle-conns-per-host"),
		MaxConnsPerHost:           viper.GetInt("max-conns-per-host"),
		IdleConnTimeout:           mustParseDur(viper.GetString("idle-conn-timeout"), httpclient.DefaultIdleConnTimeout),
		EmailEnabled:              viper.GetBool("email-enabled"),
		EmailAttachHTML:           viper.GetBool("email-attach-html"),
		NotifyDigest:              viper.GetBool("notify-digest"),
		SMTPServer:                viper.GetString("smtp-server"),
		SMTPPort:                  viper.GetInt("smtp-port"),
		SMTPUser:                  viper.GetString("smtp-user"),
		SMTPPassword:              viper.GetString("smtp-password"),
		EmailFrom:                 viper.GetString("email-from"),
		EmailTo:                   splitCSV(viper.GetString("email-to")),
		EmailUseTLS:               viper.GetBool("email-use-tls"),
		SMTPInsecureSkipVerify:    viper.GetBool("smtp-insecure-skip-verify"),
		EmailSubjectTemplate:      viper.GetString("email-subject-template"),
		EmailBodyTemplate:         viper.GetString("email-body-template"),
		WebhookEnabled:            viper.GetBool("webhook-enabled"),
		WebhookIncludeHTML:        viper.GetBool("webhook-include-html"),
		WebhookURL:                viper.GetString("webhook-url"),
		WebhookHeaders:            viper.GetStringMapString("webhook-headers"),
		WebhookTemplate:           viper.GetString("webhook-template"),
		WebhookSecret:             viper.GetString("webhook-secret"),
		NotificationDeadLetterDir: strings.TrimSpace(viper.GetString("notification-deadletter-dir")),
		SeverityFilter:            splitCSV(viper.GetString("severity-filter")),
		ExcludeAlertTitles:        splitCSV(viper.GetString("exclude-alert-titles")),
		ExcludeAlertTitlesFile:    strings.TrimSpace(viper.GetString("exclude-alert-titles-file")),
		ExcludeAlertMatchMode:     strings.TrimSpace(viper.GetString("exclude-alert-match-mode")),
		DryRun:                    viper.GetBool("dry-run"),
		RunHistoryEnabled:         viper.GetBool("run-history"),
		RunHistoryDir:             strings.TrimSpace(viper.GetString("run-history-dir")),
		RetainLastRuns:            viper.GetInt("retain-last"),
		RetainDays:                viper.GetInt("retain-days"),
		ArtifactRetainDays:        viper.GetInt("artifact-retain-days"),
		ArtifactRetainMaxFiles:    viper.GetInt("artifact-retain-max-files"),
		SingleReport:              viper.GetBool("single-report"),
		NotifyOnRegression:        viper.GetBool("notify-on-regression"),
		AdaptiveParallelism:       viper.GetBool("adaptive-parallelism"),
		PolicyGates:               splitCSV(viper.GetString("policy-gates")),
		QuietHours:                strings.TrimSpace(viper.GetString("quiet-hours")),
		MaintenanceWindows:        splitCSV(viper.GetString("maintenance-windows")),
		FlakyLookbackRuns:         viper.GetInt("flaky-lookback-runs"),
		FlakyMinTransitions:       viper.GetInt("flaky-min-transitions"),
		SlackEnabled:              viper.GetBool("slack-enabled"),
		SlackWebhookURL:           viper.GetString("slack-webhook-url"),
		SlackChannel:              viper.GetString("slack-channel"),
		SecretsProvider:           strings.TrimSpace(viper.GetString("secrets-provider")),
		SecretsFile:               strings.TrimSpace(viper.GetString("secrets-file")),
		NCCAPIVersion:             nccAPIVer,
		NutanixV4APIVersion:       strings.ToLower(strings.TrimSpace(viper.GetString("nutanix-v4-api-version"))),
	}
	// Apply defaults
	if cfg.NutanixV4APIVersion == "" {
		cfg.NutanixV4APIVersion = defaultNutanixV4APIVersion
	}
	if cfg.OutputDirLogs == "" {
		cfg.OutputDirLogs = defaultOutputDirLogs
	}
	if cfg.OutputDirFiltered == "" {
		cfg.OutputDirFiltered = defaultOutputDirFiltered
	}
	// Self-healing path resolution: anchor relative output directories to the
	// config file's directory so a scheduled run (cron, arbitrary cwd) writes to
	// the same place an interactive run from the install dir does. Resolved here
	// (before RunHistoryDir is derived from OutputDirFiltered) so the history dir
	// inherits the absolute base too. Absolute paths are left untouched.
	if cfgBase := configBaseDir(cfgFile); cfgBase != "" {
		cfg.OutputDirLogs = resolveUnderBase(cfg.OutputDirLogs, cfgBase)
		cfg.OutputDirFiltered = resolveUnderBase(cfg.OutputDirFiltered, cfgBase)
	}
	if len(cfg.OutputFormats) == 0 {
		cfg.OutputFormats = []string{defaultOutputFormat}
	}
	if cfg.MaxParallel <= 0 {
		cfg.MaxParallel = defaultMaxParallel
	}
	if cfg.LogFile == "" {
		cfg.LogFile = defaultLogFile
	}
	if viper.IsSet("prom-enabled") {
		cfg.PromEnabled = viper.GetBool("prom-enabled")
	} else {
		cfg.PromEnabled = false
	}
	cfg.PromDir = viper.GetString("prom-dir")
	if cfg.PromDir == "" {
		cfg.PromDir = defaultPromDir
	}
	if cfg.ExcludeAlertMatchMode == "" {
		cfg.ExcludeAlertMatchMode = defaultExcludeMatchMode
	}
	mode, err := normalizeClusterSourceMode(cfg.ClusterSourceMode)
	if err != nil {
		return cfg, err
	}
	cfg.ClusterSourceMode = mode
	if cfg.DiscoverAPIVersion == "" {
		cfg.DiscoverAPIVersion = defaultDiscoverAPIVersion
	}
	cfg.DiscoverAPIVersion = strings.ToLower(strings.TrimSpace(cfg.DiscoverAPIVersion))
	if cfg.DiscoverAPIVersion != "v3" && cfg.DiscoverAPIVersion != "v4" {
		return cfg, fmt.Errorf("discover-api-version must be v3 or v4, got %q", cfg.DiscoverAPIVersion)
	}
	if cfg.ClusterSourceMode == "pc" {
		if len(cfg.PCs) == 0 && strings.TrimSpace(cfg.PrismCentralURL) != "" {
			cfg.PCs = []string{cfg.PrismCentralURL}
		}
	}
	if cfg.ExcludeAlertTitlesFile != "" {
		fileTitles, err := loadExcludeAlertTitlesFromFile(cfg.ExcludeAlertTitlesFile)
		if err != nil {
			return cfg, fmt.Errorf("exclude-alert-titles-file: %w", err)
		}
		cfg.ExcludeAlertTitles = mergeUniqueStrings(cfg.ExcludeAlertTitles, fileTitles)
	}
	excludeMode, err := normalizeExcludeAlertMatchMode(cfg.ExcludeAlertMatchMode)
	if err != nil {
		return cfg, err
	}
	cfg.ExcludeAlertMatchMode = excludeMode
	if cfg.RunHistoryDir == "" {
		cfg.RunHistoryDir = filepath.Join(cfg.OutputDirFiltered, "runs")
	} else if cfgBase := configBaseDir(cfgFile); cfgBase != "" {
		cfg.RunHistoryDir = resolveUnderBase(cfg.RunHistoryDir, cfgBase)
	}
	if cfg.FlakyLookbackRuns <= 0 {
		cfg.FlakyLookbackRuns = defaultFlakyLookbackRuns
	}
	if cfg.FlakyMinTransitions <= 0 {
		cfg.FlakyMinTransitions = defaultFlakyTransitions
	}
	if cfg.RetryMaxAttempts <= 0 {
		cfg.RetryMaxAttempts = defaultRetryAttempts
	}
	if cfg.RetryBaseDelay <= 0 {
		cfg.RetryBaseDelay = defaultRetryBaseDelay
	}
	if cfg.RetryMaxDelay <= 0 {
		cfg.RetryMaxDelay = defaultRetryMaxDelay
	}
	if cfg.RetryCircuitBreaker <= 0 {
		cfg.RetryCircuitBreaker = defaultRetryCircuitBreaker
	}
	if err := applySecretsToConfig(&cfg); err != nil {
		return cfg, fmt.Errorf("secret resolution failed: %w", err)
	}

	// Validate configuration
	if err := validateConfig(cfg); err != nil {
		return cfg, fmt.Errorf("configuration validation failed: %w", err)
	}

	return cfg, nil
}

// checkOutputPermissions verifies the process can create/open files for write in the log path
// and in each output directory. Returns a clear error on first failure so permission issues
// are reported early instead of during normal writes.
//
// The probe never truncates an existing target file (it opens with O_APPEND
// only) and, when it has to create the file itself, removes it again once the
// check passes. This matters most for "aggregated index.html": that path is
// also the run's real report output, and in the scoped/per-run execution
// model used by the API server it lives in a fresh, run-specific directory.
// If a run aborts early (e.g. cluster discovery failure) after this probe
// ran but before the real report was ever written, a truncated 0-byte
// index.html left behind here would later be read by mergeIndexHTML (see
// cmd/ncc-api-server/runmanager.go) as "this run legitimately produced zero
// rows", wiping every owned cluster's entries from the canonical aggregated
// report. Cleaning up the probe artifact (and never truncating real content)
// avoids that.
func checkOutputPermissions(cfg *Config) error {
	probeName := ".ncc-writecheck"
	indexHTML := filepath.Join(cfg.OutputDirFiltered, "index.html")
	checks := []struct {
		label  string
		path   string
		remove bool
	}{
		{"log file", cfg.LogFile, false},
		{"output dir (raw logs)", filepath.Join(cfg.OutputDirLogs, probeName), true},
		{"output dir (filtered)", filepath.Join(cfg.OutputDirFiltered, probeName), true},
		{"aggregated index.html", indexHTML, true},
	}
	if cfg.PromEnabled {
		checks = append(checks, struct {
			label  string
			path   string
			remove bool
		}{"prom dir", filepath.Join(cfg.PromDir, probeName), true})
	}
	for _, c := range checks {
		dir := filepath.Dir(c.path)
		if dir != "." {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return fmt.Errorf("cannot create directory %s: %w", dir, err)
			}
		}
		existedBefore := false
		if _, statErr := os.Stat(c.path); statErr == nil {
			existedBefore = true
		}
		f, err := os.OpenFile(c.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("cannot open/create file for write (%s): %w", c.label, err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close probe file %s: %w", c.path, err)
		}
		if c.remove && !existedBefore {
			_ = os.Remove(c.path)
		}
	}
	return nil
}

// ==================== Logging ====================

// In setupFileLogger, add the new version fields to the global logger context
func setupFileLogger(logPath string, lvl zerolog.Level) error {
	dir := filepath.Dir(logPath)
	if dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	fileWriter := &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    20, // MB
		MaxBackups: 5,
		MaxAge:     30, // days
		Compress:   true,
	}
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	var gitRevision string
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" {
				gitRevision = s.Value
				break
			}
		}
		log.Logger = zerolog.New(fileWriter).Level(lvl).With().
			Timestamp().
			Str("git_revision", gitRevision).
			Str("go_version", bi.GoVersion).
			Str("Version", Version).
			Str("stream", Stream).
			Logger()
	} else {
		log.Logger = zerolog.New(fileWriter).Level(lvl).With().Timestamp().Logger()
	}
	return nil
}

// ==================== Retry Helpers ====================
// The shared retry/backoff helpers were extracted to goncc/internal/retryutil
// (so goncc/internal/notify can reuse them without an import cycle). These
// aliases keep the existing call sites in this package unchanged.
var (
	jitteredBackoff   = retryutil.JitteredBackoff
	isRetryableStatus = retryutil.IsRetryableStatus
	retryAfterDelay   = retryutil.RetryAfterDelay
)

// maxRateLimitBackoff caps Retry-After / computed backoff for HTTP 429 so a misbehaving server cannot sleep unbounded.
const maxRateLimitBackoff = 2 * time.Minute

const (
	maxRetryRequestBodyBytes  int64 = 2 << 20  // 2 MiB
	maxRetryResponseBodyBytes int64 = 64 << 20 // 64 MiB
)

var (
	adaptiveCurrentParallel int32
	adaptiveMaxParallel     int32
	adaptiveNotifyMu        sync.Mutex
	adaptiveNotifyCh        chan struct{}
)

func setAdaptiveParallelismNotify(ch chan struct{}) {
	adaptiveNotifyMu.Lock()
	defer adaptiveNotifyMu.Unlock()
	adaptiveNotifyCh = ch
}

func notifyAdaptiveParallelismChanged() {
	adaptiveNotifyMu.Lock()
	ch := adaptiveNotifyCh
	adaptiveNotifyMu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

func resetAdaptiveParallelism(maxParallel int) {
	if maxParallel < 1 {
		maxParallel = 1
	}
	atomic.StoreInt32(&adaptiveMaxParallel, int32(maxParallel))
	atomic.StoreInt32(&adaptiveCurrentParallel, int32(maxParallel))
}

func currentAdaptiveParallel(maxParallel int) int {
	cur := int(atomic.LoadInt32(&adaptiveCurrentParallel))
	if cur < 1 {
		return 1
	}
	if cur > maxParallel {
		return maxParallel
	}
	return cur
}

func noteHTTPStatusForAdaptiveParallelism(status int, cfg Config) {
	if !cfg.AdaptiveParallelism {
		return
	}
	maxP := int(atomic.LoadInt32(&adaptiveMaxParallel))
	if maxP <= 1 {
		return
	}
	switch status {
	case http.StatusTooManyRequests:
		for {
			cur := atomic.LoadInt32(&adaptiveCurrentParallel)
			if cur <= 1 {
				return
			}
			next := cur - 1
			if atomic.CompareAndSwapInt32(&adaptiveCurrentParallel, cur, next) {
				log.Warn().Int("adaptive_parallel", int(next)).Int("max_parallel", maxP).Msg("adaptive parallelism reduced after 429")
				notifyAdaptiveParallelismChanged()
				return
			}
		}
	default:
		if status >= 200 && status < 300 {
			for {
				cur := atomic.LoadInt32(&adaptiveCurrentParallel)
				max := atomic.LoadInt32(&adaptiveMaxParallel)
				if cur >= max {
					return
				}
				next := cur + 1
				if atomic.CompareAndSwapInt32(&adaptiveCurrentParallel, cur, next) {
					notifyAdaptiveParallelismChanged()
					return
				}
			}
		}
	}
}

func capRateLimitWait(d time.Duration) time.Duration {
	if d > maxRateLimitBackoff {
		return maxRateLimitBackoff
	}
	return d
}

func logRateLimitHeaders(op string, resp *http.Response) {
	if resp == nil {
		return
	}
	rem := resp.Header.Get("X-RateLimit-Remaining")
	reset := resp.Header.Get("X-RateLimit-Reset")
	apiRem := resp.Header.Get("X-Api-Ratelimit-Remaining")
	if rem == "" && reset == "" && apiRem == "" {
		return
	}
	log.Warn().Str("op", op).Str("X-RateLimit-Remaining", rem).Str("X-RateLimit-Reset", reset).
		Str("X-Api-Ratelimit-Remaining", apiRem).Msg("rate limit headers")
}

func readBodyWithLimit(body io.ReadCloser, maxBytes int64, context string) ([]byte, error) {
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("%s exceeds max size (%d bytes)", context, maxBytes)
	}
	return data, nil
}

// ==================== HTTP Client and File System ====================
// The HTTP client builder (connection pooling, TLS policy, redacted logging
// transport) was extracted to goncc/internal/httpclient. OSFS (the os-backed
// FS implementation) now lives in goncc/internal/model next to the FS
// interface. These aliases keep the existing call sites in this package
// unchanged.
var NewHTTPClient = httpclient.New

type OSFS = model.OSFS

// ==================== Prometheus Metrics ====================
// The Prometheus textfile writers were extracted to goncc/internal/promtext.
// sanitizeLabel/writePrometheusFile are aliases so existing call sites in this
// package are unchanged.
var (
	sanitizeLabel       = promtext.SanitizeLabel
	writePrometheusFile = promtext.WritePrometheusFile
)

// writeNotificationMetricsFile snapshots the run's notification accumulator
// (owned by internal/notify) and delegates the textfile rendering to promtext.
func writeNotificationMetricsFile(fs FS, promDir string) error {
	attempts, failures := notify.SnapshotMetrics()
	return promtext.WriteNotificationMetricsFile(fs, promDir, notify.Channels, attempts, failures)
}

// ==================== API Types ====================

type TaskStatus struct {
	PercentageComplete int    `json:"percentage_complete"`
	ProgressStatus     string `json:"progress_status"`
}

type NCCSummary struct {
	RunSummary string `json:"runSummary"`
}

// ==================== Parser ====================
// The NCC summary parser was extracted to goncc/internal/nccparse. These
// aliases keep the existing call sites in this package unchanged. splitLines is
// also used by non-parser code here, so it is aliased too.
var (
	splitLines                               = nccparse.SplitLines
	ParseSummary                             = nccparse.ParseSummary
	validateParsedAlertsAgainstPluginResults = nccparse.ValidateParsedAlertsAgainstPluginResults
)

type Row struct {
	Severity  string
	CheckName string
	Detail    template.HTML
}

func parseNCCHeader(path string) (HTMLMeta, error) {
	f, err := os.Open(path)
	if err != nil {
		return HTMLMeta{}, err
	}
	defer f.Close()

	var meta HTMLMeta
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		switch {
		case strings.HasPrefix(line, "Cluster Name:"):
			meta.ClusterName = strings.TrimSpace(strings.TrimPrefix(line, "Cluster Name:"))
		case strings.HasPrefix(line, "Cluster Version:"):
			meta.ClusterVersion = strings.TrimSpace(strings.TrimPrefix(line, "Cluster Version:"))
		case strings.HasPrefix(line, "NCC Version:"):
			meta.NCCVersion = strings.TrimSpace(strings.TrimPrefix(line, "NCC Version:"))
		}
	}
	if err := scanner.Err(); err != nil {
		return HTMLMeta{}, err
	}
	return meta, nil
}

// ==================== Notifications ====================
// The email/webhook/slack senders, retry wrappers, text/template overrides,
// and the per-channel delivery-metrics accumulator were extracted to
// goncc/internal/notify. These aliases keep the existing call sites in this
// package unchanged; the run-level accumulator now lives in that package and
// is read via notify.ResetMetrics / notify.SnapshotMetrics.
var (
	sendEmailWithRetry   = notify.SendEmailWithRetry
	sendWebhookWithRetry = notify.SendWebhookWithRetry
	sendSlackWithRetry   = notify.SendSlackWithRetry
	applyEmailTemplates  = notify.ApplyEmailTemplates
)

// ==================== Report Renderers ====================

// func generateHTMLNoMeta(fs FS, rows []Row, filename string) error {
// 	return generateHTML(fs, rows, filename, HTMLMeta{})
// }

// htmlNowForReport supplies the "Generated" timestamp for per-cluster HTML reports.
// Tests may replace it with a fixed clock for golden-file stability.
var htmlNowForReport = func() time.Time { return time.Now() }

func generateHTML(fs FS, rows []Row, filename string, meta HTMLMeta) error {
	if err := fs.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}
	const tmpl = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NCC Report - {{.Meta.ClusterName}}</title>
  <link rel="icon" type="image/svg+xml" href="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMzIiIGhlaWdodD0iMzIiIHZpZXdCb3g9IjAgMCAzMiAzMiIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHJlY3QgeD0iMiIgeT0iMiIgd2lkdGg9IjI4IiBoZWlnaHQ9IjI4IiByeD0iOCIgZmlsbD0iIzBCMTIyMCIvPgo8cGF0aCBkPSJNOSAyM1Y5SDEyTDIwIDE5VjlIMjNWMjNIMjBMMTIgMTNWMjNIOVoiIGZpbGw9IiMyMkM1NUUiLz4KPGNpcmNsZSBjeD0iMjQiIGN5PSI4IiByPSIyIiBmaWxsPSIjMzhCREY4Ii8+CjxjaXJjbGUgY3g9IjgiIGN5PSIyNCIgcj0iMiIgZmlsbD0iI0Y1OUUwQiIvPgo8L3N2Zz4=">
  <style>
    :root {
      --fail: #ef4444;
      --warn: #f59e0b;
      --info: #3b82f6;
      --err:  #374151;
      --border: #d1d5db;
      --thead: #f3f4f6;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      color: #111827;
      background: #f9fafb;
    }
    .page { max-width: 1100px; margin: 24px auto; padding: 0 16px 24px; }
    .header {
      display: flex;
      justify-content: space-between;
      align-items: flex-end;
      gap: 16px;
      margin-bottom: 16px;
      flex-wrap: wrap;
    }
    h1 { margin: 0; font-size: 22px; }
    .subtitle { margin-top: 4px; font-size: 13px; color: #6b7280; }
    .summary-line { font-size: 13px; color: #6b7280; margin-top: 6px; }
    .summary-line strong { color: #111827; }
    .tags { display: flex; flex-wrap: wrap; gap: 8px; justify-content: flex-end; }
    .tag {
      background: #fff;
      border: 1px solid #e5e7eb;
      border-radius: 999px;
      padding: 4px 10px;
      font-size: 11px;
      display: inline-flex;
      gap: 4px;
    }
    .tag .label { color: #6b7280; }
    .tag .value { font-weight: 600; color: #111827; }
    table { border-collapse: collapse; width: 100%; border: 1px solid var(--border); }
    thead th {
      position: sticky; top: 0; background: var(--thead);
      border-bottom: 1px solid var(--border);
      padding: 10px; text-align: left; font-size: 13px;
    }
    tbody td { border-bottom: 1px solid var(--border); padding: 10px; vertical-align: top; }
    tbody tr:nth-child(odd) { background: #fafafa; }
    .sev { display: inline-block; padding: 2px 8px; border-radius: 999px; font-weight: 600; font-size: 12px; }
    .sev.FAIL { color: #fff; background: var(--fail); }
    .sev.WARN { color: #111827; background: #fde68a; }
    .sev.INFO { color: #fff; background: var(--info); }
    .sev.ERR  { color: #111827; background: #e5e7eb; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; white-space: pre-wrap; word-break: break-word; }
  </style>
</head>
<body>
  <div class="page">
    <header class="header">
      <div>
        <h1>NCC Report</h1>
        <div class="subtitle">{{.Meta.ClusterName}}</div>
        <div class="summary-line">
          {{.Summary.FAIL}} FAIL, {{.Summary.WARN}} WARN, {{.Summary.ERR}} ERR, {{.Summary.INFO}} INFO
        </div>
      </div>
      <div class="tags">
        <div class="tag"><span class="label">Cluster</span><span class="value">{{.Meta.ClusterName}}</span></div>
        <div class="tag"><span class="label">Cluster Version</span><span class="value">{{.Meta.ClusterVersion}}</span></div>
        <div class="tag"><span class="label">NCC Version</span><span class="value">{{.Meta.NCCVersion}}</span></div>
        <div class="tag"><span class="label">Generated</span><span class="value">{{.Now}}</span></div>
      </div>
    </header>
    <main>
      <table>
        <thead>
          <tr>
            <th style="width:110px">Severity</th>
            <th style="width:320px">NCC Check Name</th>
            <th>Detail Information</th>
          </tr>
        </thead>
        <tbody>
        {{range .Rows}}
          <tr>
            <td><span class="sev {{.Severity}}">{{.Severity}}</span></td>
            <td class="mono">{{.CheckName}}</td>
            <td class="mono">{{.Detail}}</td>
          </tr>
        {{end}}
        </tbody>
      </table>
    </main>
  </div>
</body>
</html>`
	f, err := fs.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	sum := SummaryCounts{}
	for _, r := range rows {
		switch r.Severity {
		case "FAIL":
			sum.FAIL++
		case "WARN":
			sum.WARN++
		case "ERR":
			sum.ERR++
		default:
			sum.INFO++
		}
	}
	data := HTMLData{
		Rows:    rows,
		Now:     htmlNowForReport().UTC().Format(time.RFC3339),
		Meta:    meta,
		Summary: sum,
	}
	t := template.Must(template.New("table").Parse(tmpl))
	return t.Execute(f, data)
}

func generateCSV(fs FS, blocks []ParsedBlock, filename string) error {
	f, err := fs.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{"Severity", "CheckName", "Detail"}); err != nil {
		return err
	}
	for _, b := range blocks {
		if err := w.Write([]string{b.Severity, b.CheckName, b.DetailRaw}); err != nil {
			return err
		}
	}
	return w.Error()
}

func generateMarkdown(fs FS, blocks []ParsedBlock, filename string, meta HTMLMeta) error {
	if err := fs.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}
	var b strings.Builder
	b.WriteString("# NCC Report\n\n")
	if meta.ClusterName != "" || meta.ClusterVersion != "" || meta.NCCVersion != "" {
		b.WriteString("| Field | Value |\n|-------|-------|\n")
		if meta.ClusterName != "" {
			b.WriteString(fmt.Sprintf("| Cluster | %s |\n", escapeMarkdown(meta.ClusterName)))
		}
		if meta.ClusterVersion != "" {
			b.WriteString(fmt.Sprintf("| Version | %s |\n", escapeMarkdown(meta.ClusterVersion)))
		}
		if meta.NCCVersion != "" {
			b.WriteString(fmt.Sprintf("| NCC Version | %s |\n", escapeMarkdown(meta.NCCVersion)))
		}
		b.WriteString("\n")
	}
	b.WriteString("| Severity | Check | Detail |\n|----------|-------|--------|\n")
	for _, block := range blocks {
		sev := block.Severity
		if sev == "" {
			sev = "INFO"
		}
		b.WriteString(fmt.Sprintf("| %s | %s | %s |\n",
			escapeMarkdown(sev),
			escapeMarkdown(block.CheckName),
			escapeMarkdown(strings.ReplaceAll(block.DetailRaw, "\n", " "))))
	}
	return fs.WriteFile(filename, []byte(b.String()), 0644)
}

func escapeMarkdown(s string) string {
	return strings.NewReplacer("|", "\\|", "\n", " ").Replace(s)
}

type JSONOutput struct {
	GeneratedAt string      `json:"generated_at"`
	Checks      []JSONCheck `json:"checks"`
	Summary     JSONSummary `json:"summary"`
}

type JSONCheck struct {
	Severity  string `json:"severity"`
	CheckName string `json:"check_name"`
	Detail    string `json:"detail"`
}

type JSONSummary struct {
	Total int            `json:"total"`
	Count map[string]int `json:"count"`
}

// RunSummaryJSON is the machine-readable run result written to run-summary.json.
type RunSummaryJSON struct {
	Timestamp      string              `json:"timestamp"`
	DurationS      float64             `json:"duration_s"`
	ClustersOK     int                 `json:"clusters_ok"`
	ClustersFailed int                 `json:"clusters_failed"`
	FailedClusters []string            `json:"failed_clusters,omitempty"`
	Clusters       []RunClusterSummary `json:"clusters,omitempty"`
	ExitCode       int                 `json:"exit_code,omitempty"`
	IndexHTML      string              `json:"index_html"`
	TotalChecks    int                 `json:"total_checks,omitempty"`
	AvgHealthScore int                 `json:"avg_health_score,omitempty"`
	MinHealthScore int                 `json:"min_health_score,omitempty"`
	FailureClasses map[string]int      `json:"failure_classes,omitempty"`
	// Source records how the run was launched ("scheduled", "manual", ""),
	// taken from NCC_RUN_SOURCE, so the dashboard can distinguish a systemd-
	// timer/cron scheduled run from an interactive/API-triggered one.
	Source string `json:"source,omitempty"`
}

// RunClusterSummary is per-cluster stats for automation (run-summary.json).
type RunClusterSummary struct {
	Address     string `json:"address"`
	OK          bool   `json:"ok"`
	Error       string `json:"error,omitempty"`
	ErrorClass  string `json:"error_class,omitempty"`
	FailCount   int    `json:"fail_count,omitempty"`
	WarnCount   int    `json:"warn_count,omitempty"`
	ErrCount    int    `json:"err_count,omitempty"`
	InfoCount   int    `json:"info_count,omitempty"`
	ChecksTotal int    `json:"checks_total,omitempty"`
	HealthScore int    `json:"health_score,omitempty"`
}

type CheckSnapshotEntry struct {
	CheckName string `json:"check_name"`
	Severity  string `json:"severity"`
	Detail    string `json:"detail,omitempty"`
}

type ClusterChecksSnapshot struct {
	Address     string               `json:"address"`
	Checks      []CheckSnapshotEntry `json:"checks,omitempty"`
	FailCount   int                  `json:"fail_count,omitempty"`
	WarnCount   int                  `json:"warn_count,omitempty"`
	ErrCount    int                  `json:"err_count,omitempty"`
	InfoCount   int                  `json:"info_count,omitempty"`
	ChecksTotal int                  `json:"checks_total,omitempty"`
	HealthScore int                  `json:"health_score,omitempty"`
}

type ChecksSnapshotJSON struct {
	Timestamp string                  `json:"timestamp"`
	Clusters  []ClusterChecksSnapshot `json:"clusters,omitempty"`
}

type SeverityChange struct {
	CheckName string `json:"check_name"`
	From      string `json:"from"`
	To        string `json:"to"`
}

type ClusterDiffSummary struct {
	Address          string           `json:"address"`
	NewFailures      []string         `json:"new_failures,omitempty"`
	ResolvedFailures []string         `json:"resolved_failures,omitempty"`
	NewChecks        []string         `json:"new_checks,omitempty"`
	RemovedChecks    []string         `json:"removed_checks,omitempty"`
	SeverityChanges  []SeverityChange `json:"severity_changes,omitempty"`
}

type DrillDownDiffJSON struct {
	Timestamp         string               `json:"timestamp"`
	PreviousTimestamp string               `json:"previous_timestamp,omitempty"`
	NewFailCount      int                  `json:"new_fail_count"`
	ResolvedFailCount int                  `json:"resolved_fail_count"`
	Clusters          []ClusterDiffSummary `json:"clusters,omitempty"`
}

type FlakyCheckSummary struct {
	Cluster      string   `json:"cluster"`
	CheckName    string   `json:"check_name"`
	Transitions  int      `json:"transitions"`
	Observations int      `json:"observations"`
	States       []string `json:"states,omitempty"`
	Current      string   `json:"current"`
}

type FlakyChecksJSON struct {
	Timestamp        string              `json:"timestamp"`
	LookbackRuns     int                 `json:"lookback_runs"`
	MinTransitions   int                 `json:"min_transitions"`
	TotalFlakyChecks int                 `json:"total_flaky_checks"`
	Checks           []FlakyCheckSummary `json:"checks,omitempty"`
}

type SLOClusterExport struct {
	Address         string  `json:"address"`
	ChecksTotal     int     `json:"checks_total"`
	FailCount       int     `json:"fail_count"`
	WarnCount       int     `json:"warn_count"`
	ErrCount        int     `json:"err_count"`
	InfoCount       int     `json:"info_count"`
	FailRatePercent float64 `json:"fail_rate_percent"`
	HealthScore     int     `json:"health_score"`
	Status          string  `json:"status"`
}

type SLODashboardJSON struct {
	Timestamp string             `json:"timestamp"`
	DurationS float64            `json:"duration_s"`
	Clusters  []SLOClusterExport `json:"clusters,omitempty"`
}

func writeRunSummaryJSON(fs FS, outDir string, summary RunSummaryJSON) error {
	path := filepath.Join(outDir, "run-summary.json")
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

// NCCRunRecord is a versioned machine-readable bundle (ncc-run-record.json) for automation pipelines.
type NCCRunRecord struct {
	SchemaVersion       string         `json:"schema_version"`
	OrchestratorVersion string         `json:"orchestrator_version"`
	GitRevision         string         `json:"git_revision,omitempty"`
	Hostname            string         `json:"hostname,omitempty"`
	SchedulerSource     string         `json:"scheduler_source,omitempty"`
	Stream              string         `json:"stream,omitempty"`
	Run                 RunSummaryJSON `json:"run"`
}

func writeNCCRunRecordJSON(fs FS, outDir string, summary RunSummaryJSON) error {
	path := filepath.Join(outDir, "ncc-run-record.json")
	hostname, _ := os.Hostname()
	schedulerSource := strings.TrimSpace(os.Getenv("NCC_SCHEDULER_SOURCE"))
	if schedulerSource == "" {
		schedulerSource = "manual"
	}
	gitRevision := ""
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" && s.Value != "" {
				gitRevision = s.Value
				break
			}
		}
	}
	rec := NCCRunRecord{
		SchemaVersion:       "1.0",
		OrchestratorVersion: Version,
		GitRevision:         gitRevision,
		Hostname:            hostname,
		SchedulerSource:     schedulerSource,
		Stream:              Stream,
		Run:                 summary,
	}
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

// RegressionSummary captures change in FAIL counts vs the previous run-summary.json.
type RegressionSummary struct {
	Timestamp         string         `json:"timestamp"`
	PreviousTimestamp string         `json:"previous_timestamp,omitempty"`
	Current           RunSummaryJSON `json:"current"`
	PreviousFailTotal int            `json:"previous_fail_total"`
	CurrentFailTotal  int            `json:"current_fail_total"`
	DeltaFailTotal    int            `json:"delta_fail_total"`
	HasRegression     bool           `json:"has_regression"`
	IncreasedClusters []string       `json:"increased_clusters,omitempty"`
	DecreasedClusters []string       `json:"decreased_clusters,omitempty"`
	UnchangedClusters []string       `json:"unchanged_clusters,omitempty"`
}

func failCountByCluster(summary RunSummaryJSON) map[string]int {
	out := make(map[string]int, len(summary.Clusters))
	for _, c := range summary.Clusters {
		out[strings.TrimSpace(c.Address)] = c.FailCount
	}
	return out
}

func loadRunSummaryJSON(path string) (RunSummaryJSON, bool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return RunSummaryJSON{}, false, nil
		}
		return RunSummaryJSON{}, false, err
	}
	var s RunSummaryJSON
	if err := json.Unmarshal(b, &s); err != nil {
		return RunSummaryJSON{}, false, err
	}
	return s, true, nil
}

func computeRegressionSummary(previous RunSummaryJSON, hasPrevious bool, current RunSummaryJSON) RegressionSummary {
	reg := RegressionSummary{
		Timestamp:         time.Now().UTC().Format(time.RFC3339),
		Current:           current,
		CurrentFailTotal:  0,
		PreviousFailTotal: 0,
	}
	if hasPrevious {
		reg.PreviousTimestamp = previous.Timestamp
	}

	prevMap := failCountByCluster(previous)
	currMap := failCountByCluster(current)
	seen := make(map[string]bool, len(prevMap)+len(currMap))

	for k := range prevMap {
		seen[k] = true
	}
	for k := range currMap {
		seen[k] = true
	}

	for cluster := range seen {
		prev := prevMap[cluster]
		curr := currMap[cluster]
		reg.PreviousFailTotal += prev
		reg.CurrentFailTotal += curr
		switch {
		case curr > prev:
			reg.IncreasedClusters = append(reg.IncreasedClusters, cluster)
		case curr < prev:
			reg.DecreasedClusters = append(reg.DecreasedClusters, cluster)
		default:
			reg.UnchangedClusters = append(reg.UnchangedClusters, cluster)
		}
	}
	sort.Strings(reg.IncreasedClusters)
	sort.Strings(reg.DecreasedClusters)
	sort.Strings(reg.UnchangedClusters)
	reg.DeltaFailTotal = reg.CurrentFailTotal - reg.PreviousFailTotal
	reg.HasRegression = len(reg.IncreasedClusters) > 0
	return reg
}

func writeRegressionSummaryJSON(fs FS, outDir string, reg RegressionSummary) error {
	path := filepath.Join(outDir, "regression-summary.json")
	data, err := json.MarshalIndent(reg, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

func writeChecksSnapshotJSON(fs FS, outDir string, snap ChecksSnapshotJSON) error {
	path := filepath.Join(outDir, "checks-snapshot.json")
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

func loadChecksSnapshotJSON(path string) (ChecksSnapshotJSON, bool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ChecksSnapshotJSON{}, false, nil
		}
		return ChecksSnapshotJSON{}, false, err
	}
	var s ChecksSnapshotJSON
	if err := json.Unmarshal(b, &s); err != nil {
		return ChecksSnapshotJSON{}, false, err
	}
	return s, true, nil
}

func writeDrillDownDiffJSON(fs FS, outDir string, diff DrillDownDiffJSON) error {
	path := filepath.Join(outDir, "drilldown-diff.json")
	data, err := json.MarshalIndent(diff, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

func writeFlakyChecksJSON(fs FS, outDir string, flaky FlakyChecksJSON) error {
	path := filepath.Join(outDir, "flaky-checks.json")
	data, err := json.MarshalIndent(flaky, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

func writeSLODashboardJSON(fs FS, outDir string, slo SLODashboardJSON) error {
	path := filepath.Join(outDir, "slo-dashboard.json")
	data, err := json.MarshalIndent(slo, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

type ExcludedAlertsClusterAudit struct {
	Cluster       string          `json:"cluster"`
	ExcludedCount int             `json:"excluded_count"`
	Alerts        []ExcludedAlert `json:"alerts,omitempty"`
}

type ExcludedAlertsAudit struct {
	SchemaVersion      string                       `json:"schema_version"`
	Timestamp          string                       `json:"timestamp"`
	MatchMode          string                       `json:"match_mode"`
	ExcludeAlertTitles []string                     `json:"exclude_alert_titles"`
	TotalExcluded      int                          `json:"total_excluded"`
	Clusters           []ExcludedAlertsClusterAudit `json:"clusters"`
}

func writeExcludedAlertsAuditJSON(fs FS, outDir string, matchMode string, titles []string, perCluster map[string][]ExcludedAlert) error {
	path := filepath.Join(outDir, "excluded-alerts.json")
	clusters := make([]ExcludedAlertsClusterAudit, 0, len(perCluster))
	total := 0
	for cluster, alerts := range perCluster {
		cp := make([]ExcludedAlert, len(alerts))
		copy(cp, alerts)
		clusters = append(clusters, ExcludedAlertsClusterAudit{
			Cluster:       cluster,
			ExcludedCount: len(cp),
			Alerts:        cp,
		})
		total += len(cp)
	}
	sort.Slice(clusters, func(i, j int) bool { return clusters[i].Cluster < clusters[j].Cluster })
	payload := ExcludedAlertsAudit{
		SchemaVersion:      "1.0",
		Timestamp:          time.Now().UTC().Format(time.RFC3339),
		MatchMode:          matchMode,
		ExcludeAlertTitles: append([]string{}, titles...),
		TotalExcluded:      total,
		Clusters:           clusters,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

// clusterHealthScore is an alias for model.ClusterHealthScore (moved to the
// leaf model package); existing call sites in this package are unchanged.
var clusterHealthScore = model.ClusterHealthScore

func buildChecksSnapshot(results []ClusterResult) ChecksSnapshotJSON {
	snap := ChecksSnapshotJSON{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Clusters:  make([]ClusterChecksSnapshot, 0, len(results)),
	}
	for _, r := range results {
		snap.Clusters = append(snap.Clusters, buildClusterChecksSnapshotFromResult(r))
	}
	sort.Slice(snap.Clusters, func(i, j int) bool { return snap.Clusters[i].Address < snap.Clusters[j].Address })
	return snap
}

// clusterRunFailedCheckName is the synthetic check title used to surface a
// cluster's connection/run failure as a real, visible alert row instead of
// leaving the cluster's entry with no checks at all. Without this, a cluster
// that fails to run simply vanishes from the Alerts table (and from any
// downstream consumer that iterates a cluster's checks) with no indication
// anything went wrong, and previously-known-good rows for that cluster can be
// silently dropped when concurrent/scoped runs merge into the canonical
// report — a state that only self-heals once a new run succeeds.
//
// Severity is UNKNOWN, not FAIL: the orchestrator couldn't even reach the
// cluster to run any NCC checks, so it has no finding to report — FAIL would
// misrepresent "we don't know this cluster's health" as "NCC found a real
// failing check". There's no NCC KB for "the run itself didn't happen", so
// the KB column stays empty; Detail instead carries an actionable remediation
// hint (see runFailedRemediation) so the row is still useful without one.
const clusterRunFailedCheckName = "NCC run failed"

func buildClusterChecksSnapshotFromResult(r ClusterResult) ClusterChecksSnapshot {
	cluster := ClusterChecksSnapshot{Address: r.Cluster}
	if r.Err != nil {
		cluster.Checks = []CheckSnapshotEntry{{
			CheckName: clusterRunFailedCheckName,
			Severity:  "UNKNOWN",
			Detail:    r.Err.Error() + " — " + runFailedRemediation(r.Err, r.ErrorClass),
		}}
		cluster.ChecksTotal = 1
		cluster.HealthScore = 0
		return cluster
	}
	counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
	cluster.Checks = make([]CheckSnapshotEntry, 0, len(r.Blocks))
	for _, b := range r.Blocks {
		sev := strings.ToUpper(strings.TrimSpace(b.Severity))
		if sev == "" {
			sev = "INFO"
		}
		if _, ok := counts[sev]; !ok {
			sev = "INFO"
		}
		counts[sev]++
		cluster.Checks = append(cluster.Checks, CheckSnapshotEntry{
			CheckName: strings.TrimSpace(b.CheckName),
			Severity:  sev,
		})
	}
	sort.Slice(cluster.Checks, func(i, j int) bool {
		if cluster.Checks[i].CheckName == cluster.Checks[j].CheckName {
			return cluster.Checks[i].Severity < cluster.Checks[j].Severity
		}
		return cluster.Checks[i].CheckName < cluster.Checks[j].CheckName
	})
	cluster.FailCount = counts["FAIL"]
	cluster.WarnCount = counts["WARN"]
	cluster.ErrCount = counts["ERR"]
	cluster.InfoCount = counts["INFO"]
	cluster.ChecksTotal = len(r.Blocks)
	cluster.HealthScore = clusterHealthScore(cluster.FailCount, cluster.WarnCount, cluster.ErrCount, cluster.ChecksTotal)
	return cluster
}

func checksMapForCluster(c ClusterChecksSnapshot) map[string]string {
	out := make(map[string]string, len(c.Checks))
	for _, ch := range c.Checks {
		name := strings.TrimSpace(ch.CheckName)
		if name == "" {
			continue
		}
		out[name] = strings.ToUpper(strings.TrimSpace(ch.Severity))
	}
	return out
}

func computeDrillDownDiff(previous ChecksSnapshotJSON, hasPrevious bool, current ChecksSnapshotJSON) DrillDownDiffJSON {
	diff := DrillDownDiffJSON{
		Timestamp: current.Timestamp,
	}
	if !hasPrevious {
		return diff
	}
	if hasPrevious {
		diff.PreviousTimestamp = previous.Timestamp
	}
	prevClusters := map[string]ClusterChecksSnapshot{}
	currClusters := map[string]ClusterChecksSnapshot{}
	for _, c := range previous.Clusters {
		prevClusters[c.Address] = c
	}
	for _, c := range current.Clusters {
		currClusters[c.Address] = c
	}
	allClusters := map[string]bool{}
	for k := range prevClusters {
		allClusters[k] = true
	}
	for k := range currClusters {
		allClusters[k] = true
	}
	for addr := range allClusters {
		prevMap := checksMapForCluster(prevClusters[addr])
		currMap := checksMapForCluster(currClusters[addr])
		cd := ClusterDiffSummary{Address: addr}
		for name, currSev := range currMap {
			prevSev, hadPrev := prevMap[name]
			if !hadPrev {
				cd.NewChecks = append(cd.NewChecks, name)
				if currSev == "FAIL" {
					cd.NewFailures = append(cd.NewFailures, name)
					diff.NewFailCount++
				}
				continue
			}
			if currSev != prevSev {
				cd.SeverityChanges = append(cd.SeverityChanges, SeverityChange{
					CheckName: name, From: prevSev, To: currSev,
				})
			}
			if prevSev != "FAIL" && currSev == "FAIL" {
				cd.NewFailures = append(cd.NewFailures, name)
				diff.NewFailCount++
			}
			if prevSev == "FAIL" && currSev != "FAIL" {
				cd.ResolvedFailures = append(cd.ResolvedFailures, name)
				diff.ResolvedFailCount++
			}
		}
		for name := range prevMap {
			if _, ok := currMap[name]; !ok {
				cd.RemovedChecks = append(cd.RemovedChecks, name)
				if prevMap[name] == "FAIL" {
					cd.ResolvedFailures = append(cd.ResolvedFailures, name)
					diff.ResolvedFailCount++
				}
			}
		}
		sort.Strings(cd.NewFailures)
		sort.Strings(cd.ResolvedFailures)
		sort.Strings(cd.NewChecks)
		sort.Strings(cd.RemovedChecks)
		sort.Slice(cd.SeverityChanges, func(i, j int) bool { return cd.SeverityChanges[i].CheckName < cd.SeverityChanges[j].CheckName })
		if len(cd.NewFailures) > 0 || len(cd.ResolvedFailures) > 0 || len(cd.NewChecks) > 0 || len(cd.RemovedChecks) > 0 || len(cd.SeverityChanges) > 0 {
			diff.Clusters = append(diff.Clusters, cd)
		}
	}
	sort.Slice(diff.Clusters, func(i, j int) bool { return diff.Clusters[i].Address < diff.Clusters[j].Address })
	return diff
}

func computeFlakyChecks(snapshots []ChecksSnapshotJSON, minTransitions int) FlakyChecksJSON {
	report := FlakyChecksJSON{
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
		LookbackRuns:   len(snapshots),
		MinTransitions: minTransitions,
		Checks:         []FlakyCheckSummary{},
	}
	if len(snapshots) < 2 {
		return report
	}
	type key struct{ cluster, check string }
	series := map[key][]string{}
	for _, snap := range snapshots {
		for _, c := range snap.Clusters {
			for _, ch := range c.Checks {
				k := key{cluster: c.Address, check: strings.TrimSpace(ch.CheckName)}
				if k.check == "" {
					continue
				}
				series[k] = append(series[k], strings.ToUpper(strings.TrimSpace(ch.Severity)))
			}
		}
	}
	for k, states := range series {
		if len(states) < 2 {
			continue
		}
		transitions := 0
		uniq := map[string]bool{}
		prev := states[0]
		uniq[prev] = true
		for i := 1; i < len(states); i++ {
			cur := states[i]
			uniq[cur] = true
			if cur != prev {
				transitions++
			}
			prev = cur
		}
		if transitions >= minTransitions && len(uniq) > 1 {
			report.Checks = append(report.Checks, FlakyCheckSummary{
				Cluster:      k.cluster,
				CheckName:    k.check,
				Transitions:  transitions,
				Observations: len(states),
				States:       states,
				Current:      states[len(states)-1],
			})
		}
	}
	sort.Slice(report.Checks, func(i, j int) bool {
		if report.Checks[i].Transitions == report.Checks[j].Transitions {
			if report.Checks[i].Cluster == report.Checks[j].Cluster {
				return report.Checks[i].CheckName < report.Checks[j].CheckName
			}
			return report.Checks[i].Cluster < report.Checks[j].Cluster
		}
		return report.Checks[i].Transitions > report.Checks[j].Transitions
	})
	report.TotalFlakyChecks = len(report.Checks)
	return report
}

func loadRecentCheckSnapshots(historyDir string, maxRuns int) ([]ChecksSnapshotJSON, error) {
	if maxRuns <= 0 {
		return nil, nil
	}
	dirs, err := listHistoryRunDirs(historyDir)
	if err != nil {
		return nil, err
	}
	if len(dirs) > maxRuns {
		dirs = dirs[:maxRuns]
	}
	outNewest := make([]ChecksSnapshotJSON, 0, len(dirs))
	for _, d := range dirs {
		path := filepath.Join(historyDir, d, "checks-snapshot.json")
		s, ok, err := loadChecksSnapshotJSON(path)
		if err != nil {
			log.Warn().Err(err).Str("path", path).Msg("failed to load checks snapshot from history (ignored)")
			continue
		}
		if ok {
			outNewest = append(outNewest, s)
		}
	}
	// Return oldest -> newest
	for i, j := 0, len(outNewest)-1; i < j; i, j = i+1, j-1 {
		outNewest[i], outNewest[j] = outNewest[j], outNewest[i]
	}
	return outNewest, nil
}

func buildSLODashboard(run RunSummaryJSON) SLODashboardJSON {
	out := SLODashboardJSON{
		Timestamp: run.Timestamp,
		DurationS: run.DurationS,
		Clusters:  make([]SLOClusterExport, 0, len(run.Clusters)),
	}
	for _, c := range run.Clusters {
		failRate := 0.0
		if c.ChecksTotal > 0 {
			failRate = float64(c.FailCount) * 100.0 / float64(c.ChecksTotal)
		}
		status := "ok"
		if !c.OK {
			status = "error"
		} else if c.FailCount > 0 {
			status = "degraded"
		} else if c.WarnCount > 0 {
			status = "warn"
		}
		out.Clusters = append(out.Clusters, SLOClusterExport{
			Address:         c.Address,
			ChecksTotal:     c.ChecksTotal,
			FailCount:       c.FailCount,
			WarnCount:       c.WarnCount,
			ErrCount:        c.ErrCount,
			InfoCount:       c.InfoCount,
			FailRatePercent: math.Round(failRate*100) / 100,
			HealthScore:     c.HealthScore,
			Status:          status,
		})
	}
	sort.Slice(out.Clusters, func(i, j int) bool { return out.Clusters[i].Address < out.Clusters[j].Address })
	return out
}

type parsedPolicyGate struct {
	Raw       string
	Metric    string
	Operator  string
	Threshold float64
}

var policyGateRe = regexp.MustCompile(`^\s*([a-zA-Z0-9\-_]+)\s*(>=|<=|==|!=|>|<)\s*([0-9]+(?:\.[0-9]+)?)\s*$`)

func parsePolicyGate(expr string) (parsedPolicyGate, error) {
	expr = strings.TrimSpace(expr)
	m := policyGateRe.FindStringSubmatch(expr)
	if len(m) != 4 {
		return parsedPolicyGate{}, fmt.Errorf("invalid policy gate %q (expected metric<op>number)", expr)
	}
	th, err := strconv.ParseFloat(m[3], 64)
	if err != nil {
		return parsedPolicyGate{}, err
	}
	return parsedPolicyGate{
		Raw:       expr,
		Metric:    strings.ToLower(strings.TrimSpace(m[1])),
		Operator:  m[2],
		Threshold: th,
	}, nil
}

func compareFloat(lhs float64, op string, rhs float64) bool {
	switch op {
	case ">":
		return lhs > rhs
	case ">=":
		return lhs >= rhs
	case "<":
		return lhs < rhs
	case "<=":
		return lhs <= rhs
	case "==":
		return lhs == rhs
	case "!=":
		return lhs != rhs
	default:
		return false
	}
}

func evaluatePolicyGates(gates []string, metrics map[string]float64) ([]string, error) {
	var violations []string
	for _, g := range gates {
		if strings.TrimSpace(g) == "" {
			continue
		}
		p, err := parsePolicyGate(g)
		if err != nil {
			return nil, err
		}
		val, ok := metrics[p.Metric]
		if !ok {
			return nil, fmt.Errorf("unsupported policy gate metric %q", p.Metric)
		}
		if compareFloat(val, p.Operator, p.Threshold) {
			violations = append(violations, fmt.Sprintf("%s violated (actual=%.2f)", p.Raw, val))
		}
	}
	return violations, nil
}

func copyFile(src, dst string) error {
	b, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	return os.WriteFile(dst, b, 0644)
}

func runHistoryTimestamp(now time.Time) string {
	return now.UTC().Format("20060102T150405Z")
}

func listHistoryRunDirs(historyDir string) ([]string, error) {
	ents, err := os.ReadDir(historyDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	type item struct {
		name string
		t    time.Time
	}
	var items []item
	for _, e := range ents {
		if !e.IsDir() {
			continue
		}
		ts, err := time.Parse("20060102T150405Z", e.Name())
		if err != nil {
			continue
		}
		items = append(items, item{name: e.Name(), t: ts})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].t.After(items[j].t) })
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.name)
	}
	return out, nil
}

func applyRunHistoryRetention(historyDir string, retainLast, retainDays int, now time.Time) error {
	dirs, err := listHistoryRunDirs(historyDir)
	if err != nil {
		return err
	}
	for i, d := range dirs {
		remove := false
		if retainLast > 0 && i >= retainLast {
			remove = true
		}
		if retainDays > 0 {
			ts, err := time.Parse("20060102T150405Z", d)
			if err == nil {
				ageDays := int(now.UTC().Sub(ts).Hours() / 24)
				if ageDays >= retainDays {
					remove = true
				}
			}
		}
		if remove {
			if err := os.RemoveAll(filepath.Join(historyDir, d)); err != nil {
				return err
			}
		}
	}
	return nil
}

func applyArtifactRetentionPolicies(outputDir string, retainDays, retainMaxFiles int, now time.Time) (int, error) {
	if retainDays <= 0 && retainMaxFiles <= 0 {
		return 0, nil
	}
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	protected := map[string]bool{
		"index.html":              true,
		"ncc-report-single.html":  true,
		"run-summary.json":        true,
		"ncc-run-record.json":     true,
		"regression-summary.json": true,
		"checks-snapshot.json":    true,
		"drilldown-diff.json":     true,
		"flaky-checks.json":       true,
		"slo-dashboard.json":      true,
		"policy-gates.txt":        true,
		"excluded-alerts.json":    true,
	}
	type item struct {
		name string
		path string
		mod  time.Time
	}
	files := make([]item, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if protected[name] || name == ".ncc-writecheck" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, item{
			name: name,
			path: filepath.Join(outputDir, name),
			mod:  info.ModTime(),
		})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].mod.After(files[j].mod) })
	deleted := 0
	retainDuration := time.Duration(retainDays) * 24 * time.Hour
	for i, f := range files {
		remove := false
		if retainDays > 0 && now.Sub(f.mod) >= retainDuration {
			remove = true
		}
		if retainMaxFiles > 0 && i >= retainMaxFiles {
			remove = true
		}
		if !remove {
			continue
		}
		if err := os.Remove(f.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func writeRunHistorySnapshot(cfg Config) (string, error) {
	ts := runHistoryTimestamp(time.Now())
	runDir := filepath.Join(cfg.RunHistoryDir, ts)
	if err := os.MkdirAll(runDir, 0755); err != nil {
		return "", err
	}
	candidates := []string{
		"index.html",
		"run-summary.json",
		"ncc-run-record.json",
		"regression-summary.json",
		"checks-snapshot.json",
		"drilldown-diff.json",
		"flaky-checks.json",
		"slo-dashboard.json",
	}
	for _, rel := range candidates {
		src := filepath.Join(cfg.OutputDirFiltered, rel)
		if _, err := os.Stat(src); err != nil {
			continue
		}
		if err := copyFile(src, filepath.Join(runDir, rel)); err != nil {
			return "", err
		}
	}
	if err := os.WriteFile(filepath.Join(cfg.RunHistoryDir, "latest.txt"), []byte(ts+"\n"), 0644); err != nil {
		return "", err
	}
	if err := applyRunHistoryRetention(cfg.RunHistoryDir, cfg.RetainLastRuns, cfg.RetainDays, time.Now()); err != nil {
		return "", err
	}
	return runDir, nil
}

// classifyClusterError maps a failed cluster's error chain into one of the
// canonical buckets surfaced in run-summary.json's `failure_classes`.
//
// IMPORTANT: the order matters. Network signals (no-such-host, dial tcp,
// connection refused) are evaluated BEFORE the "retry circuit breaker opened"
// match, because the retry circuit breaker opens on *any* repeated transport
// failure — including DNS resolution failures. If we matched the breaker first
// we would misclassify a DNS lookup failure as `rate_limit`, falsely
// implicating max-parallel/throttling when the real fix is "configure DNS or
// use an IP address".
func classifyClusterError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(msg, "context deadline exceeded"), strings.Contains(msg, "timed out"), strings.Contains(msg, "timeout"):
		return "timeout"
	case strings.Contains(msg, "http 401"), strings.Contains(msg, "http 403"), strings.Contains(msg, "unauthorized"), strings.Contains(msg, "forbidden"), strings.Contains(msg, "authentication"):
		return "auth"
	// Network-layer signals first so DNS/refused/unreachable errors are not
	// swallowed by a downstream circuit-breaker match below.
	case strings.Contains(msg, "no such host"),
		strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "no route to host"),
		strings.Contains(msg, "network is unreachable"),
		strings.Contains(msg, "host is down"),
		strings.Contains(msg, "tls"),
		strings.Contains(msg, "x509"):
		return "network"
	// Only classify as rate-limit when there's a *real* rate-limit signal —
	// HTTP 429, an explicit "rate limit" phrase, "too many requests", or a
	// Retry-After header. A circuit breaker that opened because the TCP dial
	// kept failing is NOT a rate-limit problem.
	case strings.Contains(msg, "http 429"),
		strings.Contains(msg, "too many requests"),
		strings.Contains(msg, "rate limit"),
		strings.Contains(msg, "rate-limit"),
		strings.Contains(msg, "retry-after"):
		return "rate_limit"
	case strings.Contains(msg, "parse filtered"), strings.Contains(msg, "parse summary"), strings.Contains(msg, "parser"):
		return "parser"
	// Generic transport bucket: circuit breakers triggered by repeated
	// transport failures (without an explicit rate-limit signal) land here.
	case strings.Contains(msg, "retry circuit breaker opened"),
		strings.Contains(msg, "transport error"),
		strings.Contains(msg, "dial tcp"),
		strings.Contains(msg, "i/o timeout"),
		strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "connection reset"):
		return "network"
	case strings.Contains(msg, "http 4"), strings.Contains(msg, "http 5"), strings.Contains(msg, "start checks failed"), strings.Contains(msg, "get summary failed"), strings.Contains(msg, "poll failed"):
		return "api"
	default:
		return "unknown"
	}
}

// runFailedRemediation returns an actionable, single-line hint for the
// synthetic clusterRunFailedCheckName alert's Detail field, based on the
// cluster's classified error bucket (classifyClusterError). Deliberately
// plain text with no URL — the Alerts table's KB column derives from a
// portal.nutanix.com link in Detail, and there's no real NCC KB article for
// "the run couldn't reach this cluster", so KB is left empty on purpose.
func runFailedRemediation(err error, errorClass string) string {
	if err == nil {
		return ""
	}
	class := strings.TrimSpace(errorClass)
	if class == "" {
		class = classifyClusterError(err)
	}
	var hint string
	switch class {
	case "auth":
		hint = "Verify the username/password (or secret:// source) and check for an account lockout in Prism. Try: ncc-orchestrator preflight-check --config <config.yaml>"
	case "timeout":
		hint = "The cluster didn't respond in time. Increase --timeout/--request-timeout, reduce --max-parallel, and verify routing to the cluster."
	case "network":
		hint = "Could not reach the cluster (DNS/connection failure). Verify the address resolves and is routable, and check firewall/VPN. Try: ncc-orchestrator discover-clusters --prism-central-url <pc-url>"
	case "rate_limit":
		hint = "The cluster is rate-limiting requests. Lower --max-parallel and increase --retry-max-attempts/--retry-base-delay."
	case "api":
		hint = "The Prism API rejected the request. Verify --ncc-api-version/--nutanix-v4-api-version and that Prism services are healthy."
	case "parser":
		hint = "NCC output could not be parsed. Inspect the raw log under output-dir-logs for an unexpected payload format."
	default:
		hint = "Investigate connectivity/credentials for this cluster, then re-run. Try: ncc-orchestrator preflight-check --config <config.yaml>"
	}
	return hint
}

func buildRunClusterSummary(r ClusterResult) RunClusterSummary {
	s := RunClusterSummary{Address: r.Cluster, OK: r.Err == nil}
	if r.Err != nil {
		s.Error = r.Err.Error()
		if strings.TrimSpace(r.ErrorClass) != "" {
			s.ErrorClass = r.ErrorClass
		} else {
			s.ErrorClass = classifyClusterError(r.Err)
		}
		return s
	}
	counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
	for _, b := range r.Blocks {
		sev := b.Severity
		if sev == "" {
			sev = "INFO"
		}
		if _, ok := counts[sev]; ok {
			counts[sev]++
		} else {
			counts["INFO"]++
		}
	}
	s.FailCount = counts["FAIL"]
	s.WarnCount = counts["WARN"]
	s.ErrCount = counts["ERR"]
	s.InfoCount = counts["INFO"]
	s.ChecksTotal = len(r.Blocks)
	s.HealthScore = clusterHealthScore(s.FailCount, s.WarnCount, s.ErrCount, s.ChecksTotal)
	return s
}

func failureClassCounts(results []ClusterResult) map[string]int {
	counts := map[string]int{
		"timeout":    0,
		"auth":       0,
		"network":    0,
		"api":        0,
		"parser":     0,
		"rate_limit": 0,
		"unknown":    0,
	}
	for _, r := range results {
		if r.Err == nil {
			continue
		}
		class := strings.TrimSpace(r.ErrorClass)
		if class == "" {
			class = classifyClusterError(r.Err)
		}
		if _, ok := counts[class]; !ok {
			class = "unknown"
		}
		counts[class]++
	}
	return counts
}

func newFailureClassCounts() map[string]int {
	return map[string]int{
		"timeout":    0,
		"auth":       0,
		"network":    0,
		"api":        0,
		"parser":     0,
		"rate_limit": 0,
		"unknown":    0,
	}
}

func incrementFailureClassCount(counts map[string]int, r ClusterResult) {
	if r.Err == nil {
		return
	}
	class := strings.TrimSpace(r.ErrorClass)
	if class == "" {
		class = classifyClusterError(r.Err)
	}
	if _, ok := counts[class]; !ok {
		class = "unknown"
	}
	counts[class]++
}

func printFailureResolutionHints(counts map[string]int) {
	if counts == nil {
		return
	}
	hints := []string{}
	if counts["auth"] > 0 {
		hints = append(hints, "Auth failures detected: verify username/password or secret:// source and Prism account lock state. Command: ncc-orchestrator preflight-check --config <config.yaml>")
	}
	if counts["timeout"] > 0 || counts["network"] > 0 {
		hints = append(hints, "Timeout/network failures detected: reduce --max-parallel, increase --timeout/--request-timeout, verify routing/firewall. Command: ncc-orchestrator --auto --automation-level full-auto --config <config.yaml>")
	}
	if counts["rate_limit"] > 0 {
		hints = append(hints, "Rate-limit failures detected: lower --max-parallel and tune retry/backoff settings. Command: ncc-orchestrator --max-parallel 4 --retry-max-attempts 8 --config <config.yaml>")
	}
	if counts["api"] > 0 {
		hints = append(hints, "API failures detected: verify API version flags and Prism service health. Command: ncc-orchestrator discover-clusters --discover-api-version v4 --prism-central-url <pc-url>")
	}
	if counts["parser"] > 0 {
		hints = append(hints, "Parser failures detected: inspect raw NCC logs under output-dir-logs for unexpected payload formats. Command: ncc-orchestrator preflight-check --config <config.yaml>")
	}
	if len(hints) == 0 {
		return
	}
	fmt.Fprintln(os.Stderr, "Failure resolution recommendations:")
	for _, h := range hints {
		fmt.Fprintf(os.Stderr, "- %s\n", h)
	}
}

func generateJSON(fs FS, blocks []ParsedBlock, filename string, meta HTMLMeta) error {
	if err := fs.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}
	f, err := fs.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
	checks := make([]JSONCheck, 0, len(blocks))
	for _, b := range blocks {
		sev := b.Severity
		if sev == "" {
			sev = "INFO"
		}
		counts[sev]++
		checks = append(checks, JSONCheck{
			Severity:  sev,
			CheckName: b.CheckName,
			Detail:    b.DetailRaw,
		})
	}

	output := JSONOutput{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Checks:      checks,
		Summary: JSONSummary{
			Total: len(blocks),
			Count: counts,
		},
	}

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(output)
}

type sarifLog struct {
	Version string     `json:"version"`
	Schema  string     `json:"$schema"`
	Runs    []sarifRun `json:"runs"`
}
type sarifRun struct {
	Tool    sarifTool     `json:"tool"`
	Results []sarifResult `json:"results"`
}
type sarifTool struct {
	Driver sarifDriver `json:"driver"`
}
type sarifDriver struct {
	Name           string      `json:"name"`
	Version        string      `json:"version,omitempty"`
	InformationURI string      `json:"informationUri,omitempty"`
	Rules          []sarifRule `json:"rules,omitempty"`
}
type sarifRule struct {
	ID               string `json:"id"`
	ShortDescription struct {
		Text string `json:"text"`
	} `json:"shortDescription"`
}
type sarifResult struct {
	RuleID  string `json:"ruleId"`
	Level   string `json:"level"`
	Message struct {
		Text string `json:"text"`
	} `json:"message"`
}

func sarifLevelForSeverity(sev string) string {
	switch strings.ToUpper(strings.TrimSpace(sev)) {
	case "FAIL":
		return "error"
	case "WARN":
		return "warning"
	case "ERR":
		return "error"
	default:
		return "note"
	}
}

func generateSARIF(fs FS, blocks []ParsedBlock, filename string) error {
	if err := fs.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}
	ruleMap := map[string]bool{}
	rules := make([]sarifRule, 0, len(blocks))
	results := make([]sarifResult, 0, len(blocks))
	for _, b := range blocks {
		ruleID := strings.TrimSpace(b.CheckName)
		if ruleID == "" {
			ruleID = "NCC_CHECK"
		}
		if !ruleMap[ruleID] {
			ruleMap[ruleID] = true
			r := sarifRule{ID: ruleID}
			r.ShortDescription.Text = ruleID
			rules = append(rules, r)
		}
		res := sarifResult{
			RuleID: ruleID,
			Level:  sarifLevelForSeverity(b.Severity),
		}
		res.Message.Text = strings.TrimSpace(b.DetailRaw)
		if res.Message.Text == "" {
			res.Message.Text = "NCC finding"
		}
		results = append(results, res)
	}

	payload := sarifLog{
		Version: "2.1.0",
		Schema:  "https://json.schemastore.org/sarif-2.1.0.json",
		Runs: []sarifRun{{
			Tool: sarifTool{
				Driver: sarifDriver{
					Name:           "ncc-orchestrator",
					Version:        Version,
					InformationURI: "https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator",
					Rules:          rules,
				},
			},
			Results: results,
		}},
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(filename, data, 0644)
}

func filterBlocksBySeverity(blocks []ParsedBlock, allowedSeverities []string) []ParsedBlock {
	if len(allowedSeverities) == 0 {
		return blocks
	}
	allowed := make(map[string]bool)
	for _, s := range allowedSeverities {
		allowed[strings.ToUpper(strings.TrimSpace(s))] = true
	}
	filtered := make([]ParsedBlock, 0, len(blocks))
	for _, b := range blocks {
		sev := b.Severity
		if sev == "" {
			sev = "INFO"
		}
		if allowed[sev] {
			filtered = append(filtered, b)
		}
	}
	return filtered
}

type ExcludedAlert struct {
	Severity   string `json:"severity"`
	CheckName  string `json:"check_name"`
	Detail     string `json:"detail,omitempty"`
	MatchMode  string `json:"match_mode"`
	MatchValue string `json:"match_value"`
}

type excludeTitleMatcher struct {
	mode          string
	cleanedTitles []string
	lowerTitles   []string
	regexPatterns []*regexp.Regexp
}

var excludeTitleMatcherCache sync.Map

func buildExcludeTitleMatcher(excludedTitles []string, matchMode string) (*excludeTitleMatcher, error) {
	mode, err := normalizeExcludeAlertMatchMode(matchMode)
	if err != nil {
		return nil, err
	}
	cleanedTitles := make([]string, 0, len(excludedTitles))
	for _, title := range excludedTitles {
		t := strings.TrimSpace(title)
		if t == "" {
			continue
		}
		cleanedTitles = append(cleanedTitles, t)
	}
	if len(cleanedTitles) == 0 {
		return nil, nil
	}
	cacheKey := mode + "\x00" + strings.Join(cleanedTitles, "\x00")
	if cached, ok := excludeTitleMatcherCache.Load(cacheKey); ok {
		if m, ok := cached.(*excludeTitleMatcher); ok {
			return m, nil
		}
	}
	m := &excludeTitleMatcher{
		mode:          mode,
		cleanedTitles: cleanedTitles,
		lowerTitles:   make([]string, 0, len(cleanedTitles)),
	}
	for _, title := range cleanedTitles {
		m.lowerTitles = append(m.lowerTitles, strings.ToLower(title))
	}
	if mode == "regex" {
		m.regexPatterns = make([]*regexp.Regexp, 0, len(cleanedTitles))
		for _, pattern := range cleanedTitles {
			re, err := regexp.Compile(pattern)
			if err != nil {
				return nil, fmt.Errorf("exclude-alert-titles regex %q: %w", pattern, err)
			}
			m.regexPatterns = append(m.regexPatterns, re)
		}
	}
	excludeTitleMatcherCache.Store(cacheKey, m)
	return m, nil
}

func (m *excludeTitleMatcher) match(checkName string) (bool, string) {
	if m == nil {
		return false, ""
	}
	checkLower := strings.ToLower(strings.TrimSpace(checkName))
	switch m.mode {
	case "exact":
		for i, titleLower := range m.lowerTitles {
			if checkLower == titleLower {
				return true, m.cleanedTitles[i]
			}
		}
	case "contains":
		for i, titleLower := range m.lowerTitles {
			if strings.Contains(checkLower, titleLower) {
				return true, m.cleanedTitles[i]
			}
		}
	case "regex":
		for i, re := range m.regexPatterns {
			if re.MatchString(checkName) {
				return true, m.cleanedTitles[i]
			}
		}
	}
	return false, ""
}

func filterBlocksByTitle(blocks []ParsedBlock, excludedTitles []string, matchMode string) ([]ParsedBlock, []ExcludedAlert, error) {
	matcher, err := buildExcludeTitleMatcher(excludedTitles, matchMode)
	if err != nil {
		return blocks, nil, err
	}
	if matcher == nil {
		return blocks, nil, nil
	}

	filtered := make([]ParsedBlock, 0, len(blocks))
	excluded := make([]ExcludedAlert, 0)
	for _, b := range blocks {
		if ok, matchedBy := matcher.match(b.CheckName); ok {
			excluded = append(excluded, ExcludedAlert{
				Severity:   b.Severity,
				CheckName:  b.CheckName,
				Detail:     b.DetailRaw,
				MatchMode:  matcher.mode,
				MatchValue: matchedBy,
			})
			continue
		}
		filtered = append(filtered, b)
	}
	return filtered, excluded, nil
}

func rowsFromBlocks(blocks []ParsedBlock) []Row {
	rows := make([]Row, 0, len(blocks))
	for _, b := range blocks {
		detail := template.HTML(strings.ReplaceAll(html.EscapeString(b.DetailRaw), "\n", "<br>"))
		rows = append(rows, Row{
			Severity:  b.Severity,
			CheckName: html.EscapeString(strings.ReplaceAll(b.CheckName, "\n", " ")),
			Detail:    detail,
		})
	}
	return rows
}

// ==================== Aggregation ====================

// generateTestAgg produces a test index.html with n clusters and ~10–15 rows per cluster for scalability testing.
func generateTestAgg(n int, outDir string) error {
	severities := []string{"FAIL", "WARN", "ERR", "INFO"}
	checks := []string{
		"AHV host time sync", "CVM memory", "Disk health", "Network connectivity",
		"Storage pool", "Prism connectivity", "NCC version", "Cluster health",
		"Data resilience", "VM placement", "Controller VM", "License validity",
	}
	agg := make([]AggBlock, 0, n*12)
	clusterFiles := make([]struct{ Cluster, HTML, CSV string }, 0, n)
	snapshotClusters := make([]ClusterChecksSnapshot, 0, n)
	runClusters := make([]RunClusterSummary, 0, n)
	totalChecks := 0
	for i := 1; i <= n; i++ {
		cluster := fmt.Sprintf("10.0.%d.%d", (i-1)/255, (i-1)%255+1)
		clusterName := fmt.Sprintf("cluster-%03d", i)
		clusterVersion := "6.5.2"
		nccVersion := "4.2.0"
		clusterFiles = append(clusterFiles, struct{ Cluster, HTML, CSV string }{
			Cluster: cluster,
			HTML:    cluster + ".html",
			CSV:     cluster + ".csv",
		})
		numChecks := 8 + (i % 8)
		clusterChecks := make([]CheckSnapshotEntry, 0, numChecks)
		counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
		for j := 0; j < numChecks; j++ {
			sev := severities[(i+j)%len(severities)]
			check := checks[j%len(checks)]
			agg = append(agg, AggBlock{
				Cluster:        cluster,
				Severity:       sev,
				Check:          check,
				Detail:         fmt.Sprintf("Test detail for %s on %s. See https://portal.nutanix.com/kb/%d for more.", check, clusterName, 1000+i+j),
				ClusterName:    clusterName,
				ClusterVersion: clusterVersion,
				NCCVersion:     nccVersion,
			})
			clusterChecks = append(clusterChecks, CheckSnapshotEntry{
				CheckName: check,
				Severity:  sev,
			})
			counts[sev]++
		}
		sort.Slice(clusterChecks, func(a, b int) bool {
			if clusterChecks[a].CheckName == clusterChecks[b].CheckName {
				return clusterChecks[a].Severity < clusterChecks[b].Severity
			}
			return clusterChecks[a].CheckName < clusterChecks[b].CheckName
		})
		total := len(clusterChecks)
		health := clusterHealthScore(counts["FAIL"], counts["WARN"], counts["ERR"], total)
		snapshotClusters = append(snapshotClusters, ClusterChecksSnapshot{
			Address:     cluster,
			Checks:      clusterChecks,
			FailCount:   counts["FAIL"],
			WarnCount:   counts["WARN"],
			ErrCount:    counts["ERR"],
			InfoCount:   counts["INFO"],
			ChecksTotal: total,
			HealthScore: health,
		})
		runClusters = append(runClusters, RunClusterSummary{
			Address:     cluster,
			OK:          true,
			FailCount:   counts["FAIL"],
			WarnCount:   counts["WARN"],
			ErrCount:    counts["ERR"],
			InfoCount:   counts["INFO"],
			ChecksTotal: total,
			HealthScore: health,
		})
		totalChecks += total
	}
	fs := OSFS{}
	if err := fs.MkdirAll(outDir, 0755); err != nil {
		return err
	}
	sort.Slice(snapshotClusters, func(i, j int) bool { return snapshotClusters[i].Address < snapshotClusters[j].Address })
	sort.Slice(runClusters, func(i, j int) bool { return runClusters[i].Address < runClusters[j].Address })

	now := time.Now().UTC()
	currTS := now.Format(time.RFC3339)
	prevTS := now.Add(-4 * time.Hour).Format(time.RFC3339)

	avgHealth := 0
	minHealth := 100
	for _, c := range runClusters {
		avgHealth += c.HealthScore
		if c.HealthScore < minHealth {
			minHealth = c.HealthScore
		}
	}
	if len(runClusters) > 0 {
		avgHealth /= len(runClusters)
	} else {
		minHealth = 0
	}

	runSummary := RunSummaryJSON{
		Timestamp:      currTS,
		DurationS:      137.7,
		ClustersOK:     len(runClusters),
		ClustersFailed: 0,
		Clusters:       runClusters,
		IndexHTML:      filepath.ToSlash(filepath.Join(filepath.Base(outDir), "index.html")),
		TotalChecks:    totalChecks,
		AvgHealthScore: avgHealth,
		MinHealthScore: minHealth,
	}
	currentSnapshot := ChecksSnapshotJSON{
		Timestamp: currTS,
		Clusters:  snapshotClusters,
	}

	// Build a previous snapshot with small deterministic differences so
	// drill-down/regression/flaky artifacts are populated for UI simulation.
	previousSnapshot := ChecksSnapshotJSON{
		Timestamp: prevTS,
		Clusters:  make([]ClusterChecksSnapshot, 0, len(snapshotClusters)),
	}
	for idx, c := range snapshotClusters {
		cp := c
		cp.Checks = append([]CheckSnapshotEntry(nil), c.Checks...)
		if len(cp.Checks) > 0 {
			switch cp.Checks[0].Severity {
			case "FAIL":
				cp.Checks[0].Severity = "WARN"
			case "WARN":
				cp.Checks[0].Severity = "INFO"
			case "ERR":
				cp.Checks[0].Severity = "WARN"
			default:
				cp.Checks[0].Severity = "WARN"
			}
		}
		if idx%3 == 0 && len(cp.Checks) > 1 {
			cp.Checks = cp.Checks[:len(cp.Checks)-1]
		}
		if idx%4 == 0 {
			cp.Checks = append(cp.Checks, CheckSnapshotEntry{
				CheckName: "Legacy replication guard",
				Severity:  "FAIL",
			})
		}
		counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
		for _, ch := range cp.Checks {
			sev := strings.ToUpper(strings.TrimSpace(ch.Severity))
			if _, ok := counts[sev]; !ok {
				sev = "INFO"
			}
			counts[sev]++
		}
		cp.FailCount = counts["FAIL"]
		cp.WarnCount = counts["WARN"]
		cp.ErrCount = counts["ERR"]
		cp.InfoCount = counts["INFO"]
		cp.ChecksTotal = len(cp.Checks)
		cp.HealthScore = clusterHealthScore(cp.FailCount, cp.WarnCount, cp.ErrCount, cp.ChecksTotal)
		previousSnapshot.Clusters = append(previousSnapshot.Clusters, cp)
	}

	prevRunClusters := make([]RunClusterSummary, 0, len(previousSnapshot.Clusters))
	prevTotalChecks := 0
	prevAvgHealth := 0
	prevMinHealth := 100
	for _, c := range previousSnapshot.Clusters {
		prevRunClusters = append(prevRunClusters, RunClusterSummary{
			Address:     c.Address,
			OK:          true,
			FailCount:   c.FailCount,
			WarnCount:   c.WarnCount,
			ErrCount:    c.ErrCount,
			InfoCount:   c.InfoCount,
			ChecksTotal: c.ChecksTotal,
			HealthScore: c.HealthScore,
		})
		prevTotalChecks += c.ChecksTotal
		prevAvgHealth += c.HealthScore
		if c.HealthScore < prevMinHealth {
			prevMinHealth = c.HealthScore
		}
	}
	if len(prevRunClusters) > 0 {
		prevAvgHealth /= len(prevRunClusters)
	} else {
		prevMinHealth = 0
	}
	previousSummary := RunSummaryJSON{
		Timestamp:      prevTS,
		DurationS:      129.4,
		ClustersOK:     len(prevRunClusters),
		ClustersFailed: 0,
		Clusters:       prevRunClusters,
		IndexHTML:      filepath.ToSlash(filepath.Join(filepath.Base(outDir), "index.html")),
		TotalChecks:    prevTotalChecks,
		AvgHealthScore: prevAvgHealth,
		MinHealthScore: prevMinHealth,
	}

	regression := computeRegressionSummary(previousSummary, true, runSummary)
	drillDown := computeDrillDownDiff(previousSnapshot, true, currentSnapshot)

	// Build synthetic history to ensure flaky-checks.json has representative data.
	olderA := previousSnapshot
	olderA.Timestamp = now.Add(-8 * time.Hour).Format(time.RFC3339)
	olderB := previousSnapshot
	olderB.Timestamp = now.Add(-6 * time.Hour).Format(time.RFC3339)
	for i := range olderA.Clusters {
		if len(olderA.Clusters[i].Checks) > 0 {
			olderA.Clusters[i].Checks[0].Severity = "WARN"
		}
		if len(olderB.Clusters[i].Checks) > 0 {
			olderB.Clusters[i].Checks[0].Severity = "FAIL"
		}
	}
	flaky := computeFlakyChecks([]ChecksSnapshotJSON{olderA, olderB, currentSnapshot}, 2)
	slo := buildSLODashboard(runSummary)

	if err := writeRunSummaryJSON(fs, outDir, runSummary); err != nil {
		return err
	}
	if err := writeChecksSnapshotJSON(fs, outDir, currentSnapshot); err != nil {
		return err
	}
	if err := writeDrillDownDiffJSON(fs, outDir, drillDown); err != nil {
		return err
	}
	if err := writeFlakyChecksJSON(fs, outDir, flaky); err != nil {
		return err
	}
	if err := writeRegressionSummaryJSON(fs, outDir, regression); err != nil {
		return err
	}
	if err := writeSLODashboardJSON(fs, outDir, slo); err != nil {
		return err
	}
	if err := writeAggregatedHTMLSingle(fs, outDir, agg, clusterFiles, Config{}); err != nil {
		return err
	}
	return nil
}

type AggBlock struct {
	Cluster        string
	Severity       string
	Check          string
	Detail         string
	ClusterName    string
	ClusterVersion string
	NCCVersion     string
}

// writeAllClustersFailedHTML writes a minimal index.html when every cluster failed, so the report page exists.
func writeAllClustersFailedHTML(fs FS, outDir string, failedClusters []string) error {
	if err := fs.MkdirAll(outDir, 0755); err != nil {
		return fmt.Errorf("mkdir %s: %w", outDir, err)
	}
	path := filepath.Join(outDir, "index.html")
	const tmpl = `<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>NCC Aggregated Report</title></head>
<body style="font-family: system-ui; max-width: 800px; margin: 2rem auto; padding: 1rem; background: #0f172a; color: #e5e7eb;">
<h1>NCC Aggregated Report</h1>
<p style="color: #f59e0b;">All clusters failed. No data was collected.</p>
<p>Failed clusters:</p>
<ul>{{range .Failed}}
<li><code>{{.}}</code></li>{{end}}
</ul>
<p style="color: #9ca3af; font-size: 14px;">Generated at {{.GeneratedAt}}</p>
</body>
</html>`
	t := template.Must(template.New("allfailed").Parse(tmpl))
	type data struct {
		Failed      []string
		GeneratedAt string
	}
	d := data{Failed: failedClusters, GeneratedAt: time.Now().UTC().Format(time.RFC3339)}
	f, err := fs.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()
	return t.Execute(f, d)
}

func writeAggregatedHTMLSingle(fs FS, outDir string, rows []AggBlock, perCluster []struct{ Cluster, HTML, CSV string }, cfg Config) error {
	if err := fs.MkdirAll(outDir, 0755); err != nil {
		return fmt.Errorf("mkdir %s: %w", outDir, err)
	}
	path := filepath.Join(outDir, "index.html")
	abs, _ := filepath.Abs(path)
	const tmpl = `<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width, initial-scale=1">
	<title>NCC Aggregated Report</title>
	<link rel="icon" type="image/svg+xml" href="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMzIiIGhlaWdodD0iMzIiIHZpZXdCb3g9IjAgMCAzMiAzMiIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHJlY3QgeD0iMiIgeT0iMiIgd2lkdGg9IjI4IiBoZWlnaHQ9IjI4IiByeD0iOCIgZmlsbD0iIzBCMTIyMCIvPgo8cGF0aCBkPSJNOSAyM1Y5SDEyTDIwIDE5VjlIMjNWMjNIMjBMMTIgMTNWMjNIOVoiIGZpbGw9IiMyMkM1NUUiLz4KPGNpcmNsZSBjeD0iMjQiIGN5PSI4IiByPSIyIiBmaWxsPSIjMzhCREY4Ii8+CjxjaXJjbGUgY3g9IjgiIGN5PSIyNCIgcj0iMiIgZmlsbD0iI0Y1OUUwQiIvPgo8L3N2Zz4=">
	<style>
	:root {
	  --bg: #0f172a;
	  --card: #111827;
	  --text: #e5e7eb;
	  --muted: #9ca3af;
	  --accent: #2563eb;
	  --row1: #0b1224;
	  --row2: #0e1630;
	  --border: #1f2937;
	  --fail: #ef4444;
	  --warn: #f59e0b;
	  --info: #3b82f6;
	  --details: #aaa;
	  --err:  #94a3b8;
	}
	* { box-sizing: border-box; }
	html, body { height: 100%; }
	body {
	  margin: 0;
	  font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
	  background: linear-gradient(180deg,#0b1224,#0e1630);
	  color: var(--text);
    background-color: var(--row1);
	}
	.container { max-width: 1400px; margin: 0 auto; padding: 7px 12px; }
	.header { display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; margin-bottom: 16px; flex-wrap: wrap; }
	.title { flex: 1; min-width: 0; }
	.title h1 { margin: 0; font-size: 22px; font-weight: 700; }
	.title .sub { color: var(--muted); font-size: 12px; margin-top: 4px; }
	.header-actions { flex-shrink: 0; }
	.controls { display: flex; flex-wrap: wrap; gap: 12px; align-items: center; margin: 12px 0 18px 0; }
	.control { background: #0d152b; border: 1px solid var(--border); border-radius: 10px; padding: 10px 12px; display: flex; gap: 8px; align-items: center; }
	.control label { font-size: 12px; color: var(--muted); margin-right: 6px; }
	input[type="text"] { background: #0a1123; border: 1px solid var(--border); color: var(--text); padding: 8px 10px; border-radius: 8px; outline: none; width: 280px; }
	select, button { background: #0a1123; border: 1px solid var(--border); color: var(--text); padding: 8px 10px; border-radius: 8px; outline: none; }
	button:hover { border-color: var(--accent); cursor: pointer; }
	.badge { display:inline-flex; align-items:center; gap:6px; padding: 6px 10px; border-radius: 999px; background:#0a1123; border:1px solid var(--border); user-select:none; }
	.badge .dot { width: 8px; height: 8px; border-radius: 999px; display:inline-block; }
	.dot.fail{ background: var(--fail); } .dot.warn{ background: var(--warn); }
	.dot.info{ background: var(--info); } .dot.err{ background: var(--err); }
	.legend { display:flex; gap:8px; flex-wrap: wrap; }
	.card { padding: 12px; }
	
	 
	.summary { display:grid; grid-template-columns: repeat(5, 1fr); gap:12px; margin: 16px 0; }
	.sum-item { background: #0a1123; border: 1px solid var(--border); border-radius: 10px; padding: 10px; }
	.sum-item .label { font-size: 12px; color: var(--muted); }
	.sum-item .count { font-size: 18px; font-weight: 700; margin-top: 6px; }
	.progress { height: 6px; border-radius: 999px; background: #0d152b; margin-top: 8px; overflow: hidden; border:1px solid var(--border); }
	.progress > span { display:block; height:100%; }
	.progress.fail > span { background: var(--fail); } .progress.warn > span { background: var(--warn); }
	.progress.err  > span { background: var(--err); }  .progress.info > span { background: var(--info); }
	
.scroll {
  overflow-x: auto;
  overflow-y: hidden;
  -webkit-overflow-scrolling: touch; /* smooth mobile scroll */
  border-radius: 12px;
  border: 1px solid var(--border);
  margin: 0 -12px 16px -12px; /* full width minus padding */
  padding: 12px;
}

@media (max-width: 1024px) {
  .scroll {
    margin: 0 -8px 16px -8px;
    padding: 8px;
  }
}


	.scroll::-webkit-scrollbar { height: 10px; }
	.scroll::-webkit-scrollbar-thumb { background: #22304d; border-radius: 8px; }
	.scroll::-webkit-scrollbar-track { background: #0a1123; }
	
	 
	table { width: 100%; border-collapse: collapse; table-layout: fixed; }
	thead th {
	  position: sticky; top: 0; z-index: 1;
	  background: #0d152b; border-bottom: 1px solid var(--border);
	  padding: 10px; text-align: left; font-size: 12px; color: var(--muted);
	}
	tbody td { padding: 10px; border-bottom: 1px solid var(--border); vertical-align: top; }
	thead th, tbody td { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
	
	tbody tr:nth-child(odd) { background: var(--row1); }
	tbody tr:nth-child(even){ background: var(--row2); }
	
	td .severity { padding: 2px 8px; border-radius: 999px; font-size: 12px; }
	.sev-FAIL { background: #2b0d0d; color: var(--fail); border: 1px solid #4c1d1d; }
	.sev-WARN { background: #2b1f0d; color: var(--warn); border: 1px solid #4a3112; }
	.sev-INFO { background: #0c1f35; color: var(--info); border: 1px solid #173e6d; }
	.sev-ERR  { background: #1b2130; color: var(--err);  border: 1px solid #2c354a; }
	
	small.mono { color: var(--muted); font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
	.highlight { background: #3b82f655; }
	
	 
	th.col-cname, td.col-cname { width: 190px; }
	th.col-cluster, td.col-cluster   { width: 140px; }
	th.col-sev,     td.col-sev       { width: 96px; }
	th.col-title,   td.col-title     { width: 240px; }
	th.col-kb,      td.col-kb        { width: 110px; }
	th.col-detail,  td.col-detail    { width: 640px; }
	th.col-actions, td.col-actions   { width: 220px; }
	
	td.col-detail { white-space: normal; overflow: visible; }
	.detail-full { color: var(--details); font-size: 13px; line-height: 1.35; }
	
	 
	tbody tr.selected { outline: 2px solid var(--accent); outline-offset: -2px; }
	.actions { white-space: nowrap; display: inline-flex; gap: 6px; flex-wrap: wrap; }
	.actions button { background:#0a1123; border:1px solid var(--border); color:var(--text); padding:6px 8px; border-radius:8px; }
	.actions button:hover { border-color: var(--accent); cursor:pointer; }
	
	 
	a { color: #93c5fd; text-decoration: none; }
	a:hover { text-decoration: underline; color: #bfdbfe; }
	a:visited { color: #a5b4fc; }
	a[href^="http"]::after {
	  content: "↗";
	  font-size: 11px;
	  margin-left: 4px;
	  color: #64748b;
	}
	   
.control input[type="checkbox"] {
  position: absolute;
  opacity: 0;
  cursor: pointer;
  height: 0;
  width: 0;
}


.control span {
  display: flex;
  align-items: center;
  justify-content: center;
  position: relative;
  padding-left: 24px;
  min-height: 16px;  
  cursor: pointer;
  color: var(--muted);
}


.control span::before {
  content: "";
  position: absolute;
  top: 50%;
  left: 0;
  transform: translateY(-50%);  
  height: 16px;
  width: 16px;
  background-color: #0a1123;
  border: 1px solid var(--border);
  border-radius: 4px;
  box-sizing: border-box;  
}


.control span::after {
  content: "";
  width: 9px;
  height: 9px;
  background-color: var(--muted);
  position: absolute;
  top: 50%;
  left: 8px;  
  transform: translate(-50%, -50%) scale(0);  
  transition: transform 0.2s ease-in-out;
  border-radius: 2px;
}


.control input[type="checkbox"]:checked ~ span::after {
  transform: translate(-50%, -50%) scale(1);
}


 
.control span:hover::before {
  border-color: var(--accent);
}


 
.control input[type="checkbox"]:focus + span::before {
  outline: 2px solid var(--accent);
}

.cluster-meta {
  font-size: 10px;
  color: var(--muted);
  margin-top: 2px;
}

.cluster-wrapper {
  display: flex;
  gap: 8px;
  align-items: center;
  flex: 1;
}

.cluster-display {
  flex: 1;
  background: #0a1123;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 10px 14px;
  min-height: 20px;
  cursor: pointer;
  position: relative;
  font-size: 13px;
}

.cluster-display:hover {
  border-color: var(--accent);
  background: rgba(37, 99, 235, 0.05);
}

.cluster-placeholder {
  color: var(--text);
}

.cluster-toggle {
  background: #0a1123;
  border: 1px solid var(--border);
  color: var(--text);
  padding: 8px 12px;
  border-radius: 8px;
  font-size: 12px;
  cursor: pointer;
  min-width: 70px;
}

.cluster-toggle:hover {
  border-color: var(--accent);
  background: rgba(37, 99, 235, 0.1);
}

.cluster-modal {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0,0,0,0.8);
  z-index: 10000;
  display: flex;
  align-items: center;
  justify-content: center;
}

.cluster-modal-content {
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 24px;
  max-width: 500px;
  width: 90%;
  max-height: 80vh;  /* ✅ Fixed height */
  overflow-y: auto;  /* ✅ Scroll whole modal */
}

.cluster-list {
  max-height: 400px;        /* ✅ Scroll container */
  overflow-y: auto;         /* ✅ Vertical scroll */
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 8px;
  margin-bottom: 16px;
  background: #0a1123;
}

.cluster-list::-webkit-scrollbar {
  width: 8px;
}

.cluster-list::-webkit-scrollbar-track {
  background: #0a1123;
  border-radius: 4px;
}

.cluster-list::-webkit-scrollbar-thumb {
  background: #334155;
  border-radius: 4px;
}

.cluster-list::-webkit-scrollbar-thumb:hover {
  background: var(--accent);
}


.cluster-item {
  display: flex;
  align-items: center;
  padding: 12px;
  background: #0a1123;
  border: 1px solid var(--border);
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.2s;
  margin-bottom: 4px;  /* ✅ Spacing */
}

.cluster-item:hover {
  border-color: var(--accent);
  background: rgba(37, 99, 235, 0.1);
}

.cluster-item.active {
  border-color: var(--accent);
  background: rgba(37, 99, 235, 0.2);
  color: var(--accent);
}
.cluster-load-more {
  width: 100%;
  padding: 10px;
  background: #0a1123;
  border: 1px solid var(--border);
  color: var(--accent);
  border-radius: 8px;
  cursor: pointer;
  font-size: 13px;
}
.cluster-load-more:hover { border-color: var(--accent); background: rgba(37,99,235,0.1); }

.modal-buttons {
  display: flex;
  gap: 12px;
  justify-content: flex-end;
  margin-top: 20px;
  padding-top: 16px;
  border-top: 1px solid var(--border);
}

.cluster-status-wrapper {
  display: flex;
  gap: 8px;
  align-items: center;
  flex: 1;
}

.cluster-status {
  flex: 1;
  background: #0a1123;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 10px 16px;
  font-size: 13px;
  color: var(--text);
  cursor: pointer;
  position: relative;
  transition: all 0.2s ease;
  min-height: 20px;
  display: flex;
  align-items: center;
}

.cluster-status:hover {
  border-color: var(--accent);
  background: rgba(37, 99, 235, 0.1);
}

.cluster-status::after {
  content: "▼";
  margin-left: 8px;
  color: var(--muted);
  font-size: 11px;
}

.cluster-status-empty {
  color: var(--muted);
}

.cluster-edit-btn {
  background: #0a1123;
  border: 1px solid var(--border);
  color: var(--text);
  padding: 10px 14px;
  border-radius: 8px;
  font-size: 12px;
  cursor: pointer;
  white-space: nowrap;
  transition: all 0.2s ease;
}

.cluster-edit-btn:hover {
  border-color: var(--accent);
  background: rgba(37, 99, 235, 0.1);
}



.tooltip {
  position: absolute;
  z-index: 1000;
  background: var(--card);
  color: var(--text);
  padding: 12px;
  border: 1px solid var(--border);
  border-radius: 8px;
  font-size: 13px;
  line-height: 1.4;
  max-width: 450px;
  box-shadow: 0 8px 24px rgba(0,0,0,0.4);
  white-space: pre-wrap;
  pointer-events: none;
}

.cluster-expand-btn {
  background: #0a1123 !important;
  border: 1px solid var(--border) !important;
  color: var(--accent) !important;
  padding: 8px 12px !important;
  border-radius: 6px !important;
  font-size: 12px !important;
  cursor: pointer !important;
  margin-top: 8px !important;
  width: 100% !important;
  text-align: left !important;
}

.cluster-expand-btn:hover {
  background: rgba(37,99,235,0.1) !important;
}

.full-cluster-table table th,
.full-cluster-table table td {
  padding: 6px 4px !important;
  text-align: right !important;
}

.full-cluster-table table th:first-child,
.full-cluster-table table td:first-child {
  text-align: left !important;
}


.compact-cluster-table {
  width: 100%;
  font-size: 12px;
  margin-top: 12px;
}

.compact-cluster-table th,
.compact-cluster-table td {
  padding: 6px 4px;
  text-align: right;
}

.compact-cluster-table th:first-child,
.compact-cluster-table td:first-child {
  text-align: left;
}

.compact-cluster-table tr:hover {
  background: rgba(37, 99, 235, 0.05);
}

.full-cluster-table {
  display: none;
  margin-top: 12px;
  max-height: 300px;
  overflow-y: auto;
}

.full-cluster-table.show {
  display: block;
}

#detailsToggle {
  background: #0a1123;
  border: 1px solid var(--border);
  color: var(--accent);
  padding: 6px 12px;
  border-radius: 6px;
  font-size: 12px;
  cursor: pointer;
  margin-top: 8px;
}

#detailsToggle:hover {
  background: rgba(37, 99, 235, 0.1);
}

.expand-clusters-btn {
  width: 100% !important;
  background: #0a1123 !important;
  border: 1px solid var(--border) !important;
  color: var(--accent) !important;
  padding: 10px 12px !important;
  border-radius: 8px !important;
  font-size: 13px !important;
  cursor: pointer !important;
  margin-top: 12px !important;
}

.expand-clusters-btn:hover {
  background: rgba(37,99,235,0.15) !important;
  border-color: var(--accent) !important;
}

.compact-cluster-table a,
.full-cluster-table a {
  color: #93c5fd !important;
  text-decoration: none !important;
}

.compact-cluster-table a:hover,
.full-cluster-table a:hover {
  text-decoration: underline !important;
  color: #bfdbfe !important;
}


@media (max-width: 768px) {
  .controls {
    flex-direction: column !important;
    gap: 8px;
    align-items: stretch;
  }
  
  .control {
    flex: 1;
    min-width: 0; /* allow shrinking */
  }
  
  .control input[type="text"] {
    width: 100%;
    font-size: 16px; /* mobile zoom fix */
  }
}

@media (max-width: 900px) {
  th.col-kb, td.col-kb,
  th.col-actions, td.col-actions {
    display: none; /* Hide KB/Actions on small screens */
  }
  
  th.col-detail, td.col-detail {
    min-width: 200px; /* Priority for details */
  }
}

@media (max-width: 600px) {
  th.col-cluster, td.col-cluster {
    display: none; /* Hide IP on tiny screens */
  }
  
  th.col-cname, td.col-cname {
    width: 140px !important; /* Fixed width for cluster names */
  }
}

/* 6. PRINT STYLES (clean PDF export) */
@media print {
  .controls, .actions, .summary, footer {
    display: none !important;
  }
  
  .scroll {
    overflow: visible !important;
    border: none !important;
    margin: 0 !important;
  }
  
  table {
    font-size: 9px !important;
    width: 100% !important;
  }
  
  th, td {
    padding: 4px 2px !important;
    border-bottom: 1px solid #ccc !important;
  }
}

/* 7. MOBILE SUMMARY CARDS (stack vertically) */
@media (max-width: 768px) {
  .summary {
    grid-template-columns: 1fr !important;
    gap: 8px;
    margin: 12px 0;
  }
}

/* 8. TOUCH TARGETS (min 44px for mobile) */
button, .cluster-display, .cluster-toggle {
  min-height: 44px;
  min-width: 44px;
}

@media (hover: none) and (pointer: coarse) {
  .actions button {
    padding: 12px 16px; /* bigger touch targets */
    font-size: 14px;
  }
}

*:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.skip-link { position: absolute; top: -40px; left: 8px; background: var(--accent); color: #fff; padding: 8px 12px; border-radius: 6px; z-index: 100; text-decoration: none; font-size: 14px; }
.skip-link:focus { top: 8px; }
.report-footer { font-size: 0.8125rem; color: var(--muted); margin-top: 16px; padding: 12px 0; border-top: 1px solid var(--border); }
.report-meta-line { margin-top: 8px; line-height: 1.5; word-break: break-word; }
.sum-item.clickable { cursor: pointer; transition: background 0.15s, border-color 0.15s; }
.sum-item.clickable:hover { background: #0d152b; border-color: var(--accent); }
.sum-item.clickable:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.th-sort { cursor: pointer; user-select: none; }
.th-sort:hover { color: var(--text); }
.th-sort .sort-arrow { font-size: 10px; margin-left: 4px; opacity: 0.7; }
.table-info { font-size: 12px; color: var(--muted); margin-bottom: 8px; }
.pagination { display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 10px; margin-bottom: 12px; padding: 8px 10px; border: 1px solid var(--border); border-radius: 10px; background: #0a1123; }
.pagination button { background: #0f172a; border: 1px solid var(--border); color: var(--text); padding: 6px 10px; border-radius: 6px; cursor: pointer; font-size: 12px; min-width: 38px; transition: border-color 0.15s, background 0.15s; }
.pagination button:hover:not(:disabled) { border-color: var(--accent); }
.pagination button:disabled { opacity: 0.5; cursor: not-allowed; }
.pagination-info { font-size: 12px; color: var(--muted); }
.pagination-left { display: inline-flex; flex-wrap: wrap; gap: 8px; align-items: center; }
.pagination-right { display: inline-flex; flex-wrap: wrap; gap: 10px; align-items: center; justify-content: flex-end; }
.pagination-pages { display: inline-flex; flex-wrap: wrap; gap: 6px; align-items: center; }
.pagination-page.active { border-color: var(--accent); background: rgba(37,99,235,0.18); color: #bfdbfe; }
.pagination-ellipsis { color: var(--muted); padding: 0 2px; font-size: 12px; }
.pagination-size { display: flex; align-items: center; gap: 6px; font-size: 12px; color: var(--muted); }
.pagination-size select { background: #0a1123; border: 1px solid var(--border); color: var(--text); padding: 4px 8px; border-radius: 6px; font-size: 12px; }
.pagination-jump { display: inline-flex; align-items: center; gap: 6px; font-size: 12px; color: var(--muted); }
.pagination-jump input { width: 62px; background: #0a1123; border: 1px solid var(--border); color: var(--text); padding: 4px 8px; border-radius: 6px; font-size: 12px; }
.empty-state { text-align: center; padding: 48px 16px; color: var(--muted); }
.empty-state p { margin: 0 0 12px 0; font-size: 14px; }
.per-cluster-btn { font-size: 12px; padding: 8px 14px; background: transparent; border: 1px solid var(--border); border-radius: 8px; color: var(--muted); cursor: pointer; display: inline-flex; align-items: center; gap: 6px; transition: border-color 0.15s, color 0.15s; }
.per-cluster-btn:hover { border-color: var(--accent); color: #93c5fd; }
.per-cluster-btn::after { content: "↗"; font-size: 11px; opacity: 0.8; }
.per-cluster-links { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
.per-cluster-links a { font-size: 12px; padding: 4px 10px; background: #0a1123; border: 1px solid var(--border); border-radius: 6px; color: #93c5fd; text-decoration: none; }
.per-cluster-links a:hover { border-color: var(--accent); background: rgba(37,99,235,0.1); }
.row-badges { display:flex; gap:6px; flex-wrap:wrap; margin-top:6px; }
.ux-tag { font-size: 11px; padding: 2px 8px; border-radius: 999px; border: 1px solid var(--border); background:#0a1123; color: var(--muted); }
.ux-tag.tag-new { border-color: #7f1d1d; color: #fecaca; background: #3b1111; }
.ux-tag.tag-resolved { border-color: #14532d; color: #bbf7d0; background: #0f2d1d; }
.ux-tag.tag-changed { border-color: #1e3a8a; color: #bfdbfe; background: #10244f; }
.ux-tag.tag-flaky { border-color: #92400e; color: #fde68a; background: #3a2609; }
.insights { display:grid; grid-template-columns: repeat(3, 1fr); gap:12px; margin: 0 0 16px 0; }
.insights .sum-item .meta { font-size: 11px; color: var(--muted); margin-top: 6px; line-height: 1.4; max-height: 84px; overflow: auto; }
.meta { font-size: 12px; color: var(--muted); margin-top: 6px; line-height: 1.4; }
.health-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap:10px; margin-top: 10px; }
.health-card { border: 1px solid var(--border); background: #0a1123; border-radius: 10px; padding: 10px; }
.health-top { display:flex; justify-content: space-between; align-items: center; gap: 10px; }
.health-name { font-size: 13px; color: var(--text); font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.health-score { font-size: 12px; font-weight: 700; border-radius: 999px; padding: 3px 8px; border: 1px solid transparent; }
.health-score.good { color: #86efac; border-color: #14532d; background: #0f2d1d; }
.health-score.warn { color: #fde68a; border-color: #854d0e; background: #36250b; }
.health-score.bad { color: #fca5a5; border-color: #7f1d1d; background: #3b1111; }
.health-trend { margin-top: 8px; color: var(--muted); font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; }
.health-bar { margin-top: 8px; height: 6px; background: #111827; border: 1px solid var(--border); border-radius: 999px; overflow: hidden; }
.health-bar > span { display: block; height: 100%; background: linear-gradient(90deg, #ef4444 0%, #f59e0b 45%, #22c55e 100%); }

@media (max-width: 768px) {
  .insights { grid-template-columns: 1fr !important; gap: 8px; }
  .pagination { padding: 8px; }
  .pagination-left, .pagination-right { width: 100%; }
  .pagination-right { justify-content: flex-start; }
}

	</style>
	<script>

	const AGG = {{.JSON}};
	let AGG_ROWS = AGG;
	const AGG_DATA_URL = "{{.AggDataURL}}";
	var CLUSTER_LINKS = {{.ClusterLinksJSON}};
	const DIFF_FLAGS = {{.DiffFlagsJSON}};
	const FLAKY_KEYS = {{.FlakyKeysJSON}};
	const POLICY_VIOLATIONS = {{.PolicyViolationsJSON}};
	const RUN_SUMMARY = {{.RunSummaryJSON}};
	const HEALTH_TRENDS = {{.HealthTrendsJSON}};
	const ARTIFACT_LINKS = {{.ArtifactLinksJSON}};
	const REPORT_META = {{.ReportMetaJSON}};

	let state = {
  sortKey: "severity",
  sortDir: "asc",
  filterSev: new Set(["FAIL","WARN","ERR","INFO"]),
  filterClusters: new Set(),
  search: "",
  showClusterModal: false,
  allClusters: [],
  pageSize: 100,
  currentPage: 0,
  filteredRows: [],
  compareMode: "all",
  clusterModalSearch: "",
  clusterListVisible: 100,
  showPerClusterModal: false,
  perClusterSearch: "",
  perClusterListVisible: 50
};
	
	const sevRank = { FAIL: 1, WARN: 2, ERR: 3, INFO: 4 };
	let selIndex = -1;

async function init() {
  if (AGG_ROWS.length === 0 && AGG_DATA_URL) {
    try {
      const resp = await fetch(AGG_DATA_URL, { cache: "no-store" });
      if (!resp.ok) throw new Error("HTTP " + resp.status);
      AGG_ROWS = await resp.json();
    } catch (e) {
      console.error("failed to load aggregated data sidecar", e);
    }
  }
  initClusters();
  renderMetadata();
  renderHealthWidget();
  renderArtifactLinks();
  updateAndRender();
  document.addEventListener("keydown", onKey);
  var statusEl = document.getElementById('clusterStatus');
  if (statusEl) {
    statusEl.onclick = toggleClusterFilter;
    statusEl.onkeydown = function(e) { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggleClusterFilter(); } };
  }
}

function rowKeyFor(cluster, check) {
  return ((cluster || "").trim().toLowerCase() + "||" + (check || "").trim().toLowerCase());
}

function renderInsights(rows) {
  var policyViolations = Array.isArray(POLICY_VIOLATIONS) ? POLICY_VIOLATIONS : [];
  var changed = 0;
  var flaky = 0;
  (rows || []).forEach(function(r) {
    var k = rowKeyFor(r.cluster, r.check);
    var d = DIFF_FLAGS[k];
    if (d && (d.new_fail || d.resolved_fail || d.severity_changed)) changed++;
    if (FLAKY_KEYS[k]) flaky++;
  });
  var changedEl = document.getElementById("changedChecksCount");
  if (changedEl) changedEl.textContent = String(changed);
  var flakyEl = document.getElementById("flakyChecksCount");
  if (flakyEl) flakyEl.textContent = String(flaky);
  var policyStatus = document.getElementById("policyStatus");
  var policyMeta = document.getElementById("policyMeta");
  if (policyStatus) {
    if (policyViolations.length > 0) {
      policyStatus.textContent = "FAILED";
      policyStatus.style.color = "var(--fail)";
    } else {
      policyStatus.textContent = "PASS/NOT_CONFIGURED";
      policyStatus.style.color = "#86efac";
    }
  }
  if (policyMeta) {
    if (policyViolations.length > 0) {
      policyMeta.innerHTML = policyViolations.map(function(v) { return "• " + escapeHtml(v); }).join("<br>");
    } else {
      policyMeta.textContent = "No policy gate violations file for this run.";
    }
  }
}

function sparkline(values) {
  if (!values || !values.length) return "";
  var blocks = ["▁","▂","▃","▄","▅","▆","▇","█"];
  return values.map(function(v) {
    var n = Math.max(0, Math.min(100, Number(v) || 0));
    var idx = Math.round((n / 100) * (blocks.length - 1));
    return blocks[idx];
  }).join("");
}

function renderMetadata() {
  var meta = document.getElementById("reportMetaFooter");
  if (!meta) return;
  var parts = [];
  if (REPORT_META.version) parts.push("Version: " + REPORT_META.version);
  if (REPORT_META.git_revision) parts.push("Git: " + REPORT_META.git_revision);
  if (REPORT_META.stream) parts.push("Stream: " + REPORT_META.stream);
  if (REPORT_META.build_date) parts.push("Build: " + REPORT_META.build_date);
  if (RUN_SUMMARY && RUN_SUMMARY.duration_s) parts.push("Run duration: " + Number(RUN_SUMMARY.duration_s).toFixed(1) + "s");
  if (REPORT_META.quiet_hours || REPORT_META.maintenance_windows) {
    var q = REPORT_META.quiet_hours ? ("quiet-hours=" + REPORT_META.quiet_hours) : "";
    var m = REPORT_META.maintenance_windows ? ("maintenance=" + REPORT_META.maintenance_windows) : "";
    parts.push([q, m].filter(Boolean).join(" "));
  } else {
    parts.push("Quiet-hours/maintenance: not configured");
  }
  meta.textContent = parts.join(" | ");
}

function renderHealthWidget() {
  var root = document.getElementById("healthWidget");
  if (!root) return;
  var clusters = (RUN_SUMMARY && RUN_SUMMARY.clusters) ? RUN_SUMMARY.clusters.slice() : [];
  // Keep health widget aligned with currently selected cluster filters.
  if (state && state.filterClusters) {
    var selectedAddresses = new Set();
    (AGG_ROWS || []).forEach(function(r) {
      var clusterLabel = displayClusterName(r);
      if (state.filterClusters.has(clusterLabel)) {
        selectedAddresses.add((r.cluster || "").trim());
      }
    });
    clusters = clusters.filter(function(c) {
      return selectedAddresses.has((c.address || "").trim());
    });
  }
  if (!clusters.length) {
    root.innerHTML = '<div class="meta">No per-cluster health data available for selected clusters.</div>';
    return;
  }
  clusters.sort(function(a, b) {
    var sa = Math.max(0, Math.min(100, Number(a.health_score || 0)));
    var sb = Math.max(0, Math.min(100, Number(b.health_score || 0)));
    if (sa !== sb) return sa - sb; // worst first
    return (a.address || "").localeCompare(b.address || "");
  });
  var visible = clusters.slice(0, 10);
  var rows = visible.map(function(c) {
    var k = (c.address || "").toLowerCase();
    var trend = HEALTH_TRENDS[k] || [];
    var score = Math.max(0, Math.min(100, Number(c.health_score || 0)));
    var cls = score >= 90 ? "good" : (score >= 75 ? "warn" : "bad");
    var trendText = trend.length ? sparkline(trend) : "n/a";
    return '<div class="health-card">' +
      '<div class="health-top">' +
        '<div class="health-name" title="' + escapeHtml(c.address || "") + '">' + escapeHtml(c.address || "-") + '</div>' +
        '<div class="health-score ' + cls + '">' + score.toFixed(0) + '/100</div>' +
      '</div>' +
      '<div class="health-trend">Trend: ' + escapeHtml(trendText) + '</div>' +
      '<div class="health-bar"><span style="width:' + score.toFixed(0) + '%"></span></div>' +
    '</div>';
  });
  var note = clusters.length > 10
    ? '<div class="meta">Showing 10 worst clusters out of ' + clusters.length + '.</div>'
    : '<div class="meta">Showing all ' + clusters.length + ' clusters.</div>';
  root.innerHTML = note + '<div class="health-grid">' + rows.join("") + '</div>';
}

function renderArtifactLinks() {
  var root = document.getElementById("artifactLinks");
  if (!root) return;
  var links = [];
  Object.keys(ARTIFACT_LINKS || {}).forEach(function(name) {
    var href = ARTIFACT_LINKS[name];
    if (!href) return;
    links.push('<a href="' + escapeHtml(href) + '" target="_blank" rel="noopener">' + escapeHtml(name) + '</a>');
  });
  root.innerHTML = links.length ? links.join("") : '<span class="meta">No artifact links available.</span>';
}

function setCompareMode(v) {
  state.compareMode = v || "all";
  updateAndRender();
}

function parseSearchQuery(raw) {
  var out = { terms: [], sev: null, cluster: null, changed: null, flaky: null };
  if (!raw) return out;
  raw.split(/\s+/).forEach(function(tok) {
    if (!tok) return;
    var m = tok.match(/^([^:]+):(.*)$/);
    if (!m) { out.terms.push(tok.toLowerCase()); return; }
    var key = m[1].toLowerCase();
    var val = m[2].toLowerCase();
    if (key === "sev" || key === "severity") out.sev = val.toUpperCase();
    else if (key === "cluster") out.cluster = val;
    else if (key === "changed") out.changed = (val === "true" || val === "1" || val === "yes");
    else if (key === "flaky") out.flaky = (val === "true" || val === "1" || val === "yes");
    else out.terms.push(tok.toLowerCase());
  });
  return out;
}

function displayClusterName(r) {
  var name = (r && r.clusterName ? String(r.clusterName).trim() : "");
  if (name) return name;
  var version = (r && r.clusterVersion ? String(r.clusterVersion).trim().toLowerCase() : "");
  if (version.indexOf("pc.") === 0) return "Prism Central";
  return (r && r.cluster ? String(r.cluster) : "-");
}

function getSelectedClusterAddresses() {
  var selectedAddresses = new Set();
  (AGG_ROWS || []).forEach(function(r) {
    var label = displayClusterName(r);
    if (state.filterClusters.has(label)) {
      selectedAddresses.add((r.cluster || "").trim());
    }
  });
  return selectedAddresses;
}

function getClusterFilteredRows() {
  return (AGG_ROWS || []).filter(function(r) {
    return state.filterClusters.has(displayClusterName(r));
  });
}

function initClusters() {
  const clusters = Array.from(new Set(AGG_ROWS.map(function(r) { 
    return displayClusterName(r);
  }))).sort();
  console.log("Found clusters:", clusters); 
  state.allClusters = clusters;
  state.filterClusters = new Set(clusters);
  updateClusterStatus();
}

function updateClusterStatus() {
  const statusEl = document.getElementById('clusterStatus');
  if (!statusEl) return;
  const count = state.filterClusters.size;
  const total = state.allClusters.length || 0;
  let text, className;
  if (count === 0) {
    text = 'No clusters selected';
    className = 'cluster-status cluster-status-empty';
  } else if (count === total && total > 0) {
    text = total > 1 ? 'All clusters (' + total + ')' : 'All clusters selected';
    className = 'cluster-status';
  } else {
    if (total > 8) {
      text = count + ' of ' + total + ' clusters selected';
    } else {
      const names = Array.from(state.filterClusters).slice(0, 2);
      text = names.join(', ') + (count > 2 ? ' +' + (count - 2) : '') + ' (' + count + '/' + total + ')';
    }
    className = 'cluster-status';
  }
  statusEl.textContent = text;
  statusEl.className = className;
}


function toggleCluster(name) {
  if (state.filterClusters.has(name)) {
    state.filterClusters.delete(name);
  } else {
    state.filterClusters.add(name);
  }
  renderClusterList();
  updateClusterStatus();
  updateAndRender();
}

function selectAllClusters() {
  state.filterClusters = new Set(state.allClusters);
  renderClusterList();
  updateClusterStatus();
  updateAndRender();
}

function clearAllClusters() {
  state.filterClusters.clear();
  renderClusterList();
  updateClusterStatus();
  updateAndRender();
}

function toggleClusterFilter() {
  state.showClusterModal = !state.showClusterModal;
  if (state.showClusterModal) {
    showClusterModal();
  } else {
    hideClusterModal();
  }
}

function showClusterModal() {
  if (document.getElementById('clusterModal')) return;
  state.clusterModalSearch = "";
  state.clusterListVisible = 100;
  const modal = document.createElement('div');
  modal.id = 'clusterModal';
  modal.className = 'cluster-modal';
  modal.innerHTML = [
    '<div class="cluster-modal-content">',
      '<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; flex-wrap: wrap; gap: 8px;">',
        '<h3 style="margin: 0; font-size: 18px;">Select Clusters</h3>',
        '<button type="button" onclick="toggleClusterFilter()" style="background: none; border: 1px solid var(--border); color: var(--text); padding: 6px 12px; border-radius: 6px; cursor: pointer;" aria-label="Close">✕</button>',
      '</div>',
      '<div style="margin-bottom: 12px;"><input type="text" id="clusterModalSearch" placeholder="Filter clusters..." style="width:100%; background: #0a1123; border: 1px solid var(--border); color: var(--text); padding: 8px 10px; border-radius: 8px; font-size: 13px;" aria-label="Filter cluster list"></div>',
      '<div class="cluster-list" id="clusterList"></div>',
      '<div id="clusterListMore" style="margin-top: 8px;"></div>',
      '<div class="modal-buttons">',
        '<button type="button" onclick="selectAllClusters()" style="padding: 8px 16px; border-radius: 6px; border: 1px solid var(--border); background: #0a1123; color: var(--text);">Select All</button>',
        '<button type="button" onclick="clearAllClusters()" style="padding: 8px 16px; border-radius: 6px; border: 1px solid var(--border); background: #0a1123; color: var(--text);">Clear All</button>',
        '<button type="button" onclick="toggleClusterFilter()" style="padding: 8px 16px; border-radius: 6px; background: var(--accent); color: white; border: none;">Done</button>',
      '</div>',
    '</div>'
  ].join('');
  document.body.appendChild(modal);
  var searchEl = document.getElementById('clusterModalSearch');
  if (searchEl) {
    searchEl.oninput = function() { state.clusterModalSearch = this.value.trim(); renderClusterList(); };
    searchEl.onkeydown = function(e) { if (e.key === 'Escape') { this.value = ''; state.clusterModalSearch = ''; renderClusterList(); this.focus(); } };
  }
  renderClusterList();
}

function hideClusterModal() {
  const modal = document.getElementById('clusterModal');
  if (modal) modal.remove();
}
function showPerClusterModal() {
  if (typeof CLUSTER_LINKS === 'undefined' || !CLUSTER_LINKS.length) return;
  if (document.getElementById('perClusterModal')) return;
  state.perClusterSearch = "";
  state.perClusterListVisible = 50;
  var modal = document.createElement('div');
  modal.id = 'perClusterModal';
  modal.className = 'cluster-modal';
  modal.innerHTML = [
    '<div class="cluster-modal-content" style="max-width: 560px;">',
      '<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">',
        '<h3 style="margin: 0; font-size: 18px;">Per-cluster reports</h3>',
        '<button type="button" onclick="hidePerClusterModal()" style="background: none; border: 1px solid var(--border); color: var(--text); padding: 6px 12px; border-radius: 6px; cursor: pointer;" aria-label="Close">✕</button>',
      '</div>',
      '<div style="margin-bottom: 12px;"><input type="text" id="perClusterSearch" placeholder="Filter by cluster name or IP..." style="width:100%; background: #0a1123; border: 1px solid var(--border); color: var(--text); padding: 8px 10px; border-radius: 8px; font-size: 13px;" aria-label="Filter per-cluster links"></div>',
      '<div class="cluster-list" id="perClusterList"></div>',
      '<div id="perClusterListMore" style="margin-top: 8px;"></div>',
    '</div>'
  ].join('');
  document.body.appendChild(modal);
  var searchEl = document.getElementById('perClusterSearch');
  if (searchEl) {
    searchEl.oninput = function() { state.perClusterSearch = this.value.trim(); renderPerClusterList(); };
    searchEl.onkeydown = function(e) { if (e.key === 'Escape') { this.value = ''; state.perClusterSearch = ''; renderPerClusterList(); this.focus(); } };
    searchEl.focus();
  }
  state.showPerClusterModal = true;
  renderPerClusterList();
}
function hidePerClusterModal() {
  var modal = document.getElementById('perClusterModal');
  if (modal) modal.remove();
  state.showPerClusterModal = false;
}
function renderPerClusterList() {
  var list = document.getElementById('perClusterList');
  var moreEl = document.getElementById('perClusterListMore');
  if (!list || typeof CLUSTER_LINKS === 'undefined') return;
  var needle = state.perClusterSearch.toLowerCase();
  var filtered = needle ? CLUSTER_LINKS.filter(function(item) { return (item.Cluster || '').toLowerCase().indexOf(needle) !== -1; }) : CLUSTER_LINKS.slice();
  var visible = filtered.slice(0, state.perClusterListVisible);
  var items = visible.map(function(item) {
    var name = escapeHtml(item.Cluster || '');
    var href = escapeHtml(item.HTML || '#');
    return '<a href="' + href + '" target="_blank" rel="noopener" class="cluster-item" style="display:block; text-decoration: none; color: inherit;">' + name + '</a>';
  });
  list.innerHTML = items.join('');
  if (moreEl) {
    var remaining = filtered.length - state.perClusterListVisible;
    if (remaining > 0) {
      moreEl.innerHTML = '<button type="button" class="cluster-load-more" onclick="state.perClusterListVisible += 50; renderPerClusterList();">Show more (' + remaining + ' remaining)</button>';
      moreEl.style.display = 'block';
    } else {
      moreEl.innerHTML = '';
      moreEl.style.display = 'none';
    }
  }
}

function renderClusterList() {
  const list = document.getElementById('clusterList');
  const moreEl = document.getElementById('clusterListMore');
  if (!list) return;
  var needle = state.clusterModalSearch.toLowerCase();
  var filtered = needle ? state.allClusters.filter(function(name) { return name.toLowerCase().indexOf(needle) !== -1; }) : state.allClusters.slice();
  var visible = filtered.slice(0, state.clusterListVisible);
  var items = visible.map(function(name) {
    var active = state.filterClusters.has(name) ? 'active' : '';
    var safeName = escapeHtml(name);
    var safeJs = jsStrEsc(name);
    return '<div class="cluster-item ' + active + '" onclick="toggleCluster(\'' + safeJs + '\')">' + safeName + '</div>';
  });
  list.innerHTML = items.join('');
  if (moreEl) {
    var remaining = filtered.length - state.clusterListVisible;
    if (remaining > 0) {
      moreEl.innerHTML = '<button type="button" class="cluster-load-more" onclick="state.clusterListVisible += 100; renderClusterList();">Show more (' + remaining + ' remaining)</button>';
      moreEl.style.display = 'block';
    } else {
      moreEl.innerHTML = '';
      moreEl.style.display = 'none';
    }
  }
}
	
	function filterBySev(sev) {
	  if (sev === null) {
	    state.filterSev = new Set(["FAIL","WARN","ERR","INFO"]);
	  } else {
	    state.filterSev = new Set([sev]);
	  }
	  document.querySelectorAll('.control input[type="checkbox"]').forEach(function(cb) {
	    var m = (cb.getAttribute('onchange') || '').match(/'([^']+)'/);
	    if (m) cb.checked = state.filterSev.has(m[1]);
	  });
	  updateAndRender();
	}
	function clearFilters() {
	  state.search = "";
	  var sb = document.getElementById("searchBox");
	  if (sb) sb.value = "";
	  state.filterClusters = new Set(state.allClusters);
	  state.filterSev = new Set(["FAIL","WARN","ERR","INFO"]);
	  document.querySelectorAll('.control input[type="checkbox"]').forEach(function(cb) { cb.checked = true; });
	  renderClusterList();
	  updateClusterStatus();
	  updateAndRender();
	  if (state.showClusterModal) { state.showClusterModal = false; hideClusterModal(); }
	}
	function setSev(checked, sev) {
	  if (checked) state.filterSev.add(sev); else state.filterSev.delete(sev);
	  updateAndRender();
	}
	
	function onSearch(inp) {
	  state.search = inp.value.trim();
	  updateAndRender();
	}
	
	let debounceTimer;
	function onSearchDebounced(inp) {
	  clearTimeout(debounceTimer);
	  debounceTimer = setTimeout(() => onSearch(inp), 150);
	}
	
	function sortBy(key) {
	  if (state.sortKey === key) state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
	  else { state.sortKey = key; state.sortDir = "asc"; }
	  updateSortIndicators();
	  updateAndRender();
	}
	function updateSortIndicators() {
	  document.querySelectorAll('.th-sort .sort-arrow').forEach(function(el) {
	    var key = el.id ? el.id.replace('arrow-','') : '';
	    if (key === 'clusterName') key = 'clusterName';
	    if (key === 'cluster') key = 'cluster';
	    if (key === 'severity') key = 'severity';
	    if (key === 'check') key = 'check';
	    el.textContent = state.sortKey === key ? (state.sortDir === 'asc' ? ' ↑' : ' ↓') : '';
	  });
	  var keys = ['clusterName','cluster','severity','check'];
	  keys.forEach(function(key) {
	    var el = document.getElementById('arrow-' + key);
	    if (el) el.textContent = state.sortKey === key ? (state.sortDir === 'asc' ? ' ↑' : ' ↓') : '';
	  });
	}
	
function filterData() {
  const q = parseSearchQuery(state.search);
  return AGG_ROWS.filter(r => {
    if (!state.filterSev.has(r.severity)) return false;
    const clusterId = displayClusterName(r);
    if (!state.filterClusters.has(clusterId)) return false;
    const k = rowKeyFor(r.cluster, r.check);
    const d = DIFF_FLAGS[k] || {};
    const isChanged = !!(d.new_fail || d.resolved_fail || d.severity_changed);
    const isFlaky = !!FLAKY_KEYS[k];

    if (state.compareMode === "changed" && !isChanged) return false;
    if (state.compareMode === "flaky" && !isFlaky) return false;

    if (q.sev && r.severity !== q.sev) return false;
    if (q.cluster && clusterId.toLowerCase().indexOf(q.cluster) === -1) return false;
    if (q.changed !== null && isChanged !== q.changed) return false;
    if (q.flaky !== null && isFlaky !== q.flaky) return false;

    if (!q.terms.length) return true;
    const hay = (clusterId + " " + r.severity + " " + r.check + " " + r.detail).toLowerCase();
    return q.terms.every(function(t){ return hay.indexOf(t) !== -1; });
  });
}


	function sortData(rows) {
	  const k = state.sortKey, dir = state.sortDir;
	  const mul = dir === "asc" ? 1 : -1;
	  rows.sort((a,b) => {
		let av = a[k], bv = b[k];
    if (k === "clusterName") {
      av = displayClusterName(a);
      bv = displayClusterName(b);
    }
		if (k === "severity") { av = sevRank[av] || 99; bv = sevRank[bv] || 99; }
		return (av > bv ? 1 : av < bv ? -1 : 0) * mul;
	  });
	  return rows;
	}
	
function updateCounts(rows) {
  const total = rows.length;
  const cnt = { FAIL:0, WARN:0, ERR:0, INFO:0 };
  rows.forEach(r => { if (cnt[r.severity] !== undefined) cnt[r.severity]++; });

  // Update main summary counts ONLY
  document.getElementById("countTotal").textContent = total;
  document.getElementById("countFail").textContent = cnt.FAIL;
  document.getElementById("countWarn").textContent = cnt.WARN;
  document.getElementById("countErr").textContent  = cnt.ERR;
  document.getElementById("countInfo").textContent = cnt.INFO;

  const pct = {};
  Object.keys(cnt).forEach(k => pct[k] = total ? Math.round(cnt[k]*100/total) : 0);
  document.getElementById("barFail").style.width = pct.FAIL + "%";
  document.getElementById("barWarn").style.width = pct.WARN + "%";
  document.getElementById("barErr").style.width  = pct.ERR  + "%";
  document.getElementById("barInfo").style.width = pct.INFO + "%";

  // const pc = document.getElementById("perCluster"); pc.innerHTML = "";
}

	
	function extractKB(detail) {
	  const text = detail || "";
	  const re = /(https?:\/\/[^\s)]+portal\.nutanix\.com\/kb\/\d+|https?:\/\/[^\s)]+)/i;
	  const m = text.match(re);
	  return m ? m[0] : "";
	}
	function kbLabel(url) {
	  if (!url) return "";
	  const m = url.match(/\/kb\/(\d+)\b/i);
	  return m ? ('KB-' + m[1]) : 'KB';
	}
	
	function escapeHtml(s) {
	  return (s || "").toString()
		.replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;")
		.replaceAll('"',"&quot;").replaceAll("'","&#39;");
	}
	function jsStrEsc(s) {
	  return (s || "").toString()
		.replace(/\\/g, "\\\\").replace(/'/g, "\\'").replace(/\r/g, " ").replace(/\n/g, " ");
	}
	
	function highlight(text, needle) {
	  if (!needle) return escapeHtml(text);
	  const re = new RegExp("(" + needle.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\$&") + ")", "ig");
	  return escapeHtml(text).replace(re, '<span class="highlight">$1</span>');
	}
	
	function formatCheckTitle(s) {
 	 s = s || "";
  	return s.replace(/^detailed information for\s*/i, "").replace(/:$/, "");
	}

	function jsEscape(s) {
	  return (s || "").toString()
		.replaceAll("\\", "\\\\").replaceAll("\n", "\\n").replaceAll("\r", " ")
		.replaceAll("'", "\\'").replaceAll("\"", "\\\"");
	}
	
	async function copyText(text) {
	  try { await navigator.clipboard.writeText(text); }
	  catch {
		const ta = document.createElement("textarea");
		ta.value = text; document.body.appendChild(ta);
		ta.select(); document.execCommand("copy");
		document.body.removeChild(ta);
	  }
	}

	
  function renderTable(rows) {
  const tbody = document.getElementById("tbody");
  tbody.innerHTML = "";
  const needle = state.search;
  const frag = document.createDocumentFragment();
  
  rows.forEach((r, idx) => {
    const tr = document.createElement("tr");
    tr.setAttribute("tabindex", "0");
    tr.dataset.index = idx.toString();
    
    const detailEsc = (r.detail || "").replaceAll("\\n","<br>");
    const nameLine = escapeHtml(displayClusterName(r));
    const verLine = escapeHtml(r.clusterVersion || "");
    const nccLine = escapeHtml(r.nccVersion || "");
    const checkTitle = formatCheckTitle(r.check || "");
    const kb = extractKB(r.detail);
    const kbCell = kb ? ('<a href="' + kb + '" target="_blank" rel="noopener">' + kbLabel(kb) + '</a>') : '';
    const clusterUrl = 'https://' + encodeURIComponent(r.cluster) + ':9440';
    const rowText = (r.cluster + " " + r.severity + " " + r.check + " " + (r.detail || "")).trim();
    const actHTML = '<div class="actions">' +
      '<button onclick="copyText(\'' + jsEscape(rowText) + '\')">Copy row</button>' +
      '<button onclick="copyText(\'' + jsEscape(r.detail || "") + '\')">Copy detail</button>' +
      '</div>';
    
    
    const nameCell = '<td class="col-cname">' +
      '<div class="mono" style="color: var(--text); font-weight: 600;">' + highlight(nameLine, needle) + '</div>' +
      '<div class="cluster-meta mono">' +
      (verLine ? ('<div>Version: ' + highlight(verLine, needle) + '</div>') : '') +
      (nccLine ? ('<div>NCC: ' + highlight(nccLine, needle) + '</div>') : '') +
      '</div></td>';
    
    const clusterCell = '<td class="col-cluster">' +
      '<small class="mono has-tooltip" data-fulltext="' + escapeHtml(r.cluster) + '">' +
      '<a href="' + clusterUrl + '" target="_blank" rel="noopener">' + highlight(r.cluster, needle) + '</a>' +
      '</small></td>';
    
    const titleCell = '<td class="col-title">' +
      '<small class="mono has-tooltip" data-fulltext="' + escapeHtml(checkTitle) + '">' +
      highlight(checkTitle, needle) +
      '</small>' +
      (function() {
        var k = rowKeyFor(r.cluster, r.check);
        var d = DIFF_FLAGS[k] || {};
        var tags = [];
        if (d.new_fail) tags.push('<span class="ux-tag tag-new">+new FAIL</span>');
        if (d.resolved_fail) tags.push('<span class="ux-tag tag-resolved">resolved FAIL</span>');
        if (d.severity_changed) tags.push('<span class="ux-tag tag-changed">severity changed</span>');
        if (FLAKY_KEYS[k]) tags.push('<span class="ux-tag tag-flaky">flaky</span>');
        return tags.length ? ('<div class="row-badges">' + tags.join('') + '</div>') : '';
      })() +
      '</td>';
    
    const detailCell = '<td class="col-detail">' +
      '<div class="detail-full has-tooltip" data-fulltext="' + jsEscape(r.detail || "") + '"' +
      ' style="max-height: 60px; overflow-y: auto;">' +
      highlight(detailEsc, needle) +
      '</div></td>';
    
    tr.innerHTML = nameCell + clusterCell +
      '<td class="col-sev"><span class="severity sev-' + r.severity + '">' + r.severity + '</span></td>' +
      titleCell + '<td class="col-kb">' + kbCell + '</td>' + detailCell +
      '<td class="col-actions">' + actHTML + '</td>';
    
    tr.addEventListener("focus", () => selectRow(tr));
    frag.appendChild(tr);
  });
  
  tbody.appendChild(frag);
  setTimeout(initTooltips, 0);
}


let tooltip = null;
let tooltipTimeout = null;

function showTooltip(event, text) {
  
  if (tooltipTimeout) clearTimeout(tooltipTimeout);
  
  if (!text || text.length < 20) return; 
  
  
  if (tooltip) {
    tooltip.remove();
    tooltip = null;
  }
  
  tooltip = document.createElement('div');
  tooltip.className = 'tooltip';
  tooltip.textContent = text;
  document.body.appendChild(tooltip);
  
  
  const rect = event.target.getBoundingClientRect();
  const tooltipRect = tooltip.getBoundingClientRect();
  
  
  let top = event.clientY - tooltipRect.height - 10;
  let left = event.clientX - tooltipRect.width / 2;
  
  
  if (top < 10) top = event.clientY + 20;
  if (left < 10) left = 10;
  if (left + tooltipRect.width > window.innerWidth - 10) {
    left = window.innerWidth - tooltipRect.width - 10;
  }
  
  tooltip.style.position = 'fixed';
  tooltip.style.left = left + 'px';
  tooltip.style.top = top + 'px';
}

function hideTooltip() {
  if (tooltipTimeout) clearTimeout(tooltipTimeout);
  tooltipTimeout = setTimeout(() => {
    if (tooltip) {
      tooltip.remove();
      tooltip = null;
    }
  }, 50); 
}

function initTooltips() {
  
  document.querySelectorAll('.has-tooltip').forEach(el => {
    el.removeEventListener('mouseenter', el._ttEnter);
    el.removeEventListener('mouseleave', el._ttLeave);
    el.removeEventListener('mousemove', el._ttMove);
  });
  
  
  document.querySelectorAll('.has-tooltip').forEach(el => {
    const text = el.dataset.fulltext || '';
    if (text.length < 20) return; 
    
    el._ttEnter = (e) => showTooltip(e, text);
    el._ttLeave = hideTooltip;
    el._ttMove = (e) => {
      if (tooltip && text.length >= 20) {
        showTooltip(e, text); 
      }
    };
    
    el.addEventListener('mouseenter', el._ttEnter);
    el.addEventListener('mouseleave', el._ttLeave);
    el.addEventListener('mousemove', el._ttMove);
  });
}
	function selectRow(tr) {
	  const tbody = document.getElementById("tbody");
	  Array.from(tbody.querySelectorAll("tr.selected")).forEach(x => x.classList.remove("selected"));
	  tr.classList.add("selected");
	  selIndex = parseInt(tr.dataset.index || "-1", 10);
	}
	
	function focusRow(i) {
	  const rows = document.querySelectorAll("#tbody tr");
	  if (!rows.length) return;
	  if (i < 0) i = 0;
	  if (i >= rows.length) i = rows.length - 1;
	  selIndex = i;
	  const tr = rows[i];
	  tr.focus({preventScroll:false});
	  selectRow(tr);
	  tr.scrollIntoView({block:"nearest", inline:"nearest"});
	}
	
	function onKey(e) {
	  const k = e.key;
	  if (k === "/") {
		e.preventDefault();
		const sb = document.getElementById("searchBox");
		if (sb) { sb.focus(); sb.select(); }
		return;
	  }
	  if (k === "Escape") {
		if (state.showPerClusterModal) {
		  state.showPerClusterModal = false;
		  hidePerClusterModal();
		  return;
		}
		if (state.showClusterModal) {
		  state.showClusterModal = false;
		  hideClusterModal();
		  return;
		}
		if (state.search) {
		  state.search = ""; var sb = document.getElementById("searchBox"); if (sb) sb.value = "";
		  updateAndRender();
		}
		return;
	  }
	  if (k === "ArrowDown") { e.preventDefault(); focusRow(selIndex + 1); return; }
	  if (k === "ArrowUp")   { e.preventDefault(); focusRow(selIndex - 1); return; }
	}
	
	function updateAndRender() {
	  let rows = filterData();
	  var clusterScopedRows = getClusterFilteredRows();
	  renderInsights(clusterScopedRows);
	  
	  const total = rows.length;
	  const cnt = { FAIL:0, WARN:0, ERR:0, INFO:0 };
	  rows.forEach(r => { if (cnt[r.severity] !== undefined) cnt[r.severity]++; });
	  document.getElementById("countTotal").textContent = total;
	  document.getElementById("countFail").textContent = cnt.FAIL;
	  document.getElementById("countWarn").textContent = cnt.WARN;
	  document.getElementById("countErr").textContent  = cnt.ERR;
	  document.getElementById("countInfo").textContent = cnt.INFO;
	  const pct = {};
	  Object.keys(cnt).forEach(k => pct[k] = total ? Math.round(cnt[k]*100/total) : 0);
	  document.getElementById("barFail").style.width = pct.FAIL + "%";
	  document.getElementById("barWarn").style.width = pct.WARN + "%";
	  document.getElementById("barErr").style.width  = pct.ERR  + "%";
	  document.getElementById("barInfo").style.width = pct.INFO + "%";
	
	  
	  updateCounts(rows);
	  renderHealthWidget();
	  rows = sortData(rows.slice());
	  state.filteredRows = rows;
	  var totalRows = rows.length;
	  var totalPages = state.pageSize > 0 ? Math.max(1, Math.ceil(totalRows / state.pageSize)) : 1;
	  if (state.currentPage >= totalPages) state.currentPage = Math.max(0, totalPages - 1);
	  var pageStart = state.currentPage * state.pageSize;
	  var pageEnd = Math.min(pageStart + state.pageSize, totalRows);
	  var pageRows = totalRows > 0 ? rows.slice(pageStart, pageEnd) : [];
	  var infoEl = document.getElementById("tableInfo");
	  var emptyEl = document.getElementById("emptyState");
	  var tableEl = document.getElementById("main-table");
	  var paginationEl = document.getElementById("pagination");
	  if (infoEl) {
	    if (totalRows === 0) infoEl.textContent = "Showing 0 rows";
	    else if (totalRows <= state.pageSize) infoEl.textContent = "Showing " + totalRows + " row" + (totalRows === 1 ? "" : "s");
	    else infoEl.textContent = "Showing " + (pageStart + 1) + "–" + pageEnd + " of " + totalRows + " rows";
	  }
	  if (emptyEl && tableEl) {
	    if (totalRows === 0) { tableEl.style.display = "none"; emptyEl.style.display = "block"; if (paginationEl) paginationEl.style.display = "none"; }
	    else { tableEl.style.display = ""; emptyEl.style.display = "none"; if (paginationEl) paginationEl.style.display = "flex"; }
	  }
	  if (paginationEl && totalRows > 0) {
	    var pageButtons = buildPageButtons(totalPages, state.currentPage);
	    paginationEl.innerHTML =
	      '<div class="pagination-left">' +
	      '<button type="button" onclick="goToPage(0)" ' + (state.currentPage === 0 ? 'disabled' : '') + ' aria-label="First page">First</button>' +
	      '<button type="button" onclick="goToPage(' + (state.currentPage - 1) + ')" ' + (state.currentPage === 0 ? 'disabled' : '') + ' aria-label="Previous page">Prev</button>' +
	      '<span class="pagination-pages">' + pageButtons + '</span>' +
	      '<button type="button" onclick="goToPage(' + (state.currentPage + 1) + ')" ' + (state.currentPage >= totalPages - 1 ? 'disabled' : '') + ' aria-label="Next page">Next</button>' +
	      '<button type="button" onclick="goToPage(' + (totalPages - 1) + ')" ' + (state.currentPage >= totalPages - 1 ? 'disabled' : '') + ' aria-label="Last page">Last</button>' +
	      '<span class="pagination-info">Page ' + (state.currentPage + 1) + ' of ' + totalPages + '</span>' +
	      '</div>' +
	      '<div class="pagination-right">' +
	      '<label class="pagination-jump"><span>Go</span><input id="pageJumpInput" type="number" min="1" max="' + totalPages + '" value="' + (state.currentPage + 1) + '" onkeydown="if(event.key===&#39;Enter&#39;){goToPageInput(this.value,' + totalPages + ')}" aria-label="Go to page number"><button type="button" onclick="goToPageInput(document.getElementById(&#39;pageJumpInput&#39;).value,' + totalPages + ')">Go</button></label>' +
	      '<label class="pagination-size"><span>Rows</span><select id="pageSizeSelect" onchange="setPageSize(parseInt(this.value,10))">' +
	      (state.pageSize === 50 ? '<option value="50" selected>50</option>' : '<option value="50">50</option>') +
	      (state.pageSize === 100 ? '<option value="100" selected>100</option>' : '<option value="100">100</option>') +
	      (state.pageSize === 200 ? '<option value="200" selected>200</option>' : '<option value="200">200</option>') +
	      (state.pageSize === 500 ? '<option value="500" selected>500</option>' : '<option value="500">500</option>') +
	      '</select></label>' +
	      '</div>';
	  }
	  updateSortIndicators();
	  var fc = document.getElementById("footerClusterCount");
	  if (fc) fc.textContent = "Clusters: " + state.filterClusters.size + "/" + state.allClusters.length;
	  renderTable(pageRows);
	}
	function goToPage(p) {
	  state.currentPage = Math.max(0, Math.min(p, Math.ceil(state.filteredRows.length / state.pageSize) - 1));
	  updateAndRender();
	}
	function setPageSize(n) {
	  state.pageSize = n;
	  state.currentPage = 0;
	  updateAndRender();
	}
	function buildPageButtons(totalPages, currentPage) {
	  if (totalPages <= 1) return '<button type="button" class="pagination-page active" aria-current="page">1</button>';
	  var pages = [0];
	  var start = Math.max(1, currentPage - 1);
	  var end = Math.min(totalPages - 2, currentPage + 1);
	  if (start > 1) pages.push("...");
	  for (var i = start; i <= end; i++) pages.push(i);
	  if (end < totalPages - 2) pages.push("...");
	  pages.push(totalPages - 1);
	  var html = "";
	  for (var p = 0; p < pages.length; p++) {
	    var item = pages[p];
	    if (item === "...") {
	      html += '<span class="pagination-ellipsis" aria-hidden="true">...</span>';
	    } else {
	      html += '<button type="button" class="pagination-page ' + (item === currentPage ? 'active' : '') + '" onclick="goToPage(' + item + ')" ' + (item === currentPage ? 'aria-current="page"' : '') + ' aria-label="Page ' + (item + 1) + '">' + (item + 1) + '</button>';
	    }
	  }
	  return html;
	}
	function goToPageInput(raw, totalPages) {
	  var n = parseInt(raw, 10);
	  if (isNaN(n)) return;
	  if (n < 1) n = 1;
	  if (n > totalPages) n = totalPages;
	  goToPage(n - 1);
	}
	
	function downloadCSV() {
		const rows = filterData();
		const headers = ["Cluster","Severity","NCC Alert Title","Detail"];
		const lines = [headers.join(",")];
		rows.forEach(r => {
		  const title = formatCheckTitle(r.check || "");
		  const row = [r.cluster, r.severity, title, r.detail || ""].map(v => {
		    const s = (v ?? "").toString().replaceAll('"','""').replaceAll("\r"," ").replaceAll("\n","\\n");
		    return '"' + s + '"';
		  }).join(",");
		  lines.push(row);
		});
	  const blob = new Blob([lines.join("\n")], {type: "text/csv;charset=utf-8;"});
	  triggerDownload(blob, "aggregated_filtered.csv");
	}
	
	function downloadJSON() {
	  const rows = filterData();
	  const blob = new Blob([JSON.stringify(rows, null, 2)], {type: "application/json;charset=utf-8;"});
	  triggerDownload(blob, "aggregated_filtered.json");
	}
	
	function triggerDownload(blob, name) {
	  const a = document.createElement("a");
	  a.href = URL.createObjectURL(blob);
	  a.download = name;
	  document.body.appendChild(a);
	  a.click();
	  document.body.removeChild(a);
	}
	</script>
	</head>
	<body onload="init()">
	<a href="#main-table" class="skip-link">Skip to table</a>
	<div class="container">
	  <div class="header">
		<div class="title">
		  <h1>NCC Aggregated Report</h1>
		  <div class="sub">Generated at {{.GeneratedAt}}{{if .Clusters}} · {{len .Clusters}} cluster{{if eq (len .Clusters) 1}}{{else}}s{{end}}{{end}}</div>
		</div>
		{{if .Clusters}}
		<div class="header-actions">
		  <button type="button" class="per-cluster-btn" onclick="showPerClusterModal()" aria-label="Open per-cluster report links">Per-cluster reports ({{len .Clusters}})</button>
		</div>
		{{end}}
	  </div>
	
	  <div class="controls">
		<div class="control">
		  <label for="searchBox">Search</label>
		  <input id="searchBox" type="text" placeholder="Type to filter... (tokens: sev:FAIL cluster:10.1 changed:true flaky:true)" oninput="onSearchDebounced(this)" aria-label="Filter rows by search text or tokens" />
		</div>
		<div class="control">
		  <label>Severity</label>
		  <label><input type="checkbox" checked onchange="setSev(this.checked,'FAIL')"> <span style="color: var(--fail);">FAIL</span></label>
		  <label><input type="checkbox" checked onchange="setSev(this.checked,'WARN')"> <span style="color: var(--warn);">WARN</span></label>
		  <label><input type="checkbox" checked onchange="setSev(this.checked,'ERR')"> <span style="color: var(--err);">ERR</span></label>
		  <label><input type="checkbox" checked onchange="setSev(this.checked,'INFO')"> <span style="color: var(--info);">INFO</span></label>
		</div>
<div class="control">
  <label for="clusterStatus">Clusters</label>
  <div class="cluster-status-wrapper">
    <div id="clusterStatus" class="cluster-status" role="button" tabindex="0" aria-label="Select clusters to filter" aria-haspopup="dialog">All clusters selected</div>
  </div>
</div>
		<div class="control">
		  <label for="compareMode">Compare</label>
		  <select id="compareMode" onchange="setCompareMode(this.value)" aria-label="Comparison mode">
		    <option value="all">All rows</option>
		    <option value="changed">Changed only</option>
		    <option value="flaky">Flaky only</option>
		  </select>
</div>
		<div class="control">
		  <button type="button" onclick="downloadCSV()" aria-label="Export filtered rows as CSV">Export CSV</button>
		  <button type="button" onclick="downloadJSON()" aria-label="Export filtered rows as JSON">Export JSON</button>
		</div>
		<div class="control" style="min-width: 280px;">
		  <label>Artifacts</label>
		  <div class="per-cluster-links" id="artifactLinks" aria-label="Artifact quick links"></div>
		</div>
	  </div>
	
	  <div class="summary">
		<div class="sum-item clickable" id="sumTotal" role="button" tabindex="0" onclick="filterBySev(null)" onkeydown="if(event.key==='Enter'||event.key===' ') { event.preventDefault(); filterBySev(null); }" aria-label="Show all severities">
		  <div class="label">Total</div>
		  <div class="count" id="countTotal">0</div>
		</div>
		<div class="sum-item clickable" id="sumFail" role="button" tabindex="0" onclick="filterBySev('FAIL')" onkeydown="if(event.key==='Enter'||event.key===' ') { event.preventDefault(); filterBySev('FAIL'); }" aria-label="Show only FAIL">
		  <div class="label">FAIL</div>
		  <div class="count" id="countFail">0</div>
		  <div class="progress fail"><span id="barFail" style="width:0%"></span></div>
		</div>
		<div class="sum-item clickable" id="sumWarn" role="button" tabindex="0" onclick="filterBySev('WARN')" onkeydown="if(event.key==='Enter'||event.key===' ') { event.preventDefault(); filterBySev('WARN'); }" aria-label="Show only WARN">
		  <div class="label">WARN</div>
		  <div class="count" id="countWarn">0</div>
		  <div class="progress warn"><span id="barWarn" style="width:0%"></span></div>
		</div>
		<div class="sum-item clickable" id="sumErr" role="button" tabindex="0" onclick="filterBySev('ERR')" onkeydown="if(event.key==='Enter'||event.key===' ') { event.preventDefault(); filterBySev('ERR'); }" aria-label="Show only ERR">
		  <div class="label">ERR</div>
		  <div class="count" id="countErr">0</div>
		  <div class="progress err"><span id="barErr" style="width:0%"></span></div>
		</div>
		<div class="sum-item clickable" id="sumInfo" role="button" tabindex="0" onclick="filterBySev('INFO')" onkeydown="if(event.key==='Enter'||event.key===' ') { event.preventDefault(); filterBySev('INFO'); }" aria-label="Show only INFO">
		  <div class="label">INFO</div>
		  <div class="count" id="countInfo">0</div>
		  <div class="progress info"><span id="barInfo" style="width:0%"></span></div>
		</div>
	  </div>
	
	  <div class="insights">
		<div class="sum-item">
		  <div class="label">Policy Gates</div>
		  <div class="count" id="policyStatus">-</div>
		  <div class="meta" id="policyMeta"></div>
		</div>
		<div class="sum-item">
		  <div class="label">Changed checks</div>
		  <div class="count" id="changedChecksCount">0</div>
		  <div class="meta">Rows with diff markers (+new FAIL, resolved FAIL, or severity change).</div>
		</div>
		<div class="sum-item">
		  <div class="label">Flaky checks</div>
		  <div class="count" id="flakyChecksCount">0</div>
		  <div class="meta">Rows flagged as severity-oscillating in flaky analysis.</div>
		</div>
	  </div>

	  <div class="card" style="margin-bottom: 12px;">
		<div class="label">Cluster health (score + trend)</div>
		<div class="meta">Sparkline is built from recent run-history snapshots when available.</div>
		<div id="healthWidget" style="margin-top:8px;"></div>
	  </div>

	  <div class="table-info" id="tableInfo" aria-live="polite">Showing 0 rows</div>
	  <div id="pagination" class="pagination" style="display:none;"></div>
	  <div class="card">
		<div class="scroll">
		  <table id="main-table" aria-label="NCC aggregated findings table">
			<thead>
			  <tr>
			  	<th class="col-cname th-sort" data-sort="clusterName" onclick="sortBy('clusterName')">Cluster Name <span class="sort-arrow" id="arrow-clusterName"></span></th>
				<th class="col-cluster th-sort" data-sort="cluster" onclick="sortBy('cluster')">Cluster <span class="sort-arrow" id="arrow-cluster"></span></th>
				<th class="col-sev th-sort" data-sort="severity" onclick="sortBy('severity')">Severity <span class="sort-arrow" id="arrow-severity"></span></th>
				<th class="col-title th-sort" data-sort="check" onclick="sortBy('check')">NCC Alert Title <span class="sort-arrow" id="arrow-check"></span></th>
				<th class="col-kb">KB</th>
				<th class="col-detail">Detail</th>
				<th class="col-actions">Actions</th>
			  </tr>
			</thead>
			<tbody id="tbody"></tbody>
		  </table>
		  <div id="emptyState" class="empty-state" style="display:none;">
		    <p>No rows match your filters.</p>
		    <button type="button" onclick="clearFilters()">Clear filters</button>
		  </div>
		</div>
	  </div>
	
     <footer class="report-footer">
    <strong>Keyboard:</strong> / focus search · ↑/↓ move row · Esc clear search or close modal. <span id="footerClusterCount"></span><br>
    <strong>Search tokens:</strong> <code>sev:FAIL</code> <code>cluster:10.1.1.5</code> <code>changed:true</code> <code>flaky:true</code>
    <div class="report-meta-line" id="reportMetaFooter" aria-live="polite"></div>
</footer>
	</div>
	</body>
	</html>`

	// Build data for template with embedded JSON
	type tmplRow struct {
		Cluster        string `json:"cluster"`
		Severity       string `json:"severity"`
		Check          string `json:"check"`
		Detail         string `json:"detail"`
		ClusterName    string `json:"clusterName"`
		ClusterVersion string `json:"clusterVersion"`
		NCCVersion     string `json:"nccVersion"`
	}

	aggRows := make([]tmplRow, 0, len(rows))
	for _, r := range rows {
		aggRows = append(aggRows, tmplRow{
			Cluster:        r.Cluster,
			Severity:       r.Severity,
			Check:          r.Check,
			Detail:         r.Detail,
			ClusterName:    r.ClusterName,
			ClusterVersion: r.ClusterVersion,
			NCCVersion:     r.NCCVersion,
		})
	}

	const maxEmbeddedAggRows = 20000
	aggDataURL := ""
	jsonBytes, err := json.Marshal(aggRows)
	if err != nil {
		return fmt.Errorf("marshal agg json: %w", err)
	}
	if len(aggRows) > maxEmbeddedAggRows {
		sidecarPath := filepath.Join(outDir, "aggregated-data.json")
		if err := fs.WriteFile(sidecarPath, jsonBytes, 0644); err != nil {
			return fmt.Errorf("write aggregated data sidecar: %w", err)
		}
		aggDataURL = filepath.Base(sidecarPath)
		jsonBytes = []byte("[]")
	}

	clusterLinksJSON, _ := json.Marshal(perCluster)
	if clusterLinksJSON == nil {
		clusterLinksJSON = []byte("[]")
	}

	type diffFlags struct {
		NewFail         bool `json:"new_fail"`
		ResolvedFail    bool `json:"resolved_fail"`
		SeverityChanged bool `json:"severity_changed"`
	}
	rowKey := func(cluster, check string) string {
		return strings.ToLower(strings.TrimSpace(cluster)) + "||" + strings.ToLower(strings.TrimSpace(check))
	}
	diffFlagMap := map[string]diffFlags{}
	diffPath := filepath.Join(outDir, "drilldown-diff.json")
	if b, err := os.ReadFile(diffPath); err == nil {
		var d DrillDownDiffJSON
		if err := json.Unmarshal(b, &d); err == nil {
			for _, c := range d.Clusters {
				for _, name := range c.NewFailures {
					k := rowKey(c.Address, name)
					v := diffFlagMap[k]
					v.NewFail = true
					diffFlagMap[k] = v
				}
				for _, name := range c.ResolvedFailures {
					k := rowKey(c.Address, name)
					v := diffFlagMap[k]
					v.ResolvedFail = true
					diffFlagMap[k] = v
				}
				for _, ch := range c.SeverityChanges {
					k := rowKey(c.Address, ch.CheckName)
					v := diffFlagMap[k]
					v.SeverityChanged = true
					diffFlagMap[k] = v
				}
			}
		}
	}
	diffFlagsJSON, _ := json.Marshal(diffFlagMap)
	if diffFlagsJSON == nil {
		diffFlagsJSON = []byte("{}")
	}

	flakyKeys := map[string]bool{}
	flakyPath := filepath.Join(outDir, "flaky-checks.json")
	if b, err := os.ReadFile(flakyPath); err == nil {
		var f FlakyChecksJSON
		if err := json.Unmarshal(b, &f); err == nil {
			for _, c := range f.Checks {
				flakyKeys[rowKey(c.Cluster, c.CheckName)] = true
			}
		}
	}
	flakyKeysJSON, _ := json.Marshal(flakyKeys)
	if flakyKeysJSON == nil {
		flakyKeysJSON = []byte("{}")
	}

	policyViolations := []string{}
	policyPath := filepath.Join(outDir, "policy-gates.txt")
	if b, err := os.ReadFile(policyPath); err == nil {
		for _, ln := range strings.Split(string(b), "\n") {
			ln = strings.TrimSpace(ln)
			if ln != "" {
				policyViolations = append(policyViolations, ln)
			}
		}
	}
	policyViolationsJSON, _ := json.Marshal(policyViolations)
	if policyViolationsJSON == nil {
		policyViolationsJSON = []byte("[]")
	}

	var runSummary RunSummaryJSON
	if b, err := os.ReadFile(filepath.Join(outDir, "run-summary.json")); err == nil {
		_ = json.Unmarshal(b, &runSummary)
	}
	runSummaryJSON, _ := json.Marshal(runSummary)
	if runSummaryJSON == nil {
		runSummaryJSON = []byte("{}")
	}

	historyDir := strings.TrimSpace(cfg.RunHistoryDir)
	if historyDir == "" {
		historyDir = filepath.Join(outDir, "runs")
	}
	healthTrends := map[string][]int{}
	if dirs, err := listHistoryRunDirs(historyDir); err == nil && len(dirs) > 0 {
		limit := 20
		if len(dirs) > limit {
			dirs = dirs[:limit]
		}
		// oldest -> newest
		for i, j := 0, len(dirs)-1; i < j; i, j = i+1, j-1 {
			dirs[i], dirs[j] = dirs[j], dirs[i]
		}
		for _, d := range dirs {
			p := filepath.Join(historyDir, d, "run-summary.json")
			b, err := os.ReadFile(p)
			if err != nil {
				continue
			}
			var s RunSummaryJSON
			if err := json.Unmarshal(b, &s); err != nil {
				continue
			}
			for _, c := range s.Clusters {
				key := strings.ToLower(strings.TrimSpace(c.Address))
				if key == "" {
					continue
				}
				healthTrends[key] = append(healthTrends[key], c.HealthScore)
			}
		}
	}
	for _, c := range runSummary.Clusters {
		key := strings.ToLower(strings.TrimSpace(c.Address))
		if key == "" {
			continue
		}
		healthTrends[key] = append(healthTrends[key], c.HealthScore)
	}
	healthTrendsJSON, _ := json.Marshal(healthTrends)
	if healthTrendsJSON == nil {
		healthTrendsJSON = []byte("{}")
	}

	artifactLinks := map[string]string{}
	for _, name := range []string{"run-summary.json", "slo-dashboard.json", "drilldown-diff.json", "flaky-checks.json", "checks-snapshot.json", "regression-summary.json"} {
		if _, err := os.Stat(filepath.Join(outDir, name)); err == nil {
			artifactLinks[name] = name
		}
	}
	artifactLinksJSON, _ := json.Marshal(artifactLinks)
	if artifactLinksJSON == nil {
		artifactLinksJSON = []byte("{}")
	}

	reportMeta := map[string]string{
		"version":             Version,
		"build_date":          BuildDate,
		"stream":              Stream,
		"quiet_hours":         strings.TrimSpace(cfg.QuietHours),
		"maintenance_windows": strings.Join(cfg.MaintenanceWindows, ","),
	}
	if hn, err := os.Hostname(); err == nil && strings.TrimSpace(hn) != "" {
		reportMeta["hostname"] = hn
	}
	schedulerSource := strings.TrimSpace(os.Getenv("NCC_SCHEDULER_SOURCE"))
	if schedulerSource == "" {
		schedulerSource = "manual"
	}
	reportMeta["scheduler_source"] = schedulerSource
	if aggDataURL != "" {
		reportMeta["aggregated_data_sidecar"] = aggDataURL
		reportMeta["aggregated_rows"] = strconv.Itoa(len(aggRows))
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" && s.Value != "" {
				reportMeta["git_revision"] = s.Value
				break
			}
		}
	}
	reportMetaJSON, _ := json.Marshal(reportMeta)
	if reportMetaJSON == nil {
		reportMetaJSON = []byte("{}")
	}

	data := struct {
		JSON                 template.JS
		AggDataURL           string
		ClusterLinksJSON     template.JS
		DiffFlagsJSON        template.JS
		FlakyKeysJSON        template.JS
		PolicyViolationsJSON template.JS
		RunSummaryJSON       template.JS
		HealthTrendsJSON     template.JS
		ArtifactLinksJSON    template.JS
		ReportMetaJSON       template.JS
		Clusters             []struct{ Cluster, HTML, CSV string }
		GeneratedAt          string
		ClusterName          string
		ClusterVersion       string
		NCCVersion           string
	}{
		JSON:                 template.JS(jsonBytes),
		AggDataURL:           aggDataURL,
		ClusterLinksJSON:     template.JS(clusterLinksJSON),
		DiffFlagsJSON:        template.JS(diffFlagsJSON),
		FlakyKeysJSON:        template.JS(flakyKeysJSON),
		PolicyViolationsJSON: template.JS(policyViolationsJSON),
		RunSummaryJSON:       template.JS(runSummaryJSON),
		HealthTrendsJSON:     template.JS(healthTrendsJSON),
		ArtifactLinksJSON:    template.JS(artifactLinksJSON),
		ReportMetaJSON:       template.JS(reportMetaJSON),
		Clusters:             perCluster,
		GeneratedAt:          time.Now().UTC().Format(time.RFC3339),
	}

	f, err := fs.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()
	t := template.Must(template.New("index").Parse(tmpl))
	if err := t.Execute(f, data); err != nil {
		return fmt.Errorf("template execute %s: %w", path, err)
	}
	log.Info().Str("file", abs).Int("rows", len(rows)).Int("clusters", len(perCluster)).Msg("aggregated HTML generated")
	return nil
}

// ==================== Retryable HTTP Wrappers ====================

func doWithRetry(ctx context.Context, client HTTPClient, req *http.Request, cfg Config, op string) (*http.Response, []byte, error) {
	attempts := cfg.RetryMaxAttempts
	if attempts < 1 {
		attempts = 1
	}
	breaker := cfg.RetryCircuitBreaker
	if breaker <= 0 {
		breaker = defaultRetryCircuitBreaker
	}
	var lastErr error
	var resp *http.Response
	var body []byte
	consecutiveRetryableFailures := 0

	// Snapshot original body if present
	var origBody []byte
	var hasBody bool
	if req.Body != nil {
		b, err := readBodyWithLimit(req.Body, maxRetryRequestBodyBytes, "request body")
		if err != nil {
			return nil, nil, err
		}
		origBody = b
		hasBody = true
		req.Body = io.NopCloser(bytes.NewReader(origBody))
	}

	for attempt := 1; attempt <= attempts; attempt++ {
		reqCtx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
		reqClone := req.Clone(reqCtx)
		if hasBody {
			reqClone.Body = io.NopCloser(bytes.NewReader(origBody))
		}

		resp, lastErr = client.Do(reqClone)
		if lastErr != nil {
			consecutiveRetryableFailures++
			cancel()
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			if consecutiveRetryableFailures >= breaker {
				return nil, nil, fmt.Errorf("%s retry circuit breaker opened after %d consecutive transport failures: %w", op, consecutiveRetryableFailures, lastErr)
			}
			if attempt < attempts {
				back := jitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
				log.Warn().Str("op", op).Int("attempt", attempt).Err(lastErr).Dur("backoff", back).Msg("transport error, retrying")
				select {
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				case <-time.After(back):
				}
				continue
			}
			return nil, nil, lastErr
		}

		func() {
			defer cancel()
			var err error
			body, err = readBodyWithLimit(resp.Body, maxRetryResponseBodyBytes, "response body")
			if err != nil {
				lastErr = err
			} else {
				lastErr = nil
			}
		}()
		if lastErr != nil {
			consecutiveRetryableFailures++
			if consecutiveRetryableFailures >= breaker {
				return resp, nil, fmt.Errorf("%s retry circuit breaker opened after %d consecutive body read failures: %w", op, consecutiveRetryableFailures, lastErr)
			}
			if attempt < attempts {
				back := jitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
				log.Warn().Str("op", op).Int("attempt", attempt).Err(lastErr).Dur("backoff", back).Msg("read body failed, retrying")
				select {
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				case <-time.After(back):
				}
				continue
			}
			return resp, nil, lastErr
		}

		status := resp.StatusCode
		if status >= 200 && status < 300 {
			consecutiveRetryableFailures = 0
			noteHTTPStatusForAdaptiveParallelism(status, cfg)
			log.Debug().Str("op", op).Int("status", status).Msg("request succeeded")
			return resp, body, nil
		}
		noteHTTPStatusForAdaptiveParallelism(status, cfg)

		retryable := isRetryableStatus(status)
		var back time.Duration
		if status == 429 {
			logRateLimitHeaders(op, resp)
			if ra, ok := retryAfterDelay(resp); ok {
				back = capRateLimitWait(ra)
				if ra > maxRateLimitBackoff {
					log.Warn().Str("op", op).Dur("requested", ra).Dur("capped", back).Msg("rate limit wait capped")
				}
			}
		}
		if back == 0 {
			back = jitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
		}

		if retryable && attempt < attempts {
			consecutiveRetryableFailures++
			if consecutiveRetryableFailures >= breaker {
				return resp, body, fmt.Errorf("%s retry circuit breaker opened after %d consecutive retryable failures (last HTTP %d)", op, consecutiveRetryableFailures, status)
			}
			log.Warn().Str("op", op).Int("attempt", attempt).Int("status", status).Dur("backoff", back).Msg("retryable status, retrying")
			select {
			case <-ctx.Done():
				return resp, body, ctx.Err()
			case <-time.After(back):
			}
			continue
		}

		log.Error().Str("op", op).Int("status", status).Int("attempts", attempt).Msg("request failed, not retrying")
		return resp, body, fmt.Errorf("%s HTTP %d", op, status)
	}

	if lastErr != nil {
		return nil, nil, lastErr
	}
	return resp, body, fmt.Errorf("%s exhausted retries", op)
}

// ==================== NCC Client ====================

type NCCClient struct {
	baseURL    string
	cluster    string // host:port or IP for building v4 API URL
	user       string
	pass       string
	http       HTTPClient
	cfg        Config
	apiVersion string // "v1" or "v4"
	// taskExtID is the full Nutanix extId from start-checks v4 (e.g. "base:uuid"); used for GET /api/prism/{ver}/config/tasks/{extId}.
	taskExtID string
}

func NewNCCClient(cluster, user, pass string, httpc HTTPClient, cfg Config) *NCCClient {
	apiVer := strings.ToLower(strings.TrimSpace(cfg.NCCAPIVersion))
	if apiVer == "" {
		apiVer = "v4"
	}
	return &NCCClient{
		baseURL:    fmt.Sprintf("https://%s:9440/PrismGateway/services/rest", cluster),
		cluster:    cluster,
		user:       user,
		pass:       pass,
		http:       httpc,
		cfg:        cfg,
		apiVersion: apiVer,
	}
}

// generateRequestID returns a UUID v4-style string for NTNX-Request-Id header.
func generateRequestID() (string, error) {
	var b [16]byte
	if _, err := crand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// getClusterUUID fetches the cluster UUID from Prism (v1 cluster endpoint or v4 clustermgmt list).
func (c *NCCClient) getClusterUUID(ctx context.Context) (string, error) {
	if c.apiVersion == "v4" {
		uuid, err := c.getClusterUUIDV4(ctx)
		if err != nil && strings.Contains(err.Error(), "404") {
			log.Info().Str("cluster", c.cluster).Msg("v4 cluster list not available (404), using v1 cluster for uuid")
			return c.getClusterUUIDV1(ctx)
		}
		return uuid, err
	}
	return c.getClusterUUIDV1(ctx)
}

func (c *NCCClient) getClusterUUIDV1(ctx context.Context) (string, error) {
	url := c.baseURL + "/v1/cluster"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(c.user, c.pass)
	resp, body, err := doWithRetry(ctx, c.http, req, c.cfg, "get cluster uuid")
	if err != nil {
		log.Error().Err(err).Str("url", url).Msg("get cluster uuid failed")
		return "", fmt.Errorf("get cluster uuid: %w", err)
	}
	_ = resp
	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return "", fmt.Errorf("parse cluster response: %w", err)
	}
	uuid, _ := data["uuid"].(string)
	if uuid == "" {
		if id, ok := data["id"].(string); ok && id != "" {
			uuid = id
		}
	}
	if uuid == "" {
		return "", errors.New("cluster response missing uuid")
	}
	return uuid, nil
}

func (c *NCCClient) getClusterUUIDV4(ctx context.Context) (string, error) {
	extID, _, err := c.resolveClusterV4ForNCC(ctx)
	return extID, err
}

// resolveClusterV4ForNCC finds the clustermgmt entity matching c.cluster (IP, name, extId, or CVM IP),
// returns its extId and CVM IPv4s for run-system-defined-checks. Prism Central often registers multiple
// clusters (e.g. AOS + PC); using only data[0] paired with nodeIps={c.cluster} caused NCC-40023 when
// the first cluster was not the one the user addressed.
func (c *NCCClient) resolveClusterV4ForNCC(ctx context.Context) (clusterExtID string, nodeIPv4s []string, err error) {
	ref := strings.TrimSpace(c.cluster)
	if ref == "" {
		return "", nil, errors.New("cluster address is empty")
	}
	ver := nutanixV4PathSegment(c.cfg.NutanixV4APIVersion)
	base := fmt.Sprintf("https://%s:9440/api/clustermgmt/%s/config/clusters", c.cluster, ver)
	const pageSize = 100
	var scanned int

	for page := 0; page < 1000; page++ {
		u, err := url.Parse(base)
		if err != nil {
			return "", nil, fmt.Errorf("parse URL: %w", err)
		}
		q := url.Values{}
		q.Set("$limit", strconv.Itoa(pageSize))
		q.Set("$page", strconv.Itoa(page))
		u.RawQuery = q.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return "", nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.SetBasicAuth(c.user, c.pass)
		resp, body, err := doWithRetry(ctx, c.http, req, c.cfg, "get cluster uuid v4")
		if err != nil {
			log.Error().Err(err).Str("url", u.String()).Msg("get cluster uuid v4 failed")
			return "", nil, fmt.Errorf("get cluster uuid v4: %w", err)
		}
		_ = resp

		var payload struct {
			Metadata struct {
				TotalAvailableResults int `json:"totalAvailableResults"`
			} `json:"metadata"`
			Data []map[string]interface{} `json:"data"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			return "", nil, fmt.Errorf("parse cluster list response: %w", err)
		}
		if len(payload.Data) == 0 {
			break
		}
		for _, entity := range payload.Data {
			scanned++
			if entity == nil {
				continue
			}
			if !clusterEntityMatchesUserRef(ref, entity) {
				continue
			}
			extID := extractClusterExtIDV4(entity)
			if extID == "" {
				return "", nil, errors.New("matched cluster entity missing extId")
			}
			nodeIPs := extractCVMIPv4sFromClusterEntity(entity)
			if len(nodeIPs) == 0 {
				if a := extractClusterAddressV4(entity); a != "" {
					nodeIPs = []string{a}
				}
			}
			return extID, dedupeStringsKeepOrder(nodeIPs), nil
		}
		total := payload.Metadata.TotalAvailableResults
		if total > 0 && scanned >= total {
			break
		}
	}

	return "", nil, fmt.Errorf("no cluster registered in Prism matches %q (use an IP, name, or extId from discover-clusters)", ref)
}

func (c *NCCClient) StartChecks(ctx context.Context) (string, []byte, error) {
	if c.apiVersion == "v4" {
		taskID, body, err := c.startChecksV4(ctx)
		if err != nil && strings.Contains(err.Error(), "404") {
			log.Info().Str("cluster", c.cluster).Msg("v4 endpoint not available (404), falling back to v1")
			return c.startChecksV1(ctx)
		}
		if err != nil {
			return "", body, err
		}
		return taskID, body, nil
	}
	return c.startChecksV1(ctx)
}

func (c *NCCClient) startChecksV1(ctx context.Context) (string, []byte, error) {
	url := c.baseURL + "/v1/ncc/checks"
	payload := []byte(`{"sendEmail":false}`)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payload))
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(c.user, c.pass)

	resp, body, err := doWithRetry(ctx, c.http, req, c.cfg, "start checks")
	if err != nil {
		log.Error().Err(err).Str("url", url).Str("method", "POST").Msg("http do error")
		return "", body, fmt.Errorf("start checks: %w", err)
	}
	_ = resp
	log.Debug().Str("url", url).RawJSON("body", body).Msg("start checks response")

	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return "", body, err
	}
	uuid, _ := data["taskUuid"].(string)
	if uuid == "" {
		if alt, ok := data["task_uuid"].(string); ok && alt != "" {
			uuid = alt
		}
	}
	if uuid == "" {
		return "", body, errors.New("missing taskUuid in response")
	}
	return uuid, body, nil
}

// v4RunChecksRequest is the body for monitoring v4 run-system-defined-checks.
type v4RunChecksRequest struct {
	ShouldAnonymize                        bool       `json:"shouldAnonymize"`
	ShouldSendReportToConfiguredRecipients bool       `json:"shouldSendReportToConfiguredRecipients"`
	AdditionalRecipients                   []string   `json:"additionalRecipients"`
	NodeIps                                []v4NodeIP `json:"nodeIps"`
	ShouldRunAllChecks                     bool       `json:"shouldRunAllChecks"`
}

type v4NodeIP struct {
	Value        string `json:"value"`
	PrefixLength int    `json:"prefixLength"`
}

func (c *NCCClient) startChecksV4(ctx context.Context) (string, []byte, error) {
	clusterUUID, nodeIPs, err := c.resolveClusterV4ForNCC(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("get cluster uuid for v4: %w", err)
	}
	if len(nodeIPs) == 0 {
		nodeIPs = []string{c.cluster}
	}
	var nodeIpsReq []v4NodeIP
	for _, ip := range nodeIPs {
		nodeIpsReq = append(nodeIpsReq, v4NodeIP{Value: ip, PrefixLength: 32})
	}
	requestID, err := generateRequestID()
	if err != nil {
		return "", nil, fmt.Errorf("generate request id: %w", err)
	}
	ver := nutanixV4PathSegment(c.cfg.NutanixV4APIVersion)
	v4Base := fmt.Sprintf("https://%s:9440/api/monitoring/%s/serviceability/clusters/%s/$actions/run-system-defined-checks", c.cluster, ver, clusterUUID)
	// Prism Central v4 requires additionalRecipients to have at least one element matching email regex (user@domain.tld); use placeholder when not sending report
	additionalRecipients := []string{"noreply@example.com"}
	body := v4RunChecksRequest{
		ShouldAnonymize:                        false,
		ShouldSendReportToConfiguredRecipients: false,
		AdditionalRecipients:                   additionalRecipients,
		NodeIps:                                nodeIpsReq,
		ShouldRunAllChecks:                     true,
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return "", nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", v4Base, bytes.NewReader(payload))
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("NTNX-Request-Id", requestID)
	req.SetBasicAuth(c.user, c.pass)

	resp, respBody, err := doWithRetry(ctx, c.http, req, c.cfg, "start checks v4")
	if err != nil {
		log.Error().Err(err).Str("url", v4Base).Str("method", "POST").Msg("http do error")
		return "", respBody, fmt.Errorf("start checks v4: %w", err)
	}
	_ = resp
	log.Debug().Str("url", v4Base).RawJSON("body", respBody).Msg("start checks v4 response")

	var data map[string]interface{}
	if err := json.Unmarshal(respBody, &data); err != nil {
		return "", respBody, err
	}
	c.taskExtID = ""
	var taskID string
	if dataObj, _ := data["data"].(map[string]interface{}); dataObj != nil {
		if extID, _ := dataObj["extId"].(string); extID != "" {
			c.taskExtID = extID
			if idx := strings.LastIndex(extID, ":"); idx >= 0 && idx+1 < len(extID) {
				taskID = extID[idx+1:]
			} else {
				taskID = extID
			}
		}
	}
	if taskID == "" {
		taskID, _ = data["taskUuid"].(string)
	}
	if taskID == "" {
		if alt, ok := data["task_uuid"].(string); ok && alt != "" {
			taskID = alt
		}
	}
	if taskID == "" {
		if extID, ok := data["executionId"].(string); ok && extID != "" {
			taskID = extID
		}
	}
	if taskID == "" {
		return "", respBody, errors.New("v4 response missing taskUuid or executionId")
	}
	return taskID, respBody, nil
}

// mapPrismTaskJSONToTaskStatus maps Prism v4 GET .../api/prism/{ver}/config/tasks/{extId} JSON to legacy TaskStatus for the poll loop.
func mapPrismTaskJSONToTaskStatus(raw []byte) (TaskStatus, error) {
	var wrap struct {
		Data struct {
			Status             string `json:"status"`
			ProgressPercentage int    `json:"progressPercentage"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return TaskStatus{}, err
	}
	st := strings.ToUpper(strings.TrimSpace(wrap.Data.Status))
	pct := wrap.Data.ProgressPercentage
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	var ps string
	switch st {
	case "SUCCEEDED", "SUCCESS", "COMPLETED", "DONE", "COMPLETE", "SUCCEED":
		ps = "Succeeded"
		if pct < 100 {
			pct = 100
		}
	case "FAILED", "FAILURE", "ABORTED", "CANCELED", "CANCELLED", "ERROR":
		ps = "Failed"
	default:
		ps = "Running"
	}
	return TaskStatus{PercentageComplete: pct, ProgressStatus: ps}, nil
}

func (c *NCCClient) getTaskPrismV4(ctx context.Context, extID string) (TaskStatus, []byte, error) {
	ver := nutanixV4PathSegment(c.cfg.NutanixV4APIVersion)
	u := fmt.Sprintf("https://%s:9440/api/prism/%s/config/tasks/%s", c.cluster, ver, url.PathEscape(extID))
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return TaskStatus{}, nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(c.user, c.pass)

	resp, body, err := doWithRetry(ctx, c.http, req, c.cfg, "get task prism v4")
	if err != nil {
		log.Error().Err(err).Str("url", u).Msg("http do error")
		return TaskStatus{}, body, fmt.Errorf("get task prism v4: %w", err)
	}
	_ = resp
	log.Debug().Str("url", u).RawJSON("body", body).Msg("get task prism v4 response")

	st, err := mapPrismTaskJSONToTaskStatus(body)
	if err != nil {
		return TaskStatus{}, body, fmt.Errorf("parse prism task response: %w", err)
	}
	return st, body, nil
}

func (c *NCCClient) getTaskV2Legacy(ctx context.Context, taskID string) (TaskStatus, []byte, error) {
	url := c.baseURL + "/v2.0/tasks/" + taskID
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return TaskStatus{}, nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(c.user, c.pass)

	resp, body, err := doWithRetry(ctx, c.http, req, c.cfg, "get task")
	if err != nil {
		log.Error().Err(err).Str("url", url).Msg("http do error")
		return TaskStatus{}, body, fmt.Errorf("get task: %w", err)
	}
	_ = resp
	log.Debug().Str("url", url).RawJSON("body", body).Msg("get task response")

	var status TaskStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return TaskStatus{}, body, err
	}
	return status, body, nil
}

func (c *NCCClient) GetTask(ctx context.Context, taskID string) (TaskStatus, []byte, error) {
	if c.apiVersion == "v4" && c.taskExtID != "" {
		st, body, err := c.getTaskPrismV4(ctx, c.taskExtID)
		if err == nil {
			return st, body, nil
		}
		if strings.Contains(err.Error(), "404") {
			log.Info().Str("cluster", c.cluster).Msg("prism v4 task API unavailable (404), falling back to Prism Gateway v2.0/tasks")
			return c.getTaskV2Legacy(ctx, taskID)
		}
		return TaskStatus{}, body, err
	}
	return c.getTaskV2Legacy(ctx, taskID)
}

func (c *NCCClient) GetRunSummary(ctx context.Context, taskID string) (NCCSummary, []byte, error) {
	url := c.baseURL + "/v1/ncc/" + taskID
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return NCCSummary{}, nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(c.user, c.pass)

	resp, body, err := doWithRetry(ctx, c.http, req, c.cfg, "get summary")
	if err != nil {
		log.Error().Err(err).Str("url", url).Msg("http do error")
		return NCCSummary{}, body, fmt.Errorf("get summary: %w", err)
	}
	_ = resp
	log.Debug().Str("url", url).RawJSON("body", body).Msg("get summary response")

	var summary NCCSummary
	if err := json.Unmarshal(body, &summary); err != nil {
		return NCCSummary{}, body, err
	}
	return summary, body, nil
}

// ==================== Orchestration ====================

func sanitizeSummary(s string) string {
	return strings.ReplaceAll(s, "\\n", "\n")
}

func writeSummary(fs FS, folder, cluster, summary string) (string, error) {
	if err := fs.MkdirAll(folder, 0755); err != nil {
		return "", err
	}
	outPath := filepath.Join(folder, fmt.Sprintf("%s.log", cluster))
	log.Debug().Str("path", outPath).Int("bytes", len(summary)).Msg("writing summary")
	if err := fs.WriteFile(outPath, []byte(sanitizeSummary(summary)), 0644); err != nil {
		return "", err
	}
	return outPath, nil
}

func filterBlocksToFile(fs FS, inputPath, outputPath string) ([]ParsedBlock, error) {
	data, err := fs.ReadFile(inputPath)
	if err != nil {
		return nil, err
	}
	log.Debug().Str("path", inputPath).Int("bytes", len(data)).Msg("read raw log")
	blocks, err := ParseSummary(string(data))
	if err != nil {
		return nil, err
	}
	if err := validateParsedAlertsAgainstPluginResults(string(data), blocks); err != nil {
		log.Warn().Err(err).Str("path", inputPath).Msg("parser validation: mismatch between parsed alerts and NCC plugin results")
	}
	if err := fs.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return nil, err
	}
	var b strings.Builder
	for _, pb := range blocks {
		b.WriteString(pb.CheckName)
		b.WriteString("\n")
		b.WriteString(pb.DetailRaw)
		b.WriteString("\n\n---------------------------------------\n")
	}
	if err := fs.WriteFile(outputPath, []byte(b.String()), 0644); err != nil {
		return nil, err
	}
	log.Debug().Str("path", outputPath).Int("bytes", len(b.String())).Msg("wrote filtered")
	return blocks, nil
}

func runClusterWithBars(
	ctx context.Context,
	cfg Config,
	fs FS,
	httpc HTTPClient,
	cluster string,
	username string,
	password string,
	onPct func(int),
	setPhase func(string),
) (ClusterRunOutput, error) {
	l := log.With().Str("cluster", cluster).Logger()
	client := NewNCCClient(cluster, username, password, httpc, cfg)
	var clusterStart = time.Now()
	setPhase("starting")
	l.Info().Msg("starting NCC checks")
	taskID, body, err := client.StartChecks(ctx)
	if err != nil {
		l.Error().Err(err).RawJSON("response_body", body).Msg("start checks failed")
		return ClusterRunOutput{}, fmt.Errorf("start checks failed: %w", err)
	}
	l.Info().Str("taskID", taskID).Msg("ncc task started")
	onPct(1)

	last := 1
	setPhase("polling")
	for {
		select {
		case <-ctx.Done():
			l.Warn().Err(ctx.Err()).Msg("context cancelled during polling, stopping gracefully")
			return ClusterRunOutput{}, fmt.Errorf("operation cancelled: %w", ctx.Err())
		case <-time.After(cfg.PollInterval + time.Duration(rand.Int63n(int64(cfg.PollJitter)))):
			if dl, ok := ctx.Deadline(); ok {
				rem := time.Until(dl)
				if rem < 10*time.Second {
					l.Warn().Dur("remaining", rem).Msg("cluster deadline near")
				}
			}
			status, body, err := client.GetTask(ctx, taskID)
			if err != nil {
				l.Error().Err(err).RawJSON("response_body", body).Msg("poll failed")
				return ClusterRunOutput{}, fmt.Errorf("poll failed: %w", err)
			}
			pct := status.PercentageComplete
			if pct < last {
				pct = last
			}
			if pct > 100 {
				pct = 100
			}
			onPct(pct)
			l.Debug().Int("pct", pct).Str("progress", status.ProgressStatus).Msg("task status")
			last = pct

			if status.ProgressStatus == "Failed" {
				return ClusterRunOutput{}, fmt.Errorf("ncc task failed")
			}
			if pct >= 100 {
				goto SUMMARY
			}
		}
	}

SUMMARY:
	setPhase("summary")
	summary, body, err := client.GetRunSummary(ctx, taskID)
	if err != nil {
		l.Error().Err(err).RawJSON("response_body", body).Msg("get summary failed")
		return ClusterRunOutput{}, fmt.Errorf("get summary failed: %w", err)
	}

	setPhase("writing")
	logPath, err := writeSummary(fs, cfg.OutputDirLogs, cluster, summary.RunSummary)
	if err != nil {
		l.Error().Err(err).Msg("write summary failed")
		return ClusterRunOutput{}, err
	}
	l.Info().Str("logPath", logPath).Msg("summary written")

	// Parse blocks and write filtered file in one operation to avoid duplicate parsing
	filteredPath := filepath.Join(cfg.OutputDirFiltered, fmt.Sprintf("%s.log", cluster))
	blocks, err := filterBlocksToFile(fs, logPath, filteredPath)
	if err != nil {
		l.Error().Err(err).Msg("filter blocks failed")
		return ClusterRunOutput{}, err
	}
	l.Info().Str("filteredPath", filteredPath).Msg("filtered written")

	// Apply severity filtering if configured
	if len(cfg.SeverityFilter) > 0 {
		originalCount := len(blocks)
		blocks = filterBlocksBySeverity(blocks, cfg.SeverityFilter)
		l.Info().Int("original", originalCount).Int("filtered", len(blocks)).Strs("severities", cfg.SeverityFilter).Msg("applied severity filter")
	}
	excludedByTitle := make([]ExcludedAlert, 0)
	if len(cfg.ExcludeAlertTitles) > 0 {
		originalCount := len(blocks)
		filteredBlocks, excludedAlerts, err := filterBlocksByTitle(blocks, cfg.ExcludeAlertTitles, cfg.ExcludeAlertMatchMode)
		if err != nil {
			return ClusterRunOutput{}, err
		}
		blocks = filteredBlocks
		excludedByTitle = excludedAlerts
		l.Info().
			Int("original", originalCount).
			Int("filtered", len(blocks)).
			Int("excluded", len(excludedByTitle)).
			Str("mode", cfg.ExcludeAlertMatchMode).
			Strs("titles", cfg.ExcludeAlertTitles).
			Msg("applied alert title exclusion filter")
	}

	counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
	for _, b := range blocks {
		sev := b.Severity
		if sev == "" {
			sev = "INFO"
		}
		counts[sev]++
	}
	// Write Prometheus file and output formats (HTML/CSV/JSON) before notifications so we can attach HTML
	if cfg.PromEnabled {
		if err := writePrometheusFile(fs, cfg.PromDir, cluster, blocks); err != nil {
			l.Error().Err(err).Msg("write Prometheus .prom failed")
		} else {
			log.Info().Str("cluster", cluster).Str("prom_dir", cfg.PromDir).Msg("Prometheus .prom written")
		}
	} else {
		log.Debug().Str("cluster", cluster).Msg("Prometheus metrics export disabled")
	}

	var htmlPathForNotify string
	base := filteredPath
	rawPath := filepath.Join(cfg.OutputDirLogs, fmt.Sprintf("%s.log", cluster))
	var meta HTMLMeta
	metaLoaded := false
	loadMeta := func() HTMLMeta {
		if !metaLoaded {
			meta, _ = parseNCCHeader(rawPath) // best effort
			metaLoaded = true
		}
		return meta
	}
	for _, f := range cfg.OutputFormats {
		switch strings.ToLower(strings.TrimSpace(f)) {
		case "html":
			htmlFile := base + ".html"
			rows := rowsFromBlocks(blocks)
			if err := generateHTML(fs, rows, htmlFile, loadMeta()); err != nil {
				l.Error().Err(err).Str("file", htmlFile).Msg("write HTML failed")
				return ClusterRunOutput{}, err
			}
			l.Info().Str("file", htmlFile).Msg("HTML generated")
			htmlPathForNotify = htmlFile
		case "csv":
			csvFile := base + ".csv"
			if err := generateCSV(fs, blocks, csvFile); err != nil {
				l.Error().Err(err).Str("file", csvFile).Msg("write CSV failed")
				return ClusterRunOutput{}, err
			}
			l.Info().Str("file", csvFile).Msg("CSV generated")
		case "json":
			jsonFile := base + ".json"
			if err := generateJSON(fs, blocks, jsonFile, loadMeta()); err != nil {
				l.Error().Err(err).Str("file", jsonFile).Msg("write JSON failed")
				return ClusterRunOutput{}, err
			}
			l.Info().Str("file", jsonFile).Msg("JSON generated")
		case "markdown":
			mdFile := base + ".md"
			if err := generateMarkdown(fs, blocks, mdFile, loadMeta()); err != nil {
				l.Error().Err(err).Str("file", mdFile).Msg("write Markdown failed")
				return ClusterRunOutput{}, err
			}
			l.Info().Str("file", mdFile).Msg("Markdown generated")
		case "sarif":
			sarifFile := base + ".sarif"
			if err := generateSARIF(fs, blocks, sarifFile); err != nil {
				l.Error().Err(err).Str("file", sarifFile).Msg("write SARIF failed")
				return ClusterRunOutput{}, err
			}
			l.Info().Str("file", sarifFile).Msg("SARIF generated")
		default:
			l.Warn().Str("format", f).Msg("unknown output format")
		}
	}

	if len(blocks) == 0 {
		l.Warn().Str("path", filteredPath).Msg("no blocks parsed from summary")
	}

	// Build notification summary with brief overview; optionally include HTML in webhook
	overview := fmt.Sprintf("NCC run completed for cluster %s. FAIL: %d, WARN: %d, ERR: %d, INFO: %d. Total: %d checks.",
		cluster, counts["FAIL"], counts["WARN"], counts["ERR"], counts["INFO"], len(blocks))
	summaryNotify := NotificationSummary{
		Cluster:     cluster,
		StartedAt:   clusterStart,
		FinishedAt:  time.Now(),
		FailCount:   counts["FAIL"],
		WarnCount:   counts["WARN"],
		ErrCount:    counts["ERR"],
		InfoCount:   counts["INFO"],
		TotalChecks: len(blocks),
		OutputFiles: []string{filteredPath},
		Overview:    overview,
	}
	if cfg.WebhookIncludeHTML && htmlPathForNotify != "" {
		if b, err := os.ReadFile(htmlPathForNotify); err == nil {
			summaryNotify.ReportHTMLBase64 = base64.StdEncoding.EncodeToString(b)
		}
	}

	subj := fmt.Sprintf("NCC %s: FAIL=%d WARN=%d", summaryNotify.Cluster,
		summaryNotify.FailCount, summaryNotify.WarnCount)
	bodyEmail := overview + "\n\n" + fmt.Sprintf("FAIL: %d | WARN: %d | Total: %d\nFiltered: %s",
		summaryNotify.FailCount, summaryNotify.WarnCount, len(blocks), filteredPath)
	subj, bodyEmail = applyEmailTemplates(cfg, subj, bodyEmail, summaryNotify, l)

	if !cfg.NotifyDigest {
		if suppressed, reason := notificationsSuppressedNow(cfg, time.Now()); suppressed {
			l.Info().Str("reason", reason).Msg("notifications suppressed by quiet-hours/maintenance-window")
			setPhase("done")
			return ClusterRunOutput{Blocks: blocks, ExcludedByTitle: excludedByTitle}, nil
		}
		if cfg.NotifyOnRegression {
			prevFail := cfg.PreviousClusterFailCount[cluster]
			if summaryNotify.FailCount <= prevFail {
				l.Info().
					Int("current_fail", summaryNotify.FailCount).
					Int("previous_fail", prevFail).
					Msg("notify-on-regression enabled: skipping non-regression cluster notification")
				setPhase("done")
				return ClusterRunOutput{Blocks: blocks, ExcludedByTitle: excludedByTitle}, nil
			}
		}
		if err := sendEmailWithRetry(cfg, subj, bodyEmail, htmlPathForNotify); err != nil {
			l.Error().Err(err).Msg("email failed")
		}
		if err := sendWebhookWithRetry(ctx, httpc, cfg, summaryNotify); err != nil {
			l.Error().Err(err).Msg("webhook failed")
		}
		if err := sendSlackWithRetry(ctx, httpc, cfg, summaryNotify); err != nil {
			l.Error().Err(err).Msg("slack notification failed")
		}
		l.Info().Int("fail", summaryNotify.FailCount).Int("warn", summaryNotify.WarnCount).Msg("notifications sent")
	}

	setPhase("done")
	if len(excludedByTitle) > 0 {
		l.Info().Int("excluded_alerts", len(excludedByTitle)).Msg("alerts excluded by title filters")
	}
	return ClusterRunOutput{Blocks: blocks, ExcludedByTitle: excludedByTitle}, nil
}

// ==================== CLI ====================

type ClusterResult struct {
	Cluster         string
	Blocks          []ParsedBlock
	ExcludedByTitle []ExcludedAlert
	Err             error
	ErrorClass      string
}

type ClusterRunOutput struct {
	Blocks          []ParsedBlock
	ExcludedByTitle []ExcludedAlert
}

type proxyDecorator struct{ text string }

func (p *proxyDecorator) Decor(ctx decor.Statistics) string { return p.text }
func (p *proxyDecorator) Sync() (chan int, bool)            { return nil, false }
func (p *proxyDecorator) GetConf() decor.WC                 { return decor.WC{} }
func (p *proxyDecorator) SetConf(wc decor.WC)               {}
func (p *proxyDecorator) SetText(s string)                  { p.text = s }

func promptPasswordIfEmpty(p string, Username string) (string, error) {
	if p != "" {
		return p, nil
	}
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return "", errors.New("password is empty and stdin is not interactive; set password in config/clusters-file or export NCC_PASSWORD")
	}
	fmt.Printf("Prism Password (%s): ", Username)
	bytePw, err := term.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		return "", fmt.Errorf("read password from terminal: %w", err)
	}
	return strings.TrimSpace(string(bytePw)), nil
}

// defaultGitHubRepo is used by update to fetch releases (format: owner/repo).
const defaultGitHubRepo = "lTSPV75BRO/Nutanix-ncc-orchestrator"

const v1ToV2MigrationDocURL = "https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/blob/main/docs/V2_BACKEND_FRONTEND_MVP.md"

// stripGoBuildGitSuffix removes a trailing -<hex> from ldflags/git injection (e.g. 1.0.0-deadbeef...).
// Does not strip semver prereleases like 1.0.0-rc1 (suffix is not all hex).
func stripGoBuildGitSuffix(s string) string {
	i := strings.LastIndex(s, "-")
	if i <= 0 {
		return s
	}
	tail := s[i+1:]
	if len(tail) < 7 || len(tail) > 64 {
		return s
	}
	for _, c := range tail {
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') {
			continue
		}
		return s
	}
	return s[:i]
}

// versionLess returns true if a is semantically less than b (e.g. "0.9.0" < "1.0.0").
// Uses semver when both strings parse; otherwise falls back to simple numeric parsing.
func versionLess(a, b string) bool {
	a = strings.TrimPrefix(strings.TrimSpace(a), "v")
	b = strings.TrimPrefix(strings.TrimSpace(b), "v")
	if va, ea := semver.NewVersion(a); ea == nil {
		if vb, eb := semver.NewVersion(b); eb == nil {
			return va.LessThan(vb)
		}
	}
	a2 := stripGoBuildGitSuffix(a)
	b2 := stripGoBuildGitSuffix(b)
	if va, ea := semver.NewVersion(a2); ea == nil {
		if vb, eb := semver.NewVersion(b2); eb == nil {
			return va.LessThan(vb)
		}
	}
	return versionLessLegacy(a2, b2)
}

func versionLessLegacy(a, b string) bool {
	parse := func(s string) (major, minor, patch int) {
		parts := strings.SplitN(s, ".", 4)
		if len(parts) >= 1 {
			major, _ = strconv.Atoi(parts[0])
		}
		if len(parts) >= 2 {
			minor, _ = strconv.Atoi(parts[1])
		}
		if len(parts) >= 3 {
			patch, _ = strconv.Atoi(strings.SplitN(parts[2], "-", 2)[0])
		}
		return major, minor, patch
	}
	ma, mia, pa := parse(a)
	mb, mib, pb := parse(b)
	if ma != mb {
		return ma < mb
	}
	if mia != mib {
		return mia < mib
	}
	return pa < pb
}

var (
	Version     string
	BuildDate   string
	GoVersion   string
	Stream      string // e.g., "prod", "dev", "beta"
	GitRevision string
)

func init() {
	// Defaults
	if Version == "" {
		Version = "2.1.1"
	}
	if BuildDate == "" {
		BuildDate = "unknown"
	}
	if Stream == "" {
		Stream = "dev"
	}

	// Optional build info enrichment
	if bi, ok := debug.ReadBuildInfo(); ok {
		if GoVersion == "" {
			GoVersion = bi.GoVersion
		}

		var gitRevision string
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				if s.Value != "" {
					gitRevision = s.Value
					GitRevision = s.Value
				}
			case "vcs.time":
				if BuildDate == "unknown" && s.Value != "" {
					BuildDate = s.Value
				}
			}
		}
		if gitRevision != "" {
			Version = Version + "-" + gitRevision
		}
	}

	if GoVersion == "" {
		GoVersion = "unknown"
	}
}

// githubRelease represents the minimal GitHub releases/latest API response.
type githubRelease struct {
	TagName    string        `json:"tag_name"`
	Assets     []githubAsset `json:"assets"`
	HTMLURL    string        `json:"html_url"`
	Draft      bool          `json:"draft"`
	Prerelease bool          `json:"prerelease"`
}

type githubAsset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
}

type updateOptions struct {
	CheckOnly          bool
	AllowMajorUpgrade  bool
	Repo               string
	BinaryURL          string
	BinarySHA256       string
	TargetVersion      string
	SkipChecksumVerify bool
	JSONOut            bool // emit a machine-readable result line for --check (consumed by the api-server)
}

// updateCheckJSONPrefix is the sentinel that precedes the machine-readable
// JSON object printed by `update --check --json`. The orchestrator mixes
// human-readable progress on stderr with this single stdout line, and callers
// (the api-server) scan combined output for this prefix to parse the result
// without depending on the wording of the human-readable lines.
const updateCheckJSONPrefix = "NCC_UPDATE_JSON "

func parseVersionMajor(raw string) (int64, error) {
	clean := strings.TrimPrefix(stripGoBuildGitSuffix(strings.TrimSpace(raw)), "v")
	if v, err := semver.NewVersion(clean); err == nil {
		return int64(v.Major()), nil
	}
	parts := strings.SplitN(clean, ".", 2)
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return 0, fmt.Errorf("invalid version %q", raw)
	}
	major, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid version %q", raw)
	}
	return major, nil
}

func normalizeGitHubRepo(repo string) (string, error) {
	repo = strings.TrimSpace(repo)
	if repo == "" {
		return "", errors.New("repo cannot be empty")
	}
	if strings.HasPrefix(repo, "http://") || strings.HasPrefix(repo, "https://") {
		u, err := url.Parse(repo)
		if err != nil {
			return "", fmt.Errorf("parse repo URL: %w", err)
		}
		if !strings.Contains(strings.ToLower(u.Host), "github.com") {
			return "", fmt.Errorf("repo URL host %q is not github.com", u.Host)
		}
		p := strings.Trim(strings.TrimSuffix(u.Path, ".git"), "/")
		parts := strings.Split(p, "/")
		if len(parts) < 2 {
			return "", fmt.Errorf("repo URL must include owner/repo path, got %q", repo)
		}
		return parts[0] + "/" + parts[1], nil
	}
	parts := strings.Split(strings.Trim(repo, "/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", fmt.Errorf("repo must be in owner/repo format, got %q", repo)
	}
	return parts[0] + "/" + parts[1], nil
}

func isArchiveAssetURL(raw string) bool {
	u := strings.ToLower(strings.TrimSpace(raw))
	return strings.HasSuffix(u, ".tar.gz") || strings.HasSuffix(u, ".zip")
}

func normalizeSHA256Hex(raw string) (string, error) {
	sum := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(raw, "sha256:")))
	if len(sum) != 64 {
		return "", fmt.Errorf("sha256 must be 64 hex characters, got %d", len(sum))
	}
	for _, r := range sum {
		if (r >= 'a' && r <= 'f') || (r >= '0' && r <= '9') {
			continue
		}
		return "", fmt.Errorf("sha256 contains invalid character %q", r)
	}
	return sum, nil
}

func printV1ToV2MigrationSteps() {
	fmt.Fprintln(os.Stderr, "To upgrade from v1.x to v2.x, review migration steps first:")
	fmt.Fprintln(os.Stderr, "1) Deploy/enable v2 services (ncc-api-server and ncc-ui-server).")
	fmt.Fprintln(os.Stderr, "2) Update automation to use API/UI endpoints where needed.")
	fmt.Fprintln(os.Stderr, "3) Validate config compatibility and run preflight checks.")
	fmt.Fprintf(os.Stderr, "Migration guide: %s\n", v1ToV2MigrationDocURL)
}

func enforceMajorUpgradePolicy(currentVer, targetVer string, allowMajorUpgrade bool) error {
	currentMajor, currentErr := parseVersionMajor(currentVer)
	targetMajor, targetErr := parseVersionMajor(targetVer)
	if currentErr != nil || targetErr != nil {
		return nil
	}
	if currentMajor < targetMajor && !allowMajorUpgrade {
		if currentMajor == 1 && targetMajor >= 2 {
			fmt.Fprintln(os.Stderr, "Major upgrade blocked: current binary is v1.x and target release is v2.x.")
			fmt.Fprintln(os.Stderr, "Use --allow-major-upgrade to proceed after migration review.")
			printV1ToV2MigrationSteps()
			return errors.New("major upgrade requires explicit opt-in")
		}
		return fmt.Errorf("major upgrade from v%d to v%d requires --allow-major-upgrade", currentMajor, targetMajor)
	}
	return nil
}

func pickLatestSemverRelease(releases []githubRelease, majorFilter int64) *githubRelease {
	var best *githubRelease
	for i := range releases {
		rel := &releases[i]
		if rel.Draft || rel.Prerelease {
			continue
		}
		tag := strings.TrimSpace(rel.TagName)
		if tag == "" {
			continue
		}
		if majorFilter > 0 {
			maj, err := parseVersionMajor(tag)
			if err != nil || maj != majorFilter {
				continue
			}
		}
		if best == nil || versionLess(best.TagName, rel.TagName) {
			best = rel
		}
	}
	return best
}

func fetchGitHubReleases(repo string, client *http.Client) ([]githubRelease, error) {
	apiURL := "https://api.github.com/repos/" + repo + "/releases?per_page=100"
	req, err := http.NewRequest(http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	if token := os.Getenv("GITHUB_TOKEN"); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch releases: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 403 && resp.Header.Get("X-RateLimit-Remaining") == "0" {
		fmt.Fprintln(os.Stderr, "GitHub API rate limited. Set GITHUB_TOKEN for higher limits or try again later.")
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("GitHub API returned %d: %s", resp.StatusCode, string(body))
	}
	var releases []githubRelease
	if err := json.NewDecoder(resp.Body).Decode(&releases); err != nil {
		return nil, fmt.Errorf("parse releases: %w", err)
	}
	return releases, nil
}

func pickAssetForCurrentPlatform(rel githubRelease) (downloadURL string, assetName string) {
	return pickAssetForPlatform(rel, runtime.GOOS, runtime.GOARCH, currentExecutableBasename())
}

// pickStackAssetForCurrentPlatform returns the v2 stack archive URL+name for
// this platform, or empty strings if no stack archive is published in the
// release.
//
// The stack archive is the canonical "update-the-whole-package" distribution
// path. It is independent of which binary (orchestrator, api-server,
// ui-server) was invoked, independent of how that binary was renamed, and
// upgrades all v2 components atomically (orchestrator + api-server +
// ui-server + frontend-dist + example_config.yaml). For releases that do not
// publish a stack archive (e.g. legacy v1.x releases), this returns empty
// strings and callers fall back to the single-binary update path via
// pickAssetForCurrentPlatform.
func pickStackAssetForCurrentPlatform(rel githubRelease) (downloadURL string, assetName string) {
	return pickStackAssetForPlatform(rel, runtime.GOOS, runtime.GOARCH)
}

// pickStackAssetForPlatform is the testable inner function of
// pickStackAssetForCurrentPlatform. Returns the first archive asset whose
// name contains "ncc-v2-stack", goos, and goarch.
func pickStackAssetForPlatform(rel githubRelease, goos, goarch string) (downloadURL string, assetName string) {
	goos = strings.ToLower(strings.TrimSpace(goos))
	goarch = strings.ToLower(strings.TrimSpace(goarch))
	for _, a := range rel.Assets {
		name := strings.ToLower(a.Name)
		if !strings.Contains(name, "ncc-v2-stack") {
			continue
		}
		if !strings.Contains(name, goos) || !strings.Contains(name, goarch) {
			continue
		}
		if !isArchiveAssetURL(a.BrowserDownloadURL) {
			continue
		}
		return a.BrowserDownloadURL, a.Name
	}
	return "", ""
}

// currentExecutableBasename returns the basename of the running executable
// with any ".exe" suffix stripped, lowercased. Empty string if it cannot be
// determined (callers fall back to the legacy first-match behavior).
func currentExecutableBasename() string {
	p, err := os.Executable()
	if err != nil || p == "" {
		return ""
	}
	return strings.ToLower(strings.TrimSuffix(filepath.Base(p), ".exe"))
}

// pickAssetForPlatform selects the most appropriate release asset for the
// given GOOS/GOARCH and the running executable's basename (e.g.
// "ncc-orchestrator"). Preference order:
//
//  1. Non-archive asset whose name starts with "<exeBase>-" AND contains both
//     goos and goarch. This guards against silent corruption when a release
//     ships multiple binaries per platform (regression that affected the
//     v1.x→v2.0.0 self-updater, see RELEASE_NOTES_v2.0.0 known-issues
//     section).
//  2. Any other non-archive asset whose name contains both goos and goarch
//     (legacy v1.x behavior, preserved for binaries renamed by the user or
//     custom forks).
//  3. Archive asset (.tar.gz / .zip) whose name contains both goos and
//     goarch, returned for inspection by the caller — the install path then
//     emits a "download and extract" hint rather than overwriting.
//
// Returns empty strings when no asset matches.
func pickAssetForPlatform(rel githubRelease, goos, goarch, exeBase string) (downloadURL string, assetName string) {
	goos = strings.ToLower(strings.TrimSpace(goos))
	goarch = strings.ToLower(strings.TrimSpace(goarch))
	exeBase = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(exeBase), ".exe"))

	var (
		prefixedURL, prefixedName string
		fallbackURL, fallbackName string
		archiveURL, archiveName   string
	)
	for _, a := range rel.Assets {
		name := strings.ToLower(a.Name)
		if !strings.Contains(name, goos) || !strings.Contains(name, goarch) {
			continue
		}
		isArchive := isArchiveAssetURL(a.BrowserDownloadURL)

		if exeBase != "" && strings.HasPrefix(name, exeBase+"-") {
			if isArchive {
				if archiveURL == "" {
					archiveURL = a.BrowserDownloadURL
					archiveName = a.Name
				}
				continue
			}
			if prefixedURL == "" {
				prefixedURL = a.BrowserDownloadURL
				prefixedName = a.Name
			}
			continue
		}

		if isArchive {
			if archiveURL == "" {
				archiveURL = a.BrowserDownloadURL
				archiveName = a.Name
			}
			continue
		}

		if fallbackURL == "" {
			fallbackURL = a.BrowserDownloadURL
			fallbackName = a.Name
		}
	}

	if prefixedURL != "" {
		return prefixedURL, prefixedName
	}
	if fallbackURL != "" {
		return fallbackURL, fallbackName
	}
	return archiveURL, archiveName
}

func downloadBinaryURL(client *http.Client, downloadURL string) ([]byte, error) {
	dlReq, err := http.NewRequest(http.MethodGet, downloadURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create download request: %w", err)
	}
	if token := os.Getenv("GITHUB_TOKEN"); token != "" && (strings.Contains(downloadURL, "github.com") || strings.Contains(downloadURL, "githubusercontent.com")) {
		dlReq.Header.Set("Authorization", "Bearer "+token)
	}
	dlResp, err := client.Do(dlReq)
	if err != nil {
		return nil, fmt.Errorf("download: %w", err)
	}
	defer dlResp.Body.Close()
	if dlResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download returned %d", dlResp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(dlResp.Body, 200*1024*1024))
	if err != nil {
		return nil, fmt.Errorf("read download: %w", err)
	}
	return body, nil
}

func installDownloadedBinary(body []byte, targetVer string) error {
	selfPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("get executable path: %w", err)
	}
	dir := filepath.Dir(selfPath)
	if runtime.GOOS == "windows" {
		newPath := selfPath + ".new.exe"
		if err := os.WriteFile(newPath, body, 0755); err != nil {
			return fmt.Errorf("write %s: %w", newPath, err)
		}
		// Windows cannot overwrite a running .exe in place. Generate a small
		// helper .cmd that waits for this process to release the lock, swaps
		// in the new binary, and self-deletes — turning the old manual
		// copy-the-file dance into a single command.
		if helperPath, herr := writeWindowsUpdateSwapHelper(selfPath, newPath); herr == nil {
			fmt.Fprintf(os.Stderr, "Update downloaded to %s\n", newPath)
			fmt.Fprintf(os.Stderr, "To finish: exit this program, then run:\n  %s\n", helperPath)
			fmt.Fprintln(os.Stderr, "(it waits for the running binary to close, replaces it, then removes itself)")
		} else {
			fmt.Fprintf(os.Stderr, "Update saved as %s. Exit this program, then replace %s with it and run again.\n", newPath, selfPath)
		}
		return nil
	}
	tmpPath := filepath.Join(dir, ".ncc-orchestrator-update."+strconv.Itoa(os.Getpid()))
	if err := os.WriteFile(tmpPath, body, 0755); err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := os.Rename(tmpPath, selfPath); err != nil {
		_ = os.Remove(tmpPath)
		fallback := filepath.Join(".", "ncc-orchestrator-"+strings.TrimPrefix(strings.TrimSpace(targetVer), "v"))
		if wErr := os.WriteFile(fallback, body, 0755); wErr != nil {
			return fmt.Errorf("replace binary failed (%v); write to %s failed: %w", err, fallback, wErr)
		}
		fmt.Fprintf(os.Stderr, "Could not replace running binary. New binary saved as %s — move it to replace the current one.\n", fallback)
		return nil
	}
	fmt.Fprintln(os.Stderr, "Update complete. Run the binary again to use the new version.")
	return nil
}

// writeWindowsUpdateSwapHelper writes an apply-ncc-update.cmd next to the
// running executable. The script polls `move` until the old binary's lock is
// released (i.e. this process has exited), swaps the freshly downloaded
// `.new.exe` over it, and then deletes itself. This replaces the previous
// "manually copy the file yourself" instruction with a single command and is
// safe to re-run. The generator is pure (no Windows-only APIs) so it can be
// unit-tested on any platform.
func writeWindowsUpdateSwapHelper(selfPath, newPath string) (string, error) {
	dir := filepath.Dir(selfPath)
	base := filepath.Base(selfPath)
	helperPath := filepath.Join(dir, "apply-ncc-update.cmd")
	var b strings.Builder
	b.WriteString("@echo off\r\n")
	b.WriteString("setlocal\r\n")
	b.WriteString("echo Waiting for " + base + " to close...\r\n")
	b.WriteString(":retry\r\n")
	b.WriteString("move /y \"" + newPath + "\" \"" + selfPath + "\" >nul 2>&1\r\n")
	b.WriteString("if errorlevel 1 (\r\n")
	b.WriteString("  timeout /t 1 /nobreak >nul\r\n")
	b.WriteString("  goto retry\r\n")
	b.WriteString(")\r\n")
	b.WriteString("echo Update applied to " + selfPath + "\r\n")
	b.WriteString("del \"%~f0\"\r\n")
	if err := os.WriteFile(helperPath, []byte(b.String()), 0o755); err != nil {
		return "", err
	}
	return helperPath, nil
}

// resolvePackageInstallDir returns the directory that should receive the
// extracted v2 stack components when the user runs `update`. It is invariant
// to which binary (orchestrator, api-server, ui-server) was invoked and to
// any user-side renames — only the running executable's location matters.
//
// Resolution order:
//
//  1. If the running binary lives inside <X>/bin/, treat <X> as the install
//     dir (the binary is already inside a bootstrapped stack layout, so
//     refresh that layout in place).
//  2. Otherwise, treat the running binary's directory as the install dir
//     (flat layout: <install-dir>/<self>, <install-dir>/bin/*,
//     <install-dir>/frontend-dist/, <install-dir>/example_config.yaml).
//
// Callers may still self-replace the running binary atomically via
// installDownloadedBinary, regardless of the install-dir result.
func resolvePackageInstallDir(selfPath string) string {
	dir := filepath.Dir(selfPath)
	if filepath.Base(dir) == "bin" {
		return filepath.Dir(dir)
	}
	return dir
}

// installPackageUpdate downloads the stack archive, verifies the checksum
// against the release's checksums.txt asset, extracts to a temp dir, copies
// the v2 stack components (bin/*, frontend-dist/, example_config.yaml) into
// the resolved install dir, and atomically self-replaces the running binary
// with the canonically-named match from the extracted bin/.
//
// This is the canonical "upgrade-the-whole-package" code path that handles
// the running binary being orchestrator OR api-server OR ui-server (or any
// renamed variant of those). The selection is based on the running
// executable's basename; if no match is found in the archive, the function
// falls back to bin/ncc-orchestrator and emits a guidance message.
func installPackageUpdate(stackURL, stackName string, rel *githubRelease, targetVer string, client *http.Client, skipChecksumVerify bool) error {
	fmt.Fprintf(os.Stderr, "Downloading package %s ...\n", stackURL)
	body, err := downloadBinaryURL(client, stackURL)
	if err != nil {
		return err
	}
	if err := verifyDownloadedAsset(rel, stackName, body, client, skipChecksumVerify); err != nil {
		return err
	}

	tmpRoot, err := os.MkdirTemp("", "ncc-update-package-")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpRoot)

	if err := extractArchiveByAssetName(body, stackName, tmpRoot); err != nil {
		return fmt.Errorf("extract stack archive %s: %w", stackName, err)
	}
	if !hasBootstrappedV2Layout(tmpRoot) {
		return fmt.Errorf("stack archive %s extracted but required layout was not found (expected bin/ncc-api-server, bin/ncc-ui-server, frontend-dist/)", stackName)
	}

	selfPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("get executable path: %w", err)
	}
	installDir := resolvePackageInstallDir(selfPath)
	fmt.Fprintf(os.Stderr, "Installing package into %s ...\n", installDir)

	if err := installStackComponents(tmpRoot, installDir); err != nil {
		return fmt.Errorf("install stack components: %w", err)
	}

	// Locate the new binary that matches the currently-running one so we
	// can self-replace. We honor the user's renamed binaries by matching
	// the running exe's basename first, then fall back to the canonical
	// ncc-orchestrator name.
	exeBase := currentExecutableBasename()
	newSelfPath := ""
	if exeBase != "" {
		newSelfPath = existingBinaryInInstallDir(tmpRoot, exeBase)
	}
	if newSelfPath == "" {
		newSelfPath = existingBinaryInInstallDir(tmpRoot, "ncc-orchestrator")
	}
	if newSelfPath == "" {
		fmt.Fprintf(os.Stderr, "Package update complete. The running binary was not replaced because no matching binary was found in the stack archive. Re-launch from %s.\n", filepath.Join(installDir, "bin"))
		return nil
	}
	newBody, err := os.ReadFile(newSelfPath)
	if err != nil {
		return fmt.Errorf("read new binary %s: %w", newSelfPath, err)
	}
	return installDownloadedBinary(newBody, targetVer)
}

// verifyDownloadedAsset verifies `body` against the release's published
// checksum for `assetName` unless skipVerify is set. When verification is
// skipped it prints an explicit, support-friendly warning so the operator
// knows the bytes were NOT authenticated (intended only for air-gapped or
// internally-mirrored installs). When not skipped it delegates to
// verifyAssetAgainstReleaseChecksum, which hard-fails on a missing checksum
// asset or a hash mismatch.
func verifyDownloadedAsset(rel *githubRelease, assetName string, body []byte, client *http.Client, skipVerify bool) error {
	if skipVerify {
		fmt.Fprintf(os.Stderr, "warning: --skip-checksum-verify set; NOT verifying %s against release checksums.txt\n", assetName)
		return nil
	}
	return verifyAssetAgainstReleaseChecksum(rel, assetName, body, client)
}

// verifyAssetAgainstReleaseChecksum looks up `assetName` in the release's
// checksums.txt asset (or sha256/.sha256 asset) and compares its hash to a
// fresh sha256 of `body`. Returns nil on match, an error otherwise. If no
// checksum asset is present on the release the function returns an error
// (package updates must be authenticated).
func verifyAssetAgainstReleaseChecksum(rel *githubRelease, assetName string, body []byte, client *http.Client) error {
	if rel == nil {
		return errors.New("nil release passed to checksum verifier")
	}
	for _, a := range rel.Assets {
		an := strings.ToLower(a.Name)
		if strings.HasSuffix(an, ".sig") {
			continue // the detached signature asset, not the checksum list
		}
		if !(strings.Contains(an, "checksum") || strings.Contains(an, "sha256") || strings.HasSuffix(an, ".sha256")) {
			continue
		}
		csBody, err := fetchURL(a.BrowserDownloadURL, client)
		if err != nil {
			return fmt.Errorf("fetch checksum asset %s: %w", a.Name, err)
		}
		// When this build embeds a release public key, the checksums.txt must
		// carry a valid Ed25519 signature before we trust any hash in it —
		// otherwise an attacker who can swap the asset could swap its checksum
		// too. Unsigned/dev builds (no embedded key) keep the prior behavior.
		if sigStatus, sigErr := verifyChecksumSignature(rel, csBody, client, false); sigErr != nil {
			return fmt.Errorf("refusing to install %s: checksums.txt signature %s: %w", assetName, sigStatus, sigErr)
		} else if sigStatus == sigValid {
			fmt.Fprintln(os.Stderr, "Checksum signature verified (Ed25519).")
		}
		expectedHash := parseChecksumFile(csBody, assetName)
		if expectedHash == "" {
			return fmt.Errorf("checksum entry for %s not found in %s", assetName, a.Name)
		}
		sum := sha256.Sum256(body)
		got := hex.EncodeToString(sum[:])
		if !strings.EqualFold(got, expectedHash) {
			return fmt.Errorf("checksum mismatch for %s: expected %s, got %s", assetName, expectedHash, got)
		}
		fmt.Fprintln(os.Stderr, "Checksum verified.")
		return nil
	}
	return fmt.Errorf("no checksum asset found for release %s; refusing to install %s without authentication", rel.TagName, assetName)
}

// installStackComponents copies bin/*, frontend-dist/, and example_config.yaml
// from the extracted stack root (`src`) to the target install directory
// (`dst`). Existing files are replaced atomically where possible.
func installStackComponents(src, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dst, err)
	}
	srcBin := filepath.Join(src, "bin")
	dstBin := filepath.Join(dst, "bin")
	if existingDir(srcBin) {
		if err := os.MkdirAll(dstBin, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", dstBin, err)
		}
		entries, err := os.ReadDir(srcBin)
		if err != nil {
			return fmt.Errorf("read %s: %w", srcBin, err)
		}
		for _, ent := range entries {
			if ent.IsDir() {
				continue
			}
			from := filepath.Join(srcBin, ent.Name())
			to := filepath.Join(dstBin, ent.Name())
			if err := copyFileAtomic(from, to, 0o755); err != nil {
				return fmt.Errorf("install binary %s: %w", ent.Name(), err)
			}
		}
	}
	srcFront := filepath.Join(src, "frontend-dist")
	dstFront := filepath.Join(dst, "frontend-dist")
	if existingDir(srcFront) {
		if err := os.RemoveAll(dstFront); err != nil {
			return fmt.Errorf("remove old frontend-dist: %w", err)
		}
		if err := copyDir(srcFront, dstFront); err != nil {
			return fmt.Errorf("install frontend-dist: %w", err)
		}
	}
	srcCfg := filepath.Join(src, "example_config.yaml")
	dstCfg := filepath.Join(dst, "example_config.yaml")
	if existingFile(srcCfg) {
		if err := copyFileAtomic(srcCfg, dstCfg, 0o644); err != nil {
			return fmt.Errorf("install example_config.yaml: %w", err)
		}
	}
	return nil
}

// copyFileAtomic writes the source file to dst via a sibling temp file and
// rename, so that the destination is never observed half-written. Existing
// destination files are replaced.
func copyFileAtomic(src, dst string, mode os.FileMode) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".ncc-update-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	_, writeErr := tmp.Write(data)
	closeErr := tmp.Close()
	if writeErr != nil {
		_ = os.Remove(tmpName)
		return writeErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpName)
		return closeErr
	}
	if err := os.Chmod(tmpName, mode); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, dst); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}

// copyDir recursively copies the contents of src into dst. Destination must
// not exist (caller is responsible for cleaning up beforehand if needed).
func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		return copyFileAtomic(path, target, info.Mode().Perm())
	})
}

type v2BootstrapOptions struct {
	Repo               string
	Version            string
	InstallDir         string
	ConfigPath         string
	OutputDir          string
	LogDir             string
	OrchestratorBin    string
	APIListen          string
	UIListen           string
	TokenFile          string
	CheckOnly          bool
	SkipChecksumVerify bool
}

type v2StartOptions struct {
	InstallDir                  string
	ConfigPath                  string
	OutputDir                   string
	LogDir                      string
	OrchestratorBin             string
	APIListen                   string
	UIListen                    string
	APIAdvertiseURL             string
	UIAdvertiseURL              string
	UIBackendURL                string
	APICORSOrigins              string
	UIAllowedOrigins            string
	TokenFile                   string
	UsersDB                     string
	APIAuthMode                 string
	APISessionTTL               time.Duration
	APISessionSecret            string
	APISessionSecretFile        string
	APIRunTimeout               time.Duration
	APIRateLimitPerMinute       int
	APIReadTimeout              time.Duration
	APIWriteTimeout             time.Duration
	APIIdleTimeout              time.Duration
	APITLSCertFile              string
	APITLSKeyFile               string
	APITLSClientCAFile          string
	APICookieInsecure           bool
	UITLSCertFile               string
	UITLSKeyFile                string
	UIInsecureHTTP              bool
	UIBackendCAFile             string
	UIBackendClientCertFile     string
	UIBackendClientKeyFile      string
	UIBackendInsecureSkipVerify bool
	WaitReady                   bool
	ReadyTimeout                time.Duration
	Detach                      bool
	APIOnly                     bool
	APILogFile                  string
	UILogFile                   string
	APIPIDFile                  string
	UIPIDFile                   string
	SelfHeal                    bool
	SelfHealMaxRestarts         int
	SelfHealWindow              time.Duration
	// SelfHealProbeInterval is how often the supervisor runs an HTTP health
	// probe against a still-alive process to catch hangs/deadlocks (not just
	// crashes). SelfHealUnhealthyThreshold is the number of consecutive failed
	// probes that triggers a restart. Zero values fall back to defaults.
	SelfHealProbeInterval      time.Duration
	SelfHealUnhealthyThreshold int
	// Supervise runs the stack under the native foreground supervisor instead
	// of detaching: a single long-lived process launches and keeps API/UI
	// alive (liveness + health-probe restarts, backoff, cooldown-and-resume).
	// Intended to be run as a Type=simple systemd service for reboot
	// persistence. Mutually preferred over Detach when both are set.
	Supervise bool
}

type v2StopOptions struct {
	InstallDir  string
	APIPIDFile  string
	UIPIDFile   string
	Force       bool
	StopTimeout time.Duration
}

type uninstallOptions struct {
	ConfigPath      string
	InstallDir      string
	TaskName        string
	Force           bool
	DryRun          bool
	RemoveLocal     bool
	RemoveSchedule  bool
	RemoveV2Runtime bool
}

func findAsset(rel githubRelease, pred func(name string) bool) (githubAsset, bool) {
	for _, a := range rel.Assets {
		if pred(strings.ToLower(strings.TrimSpace(a.Name))) {
			return a, true
		}
	}
	return githubAsset{}, false
}

func writeExecutable(path string, body []byte) error {
	return os.WriteFile(path, body, 0755)
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}

func binaryPathInInstallDir(installDir, base string) string {
	path := filepath.Join(installDir, "bin", base)
	if runtime.GOOS == "windows" {
		return path + ".exe"
	}
	return path
}

// existingBinaryInInstallDir returns the path to a v2 runtime binary inside
// <installDir>/bin/, accepting either the canonical name (`ncc-api-server`,
// preferred for newly-packaged stack archives) or the platform-suffixed name
// (`ncc-api-server-<os>-<arch>`, used by legacy v2.0.0 stack archives).
// Returns "" if no candidate exists.
func existingBinaryInInstallDir(installDir, base string) string {
	binDir := filepath.Join(installDir, "bin")
	for _, name := range v2BinaryNameCandidates(base) {
		p := filepath.Join(binDir, name)
		if existingFile(p) {
			return p
		}
	}
	return ""
}

func existingFile(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

func existingDir(path string) bool {
	st, err := os.Stat(path)
	return err == nil && st.IsDir()
}

func v2BinaryNameCandidates(base string) []string {
	names := []string{
		base,
		fmt.Sprintf("%s-%s-%s", base, runtime.GOOS, runtime.GOARCH),
	}
	if runtime.GOOS == "windows" {
		withExe := make([]string, 0, len(names)*2)
		for _, n := range names {
			withExe = append(withExe, n, n+".exe")
		}
		return withExe
	}
	return names
}

func firstExistingBinary(searchDirs []string, base string) string {
	candidates := v2BinaryNameCandidates(base)
	for _, dir := range searchDirs {
		d := strings.TrimSpace(dir)
		if d == "" {
			continue
		}
		for _, name := range candidates {
			p := filepath.Join(d, name)
			if existingFile(p) {
				return p
			}
		}
	}
	return ""
}

// resolveV2RepoRoot picks the api-server's --repo-root value. The api-server
// uses repo-root as the path-traversal sandbox boundary for every file it
// touches (config, outputs, logs, token), so it must contain ALL of those
// paths. We pick the directory that contains both the install-dir and the
// CWD when one is an ancestor of the other; otherwise prefer install-dir
// (since v2.0.2 defaults all secondary paths relative to install-dir).
//
// We also EvalSymlinks the chosen root so the api-server's
// normalizeAndConfinePath comparison is consistent on platforms where /tmp
// is a symlink to /private/tmp (macOS): the api-server EvalSymlinks the
// rootAbs but compares against the user-supplied (unresolved) absolute
// path. Without orchestrator-side resolution, /tmp/.../config.yaml would
// fail the prefix check against /private/tmp/.../<repo-root>.
func resolveV2RepoRoot(installDir, cwd string) string {
	installAbs, err := filepath.Abs(installDir)
	if err != nil {
		installAbs = installDir
	}
	cwdAbs, err := filepath.Abs(cwd)
	if err != nil {
		cwdAbs = cwd
	}
	installAbs = filepath.Clean(installAbs)
	cwdAbs = filepath.Clean(cwdAbs)

	chosen := installAbs
	switch {
	case installAbs == cwdAbs:
		chosen = installAbs
	case isPathAncestor(installAbs, cwdAbs):
		chosen = installAbs
	case isPathAncestor(cwdAbs, installAbs):
		chosen = cwdAbs
	}
	if real, err := filepath.EvalSymlinks(chosen); err == nil {
		return filepath.Clean(real)
	}
	return chosen
}

// isPathAncestor reports whether parent is an ancestor of (or equal to)
// child, using lexical path comparison after Clean. Caller is expected to
// supply absolute paths. Handles the filesystem root ("/" on unix, "C:\"
// on windows) where Clean(root) already ends in the separator and we must
// not append a second separator before the prefix check.
func isPathAncestor(parent, child string) bool {
	parent = filepath.Clean(parent)
	child = filepath.Clean(child)
	if parent == child {
		return true
	}
	sep := string(os.PathSeparator)
	if strings.HasSuffix(parent, sep) {
		return strings.HasPrefix(child, parent)
	}
	return strings.HasPrefix(child, parent+sep)
}

// resolveV2PathToReal returns p as an absolute, symlink-resolved path. If
// the path doesn't exist (e.g. output-dir before mkdir, log-dir before
// mkdir, token-file before write), it walks up to the first existing
// ancestor, EvalSymlinks that, then re-attaches the non-existing suffix.
// This is critical for the api-server's normalizeAndConfinePath sandbox
// check on platforms where /tmp is a symlink (macOS): without consistent
// resolution, repo-root would be /private/tmp/X but output-dir would
// remain /tmp/X/outputfiles — failing the prefix check.
func resolveV2PathToReal(p string) string {
	if strings.TrimSpace(p) == "" {
		return p
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	abs = filepath.Clean(abs)
	if real, err := filepath.EvalSymlinks(abs); err == nil {
		return filepath.Clean(real)
	}
	// Walk up until we find an existing ancestor, then re-attach the
	// non-existing suffix. Bounded by the filesystem root.
	parent := filepath.Dir(abs)
	rest := filepath.Base(abs)
	for {
		if parent == filepath.Dir(parent) {
			return abs // hit the root without finding an existing ancestor
		}
		if real, err := filepath.EvalSymlinks(parent); err == nil {
			return filepath.Clean(filepath.Join(real, rest))
		}
		rest = filepath.Join(filepath.Base(parent), rest)
		parent = filepath.Dir(parent)
	}
}

// defaultV2InstallDir returns the install directory used by run-time consumer
// commands (v2-check, v2-start, v2-stop, uninstall) when the user does not
// pass an explicit --install-dir. The detection mirrors
// resolvePackageInstallDir so that running from inside an extracted/
// bootstrapped stack layout (`<X>/bin/<exe>`) "just works" without forcing
// the user to type `--install-dir <X>` every time.
//
// Resolution:
//
//  1. If os.Executable() is at <X>/bin/<exe> AND <X> contains the v2 layout
//     (frontend-dist/ present, or bin/ncc-api-server present under either
//     canonical or platform-suffixed naming) → return <X>.
//  2. Otherwise → return ".ncc-v2" (the historic default; preserves backward
//     compatibility with existing scripts, docs, and CI invocations that
//     bootstrap into ./.ncc-v2 from a project root).
func defaultV2InstallDir() string {
	exe, err := os.Executable()
	if err != nil {
		return ".ncc-v2"
	}
	return defaultV2InstallDirForExeDir(filepath.Dir(exe))
}

// defaultV2InstallDirForExeDir is the testable inner of defaultV2InstallDir.
// Split out so unit tests can drive the helper without having to redirect
// os.Executable() at runtime.
func defaultV2InstallDirForExeDir(exeDir string) string {
	if exeDir == "" || filepath.Base(exeDir) != "bin" {
		return ".ncc-v2"
	}
	parent := filepath.Dir(exeDir)
	if existingDir(filepath.Join(parent, "frontend-dist")) {
		return parent
	}
	if existingBinaryInInstallDir(parent, "ncc-api-server") != "" {
		return parent
	}
	return ".ncc-v2"
}

func resolveV2RuntimeLayout(installDir string) (string, string, string, string) {
	installDir = strings.TrimSpace(installDir)
	if installDir == "" {
		installDir = ".ncc-v2"
	}
	cwd, _ := os.Getwd()
	exeDir := ""
	if exe, err := os.Executable(); err == nil {
		exeDir = filepath.Dir(exe)
	}

	// Preferred layout from v2-bootstrap: <install>/bin/* + <install>/frontend-dist
	// Accepts either canonical (ncc-api-server) or platform-suffixed
	// (ncc-api-server-<os>-<arch>) binary names; legacy v2.0.0 stack archives
	// shipped the suffixed form, current/future archives ship canonical.
	apiBoot := existingBinaryInInstallDir(installDir, "ncc-api-server")
	uiBoot := existingBinaryInInstallDir(installDir, "ncc-ui-server")
	frontBoot := filepath.Join(installDir, "frontend-dist")
	if apiBoot != "" && uiBoot != "" && existingDir(frontBoot) {
		return apiBoot, uiBoot, frontBoot, "install-dir"
	}

	// Fallback for release asset folders (flat binaries + frontend-dist).
	searchDirs := []string{installDir, cwd, exeDir}
	apiFlat := firstExistingBinary(searchDirs, "ncc-api-server")
	uiFlat := firstExistingBinary(searchDirs, "ncc-ui-server")
	if apiFlat != "" && uiFlat != "" {
		for _, d := range []string{
			filepath.Join(installDir, "frontend-dist"),
			filepath.Join(cwd, "frontend-dist"),
			filepath.Join(exeDir, "frontend-dist"),
			filepath.Join(cwd, "frontend", "dist"),
			filepath.Join(exeDir, "frontend", "dist"),
		} {
			if existingDir(d) {
				return apiFlat, uiFlat, d, "local-release-assets"
			}
		}
	}
	return "", "", "", ""
}

func resolveV2APIBinary(installDir string) (string, string) {
	installDir = strings.TrimSpace(installDir)
	if installDir == "" {
		installDir = ".ncc-v2"
	}
	if apiBoot := existingBinaryInInstallDir(installDir, "ncc-api-server"); apiBoot != "" {
		return apiBoot, "install-dir"
	}
	cwd, _ := os.Getwd()
	exeDir := ""
	if exe, err := os.Executable(); err == nil {
		exeDir = filepath.Dir(exe)
	}
	apiFlat := firstExistingBinary([]string{installDir, cwd, exeDir}, "ncc-api-server")
	if apiFlat != "" {
		return apiFlat, "local-release-assets"
	}
	return "", ""
}

func resolveV2OrchestratorBin(preferred string) string {
	// Return an absolute (and symlink-resolved when possible) path so the
	// api-server doesn't need to interpret it relative to its CWD
	// (repo-root). Without this, "./ncc-orchestrator" would be valid for
	// the orchestrator's own CWD but not for the api-server's CWD when
	// they differ (e.g. running v2-start from <stack>/bin/ resolves
	// repo-root to <stack>, where ncc-orchestrator is at bin/, not the
	// repo-root itself).
	asAbs := func(p string) string {
		if abs, err := filepath.Abs(p); err == nil {
			if real, err := filepath.EvalSymlinks(abs); err == nil {
				return filepath.Clean(real)
			}
			return filepath.Clean(abs)
		}
		return p
	}
	clean := strings.TrimSpace(preferred)
	if clean != "" && existingFile(clean) {
		return asAbs(clean)
	}
	cwd, _ := os.Getwd()
	exePath, _ := os.Executable()
	exeDir := filepath.Dir(exePath)
	for _, p := range []string{
		clean,
		filepath.Join(cwd, "ncc-orchestrator"),
		filepath.Join(cwd, fmt.Sprintf("ncc-orchestrator-%s-%s", runtime.GOOS, runtime.GOARCH)),
		filepath.Join(exeDir, "ncc-orchestrator"),
		filepath.Join(exeDir, fmt.Sprintf("ncc-orchestrator-%s-%s", runtime.GOOS, runtime.GOARCH)),
		exePath,
	} {
		if strings.TrimSpace(p) != "" && existingFile(p) {
			return asAbs(p)
		}
	}
	return clean
}

// loopbackAltOriginFromListen returns the "http://localhost:port" variant
// for a listen address that's bound to an explicit loopback IP
// (127.0.0.1 or ::1). Used to ensure CORS allow-list contains both forms
// so browser clients that type either work, even though the connection
// URL produced by localHTTPURLFromListen prefers the actual IP for
// IPv4/IPv6 disambiguation.
//
// Returns empty string when the listen address is not loopback (e.g.
// 0.0.0.0, ::, hostnames, or external IPs) — in those cases the
// localhost-form would be misleading.
func loopbackAltOriginFromListen(listenAddr, defaultPort string) string {
	addr := strings.TrimSpace(listenAddr)
	if addr == "" || strings.HasPrefix(addr, ":") {
		return ""
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return ""
	}
	switch host {
	case "127.0.0.1", "::1", "[::1]":
		return "http://localhost:" + port
	}
	return ""
}

func localHTTPURLFromListen(listenAddr, defaultPort string) string {
	addr := strings.TrimSpace(listenAddr)
	if addr == "" {
		addr = ":" + defaultPort
	}
	if strings.HasPrefix(addr, ":") {
		// Server binds to all interfaces; "localhost" works for client.
		return "http://localhost" + addr
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "http://" + addr
	}
	// Preserve the user-supplied host so the derived URL targets the same
	// address family the server bound to. Critical on macOS where the
	// server binds 127.0.0.1 (IPv4) but `localhost` resolves to ::1 (IPv6)
	// first, causing wait-ready / UI backend connections to fail with
	// "connection refused" even though the server is healthy.
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		return "http://localhost:" + port
	}
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		return "http://[" + host + "]:" + port
	}
	return "http://" + host + ":" + port
}

// listenHostForCert extracts a concrete host from a listen address for use as a
// certificate SAN. A wildcard bind (":8080", "0.0.0.0:8080", "[::]:8080")
// returns "" since there is no specific host to certify.
func listenHostForCert(listenAddr string) string {
	addr := strings.TrimSpace(listenAddr)
	if addr == "" || strings.HasPrefix(addr, ":") {
		return ""
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return ""
	}
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		return ""
	}
	return host
}

// ensureDefaultUISelfSignedCert returns the path to a self-signed UI cert/key,
// generating it under <installDir>/tls on first use. An existing pair is reused
// across restarts so the browser's trust decision sticks. The certificate
// covers localhost/loopback plus the UI advertise host and any concrete listen
// host, so https://<that-host> validates the name (self-signed trust aside).
func ensureDefaultUISelfSignedCert(installDir string, opts v2StartOptions) (certPath, keyPath string, err error) {
	dir := filepath.Join(installDir, "tls")
	certPath = filepath.Join(dir, "ui-selfsigned.crt")
	keyPath = filepath.Join(dir, "ui-selfsigned.key")
	if st, e := os.Stat(certPath); e == nil && !st.IsDir() {
		if st2, e2 := os.Stat(keyPath); e2 == nil && !st2.IsDir() {
			return certPath, keyPath, nil
		}
	}
	if mkErr := os.MkdirAll(dir, 0o700); mkErr != nil {
		return "", "", mkErr
	}
	var hosts []string
	if u := strings.TrimSpace(opts.UIAdvertiseURL); u != "" {
		if parsed, perr := url.Parse(u); perr == nil && parsed.Hostname() != "" {
			hosts = append(hosts, parsed.Hostname())
		}
	}
	if h := listenHostForCert(opts.UIListen); h != "" {
		hosts = append(hosts, h)
	}
	certPEM, keyPEM, gerr := selfsigned.Generate(hosts, 0)
	if gerr != nil {
		return "", "", gerr
	}
	if wErr := os.WriteFile(certPath, certPEM, 0o600); wErr != nil {
		return "", "", wErr
	}
	if wErr := os.WriteFile(keyPath, keyPEM, 0o600); wErr != nil {
		return "", "", wErr
	}
	return certPath, keyPath, nil
}

func mergeAllowedOriginsCSV(baseOrigin string, extraCSV string) string {
	seen := map[string]bool{}
	out := make([]string, 0, 4)
	add := func(v string) {
		v = strings.TrimSpace(v)
		if v == "" {
			return
		}
		if seen[v] {
			return
		}
		seen[v] = true
		out = append(out, v)
	}
	add(baseOrigin)
	for _, part := range strings.Split(extraCSV, ",") {
		add(part)
	}
	return strings.Join(out, ",")
}

func waitForFile(path string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

func signalProcessStop(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	if runtime.GOOS == "windows" {
		_ = cmd.Process.Kill()
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
}

func readPIDFromFile(path string) (int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	raw := strings.TrimSpace(string(b))
	if raw == "" {
		return 0, fmt.Errorf("pid file %s is empty", path)
	}
	pid, err := strconv.Atoi(raw)
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("invalid pid %q in %s", raw, path)
	}
	return pid, nil
}

func signalPIDStop(pid int, force bool) error {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	if runtime.GOOS == "windows" || force {
		return proc.Kill()
	}
	return proc.Signal(syscall.SIGTERM)
}

func readTrimmedFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

func waitForHTTPReady(url string, timeout time.Duration) error {
	if strings.TrimSpace(url) == "" {
		return nil
	}
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil && resp != nil {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 500 {
				return nil
			}
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
		} else {
			lastErr = err
		}
		time.Sleep(250 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = errors.New("timed out")
	}
	return fmt.Errorf("endpoint %s not ready: %w", url, lastErr)
}

// isExecutableFile reports whether path is a runnable binary on the
// current OS. Delegates to v2layout.IsExecutable so the Unix
// executable-bit check and the Windows PATHEXT-extension check stay in
// one place (shared with the api-server's startup guard). On Windows
// the Unix mode bits are meaningless, so a bit-only test wrongly
// rejected every shipped ncc-*.exe with "not executable".
func isExecutableFile(path string) bool {
	return v2layout.IsExecutable(path)
}

func canBindListenAddress(listenAddr string) error {
	addr := strings.TrimSpace(listenAddr)
	if addr == "" {
		return errors.New("empty listen address")
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	_ = ln.Close()
	return nil
}

// v2StatusOptions wires the `status` subcommand's flags to runV2Status.
// Mirrors v2StartOptions where overlap exists so an operator who
// recalls v2-start's flag names can reuse them on status without
// re-reading the help.
type v2StatusOptions struct {
	InstallDir string
	APIListen  string
	UIListen   string
	JSON       bool
}

// v2StatusEntry is the per-service row populated by runV2Status. It's
// also the JSON-array element when --json is set, so its field tags
// are part of the public output contract: monitoring tooling can rely
// on them. Add new fields rather than renaming existing ones.
type v2StatusEntry struct {
	Service  string `json:"service"`
	PIDPath  string `json:"pid_path"`
	PID      int    `json:"pid,omitempty"`
	State    string `json:"state"` // "alive", "dead-stale-pid", "missing-pid", "unreadable-pid"
	Listen   string `json:"listen,omitempty"`
	Health   string `json:"health,omitempty"` // "ok", "unhealthy", "unreachable", "n/a"
	HealthMS int64  `json:"health_response_ms,omitempty"`
	LogPath  string `json:"log_path,omitempty"`
	Error    string `json:"error,omitempty"`
}

// runV2Status reports the live state of a v2 stack. Reads the PID
// files written by v2-start --detach, probes each PID with signal 0,
// and (for the api-server) hits /api/v1/health to verify the
// listener is actually accepting requests.
//
// Designed to be run from anywhere — does NOT require the orchestrator
// to be the same binary that started the stack, and does NOT modify
// any state. Always exits 0 unless --json failed to marshal; the
// caller can grep "state=alive" / "health=ok" if they want a
// strict liveness check.
func runV2Status(opts v2StatusOptions) error {
	installDir := strings.TrimSpace(opts.InstallDir)
	if installDir == "" {
		installDir = defaultV2InstallDir()
	}
	runDir := filepath.Join(installDir, "run")
	logDir := filepath.Join(installDir, "logs")

	apiListen := strings.TrimSpace(opts.APIListen)
	if apiListen == "" {
		apiListen = ":8081"
	}
	uiListen := strings.TrimSpace(opts.UIListen)
	if uiListen == "" {
		uiListen = ":8080"
	}

	tokenPath := filepath.Join(installDir, ".ncc-api-token")

	type svc struct {
		name     string
		pidFile  string
		listen   string
		logFile  string
		probe    bool
		identity []string
	}
	services := []svc{
		// Native foreground supervisor (v2-supervise); owns both children.
		{"supervisor", filepath.Join(runDir, "v2-supervisor.pid"), "", filepath.Join(logDir, "v2-supervisor.log"), false, []string{"ncc-orchestrator", "v2-supervise"}},
		// Legacy per-service sh supervisors (v2-start --detach --self-heal).
		{"api-supervisor", filepath.Join(runDir, "v2-api-supervisor.pid"), "", filepath.Join(logDir, "v2-api-supervisor.log"), false, []string{"v2-api-supervisor", "ncc-api-server"}},
		{"ncc-api-server", filepath.Join(runDir, "v2-api.pid"), apiListen, filepath.Join(logDir, "v2-api.log"), true, []string{"ncc-api-server"}},
		{"ui-supervisor", filepath.Join(runDir, "v2-ui-supervisor.pid"), "", filepath.Join(logDir, "v2-ui-supervisor.log"), false, []string{"v2-ui-supervisor", "ncc-ui-server"}},
		{"ncc-ui-server", filepath.Join(runDir, "v2-ui.pid"), uiListen, filepath.Join(logDir, "v2-ui.log"), false, []string{"ncc-ui-server"}},
	}

	out := make([]v2StatusEntry, 0, len(services))
	httpClient := &http.Client{Timeout: 3 * time.Second}
	apiToken := ""
	if b, err := os.ReadFile(tokenPath); err == nil {
		apiToken = strings.TrimSpace(string(b))
	}

	for _, s := range services {
		entry := v2StatusEntry{
			Service: s.name,
			PIDPath: s.pidFile,
			Listen:  s.listen,
			LogPath: s.logFile,
			Health:  "n/a",
		}
		pid, err := readPIDFromFile(s.pidFile)
		switch {
		case errors.Is(err, os.ErrNotExist):
			entry.State = "missing-pid"
		case err != nil:
			entry.State = "unreadable-pid"
			entry.Error = err.Error()
		default:
			entry.PID = pid
			if processIsExpected(pid, s.identity...) {
				entry.State = "alive"
			} else {
				entry.State = "dead-stale-pid"
				entry.Error = fmt.Sprintf("pid %d is not an NCC %s process", pid, s.name)
			}
		}

		if s.probe && entry.State == "alive" {
			url := localHTTPURLFromListen(s.listen, "8081") + "/api/v1/health"
			req, _ := http.NewRequest(http.MethodGet, url, nil)
			if apiToken != "" {
				req.Header.Set("X-Api-Token", apiToken)
			}
			start := time.Now()
			resp, err := httpClient.Do(req)
			entry.HealthMS = time.Since(start).Milliseconds()
			if err != nil {
				entry.Health = "unreachable"
				entry.Error = err.Error()
			} else {
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					entry.Health = "ok"
				} else {
					entry.Health = fmt.Sprintf("http-%d", resp.StatusCode)
				}
			}
		}
		out = append(out, entry)
	}

	if opts.JSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(map[string]any{
			"install_dir": installDir,
			"services":    out,
		})
	}

	fmt.Printf("ncc-orchestrator v2 stack status\n")
	fmt.Printf("--------------------------------\n")
	fmt.Printf("install-dir: %s\n\n", installDir)
	fmt.Printf("%-18s %-7s %-15s %-22s %-12s %s\n",
		"SERVICE", "PID", "STATE", "LISTEN", "HEALTH", "LOG")
	for _, e := range out {
		pidStr := "-"
		if e.PID > 0 {
			pidStr = strconv.Itoa(e.PID)
		}
		listen := e.Listen
		if listen == "" {
			listen = "-"
		}
		health := e.Health
		if e.HealthMS > 0 && (health == "ok" || strings.HasPrefix(health, "http-")) {
			health = fmt.Sprintf("%s (%dms)", health, e.HealthMS)
		}
		fmt.Printf("%-18s %-7s %-15s %-22s %-12s %s\n",
			e.Service, pidStr, e.State, listen, health, e.LogPath)
		if e.Error != "" {
			fmt.Printf("%-18s   error: %s\n", "", e.Error)
		}
	}
	return nil
}

// processIsAlive returns true when a process with the given pid exists
// and signal 0 reports "yes" (not "no such process" / EPERM-on-others).
// Cross-platform: on Windows os.FindProcess always succeeds and Signal
// is unsupported, so we fall back to a stat under /proc on linux/darwin
// and assume alive on Windows (the readPIDFromFile path already
// validated the file is fresh enough for our purposes).
func processIsAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	if runtime.GOOS == "windows" {
		// FindProcess on Windows returns a handle even for dead PIDs,
		// so probe via Wait with a 0-timeout fallback. The simplest
		// robust check is signal 0 emulation:
		err := p.Signal(syscall.Signal(0))
		return err == nil
	}
	if err := p.Signal(syscall.Signal(0)); err != nil {
		return false
	}
	return true
}

// ---- v2 backup / restore --------------------------------------------------
//
// backup/restore capture the *stateful* contents of a v2 install dir — the
// config and its referenced files (clusters / alert-exclusions / secrets), the
// local user database (accounts, roles, SAML config, session policy), the API
// token, the first-run admin password (if still present), and the scheduler /
// notifications state — into a single tar.gz. Regenerable artifacts (binaries,
// frontend bundle, logs, run/ pid files, output/ncc files) are intentionally
// excluded. The archive contains secrets, so it is written 0600 and restore is
// confined strictly to the install dir.

// backupManifest is the JSON index written at the root of a backup archive so
// restore can validate the archive and report its provenance. Tool/Version
// (plus Stream/BuildDate/GoVersion) record the exact ncc-orchestrator binary
// that produced the backup, so restore can report it and warn on a version
// mismatch.
type backupManifest struct {
	Tool       string       `json:"tool"`
	Version    string       `json:"version"`              // ncc-orchestrator version that created the backup
	Stream     string       `json:"stream,omitempty"`     // release stream (prod/dev/beta)
	BuildDate  string       `json:"build_date,omitempty"` // build timestamp of the creating binary
	GoVersion  string       `json:"go_version,omitempty"` // Go toolchain of the creating binary
	CreatedAt  string       `json:"created_at"`
	InstallDir string       `json:"install_dir"`
	Files      []string     `json:"files"`
	Auth       *authSummary `json:"auth,omitempty"` // which auth providers/secrets the archive carries
}

// authSummary records which authentication providers a backed-up user database
// holds and whether their server-side secrets came along. SAML and LDAP config
// (including the SP signing key and the LDAP bind password) live inside
// .ncc-api-users.json rather than in their own files, so this summary makes that
// coverage explicit in the manifest and in backup/restore output instead of
// leaving it implicit.
type authSummary struct {
	LocalAccounts       int  `json:"local_accounts"`
	SAMLPresent         bool `json:"saml_present"`
	SAMLEnabled         bool `json:"saml_enabled"`
	SAMLHasSPKey        bool `json:"saml_has_sp_key"`
	LDAPPresent         bool `json:"ldap_present"`
	LDAPEnabled         bool `json:"ldap_enabled"`
	LDAPHasBindPassword bool `json:"ldap_has_bind_password"`
}

// backupEntry pairs an on-disk absolute path with the archive-relative path it
// is stored under (always relative to the install dir).
type backupEntry struct {
	Rel string
	Abs string
}

type v2BackupOptions struct {
	InstallDir string
	OutputFile string
	// OutputDir, when set and OutputFile is empty, writes the default
	// timestamped ncc-backup-<UTC>.tar.gz into this directory instead of the
	// current working directory. Lets a scheduled backup land in a fixed
	// location (e.g. <install>/backups) without the caller computing the stamp.
	OutputDir string
	// Retain, when > 0, prunes older ncc-backup-*.tar.gz siblings of the output
	// file so at most Retain backups are kept (newest wins).
	Retain int
	// Encrypt seals the finished archive with AES-256-GCM. Key material comes
	// from Passphrase/NCC_BACKUP_PASSPHRASE (scrypt) or KeyFile/NCC_BACKUP_KEY_*
	// (raw 32-byte key).
	Encrypt    bool
	KeyFile    string
	Passphrase string
}

type v2RestoreOptions struct {
	InstallDir string
	InputFile  string
	Force      bool
	Restart    bool
	NoRestart  bool
	// VerifyOnly validates the archive (gzip+tar integrity, manifest present,
	// confined paths) and reports, without extracting anything.
	VerifyOnly bool
	// KeyFile/Passphrase decrypt an encrypted backup; an unencrypted archive
	// ignores them.
	KeyFile    string
	Passphrase string
}

// Backup archive limits, shared by the verifier and the restorer so the two
// agree on what is acceptable (a file that verifies must also restore without
// silent truncation). They also bound the work a malicious or corrupt archive
// can force: a decompression bomb (a tiny gzip that expands to gigabytes)
// cannot exhaust memory or disk during restore.
const (
	maxBackupFileBytes  = 64 << 20  // per-file uncompressed cap (64 MiB)
	maxBackupTotalBytes = 512 << 20 // total uncompressed cap across all files (512 MiB)
	maxBackupFileCount  = 10000     // sanity cap on the number of entries
)

// backupVerifyResult summarizes a validated backup archive.
type backupVerifyResult struct {
	Manifest  backupManifest
	DataFiles int
	Bytes     int64
}

// verifyBackupArchive reads an archive end-to-end and validates it is a
// restorable ncc backup: it must decompress cleanly (gzip CRC), untar without
// error, carry a parseable manifest.json, contain at least one data/ file, and
// hold no path-traversal/absolute entries. It writes nothing. A "backup" that
// cannot be restored is worse than no backup, so create/restore and the doctor
// all gate on this.
func verifyBackupArchive(path string) (backupVerifyResult, error) {
	var res backupVerifyResult
	f, err := os.Open(path)
	if err != nil {
		return res, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return res, fmt.Errorf("not a gzip archive: %w", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	sawManifest := false
	sep := string(filepath.Separator)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return res, fmt.Errorf("corrupt tar stream: %w", err)
		}
		if hdr.FileInfo().IsDir() {
			continue
		}
		switch {
		case hdr.Name == "manifest.json":
			data, err := io.ReadAll(io.LimitReader(tr, 1<<20))
			if err != nil {
				return res, err
			}
			if err := json.Unmarshal(data, &res.Manifest); err != nil {
				return res, fmt.Errorf("invalid manifest.json: %w", err)
			}
			sawManifest = true
		case strings.HasPrefix(hdr.Name, "data/"):
			rel := strings.TrimPrefix(hdr.Name, "data/")
			clean := filepath.Clean(filepath.FromSlash(rel))
			if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+sep) || filepath.IsAbs(clean) {
				return res, fmt.Errorf("unsafe path in archive: %q", hdr.Name)
			}
			// +1 so we can tell "exactly at the cap" from "over the cap" and
			// reject an oversize file rather than let it through (the restorer
			// would otherwise truncate it).
			n, err := io.Copy(io.Discard, io.LimitReader(tr, maxBackupFileBytes+1))
			if err != nil {
				return res, fmt.Errorf("read %s: %w", hdr.Name, err)
			}
			if n > maxBackupFileBytes {
				return res, fmt.Errorf("archive file %q exceeds the per-file limit (%d bytes)", hdr.Name, maxBackupFileBytes)
			}
			res.DataFiles++
			if res.DataFiles > maxBackupFileCount {
				return res, fmt.Errorf("archive contains too many files (>%d)", maxBackupFileCount)
			}
			res.Bytes += n
			if res.Bytes > maxBackupTotalBytes {
				return res, fmt.Errorf("archive exceeds the total size limit (%d bytes)", maxBackupTotalBytes)
			}
		}
	}
	if !sawManifest {
		return res, fmt.Errorf("archive missing manifest.json (not an ncc backup?)")
	}
	if res.DataFiles == 0 {
		return res, fmt.Errorf("archive contains no data files to restore")
	}
	return res, nil
}

// pruneOldBackups keeps at most retain newest ncc-backup-*.tar.gz files in dir,
// deleting the rest. Returns the names pruned.
func pruneOldBackups(dir string, retain int) ([]string, error) {
	if retain <= 0 {
		return nil, nil
	}
	matches, err := filepath.Glob(filepath.Join(dir, "ncc-backup-*.tar.gz"))
	if err != nil {
		return nil, err
	}
	if len(matches) <= retain {
		return nil, nil
	}
	type bk struct {
		path string
		mod  time.Time
	}
	var list []bk
	for _, m := range matches {
		if st, err := os.Stat(m); err == nil && !st.IsDir() {
			list = append(list, bk{m, st.ModTime()})
		}
	}
	sort.Slice(list, func(i, j int) bool { return list[i].mod.After(list[j].mod) })
	var pruned []string
	for _, b := range list[retain:] {
		if err := os.Remove(b.path); err == nil {
			pruned = append(pruned, filepath.Base(b.path))
		}
	}
	return pruned, nil
}

type v2ResetPasswordOptions struct {
	InstallDir   string
	User         string
	UsersDB      string
	UsersDBSec   string
	UsersDBSecNS string
}

// runV2ResetPassword recovers a lost account password offline by invoking the
// api-server's --reset-password against the same user store the stack uses. It
// locates the api binary in the install dir, defaults the user database to
// <install-dir>/.ncc-api-users.json (unless a Secret or explicit path is
// given), streams the child's output (which prints the new temporary
// password), and reminds the operator to restart the stack so it takes effect.
func runV2ResetPassword(opts v2ResetPasswordOptions) error {
	installDir := strings.TrimSpace(opts.InstallDir)
	if installDir == "" {
		installDir = defaultV2InstallDir()
	}
	if abs, err := filepath.Abs(installDir); err == nil {
		installDir = abs
	}
	if st, err := os.Stat(installDir); err != nil || !st.IsDir() {
		return fmt.Errorf("install dir not found: %s", installDir)
	}
	user := strings.TrimSpace(opts.User)
	if user == "" {
		user = "admin"
	}
	apiBin, _ := resolveV2APIBinary(installDir)
	if apiBin == "" {
		return fmt.Errorf("ncc-api-server binary not found near %s; place it in the install dir or on PATH", installDir)
	}

	args := []string{"--reset-password", user}
	usingSecret := strings.TrimSpace(opts.UsersDBSec) != ""
	if usingSecret {
		args = append(args, "--users-db-secret", strings.TrimSpace(opts.UsersDBSec))
		if ns := strings.TrimSpace(opts.UsersDBSecNS); ns != "" {
			args = append(args, "--users-db-secret-namespace", ns)
		}
	} else {
		usersDB := strings.TrimSpace(opts.UsersDB)
		if usersDB == "" {
			usersDB = filepath.Join(installDir, ".ncc-api-users.json")
		}
		if abs, err := filepath.Abs(usersDB); err == nil {
			usersDB = abs
		}
		if !isRegularFile(usersDB) {
			return fmt.Errorf("user database not found: %s (start the stack once to bootstrap it, or pass --users-db / --users-db-secret)", usersDB)
		}
		args = append(args, "--users-db", usersDB)
	}

	c := exec.Command(apiBin, args...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if err := c.Run(); err != nil {
		return fmt.Errorf("reset-password (%s): %w", apiBin, err)
	}
	if running, _ := v2StackRunning(installDir); running {
		fmt.Println("\nNOTE: the stack is currently running and caches accounts in memory.")
		fmt.Println("      Run 'ncc-orchestrator v2-stop' then 'ncc-orchestrator v2-start' for the new password to take effect.")
	} else {
		fmt.Println("\nStart the stack with 'ncc-orchestrator v2-start'; log in with the temporary password and change it at first login.")
	}
	return nil
}

// isRegularFile reports whether path exists and is a regular (non-dir) file.
func isRegularFile(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

// sensitiveBackupName reports whether a backed-up file holds secrets and must be
// (re)written with 0600 perms: the auth token, user DB, bootstrap admin
// password, and any config-referenced secrets file.
func sensitiveBackupName(rel string) bool {
	base := strings.ToLower(filepath.Base(rel))
	switch {
	case strings.HasPrefix(base, ".ncc-api-"),
		base == ".ncc-initial-admin-password",
		strings.Contains(base, "secret"):
		return true
	}
	return false
}

// extractConfigRefPath pulls a `key: value` file path out of raw config.yaml
// content for the small set of file-reference keys. Kept local so the
// orchestrator binary needs no api-server internals. Returns "" when absent or
// explicitly unset.
func extractConfigRefPath(content, key string) string {
	rx := regexp.MustCompile(`(?im)^\s*` + regexp.QuoteMeta(key) + `\s*:\s*(.+?)\s*$`)
	m := rx.FindStringSubmatch(content)
	if m == nil {
		return ""
	}
	v := strings.TrimSpace(m[1])
	// Strip a trailing inline comment (best-effort; paths rarely contain " #").
	if i := strings.Index(v, " #"); i >= 0 {
		v = strings.TrimSpace(v[:i])
	}
	v = strings.Trim(v, `"'`)
	if v == "" || v == "~" || strings.EqualFold(v, "null") {
		return ""
	}
	return v
}

// collectBackupEntries returns the config/auth/state files to capture from an
// install dir. Files that resolve outside the install dir (e.g. a
// config-referenced file placed elsewhere) are reported via skipped so the
// caller can warn; the archive is always confined to the install dir so restore
// can never write outside it.
func collectBackupEntries(installDir string) (entries []backupEntry, skipped []string) {
	seen := map[string]bool{}
	add := func(abs string) {
		abs = filepath.Clean(abs)
		if !isRegularFile(abs) {
			return
		}
		rel, err := filepath.Rel(installDir, abs)
		if err != nil || rel == "." || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
			skipped = append(skipped, abs)
			return
		}
		if seen[rel] {
			return
		}
		seen[rel] = true
		entries = append(entries, backupEntry{Rel: filepath.ToSlash(rel), Abs: abs})
	}

	// Core auth/state files, by convention at the install-dir root.
	for _, name := range []string{
		"config.yaml",
		".ncc-api-users.json",
		".ncc-api-token",
		".ncc-initial-admin-password",
		".ncc-api-schedule.json",
		".ncc-api-notifications.json",
		v2StartStateFile, // persisted v2-start flags (CORS, listen, session TTL, …)
	} {
		add(filepath.Join(installDir, name))
	}

	// Sweep any other api-server state the install may hold at its root
	// (e.g. future ".ncc-api-*" files), so backups stay complete as new
	// runtime state is added without having to revisit this list. seen[]
	// dedupes against the explicit names above.
	if matches, err := filepath.Glob(filepath.Join(installDir, ".ncc-api-*")); err == nil {
		for _, m := range matches {
			add(m)
		}
	}

	// JSONL audit log (security/action history). In a standard v2 install the
	// api-server writes it under <install-dir>/logs/ncc-audit.log; add() is a
	// no-op when it is absent or rolled elsewhere.
	add(filepath.Join(installDir, "logs", "ncc-audit.log"))

	// Config-referenced files (clusters list / alert-exclusions / secrets) plus
	// the latest run's report artifacts.
	if content, err := os.ReadFile(filepath.Join(installDir, "config.yaml")); err == nil {
		cfg := string(content)
		for _, key := range []string{"clusters-file", "exclude-alert-titles-file", "secrets-file"} {
			raw := extractConfigRefPath(cfg, key)
			if raw == "" {
				continue
			}
			p := raw
			if !filepath.IsAbs(p) {
				p = filepath.Join(installDir, p)
			}
			add(p)
		}

		// Latest run data: the dashboard reads run-summary.json plus the HTML /
		// CSV / JSON report artifacts that live at the top level of
		// output-dir-filtered. Capture those regular files (non-recursive) so a
		// restored stack shows the most recent run immediately. The potentially
		// large run-history under <output-dir-filtered>/runs/ is intentionally
		// left out — add() skips directories — keeping the archive to a single
		// run's worth of artifacts rather than the full history.
		outDir := strings.TrimSpace(extractConfigRefPath(cfg, "output-dir-filtered"))
		if outDir == "" {
			outDir = defaultOutputDirFiltered
		}
		if !filepath.IsAbs(outDir) {
			outDir = filepath.Join(installDir, outDir)
		}
		if matches, err := filepath.Glob(filepath.Join(outDir, "*")); err == nil {
			for _, m := range matches {
				add(m) // add() ignores directories (e.g. the runs/ history) and non-regular files
			}
		}
	}
	return entries, skipped
}

// summarizeAuthProviders parses a backed-up .ncc-api-users.json (best-effort,
// using a local shape so the orchestrator needs no api-server internals) to
// report which auth providers and secrets it carries. SAML/LDAP config — and
// their secrets (the SAML SP signing key, the LDAP bind password) — are stored
// inside this single file, so reading it is how backup/restore verifies that
// SSO/directory config actually travelled with the archive. Returns nil when
// the file is absent or unparseable.
func summarizeAuthProviders(usersDBPath string) *authSummary {
	data, err := os.ReadFile(usersDBPath)
	if err != nil {
		return nil
	}
	var db struct {
		Users []json.RawMessage `json:"users"`
		SAML  *struct {
			Enabled  bool   `json:"enabled"`
			SPKeyPEM string `json:"sp_key_pem"`
		} `json:"saml"`
		LDAP *struct {
			Enabled      bool   `json:"enabled"`
			BindPassword string `json:"bind_password"`
		} `json:"ldap"`
	}
	if json.Unmarshal(data, &db) != nil {
		return nil
	}
	s := &authSummary{LocalAccounts: len(db.Users)}
	if db.SAML != nil {
		s.SAMLPresent = true
		s.SAMLEnabled = db.SAML.Enabled
		s.SAMLHasSPKey = strings.TrimSpace(db.SAML.SPKeyPEM) != ""
	}
	if db.LDAP != nil {
		s.LDAPPresent = true
		s.LDAPEnabled = db.LDAP.Enabled
		s.LDAPHasBindPassword = strings.TrimSpace(db.LDAP.BindPassword) != ""
	}
	return s
}

func yesNoStr(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func enabledWord(b bool) string {
	if b {
		return "enabled"
	}
	return "disabled"
}

// printAuthSummary writes a human-readable auth-provider summary to w and
// returns any warnings (a provider is enabled but its server-side secret is
// missing). A nil summary prints nothing.
func printAuthSummary(w io.Writer, s *authSummary) []string {
	if s == nil {
		return nil
	}
	fmt.Fprintf(w, "auth providers:\n")
	fmt.Fprintf(w, "  - local accounts: %d\n", s.LocalAccounts)
	if s.SAMLPresent {
		fmt.Fprintf(w, "  - SAML SSO:       %s (SP signing key: %s)\n", enabledWord(s.SAMLEnabled), yesNoStr(s.SAMLHasSPKey))
	} else {
		fmt.Fprintf(w, "  - SAML SSO:       not configured\n")
	}
	if s.LDAPPresent {
		fmt.Fprintf(w, "  - LDAP / AD:      %s (bind password: %s)\n", enabledWord(s.LDAPEnabled), yesNoStr(s.LDAPHasBindPassword))
	} else {
		fmt.Fprintf(w, "  - LDAP / AD:      not configured\n")
	}
	var warns []string
	if s.SAMLEnabled && !s.SAMLHasSPKey {
		warns = append(warns, "SAML is enabled but no SP signing key is stored; the server may regenerate one and you may need to re-publish SP metadata to the IdP.")
	}
	if s.LDAPEnabled && !s.LDAPHasBindPassword {
		warns = append(warns, "LDAP is enabled with no bind password stored (anonymous bind); confirm this is intended for your directory.")
	}
	return warns
}

// tarWriteBytes writes one in-memory file into a tar stream.
func tarWriteBytes(tw *tar.Writer, name string, data []byte, mode int64) error {
	hdr := &tar.Header{Name: name, Size: int64(len(data)), Mode: mode, ModTime: time.Now()}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	_, err := tw.Write(data)
	return err
}

// writeBackupArchive serializes the manifest + entries into a tar.gz at out
// (created 0600 because it contains secrets).
func writeBackupArchive(out string, manifest backupManifest, entries []backupEntry) error {
	f, err := os.OpenFile(out, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	mb, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	if err := tarWriteBytes(tw, "manifest.json", mb, 0o600); err != nil {
		return err
	}
	for _, e := range entries {
		data, err := os.ReadFile(e.Abs)
		if err != nil {
			return fmt.Errorf("read %s: %w", e.Abs, err)
		}
		mode := int64(0o644)
		if sensitiveBackupName(e.Rel) {
			mode = 0o600
		}
		if err := tarWriteBytes(tw, "data/"+e.Rel, data, mode); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}

func runV2Backup(opts v2BackupOptions) error {
	installDir := strings.TrimSpace(opts.InstallDir)
	if installDir == "" {
		installDir = defaultV2InstallDir()
	}
	if abs, err := filepath.Abs(installDir); err == nil {
		installDir = abs
	}
	if st, err := os.Stat(installDir); err != nil || !st.IsDir() {
		return fmt.Errorf("install dir not found: %s", installDir)
	}
	entries, skipped := collectBackupEntries(installDir)
	if len(entries) == 0 {
		return fmt.Errorf("nothing to back up under %s (no config.yaml, user database, or state files found)", installDir)
	}
	out := strings.TrimSpace(opts.OutputFile)
	if out == "" {
		name := fmt.Sprintf("ncc-backup-%s.tar.gz", time.Now().UTC().Format("20060102T150405Z"))
		if dir := strings.TrimSpace(opts.OutputDir); dir != "" {
			out = filepath.Join(dir, name)
		} else {
			out = name
		}
	}
	if abs, err := filepath.Abs(out); err == nil {
		out = abs
	}
	// Disk guard: refuse to write a backup we don't have room for (a truncated
	// archive is an unrestorable backup). Estimate from the uncompressed source
	// size plus headroom; gzip only makes the real archive smaller.
	var estBytes uint64
	for _, e := range entries {
		if st, err := os.Stat(e.Abs); err == nil {
			estBytes += uint64(st.Size())
		}
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return fmt.Errorf("create backup output dir: %w", err)
	}
	if free, ok := diskFreeBytes(filepath.Dir(out)); ok && free < estBytes+(64<<20) {
		return fmt.Errorf("insufficient disk space for backup: need ~%s, only %s free on %s; prune old backups or free space", humanBytes(estBytes), humanBytes(free), filepath.Dir(out))
	}
	rels := make([]string, 0, len(entries))
	for _, e := range entries {
		rels = append(rels, e.Rel)
	}
	auth := summarizeAuthProviders(filepath.Join(installDir, ".ncc-api-users.json"))
	manifest := backupManifest{
		Tool:       "ncc-orchestrator",
		Version:    Version,
		Stream:     Stream,
		BuildDate:  BuildDate,
		GoVersion:  GoVersion,
		CreatedAt:  time.Now().UTC().Format(time.RFC3339),
		InstallDir: installDir,
		Files:      rels,
		Auth:       auth,
	}
	if err := writeBackupArchive(out, manifest, entries); err != nil {
		return err
	}
	// Verify-after-create: immediately read the archive back and validate it is
	// restorable. If it isn't, delete the bad file so it can't be mistaken for a
	// usable backup, and fail loudly.
	vr, verr := verifyBackupArchive(out)
	if verr != nil {
		_ = os.Remove(out)
		return fmt.Errorf("backup verification failed (removed %s): %w", out, verr)
	}
	// Encrypt-at-rest: seal the verified archive after verification (the sealed
	// file is no longer a readable tar.gz, so it must be sealed last).
	encrypted := false
	if opts.Encrypt {
		key, mode, salt, kerr := resolveBackupEncKey(opts.KeyFile, opts.Passphrase)
		if kerr != nil {
			_ = os.Remove(out)
			return fmt.Errorf("encrypt backup: %w", kerr)
		}
		if eerr := encryptBackupFile(out, key, mode, salt); eerr != nil {
			_ = os.Remove(out)
			return fmt.Errorf("encrypt backup: %w", eerr)
		}
		encrypted = true
	}
	fmt.Printf("Backup written: %s\n", out)
	fmt.Printf("verified:       %d file(s) readable from archive (%s uncompressed)\n", vr.DataFiles, humanBytes(uint64(vr.Bytes)))
	if encrypted {
		fmt.Printf("encrypted:      yes (AES-256-GCM) — restore needs the same passphrase/key\n")
	}
	fmt.Printf("created by:     ncc-orchestrator %s (%s, built %s)\n", Version, Stream, BuildDate)
	fmt.Printf("install-dir:    %s\n", installDir)
	fmt.Printf("files (%d):\n", len(entries))
	for _, e := range entries {
		fmt.Printf("  - %s\n", e.Rel)
	}
	for _, s := range skipped {
		fmt.Printf("  ! skipped (resolves outside install dir, not archived): %s\n", s)
	}
	if auth != nil {
		fmt.Println()
		warns := printAuthSummary(os.Stdout, auth)
		for _, wmsg := range warns {
			fmt.Printf("  ! %s\n", wmsg)
		}
	}
	if opts.Retain > 0 {
		if pruned, perr := pruneOldBackups(filepath.Dir(out), opts.Retain); perr == nil && len(pruned) > 0 {
			fmt.Printf("retention:      kept newest %d, pruned %d older backup(s): %s\n", opts.Retain, len(pruned), strings.Join(pruned, ", "))
		}
	}
	if encrypted {
		fmt.Println("\nThis archive contains secrets (API token, password hashes, SAML SP key, LDAP bind password) but is encrypted at rest (AES-256-GCM). Keep the passphrase/key separate from the archive.")
	} else {
		fmt.Println("\nThis archive contains secrets (API token, password hashes, SAML SP key, LDAP bind password). It was created with 0600 permissions; store it securely, or use --encrypt to seal it.")
	}
	return nil
}

// v2StackRunning reports whether any of the stack's pid files point at a live
// process, so restore can refuse to overwrite a running install dir.
func v2StackRunning(installDir string) (bool, string) {
	runDir := filepath.Join(installDir, "run")
	for _, name := range []string{"v2-supervisor.pid", "v2-api.pid", "v2-ui.pid", "v2-api-supervisor.pid", "v2-ui-supervisor.pid"} {
		if pid, err := readPIDFromFile(filepath.Join(runDir, name)); err == nil && processIsAlive(pid) {
			return true, name
		}
	}
	return false, ""
}

func runV2Restore(opts v2RestoreOptions) error {
	installDir := strings.TrimSpace(opts.InstallDir)
	if installDir == "" {
		installDir = defaultV2InstallDir()
	}
	if abs, err := filepath.Abs(installDir); err == nil {
		installDir = abs
	}
	in := strings.TrimSpace(opts.InputFile)
	if in == "" {
		return fmt.Errorf("--input-file is required (path to a backup archive)")
	}
	if abs, err := filepath.Abs(in); err == nil {
		in = abs
	}
	// Transparently decrypt an encrypted backup (v2-backup --encrypt) into a
	// temp .tar.gz before the rest of the flow runs. An unencrypted archive is
	// untouched. The decrypted temp file is removed when we return.
	if enc, derr := backupArchiveIsEncrypted(in); derr == nil && enc {
		dec, derr := decryptBackupArchive(in, opts.KeyFile, opts.Passphrase)
		if derr != nil {
			return fmt.Errorf("decrypt backup archive: %w", derr)
		}
		defer os.Remove(dec)
		in = dec
	}
	// Restore preflight: validate the archive before touching the install dir.
	// --verify-only stops here and just reports.
	vr, verr := verifyBackupArchive(in)
	if verr != nil {
		return fmt.Errorf("backup archive failed verification: %w", verr)
	}
	if opts.VerifyOnly {
		fmt.Printf("Archive OK: %s\n", in)
		fmt.Printf("  created by:  ncc-orchestrator %s (%s, built %s)\n", vr.Manifest.Version, vr.Manifest.Stream, vr.Manifest.BuildDate)
		fmt.Printf("  created at:  %s\n", vr.Manifest.CreatedAt)
		fmt.Printf("  data files:  %d (%s uncompressed)\n", vr.DataFiles, humanBytes(uint64(vr.Bytes)))
		return nil
	}
	// Capture this host's environment-specific start settings BEFORE the archive
	// overwrites the start-state file. A backup is portable across hosts, so its
	// origins/advertise URLs/listen addresses belong to the source host;
	// adopting them here would make the UI reject this host's browser origin
	// ("origin not allowed"). We re-apply the captured values after extraction.
	preStartState, hadPreStartState := loadV2StartState(installDir)
	// Refuse to clobber a running stack unless forced.
	if !opts.Force {
		if running, which := v2StackRunning(installDir); running {
			return fmt.Errorf("stack appears to be running (%s alive); stop it with 'v2-stop' first, or pass --force", which)
		}
	}

	// Restore in two passes over the on-disk archive so memory stays bounded no
	// matter how large it decompresses to: pass 1 reads only headers (and the
	// small manifest) to validate confinement, enforce caps, and detect
	// overwrites; pass 2 streams each file body straight to its target on disk.
	// Neither pass buffers a whole file in RAM, so a decompression bomb cannot
	// exhaust memory, and the per-file/total caps bound disk use as well.
	type pendingFile struct {
		rel  string
		mode int64
	}
	var manifest *backupManifest
	var files []pendingFile

	// Pass 1: headers + manifest only (no file bodies read into memory).
	if err := func() error {
		f, err := os.Open(in)
		if err != nil {
			return err
		}
		defer f.Close()
		gz, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("open archive %s: %w", in, err)
		}
		defer gz.Close()
		tr := tar.NewReader(gz)
		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("read archive: %w", err)
			}
			if hdr.FileInfo().IsDir() {
				continue
			}
			switch {
			case hdr.Name == "manifest.json":
				data, err := io.ReadAll(io.LimitReader(tr, 1<<20))
				if err != nil {
					return err
				}
				var m backupManifest
				if err := json.Unmarshal(data, &m); err != nil {
					return fmt.Errorf("invalid manifest.json in archive: %w", err)
				}
				manifest = &m
			case strings.HasPrefix(hdr.Name, "data/"):
				rel := strings.TrimPrefix(hdr.Name, "data/")
				clean := filepath.Clean(filepath.FromSlash(rel))
				if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || filepath.IsAbs(clean) {
					return fmt.Errorf("archive contains an unsafe path: %q", hdr.Name)
				}
				dst := filepath.Join(installDir, clean)
				if rel, err := filepath.Rel(installDir, dst); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
					return fmt.Errorf("refusing to write outside install dir: %s", dst)
				}
				if len(files) >= maxBackupFileCount {
					return fmt.Errorf("archive contains too many files (>%d)", maxBackupFileCount)
				}
				files = append(files, pendingFile{rel: clean, mode: hdr.Mode})
			}
		}
		return nil
	}(); err != nil {
		return err
	}
	if manifest == nil {
		return fmt.Errorf("archive is missing manifest.json (not an ncc backup?)")
	}
	if len(files) == 0 {
		return fmt.Errorf("archive contains no files to restore")
	}

	// Detect overwrites up front so a non-forced restore fails before writing
	// anything.
	var existing []string
	for _, p := range files {
		if isRegularFile(filepath.Join(installDir, p.rel)) {
			existing = append(existing, p.rel)
		}
	}
	if len(existing) > 0 && !opts.Force {
		fmt.Println("The following files already exist in the install dir and would be overwritten:")
		for _, e := range existing {
			fmt.Printf("  - %s\n", e)
		}
		return fmt.Errorf("refusing to overwrite %d existing file(s); re-run with --force", len(existing))
	}

	if err := os.MkdirAll(installDir, 0o755); err != nil {
		return err
	}

	// Pass 2: stream each data/ file body straight to disk, enforcing a
	// per-file and a cumulative size cap.
	modeByRel := make(map[string]int64, len(files))
	for _, p := range files {
		modeByRel[p.rel] = p.mode
	}
	if err := func() error {
		f, err := os.Open(in)
		if err != nil {
			return err
		}
		defer f.Close()
		gz, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("open archive %s: %w", in, err)
		}
		defer gz.Close()
		tr := tar.NewReader(gz)
		var total int64
		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("read archive: %w", err)
			}
			if hdr.FileInfo().IsDir() || !strings.HasPrefix(hdr.Name, "data/") {
				continue
			}
			clean := filepath.Clean(filepath.FromSlash(strings.TrimPrefix(hdr.Name, "data/")))
			storedMode, ok := modeByRel[clean]
			if !ok {
				continue // not validated in pass 1 (only happens if the archive mutated underneath us)
			}
			dst := filepath.Join(installDir, clean)
			if rel, err := filepath.Rel(installDir, dst); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				return fmt.Errorf("refusing to write outside install dir: %s", dst)
			}
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return err
			}
			mode := os.FileMode(storedMode).Perm()
			if mode == 0 {
				mode = 0o644
			}
			if sensitiveBackupName(clean) {
				mode = 0o600
			}
			out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
			if err != nil {
				return fmt.Errorf("write %s: %w", dst, err)
			}
			// +1 so an oversize file is detected (n > cap) rather than silently
			// truncated to the cap.
			n, err := io.Copy(out, io.LimitReader(tr, maxBackupFileBytes+1))
			closeErr := out.Close()
			if err != nil {
				return fmt.Errorf("write %s: %w", dst, err)
			}
			if closeErr != nil {
				return fmt.Errorf("write %s: %w", dst, closeErr)
			}
			if n > maxBackupFileBytes {
				_ = os.Remove(dst)
				return fmt.Errorf("archive file %q exceeds the per-file limit (%d bytes)", clean, maxBackupFileBytes)
			}
			total += n
			if total > maxBackupTotalBytes {
				_ = os.Remove(dst)
				return fmt.Errorf("archive exceeds the total restore size limit (%d bytes)", maxBackupTotalBytes)
			}
		}
		return nil
	}(); err != nil {
		return err
	}

	fmt.Printf("Restored %d file(s) into %s\n", len(files), installDir)
	for _, p := range files {
		fmt.Printf("  - %s\n", p.rel)
	}
	fmt.Printf("\nSource backup: tool=%s version=%s stream=%s built=%s created_at=%s install_dir=%s\n",
		manifest.Tool, manifest.Version, defaultStr(manifest.Stream, "?"), defaultStr(manifest.BuildDate, "?"),
		manifest.CreatedAt, manifest.InstallDir)
	// Warn when this orchestrator is OLDER than the one that produced the
	// backup: a newer backup may carry config/state this binary can't read.
	if v := strings.TrimSpace(manifest.Version); v != "" && versionLess(Version, v) {
		fmt.Printf("WARNING: this ncc-orchestrator is %s but the backup was created by %s. "+
			"Restoring a newer backup with an older binary may not fully load; upgrade the orchestrator if you hit issues.\n", Version, v)
	}

	// Surface auth-provider coverage from the file that actually landed on
	// disk, so the operator can confirm SAML/LDAP config and their secrets
	// were restored (they live inside .ncc-api-users.json). Fall back to the
	// manifest's recorded summary if the restored DB can't be re-read.
	restoredAuth := summarizeAuthProviders(filepath.Join(installDir, ".ncc-api-users.json"))
	if restoredAuth == nil {
		restoredAuth = manifest.Auth
	}
	if restoredAuth != nil {
		fmt.Println()
		warns := printAuthSummary(os.Stdout, restoredAuth)
		for _, wmsg := range warns {
			fmt.Printf("  ! %s\n", wmsg)
		}
	}

	// Heal cross-OS paths in the restored config so a backup taken on one OS
	// (e.g. Windows, with drive letters / backslashes) restores cleanly onto
	// another (Linux/macOS): rewrite file-reference paths that pointed inside
	// the backup's install dir to this install dir and normalize separators.
	// Best-effort — it never fails the restore.
	if healed, warns := healRestoredConfigPaths(installDir, defaultStr(manifest.InstallDir, "")); len(healed) > 0 || len(warns) > 0 {
		if len(healed) > 0 {
			fmt.Printf("\nAdjusted %d config path(s) for this host: %s\n", len(healed), strings.Join(healed, ", "))
		}
		for _, wmsg := range warns {
			fmt.Printf("  ! %s\n", wmsg)
		}
	}

	// Re-apply this host's networking settings on top of the start-state the
	// archive just wrote, so a backup taken on another host (or with different
	// listen/origin flags) does not lock this host's browser out with
	// "origin not allowed" after the restart replays the restored state.
	if preserved := preserveHostStartStateNetworking(installDir, preStartState, hadPreStartState); len(preserved) > 0 {
		fmt.Printf("\nKept this host's start settings (overriding the backup's): %s\n", strings.Join(preserved, ", "))
	}

	// Decide whether to restart automatically — a restart is what makes the
	// restored config/accounts/token take effect. Default: restart when the
	// stack is currently running (the common "restore into a live install"
	// case). --restart forces a restart/start even if it looks stopped;
	// --no-restart suppresses it (e.g. staging a box for later).
	stackRunning, _ := v2StackRunning(installDir)
	if !opts.NoRestart && (stackRunning || opts.Restart) {
		if err := restartV2Stack(installDir); err != nil {
			fmt.Printf("\nRestore succeeded, but the automatic restart failed: %v\n", err)
			fmt.Println("Restart the stack manually with 'v2-stop' then 'v2-start'.")
			return nil
		}
		fmt.Println("\nStack restarted; the restored config, accounts, and token are now loaded.")
		return nil
	}
	if opts.NoRestart {
		fmt.Println("Restore complete (--no-restart); start or restart the stack to load the restored config and accounts.")
	} else {
		fmt.Println("Start the stack with 'v2-start' to load the restored config and accounts.")
	}
	return nil
}

// windowsAbsPath reports whether p looks like a Windows absolute path: a
// drive-letter root (C:\ or C:/) or a UNC share (\\server\share). Used during
// restore-path healing so a backup taken on Windows can be re-pathed onto a
// Unix host.
func windowsAbsPath(p string) bool {
	p = strings.TrimSpace(p)
	if len(p) >= 2 && p[1] == ':' {
		c := p[0]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') {
			return true
		}
	}
	return strings.HasPrefix(p, `\\`)
}

// healRestoredConfigPaths rewrites file-reference path values in the restored
// config.yaml so a backup is portable across operating systems. For each known
// path key it: (1) normalizes backslashes to forward slashes; and (2) if the
// value was an absolute path under the backup's original install dir
// (oldInstallDir), rebases it to a path relative to this install dir. Absolute
// paths outside the old install dir (e.g. C:\secrets\creds.yaml) can't be
// re-homed automatically and are reported as warnings. Returns the list of
// keys that were changed plus any warnings. Never errors — healing is
// best-effort and the file is left untouched on any read/write failure.
func healRestoredConfigPaths(installDir, oldInstallDir string) (changed []string, warnings []string) {
	cfgPath := filepath.Join(installDir, "config.yaml")
	content, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, nil
	}
	text := string(content)
	pathKeys := []string{
		"clusters-file", "exclude-alert-titles-file", "secrets-file", "pcs-file",
		"log-file", "output-dir-logs", "output-dir-filtered", "run-history-dir",
		"prom-dir", "ca-bundle", "notification-deadletter-dir",
	}
	// Normalize the old install dir for prefix comparison (slashes, no trailing).
	oldNorm := strings.TrimRight(strings.ReplaceAll(strings.TrimSpace(oldInstallDir), `\`, "/"), "/")
	for _, key := range pathKeys {
		raw := extractConfigRefPath(text, key)
		if raw == "" {
			continue
		}
		orig := raw
		slashed := strings.ReplaceAll(raw, `\`, "/")
		newVal := slashed
		if windowsAbsPath(slashed) || filepath.IsAbs(slashed) {
			matched := false
			if oldNorm != "" {
				lc := strings.ToLower(slashed)
				lo := strings.ToLower(oldNorm)
				if lc == lo {
					newVal = "."
					matched = true
				} else if strings.HasPrefix(lc, lo+"/") {
					newVal = strings.TrimPrefix(slashed[len(oldNorm):], "/")
					matched = true
				}
			}
			if !matched {
				// Can't rebase. Only warn when the path doesn't resolve on
				// this host (a same-host/same-path restore is still valid).
				probe := slashed
				if !filepath.IsAbs(probe) {
					probe = filepath.Join(installDir, probe)
				}
				if _, statErr := os.Stat(probe); statErr != nil {
					warnings = append(warnings, fmt.Sprintf("config key %q points at an absolute path from another host (%s); review it before starting the stack", key, orig))
				}
				// Still normalize separators below even if we can't rebase.
			}
		}
		if newVal != orig {
			text = upsertYAMLScalar(text, key, newVal)
			changed = append(changed, key)
		}
	}
	if len(changed) > 0 {
		if err := os.WriteFile(cfgPath, []byte(text), 0o600); err != nil {
			// Roll back our reported changes — we couldn't persist them.
			return nil, append(warnings, fmt.Sprintf("could not write healed config.yaml: %v", err))
		}
	}
	return changed, warnings
}

// defaultStr returns v when non-empty (trimmed), else def.
func defaultStr(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

// restartV2Stack stops and restarts the v2 stack by re-invoking THIS
// orchestrator binary (v2-stop then v2-start --detach). The restart is
// performed entirely by the binary — no external script — so a CLI restore can
// bring the stack back automatically. v2-start runs detached so the restarted
// stack outlives this short-lived restore process.
// v2StartStateFile records the portable v2-start settings of the most recent
// start at the install-dir root, so a backup can carry them and an
// orchestrator-managed restart (after a restore, or via v2-restart) relaunches
// with the same CORS/listen/session/etc. configuration instead of falling back
// to defaults.
const v2StartStateFile = ".ncc-v2-start.json"

// v2StartState is the curated, portable subset of v2StartOptions persisted to
// v2StartStateFile. Path-type flags (config-path, output/log dirs, token/users
// DB, TLS material, pid/log files) are intentionally omitted: they default
// under the install dir and an absolute path captured on one host/OS is
// meaningless after a cross-OS restore, so a restart re-derives them.
type v2StartState struct {
	APIListen                   string        `json:"api_listen,omitempty"`
	UIListen                    string        `json:"ui_listen,omitempty"`
	APIAdvertiseURL             string        `json:"api_advertise_url,omitempty"`
	UIAdvertiseURL              string        `json:"ui_advertise_url,omitempty"`
	UIBackendURL                string        `json:"ui_backend_url,omitempty"`
	APICORSOrigins              string        `json:"api_cors_origins,omitempty"`
	UIAllowedOrigins            string        `json:"ui_allowed_origins,omitempty"`
	APIAuthMode                 string        `json:"api_auth_mode,omitempty"`
	APISessionTTL               time.Duration `json:"api_session_ttl,omitempty"`
	APISessionSecret            string        `json:"api_session_secret,omitempty"`
	APIRunTimeout               time.Duration `json:"api_run_timeout,omitempty"`
	APIRateLimitPerMinute       int           `json:"api_rate_limit_per_minute"` // 0 = disabled is meaningful, so no omitempty
	APIReadTimeout              time.Duration `json:"api_read_timeout,omitempty"`
	APIWriteTimeout             time.Duration `json:"api_write_timeout,omitempty"`
	APIIdleTimeout              time.Duration `json:"api_idle_timeout,omitempty"`
	UIBackendInsecureSkipVerify bool          `json:"ui_backend_insecure_skip_verify,omitempty"`
	APICookieInsecure           bool          `json:"api_cookie_insecure,omitempty"`
	// UI TLS material is persisted (unlike the api-server's, which is treated
	// as environment-specific) because it is managed at runtime from
	// Settings → Access (TLS): enabling HTTPS writes the cert/key here so the
	// next start/restart binds the browser-facing UI server to TLS.
	UITLSCertFile string `json:"ui_tls_cert_file,omitempty"`
	UITLSKeyFile  string `json:"ui_tls_key_file,omitempty"`
	// UIInsecureHTTP opts out of the default self-signed HTTPS and serves the
	// UI over plain HTTP. Persisted so a restart keeps the operator's choice.
	UIInsecureHTTP             bool          `json:"ui_insecure_http,omitempty"`
	APIOnly                    bool          `json:"api_only,omitempty"`
	SelfHeal                   bool          `json:"self_heal,omitempty"`
	SelfHealMaxRestarts        int           `json:"self_heal_max_restarts,omitempty"`
	SelfHealWindow             time.Duration `json:"self_heal_window,omitempty"`
	SelfHealProbeInterval      time.Duration `json:"self_heal_probe_interval,omitempty"`
	SelfHealUnhealthyThreshold int           `json:"self_heal_unhealthy_threshold,omitempty"`
}

// writeV2StartState records the portable start settings of opts under
// installDir. Best-effort: a write failure must not abort the start.
func writeV2StartState(installDir string, opts v2StartOptions) error {
	st := v2StartState{
		APIListen:                   opts.APIListen,
		UIListen:                    opts.UIListen,
		APIAdvertiseURL:             opts.APIAdvertiseURL,
		UIAdvertiseURL:              opts.UIAdvertiseURL,
		UIBackendURL:                opts.UIBackendURL,
		APICORSOrigins:              opts.APICORSOrigins,
		UIAllowedOrigins:            opts.UIAllowedOrigins,
		APIAuthMode:                 opts.APIAuthMode,
		APISessionTTL:               opts.APISessionTTL,
		APISessionSecret:            opts.APISessionSecret,
		APIRunTimeout:               opts.APIRunTimeout,
		APIRateLimitPerMinute:       opts.APIRateLimitPerMinute,
		APIReadTimeout:              opts.APIReadTimeout,
		APIWriteTimeout:             opts.APIWriteTimeout,
		APIIdleTimeout:              opts.APIIdleTimeout,
		UIBackendInsecureSkipVerify: opts.UIBackendInsecureSkipVerify,
		APICookieInsecure:           opts.APICookieInsecure,
		UITLSCertFile:               opts.UITLSCertFile,
		UITLSKeyFile:                opts.UITLSKeyFile,
		UIInsecureHTTP:              opts.UIInsecureHTTP,
		APIOnly:                     opts.APIOnly,
		SelfHeal:                    opts.SelfHeal,
		SelfHealMaxRestarts:         opts.SelfHealMaxRestarts,
		SelfHealWindow:              opts.SelfHealWindow,
		SelfHealProbeInterval:       opts.SelfHealProbeInterval,
		SelfHealUnhealthyThreshold:  opts.SelfHealUnhealthyThreshold,
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(installDir, v2StartStateFile), append(data, '\n'), 0o600)
}

// preserveHostStartStateNetworking re-applies the current host's
// environment-specific v2-start networking settings (listen addresses,
// advertise URLs, backend URL, CORS/allowed origins, and UI TLS material) on
// top of a start-state that a restore just overwrote. Backups are portable
// across hosts/OSes, so those fields describe the *source* host; keeping them
// would point the restored stack at the wrong addresses and make the UI reject
// this host's browser origin. Non-network settings (auth mode, session TTL,
// timeouts, rate limit, self-heal, …) are intentionally taken from the backup.
// Returns the names of the fields it kept (for logging). Best-effort: it never
// fails the restore.
func preserveHostStartStateNetworking(installDir string, pre v2StartState, hadPre bool) []string {
	if !hadPre {
		return nil
	}
	cur, ok := loadV2StartState(installDir)
	if !ok {
		return nil
	}
	var kept []string
	keep := func(name string, dst *string, src string) {
		if strings.TrimSpace(src) != "" && *dst != src {
			*dst = src
			kept = append(kept, name)
		}
	}
	keep("api-listen", &cur.APIListen, pre.APIListen)
	keep("ui-listen", &cur.UIListen, pre.UIListen)
	keep("api-advertise-url", &cur.APIAdvertiseURL, pre.APIAdvertiseURL)
	keep("ui-advertise-url", &cur.UIAdvertiseURL, pre.UIAdvertiseURL)
	keep("ui-backend-url", &cur.UIBackendURL, pre.UIBackendURL)
	keep("api-cors-origins", &cur.APICORSOrigins, pre.APICORSOrigins)
	keep("ui-allowed-origins", &cur.UIAllowedOrigins, pre.UIAllowedOrigins)
	keep("ui-tls-cert-file", &cur.UITLSCertFile, pre.UITLSCertFile)
	keep("ui-tls-key-file", &cur.UITLSKeyFile, pre.UITLSKeyFile)
	if len(kept) == 0 {
		return nil
	}
	data, err := json.MarshalIndent(cur, "", "  ")
	if err != nil {
		return nil
	}
	if err := os.WriteFile(filepath.Join(installDir, v2StartStateFile), append(data, '\n'), 0o600); err != nil {
		return nil
	}
	return kept
}

// loadV2StartState reads the persisted start settings; ok is false when the
// file is absent or unparseable (a fresh install, or a pre-feature backup).
func loadV2StartState(installDir string) (v2StartState, bool) {
	data, err := os.ReadFile(filepath.Join(installDir, v2StartStateFile))
	if err != nil {
		return v2StartState{}, false
	}
	var st v2StartState
	if json.Unmarshal(data, &st) != nil {
		return v2StartState{}, false
	}
	return st, true
}

// v2StartArgsFromState builds the `v2-start` argument list (always detached)
// from the persisted settings. Returns ok=false when no state is available so
// the caller can fall back to a bare default start.
func v2StartArgsFromState(installDir string) (args []string, ok bool) {
	st, found := loadV2StartState(installDir)
	if !found {
		return nil, false
	}
	args = []string{"v2-start", "--install-dir", installDir, "--detach"}
	addStr := func(flag, v string) {
		if strings.TrimSpace(v) != "" {
			args = append(args, flag, v)
		}
	}
	addDur := func(flag string, d time.Duration) {
		if d > 0 {
			args = append(args, flag, d.String())
		}
	}
	addStr("--api-listen", st.APIListen)
	addStr("--ui-listen", st.UIListen)
	addStr("--api-advertise-url", st.APIAdvertiseURL)
	addStr("--ui-advertise-url", st.UIAdvertiseURL)
	addStr("--ui-backend-url", st.UIBackendURL)
	addStr("--api-cors-origins", st.APICORSOrigins)
	addStr("--ui-allowed-origins", st.UIAllowedOrigins)
	addStr("--api-auth-mode", st.APIAuthMode)
	addStr("--api-session-secret", st.APISessionSecret)
	addDur("--api-session-ttl", st.APISessionTTL)
	addDur("--api-run-timeout", st.APIRunTimeout)
	addDur("--api-read-timeout", st.APIReadTimeout)
	addDur("--api-write-timeout", st.APIWriteTimeout)
	addDur("--api-idle-timeout", st.APIIdleTimeout)
	args = append(args, "--api-rate-limit-per-minute", strconv.Itoa(st.APIRateLimitPerMinute))
	if st.UIBackendInsecureSkipVerify {
		args = append(args, "--ui-backend-insecure-skip-verify")
	}
	if st.APICookieInsecure {
		args = append(args, "--api-cookie-insecure")
	}
	addStr("--ui-tls-cert-file", st.UITLSCertFile)
	addStr("--ui-tls-key-file", st.UITLSKeyFile)
	if st.UIInsecureHTTP {
		args = append(args, "--ui-insecure-http")
	}
	if st.APIOnly {
		args = append(args, "--api-only")
	}
	if st.SelfHeal {
		args = append(args, "--self-heal")
		if st.SelfHealMaxRestarts > 0 {
			args = append(args, "--self-heal-max-restarts", strconv.Itoa(st.SelfHealMaxRestarts))
		}
		addDur("--self-heal-window", st.SelfHealWindow)
		addDur("--self-heal-probe-interval", st.SelfHealProbeInterval)
		if st.SelfHealUnhealthyThreshold > 0 {
			args = append(args, "--self-heal-unhealthy-threshold", strconv.Itoa(st.SelfHealUnhealthyThreshold))
		}
	}
	return args, true
}

func restartV2Stack(installDir string) error {
	if systemdStackIsActive() {
		fmt.Println("\nRestarting stack (systemd-managed)...")
		return restartSystemdStack()
	}
	self, err := os.Executable()
	if err != nil || strings.TrimSpace(self) == "" {
		return fmt.Errorf("locate orchestrator binary: %w", err)
	}
	fmt.Println("\nRestarting stack (binary-managed)...")
	// Stop first; ignore the error since the stack may already be down.
	stop := exec.Command(self, "v2-stop", "--install-dir", installDir)
	stop.Stdout, stop.Stderr = os.Stdout, os.Stderr
	_ = stop.Run()
	// Start detached so the relaunched API/UI survive this process exiting,
	// reusing the persisted start settings (CORS, listen, session TTL, …) so a
	// restore or v2-restart does not silently drop the operator's flags.
	startArgs, ok := v2StartArgsFromState(installDir)
	if !ok {
		startArgs = []string{"v2-start", "--install-dir", installDir, "--detach"}
	}
	start := exec.Command(self, startArgs...)
	start.Stdout, start.Stderr = os.Stdout, os.Stderr
	if err := start.Run(); err != nil {
		return fmt.Errorf("v2-start: %w", err)
	}
	return nil
}

// v2DoctorOptions wires the `doctor` subcommand's flags.
type v2DoctorOptions struct {
	InstallDir   string
	APIListen    string
	UIListen     string
	OutputFile   string // when set, writes a redacted support tarball
	NoBundle     bool
	ConfigPath   string // override for the self-heal config checks
	Fix          bool   // apply safe self-heal remediations
	JSON         bool   // emit the self-heal report as JSON (skips the human report + bundle)
	OnlyChecks   string // optional comma-separated check ids
	NoDisruptive bool   // skip disruptive checks/fixes (service restarts, etc.)
}

// runV2Doctor is the "something's broken, give me everything"
// diagnostic. Runs every existing read-only check, prints a unified
// report, and (by default) writes a redacted support tarball under
// <cwd>/ncc-support-<timestamp>.tar.gz that the operator can attach
// to a support ticket.
//
// Sections:
//
//  1. version + verify (embedded buildinfo + self SHA-256)
//  2. v2-check (install-dir layout + path readability)
//  3. v2-status (PIDs alive? api healthy?)
//  4. environment summary (GOOS/GOARCH, NCC_* env var NAMES only)
//  5. recent log tails (last 200 lines of each v2-*.log)
//  6. (optional) tar.gz bundle with the above + redacted config
//
// Always exits 0 unless the bundle write itself fails. Non-zero exit
// on missing services would defeat the "I'll run doctor when things
// look broken" use case.
func runV2Doctor(opts v2DoctorOptions) error {
	installDir := strings.TrimSpace(opts.InstallDir)
	if installDir == "" {
		installDir = defaultV2InstallDir()
	}
	apiListen := strings.TrimSpace(opts.APIListen)
	if apiListen == "" {
		apiListen = ":8081"
	}
	uiListen := strings.TrimSpace(opts.UIListen)
	if uiListen == "" {
		uiListen = ":8080"
	}

	// JSON mode is the machine-readable self-heal report only: no human report,
	// no support bundle. Used by automation and the api-server diagnostics view.
	if opts.JSON {
		hr := runSelfHealWithOptions(installDir, opts.ConfigPath, healRunOptions{
			Fix:          opts.Fix,
			OnlyChecks:   parseDoctorOnlyChecks(opts.OnlyChecks),
			NoDisruptive: opts.NoDisruptive,
		})
		out, err := json.MarshalIndent(hr, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(out))
		if hr.Worst() == healFail {
			return fmt.Errorf("self-heal found %d failing check(s)", hr.Summary["fail"])
		}
		return nil
	}

	var report bytes.Buffer
	w := io.MultiWriter(os.Stdout, &report)

	fmt.Fprintln(w, "========================================")
	fmt.Fprintln(w, "ncc-orchestrator doctor")
	fmt.Fprintln(w, "========================================")
	fmt.Fprintf(w, "generated_at:  %s\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(w, "install_dir:   %s\n", installDir)
	fmt.Fprintf(w, "cwd:           %s\n", strDefaultStr(getwdSafe(), "(unknown)"))
	fmt.Fprintln(w)

	fmt.Fprintln(w, "-- 1. verify (build provenance) --")
	_ = runVerifyCommand(w, verifyOptions{})
	fmt.Fprintln(w)

	fmt.Fprintln(w, "-- 2. v2-check (install-dir layout) --")
	checkErr := runV2Check(v2StartOptions{
		InstallDir: installDir,
		APIListen:  apiListen,
		UIListen:   uiListen,
	})
	if checkErr != nil {
		fmt.Fprintf(w, "v2-check exit: %v\n", checkErr)
	} else {
		fmt.Fprintln(w, "v2-check: ok")
	}
	fmt.Fprintln(w)

	fmt.Fprintln(w, "-- 3. v2-status (running services) --")
	_ = runV2Status(v2StatusOptions{
		InstallDir: installDir,
		APIListen:  apiListen,
		UIListen:   uiListen,
	})
	fmt.Fprintln(w)

	fmt.Fprintln(w, "-- 4. environment summary --")
	fmt.Fprintf(w, "go_version:    %s\n", runtime.Version())
	fmt.Fprintf(w, "os/arch:       %s/%s\n", runtime.GOOS, runtime.GOARCH)
	fmt.Fprintf(w, "num_cpu:       %d\n", runtime.NumCPU())
	fmt.Fprintln(w)
	fmt.Fprintln(w, "NCC_* env var names (values REDACTED for support-ticket safety):")
	doctorPrintRedactedEnv(w)
	fmt.Fprintln(w)

	fmt.Fprintln(w, "-- 5. recent log tails (last 200 lines each) --")
	logDir := filepath.Join(installDir, "logs")
	for _, name := range []string{"v2-supervisor.log", "v2-api.log", "v2-ui.log", "v2-api-supervisor.log", "v2-ui-supervisor.log"} {
		p := filepath.Join(logDir, name)
		fmt.Fprintf(w, "\n--- %s ---\n", p)
		doctorTailFile(w, p, 200)
	}
	fmt.Fprintln(w)

	heading := "-- 6. self-heal checks --"
	if opts.Fix {
		heading = "-- 6. self-heal checks (--fix: applying safe remediations) --"
	}
	fmt.Fprintln(w, heading)
	hr := runSelfHealWithOptions(installDir, opts.ConfigPath, healRunOptions{
		Fix:          opts.Fix,
		OnlyChecks:   parseDoctorOnlyChecks(opts.OnlyChecks),
		NoDisruptive: opts.NoDisruptive,
	})
	doctorPrintSelfHeal(w, hr)
	fmt.Fprintln(w)

	fmt.Fprintln(w, "========================================")
	fmt.Fprintln(w, "doctor: report complete")
	fmt.Fprintln(w, "========================================")

	if opts.NoBundle {
		return nil
	}

	bundlePath := strings.TrimSpace(opts.OutputFile)
	if bundlePath == "" {
		bundlePath = filepath.Join(".",
			fmt.Sprintf("ncc-support-%s.tar.gz", time.Now().UTC().Format("20060102T150405Z")))
	}
	if err := writeDoctorBundle(bundlePath, report.String(), installDir); err != nil {
		fmt.Fprintf(os.Stderr, "doctor: failed to write bundle %s: %v\n", bundlePath, err)
		return err
	}
	fmt.Fprintf(os.Stdout, "\nsupport bundle: %s\n", bundlePath)
	fmt.Fprintln(os.Stdout, "  attach this file to your support ticket; secrets and tokens have been redacted.")
	return nil
}

// doctorPrintSelfHeal renders a self-heal report as aligned, human-readable
// lines: a status glyph, the check title, its message, and any remediation
// applied or hint to follow up.
func doctorPrintSelfHeal(w io.Writer, hr healReport) {
	glyph := func(s healStatus) string {
		switch s {
		case healOK:
			return "[ ok ]"
		case healWarn:
			return "[warn]"
		case healFail:
			return "[FAIL]"
		default:
			return "[ ?? ]"
		}
	}
	fmt.Fprintf(w, "config: %s\n", hr.ConfigPath)
	for _, res := range hr.Results {
		fmt.Fprintf(w, "  %s %-28s %s\n", glyph(res.Status), res.Title, res.Message)
		if res.Fixed && strings.TrimSpace(res.FixMsg) != "" {
			fmt.Fprintf(w, "         fixed: %s\n", res.FixMsg)
		}
		if !res.Fixed && strings.TrimSpace(res.Hint) != "" {
			fmt.Fprintf(w, "         hint:  %s\n", res.Hint)
		}
	}
	fmt.Fprintf(w, "summary: %d ok, %d warn, %d fail\n", hr.Summary["ok"], hr.Summary["warn"], hr.Summary["fail"])
}

func parseDoctorOnlyChecks(raw string) map[string]bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	out := map[string]bool{}
	for _, part := range strings.Split(raw, ",") {
		id := strings.TrimSpace(part)
		if id == "" {
			continue
		}
		out[id] = true
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// doctorPrintRedactedEnv prints NCC_* env var NAMES only — values
// are redacted. Operators copy/paste support output straight into
// tickets, so values (which often contain Prism passwords or
// secret://refs) must never appear.
func doctorPrintRedactedEnv(w io.Writer) {
	keys := []string{}
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "NCC_") {
			continue
		}
		eq := strings.IndexByte(kv, '=')
		if eq <= 0 {
			continue
		}
		keys = append(keys, kv[:eq])
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		fmt.Fprintln(w, "  (none set)")
		return
	}
	for _, k := range keys {
		fmt.Fprintf(w, "  %s=***REDACTED***\n", k)
	}
}

// doctorTailFile prints up to maxLines trailing lines of path. If
// the file is missing or unreadable, prints a single-line marker.
// Tail rather than read-all so the doctor output (and the bundle)
// stays bounded even when a log file has rotated to gigabytes.
func doctorTailFile(w io.Writer, path string, maxLines int) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fmt.Fprintln(w, "  (not present)")
			return
		}
		fmt.Fprintf(w, "  (read error: %v)\n", err)
		return
	}
	defer f.Close()
	st, _ := f.Stat()
	if st != nil {
		const window = int64(64 * 1024)
		if st.Size() > window {
			_, _ = f.Seek(-window, io.SeekEnd)
		}
	}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<16), 1<<20)
	lines := make([]string, 0, maxLines)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) > maxLines {
			lines = lines[len(lines)-maxLines:]
		}
	}
	if len(lines) == 0 {
		fmt.Fprintln(w, "  (empty)")
		return
	}
	for _, l := range lines {
		fmt.Fprintln(w, l)
	}
}

// writeDoctorBundle emits a redacted support tarball:
//
//   - report.txt: same banner the operator just saw on stdout
//   - logs/*.log: last 1000 lines of each v2-{api,ui,*-supervisor}.log
//   - config.redacted.yaml: copy of <install-dir>/config.yaml or
//     example_config.yaml with values for any key matching
//     /password|secret|token|key|credential|cert/i replaced by
//     "***REDACTED***"
//
// gzip-compressed; safe to attach to a support ticket.
func writeDoctorBundle(path, report, installDir string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()

	addFile := func(name string, data []byte, mode int64) error {
		hdr := &tar.Header{
			Name:    name,
			Size:    int64(len(data)),
			Mode:    mode,
			ModTime: time.Now(),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		_, err := tw.Write(data)
		return err
	}

	if err := addFile("report.txt", []byte(report), 0o644); err != nil {
		return err
	}

	logDir := filepath.Join(installDir, "logs")
	for _, name := range []string{"v2-supervisor.log", "v2-api.log", "v2-ui.log", "v2-api-supervisor.log", "v2-ui-supervisor.log"} {
		p := filepath.Join(logDir, name)
		var buf bytes.Buffer
		doctorTailFile(&buf, p, 1000)
		if err := addFile(filepath.Join("logs", name), buf.Bytes(), 0o644); err != nil {
			return err
		}
	}

	cfgPath := filepath.Join(installDir, "config.yaml")
	if _, err := os.Stat(cfgPath); err != nil {
		cfgPath = filepath.Join(installDir, "example_config.yaml")
	}
	if data, err := os.ReadFile(cfgPath); err == nil {
		redacted := redactConfigYAML(data)
		if err := addFile("config.redacted.yaml", redacted, 0o644); err != nil {
			return err
		}
	}
	return nil
}

// redactConfigYAML masks values for keys whose name contains
// password / secret / token / credential / api-key / client-id (case
// insensitive). Conservative: only handles the canonical "key: value"
// line form. Good enough for the "don't leak the Prism password"
// support-bundle use case.
func redactConfigYAML(in []byte) []byte {
	rx := regexp.MustCompile(`(?i)^(\s*[-]?\s*(?:[a-z0-9_-]*(?:password|secret|token|credential|api[_-]?key|client[_-]?id)[a-z0-9_-]*))\s*:\s*(.*)$`)
	out := bytes.Buffer{}
	scanner := bufio.NewScanner(bytes.NewReader(in))
	scanner.Buffer(make([]byte, 1<<16), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if m := rx.FindStringSubmatch(line); m != nil && strings.TrimSpace(m[2]) != "" {
			out.WriteString(m[1])
			out.WriteString(": ***REDACTED***\n")
			continue
		}
		out.WriteString(line)
		out.WriteByte('\n')
	}
	return out.Bytes()
}

func getwdSafe() string {
	d, err := os.Getwd()
	if err != nil {
		return ""
	}
	return d
}

func strDefaultStr(s, dflt string) string {
	if strings.TrimSpace(s) == "" {
		return dflt
	}
	return s
}

func runV2Check(opts v2StartOptions) error {
	installDir := strings.TrimSpace(opts.InstallDir)
	if installDir == "" {
		installDir = defaultV2InstallDir()
	}
	if absInstallDir, err := filepath.Abs(installDir); err == nil {
		installDir = absInstallDir
	}
	// Secondary paths default to <install-dir>/<conventional-name> when
	// the user did not pass an explicit value. This makes `v2-check` (and
	// `v2-start`) "just work" when run from inside a bootstrapped stack
	// layout: ./bin/ncc-orchestrator v2-check now finds config, output,
	// log, and token paths colocated with the install instead of relative
	// to bin/.
	failures := make([]string, 0)
	warnings := make([]string, 0)
	if strings.TrimSpace(opts.ConfigPath) == "" {
		opts.ConfigPath = filepath.Join(installDir, "config.yaml")
	}
	if strings.TrimSpace(opts.OutputDir) == "" {
		opts.OutputDir = filepath.Join(installDir, "outputfiles")
	}
	if strings.TrimSpace(opts.LogDir) == "" {
		opts.LogDir = filepath.Join(installDir, "nccfiles")
	}
	if strings.TrimSpace(opts.TokenFile) == "" {
		opts.TokenFile = filepath.Join(installDir, ".ncc-api-token")
	}
	if absConfigPath, err := filepath.Abs(opts.ConfigPath); err == nil {
		opts.ConfigPath = absConfigPath
	}
	if absOutputDir, err := filepath.Abs(opts.OutputDir); err == nil {
		opts.OutputDir = absOutputDir
	}
	if absLogDir, err := filepath.Abs(opts.LogDir); err == nil {
		opts.LogDir = absLogDir
	}
	if absTokenPath, err := filepath.Abs(opts.TokenFile); err == nil {
		opts.TokenFile = absTokenPath
	}
	// If config.yaml is missing but the install ships an example_config.yaml
	// (every published v2 stack does), fall back to it with a warning rather
	// than failing v2-check outright. This is the common "user just extracted
	// the stack and ran v2-check" path.
	if _, err := os.Stat(opts.ConfigPath); err != nil {
		exampleCfg := filepath.Join(installDir, "example_config.yaml")
		if _, exErr := os.Stat(exampleCfg); exErr == nil {
			warnings = append(warnings, fmt.Sprintf("config-path %s not found; falling back to %s for the v2-check (replace with your own config before running v2-start in production)", opts.ConfigPath, exampleCfg))
			opts.ConfigPath = exampleCfg
		}
	}
	opts.OrchestratorBin = resolveV2OrchestratorBin(opts.OrchestratorBin)
	apiBin, uiBin, frontDir, _ := resolveV2RuntimeLayout(installDir)
	if !isExecutableFile(opts.OrchestratorBin) {
		failures = append(failures, fmt.Sprintf("orchestrator-bin not executable: %s", opts.OrchestratorBin))
	}
	if apiBin == "" || !isExecutableFile(apiBin) {
		failures = append(failures, fmt.Sprintf("api-server binary not executable under install dir: %s", installDir))
	}
	if !opts.APIOnly {
		if uiBin == "" || !isExecutableFile(uiBin) {
			failures = append(failures, fmt.Sprintf("ui-server binary not executable under install dir: %s", installDir))
		}
		if strings.TrimSpace(frontDir) == "" || !existingDir(frontDir) {
			failures = append(failures, fmt.Sprintf("frontend-dist missing under install dir: %s", installDir))
		}
	}
	if _, err := os.Stat(opts.ConfigPath); err != nil {
		failures = append(failures, fmt.Sprintf("config-path not readable: %s (%v)", opts.ConfigPath, err))
	}
	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		failures = append(failures, fmt.Sprintf("output-dir not writable: %s (%v)", opts.OutputDir, err))
	}
	if err := os.MkdirAll(opts.LogDir, 0o755); err != nil {
		failures = append(failures, fmt.Sprintf("log-dir not writable: %s (%v)", opts.LogDir, err))
	}
	if err := os.MkdirAll(filepath.Dir(opts.TokenFile), 0o755); err != nil {
		failures = append(failures, fmt.Sprintf("token-file parent dir not writable: %s (%v)", filepath.Dir(opts.TokenFile), err))
	}
	if err := canBindListenAddress(opts.APIListen); err != nil {
		failures = append(failures, fmt.Sprintf("api-listen bind failed (%s): %v", opts.APIListen, err))
	}
	if !opts.APIOnly {
		if err := canBindListenAddress(opts.UIListen); err != nil {
			failures = append(failures, fmt.Sprintf("ui-listen bind failed (%s): %v", opts.UIListen, err))
		}
	}
	if cfg, err := loadConfigForValidation(opts.ConfigPath); err == nil {
		pwd := strings.TrimSpace(cfg.Password)
		if pwd != "" && !strings.HasPrefix(strings.ToLower(pwd), "secret://") {
			warnings = append(warnings, "config contains plaintext password; prefer NCC_PASSWORD env or secret:// source")
		}
	}
	fmt.Println("v2-check results")
	fmt.Println("---------------")
	fmt.Printf("install-dir: %s\n", installDir)
	fmt.Printf("config-path: %s\n", opts.ConfigPath)
	fmt.Printf("output-dir:  %s\n", opts.OutputDir)
	fmt.Printf("log-dir:     %s\n", opts.LogDir)
	fmt.Printf("token-file:  %s\n", opts.TokenFile)
	if len(warnings) > 0 {
		fmt.Println("warnings:")
		for _, w := range warnings {
			fmt.Printf("- %s\n", w)
		}
	}
	if len(failures) > 0 {
		fmt.Println("failures:")
		for _, f := range failures {
			fmt.Printf("- %s\n", f)
		}
		return fmt.Errorf("v2-check failed (%d issues)", len(failures))
	}
	fmt.Println("status: ok (all checks passed)")
	return nil
}

func waitForPIDExit(pid int, timeout time.Duration) bool {
	if pid <= 0 {
		return true
	}
	if runtime.GOOS == "windows" {
		time.Sleep(timeout)
		return false
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		proc, err := os.FindProcess(pid)
		if err != nil {
			return true
		}
		if err := proc.Signal(syscall.Signal(0)); err != nil {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// buildAPIHealthProbeCmd returns a shell command that runs the api-server's
// built-in `--health-check` self-probe (GET /api/v1/health over loopback,
// exit 0 healthy / 1 unhealthy). The supervisor runs this against a still-alive
// api-server to detect hangs the liveness check cannot. Returns "" if the
// binary path is empty.
func buildAPIHealthProbeCmd(apiBin, listen, tokenFile, repoRoot string) string {
	if strings.TrimSpace(apiBin) == "" {
		return ""
	}
	parts := []string{shellQuote(apiBin), "--health-check"}
	if strings.TrimSpace(listen) != "" {
		parts = append(parts, "--listen", shellQuote(listen))
	}
	if strings.TrimSpace(tokenFile) != "" {
		parts = append(parts, "--token-file-path", shellQuote(tokenFile))
	}
	if strings.TrimSpace(repoRoot) != "" {
		parts = append(parts, "--repo-root", shellQuote(repoRoot))
	}
	return strings.Join(parts, " ")
}

// buildUIHealthProbeCmd returns a shell command that runs the ui-server's
// built-in `--health-check` self-probe (GET / over loopback, exit 0 healthy / 1
// unhealthy) so the supervisor can detect a hung UI process, not just a crashed
// one. tls toggles the https scheme. Returns "" if the binary path is empty.
func buildUIHealthProbeCmd(uiBin, listen string, tls bool) string {
	if strings.TrimSpace(uiBin) == "" {
		return ""
	}
	parts := []string{shellQuote(uiBin), "--health-check"}
	if strings.TrimSpace(listen) != "" {
		parts = append(parts, "--listen", shellQuote(listen))
	}
	if tls {
		// Presence of the cert/key flags switches the probe to https; pass
		// placeholder paths so the ui-server picks the https scheme. The probe
		// skips TLS verification, so the values only need to be non-empty.
		parts = append(parts, "--tls-cert-file", shellQuote("tls.crt"), "--tls-key-file", shellQuote("tls.key"))
	}
	return strings.Join(parts, " ")
}

// selfHealSupervisorScript builds the POSIX-sh supervisor loop. It is split out
// from process launch so its behavior can be unit-tested (the generated script
// is asserted by TestSelfHealSupervisorScript). healthCmd is an optional
// shell command run against a *still-alive* process to detect hangs/deadlocks
// (empty = liveness-only). probeIntervalSec/unhealthyThreshold gate that probe.
func selfHealSupervisorScript(serviceName, cmdLine, pidPath, logPath, healthCmd string, maxRestarts, windowSeconds, probeIntervalSec, unhealthyThreshold int) string {
	return strings.Join([]string{
		"set -eu",
		"MAX_RESTARTS=" + strconv.Itoa(maxRestarts),
		"WINDOW_SECONDS=" + strconv.Itoa(windowSeconds),
		"PROBE_INTERVAL=" + strconv.Itoa(probeIntervalSec),
		"UNHEALTHY_MAX=" + strconv.Itoa(unhealthyThreshold),
		// Cap exponential restart backoff and cool down (then resume) after the
		// restart budget is exhausted instead of giving up permanently, so a
		// prolonged-but-transient fault (disk full, downstream outage) still
		// self-heals once it clears.
		"BACKOFF_CAP=30",
		"COOLDOWN_SECONDS=$WINDOW_SECONDS",
		"PID_FILE=" + shellQuote(pidPath),
		"LOG_FILE=" + shellQuote(logPath),
		"CMD=" + shellQuote(cmdLine),
		"HEALTH_CMD=" + shellQuote(healthCmd),
		// SERVICE_NAME is passed as a shell-quoted variable (not concatenated
		// into the echo lines) so a name containing quotes or $(...) cannot
		// break out of the generated script.
		"SERVICE_NAME=" + shellQuote(serviceName),
		"log() { echo \"$(date -u +%Y-%m-%dT%H:%M:%SZ) $SERVICE_NAME $1\" >> \"$LOG_FILE\"; }",
		"restarts=0",
		"window_start=$(date +%s)",
		"unhealthy=0",
		"backoff=1",
		"last_probe=0",
		"while true; do",
		"  pid=\"\"",
		"  if [ -f \"$PID_FILE\" ]; then",
		"    pid=$(sed -n '1p' \"$PID_FILE\" | tr -d '\\r\\n ' || true)",
		"  fi",
		"  if [ -n \"$pid\" ] && kill -0 \"$pid\" 2>/dev/null; then",
		// Process is alive. If a health probe is configured, run it on the
		// configured interval; consecutive failures mean the process is hung
		// (alive but not serving) so kill it and let the restart path fire.
		"    if [ -n \"$HEALTH_CMD\" ]; then",
		"      now=$(date +%s)",
		"      if [ $((now-last_probe)) -ge $PROBE_INTERVAL ]; then",
		"        last_probe=$now",
		"        if sh -c \"$HEALTH_CMD\" >/dev/null 2>&1; then",
		"          unhealthy=0",
		"        else",
		"          unhealthy=$((unhealthy+1))",
		"          log \"health probe failed ($unhealthy/$UNHEALTHY_MAX) pid=$pid\"",
		"          if [ $unhealthy -ge $UNHEALTHY_MAX ]; then",
		"            log \"unhealthy threshold reached; restarting pid=$pid\"",
		"            kill \"$pid\" 2>/dev/null || true",
		"            sleep 2",
		"            kill -0 \"$pid\" 2>/dev/null && kill -9 \"$pid\" 2>/dev/null || true",
		"            unhealthy=0",
		"            continue",
		"          fi",
		"        fi",
		"      fi",
		"    fi",
		"    sleep 2",
		"    continue",
		"  fi",
		"  now=$(date +%s)",
		"  if [ $((now-window_start)) -gt $WINDOW_SECONDS ]; then",
		"    window_start=$now",
		"    restarts=0",
		"    backoff=1",
		"  fi",
		"  restarts=$((restarts+1))",
		"  if [ $restarts -gt $MAX_RESTARTS ]; then",
		"    log \"self-heal exhausted restarts ($MAX_RESTARTS within ${WINDOW_SECONDS}s); cooling down ${COOLDOWN_SECONDS}s before resuming\"",
		"    sleep \"$COOLDOWN_SECONDS\"",
		"    window_start=$(date +%s)",
		"    restarts=0",
		"    backoff=1",
		"    unhealthy=0",
		"    continue",
		"  fi",
		"  sh -c \"$CMD\" >> \"$LOG_FILE\" 2>&1 &",
		"  newpid=$!",
		"  echo \"$newpid\" > \"$PID_FILE\"",
		"  log \"self-heal restart #$restarts pid=$newpid (backoff ${backoff}s)\"",
		"  unhealthy=0",
		"  last_probe=$(date +%s)",
		"  sleep \"$backoff\"",
		"  backoff=$((backoff*2))",
		"  if [ $backoff -gt $BACKOFF_CAP ]; then backoff=$BACKOFF_CAP; fi",
		"done",
	}, "\n")
}

func startSelfHealSupervisor(serviceName string, bin string, args []string, pidPath string, logPath string, maxRestarts int, window time.Duration, healthCmd string, probeInterval time.Duration, unhealthyThreshold int) (int, error) {
	if runtime.GOOS == "windows" {
		return 0, fmt.Errorf("self-heal supervisor is currently unsupported on windows")
	}
	if maxRestarts <= 0 {
		maxRestarts = 3
	}
	if window <= 0 {
		window = 5 * time.Minute
	}
	windowSeconds := int(window.Seconds())
	if windowSeconds < 30 {
		windowSeconds = 30
	}
	probeIntervalSec := int(probeInterval.Seconds())
	if probeIntervalSec < 1 {
		probeIntervalSec = 10
	}
	if unhealthyThreshold < 1 {
		unhealthyThreshold = 3
	}
	quotedArgs := make([]string, 0, len(args))
	for _, a := range args {
		quotedArgs = append(quotedArgs, shellQuote(a))
	}
	cmdLine := shellQuote(bin)
	if len(quotedArgs) > 0 {
		cmdLine += " " + strings.Join(quotedArgs, " ")
	}
	script := selfHealSupervisorScript(serviceName, cmdLine, pidPath, logPath, healthCmd, maxRestarts, windowSeconds, probeIntervalSec, unhealthyThreshold)
	monitorCmd := exec.Command("sh", "-c", script)
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return 0, err
	}
	defer devNull.Close()
	monitorCmd.Stdin = devNull
	monitorCmd.Stdout = devNull
	monitorCmd.Stderr = devNull
	if err := monitorCmd.Start(); err != nil {
		return 0, err
	}
	pid := monitorCmd.Process.Pid
	_ = monitorCmd.Process.Release()
	return pid, nil
}

func runV2Stop(opts v2StopOptions) error {
	installDir := strings.TrimSpace(opts.InstallDir)
	if installDir == "" {
		installDir = defaultV2InstallDir()
	}
	stopTimeout := opts.StopTimeout
	if stopTimeout <= 0 {
		stopTimeout = 5 * time.Second
	}
	runDir := filepath.Join(installDir, "run")
	apiPIDPath := strings.TrimSpace(opts.APIPIDFile)
	if apiPIDPath == "" {
		apiPIDPath = filepath.Join(runDir, "v2-api.pid")
	}
	uiPIDPath := strings.TrimSpace(opts.UIPIDFile)
	if uiPIDPath == "" {
		uiPIDPath = filepath.Join(runDir, "v2-ui.pid")
	}
	targets := []struct {
		name     string
		pidPath  string
		identity []string
	}{
		// Stop the native foreground supervisor first: on SIGTERM it gracefully
		// stops its own children and clears their pid files, so the api/ui
		// targets below become no-ops (or clean up anything it missed).
		{name: "supervisor", pidPath: filepath.Join(runDir, "v2-supervisor.pid"), identity: []string{"ncc-orchestrator", "v2-supervise"}},
		{name: "api-supervisor", pidPath: filepath.Join(runDir, "v2-api-supervisor.pid"), identity: []string{"v2-api-supervisor", "ncc-api-server"}},
		{name: "ui-supervisor", pidPath: filepath.Join(runDir, "v2-ui-supervisor.pid"), identity: []string{"v2-ui-supervisor", "ncc-ui-server"}},
		{name: "api", pidPath: apiPIDPath, identity: []string{"ncc-api-server"}},
		{name: "ui", pidPath: uiPIDPath, identity: []string{"ncc-ui-server"}},
	}

	foundAny := false
	stoppedAny := false
	cleanedStaleAny := false
	for _, t := range targets {
		pid, err := readPIDFromFile(t.pidPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			fmt.Fprintf(os.Stderr, "skip %s: %v\n", t.name, err)
			continue
		}
		foundAny = true
		if processIsAlive(pid) {
			known, matches := processIdentityMatches(pid, t.identity...)
			if known && !matches {
				_ = os.Remove(t.pidPath)
				cleanedStaleAny = true
				fmt.Fprintf(os.Stderr, "removed stale %s pid file (pid=%d belongs to an unrelated process)\n", t.name, pid)
				continue
			}
		}
		if err := signalPIDStop(pid, opts.Force); err != nil {
			if errors.Is(err, os.ErrProcessDone) {
				_ = os.Remove(t.pidPath)
				cleanedStaleAny = true
				fmt.Fprintf(os.Stderr, "removed stale %s pid file (pid=%d already exited)\n", t.name, pid)
				continue
			}
			fmt.Fprintf(os.Stderr, "failed to stop %s (pid=%d): %v\n", t.name, pid, err)
			continue
		}
		if !opts.Force && !waitForPIDExit(pid, stopTimeout) {
			if err := signalPIDStop(pid, true); err != nil {
				fmt.Fprintf(os.Stderr, "failed to force-kill %s (pid=%d): %v\n", t.name, pid, err)
				continue
			}
		}
		stoppedAny = true
		_ = os.Remove(t.pidPath)
		fmt.Fprintf(os.Stderr, "stopped %s (pid=%d)\n", t.name, pid)
	}
	if !foundAny {
		return fmt.Errorf("no detached pid files found under %s", runDir)
	}
	if !stoppedAny && !cleanedStaleAny {
		return errors.New("found detached pid files but failed to stop services")
	}
	if !stoppedAny && cleanedStaleAny {
		fmt.Fprintln(os.Stderr, "detached services were already stopped; stale pid files cleaned.")
	}
	return nil
}

func runUninstallCommand(cmd *cobra.Command, args []string) error {
	configPath, _ := cmd.Flags().GetString("config")
	installDir, _ := cmd.Flags().GetString("install-dir")
	taskName, _ := cmd.Flags().GetString("task-name")
	force, _ := cmd.Flags().GetBool("force")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	removeLocal, _ := cmd.Flags().GetBool("remove-local")
	removeSchedule, _ := cmd.Flags().GetBool("remove-schedule")
	removeV2Runtime, _ := cmd.Flags().GetBool("remove-v2-runtime")

	opts := uninstallOptions{
		ConfigPath:      strings.TrimSpace(configPath),
		InstallDir:      strings.TrimSpace(installDir),
		TaskName:        strings.TrimSpace(taskName),
		Force:           force,
		DryRun:          dryRun,
		RemoveLocal:     removeLocal,
		RemoveSchedule:  removeSchedule,
		RemoveV2Runtime: removeV2Runtime,
	}
	if opts.InstallDir == "" {
		opts.InstallDir = defaultV2InstallDir()
	}
	if opts.TaskName == "" {
		opts.TaskName = "ncc-orchestrator"
	}
	fmt.Fprintf(os.Stderr, "Uninstall plan:\n")
	fmt.Fprintf(os.Stderr, "  remove-local    : %t\n", opts.RemoveLocal)
	fmt.Fprintf(os.Stderr, "  remove-schedule : %t\n", opts.RemoveSchedule)
	fmt.Fprintf(os.Stderr, "  remove-v2-runtime: %t\n", opts.RemoveV2Runtime)
	fmt.Fprintf(os.Stderr, "  dry-run         : %t\n", opts.DryRun)

	if opts.RemoveSchedule {
		if opts.DryRun {
			fmt.Fprintf(os.Stderr, "[dry-run] would remove scheduler task marker %q\n", scheduleMarker(opts.TaskName))
		} else {
			if runtime.GOOS == "windows" {
				if err := removeWindowsSchedule(opts.TaskName); err != nil {
					fmt.Fprintf(os.Stderr, "schedule remove warning: %v\n", err)
				}
			} else {
				if err := removeCronSchedule(opts.TaskName); err != nil {
					fmt.Fprintf(os.Stderr, "schedule remove warning: %v\n", err)
				}
			}
		}
	}

	if opts.RemoveV2Runtime {
		if opts.DryRun {
			fmt.Fprintf(os.Stderr, "[dry-run] would stop detached v2 services from install-dir %s\n", opts.InstallDir)
		} else {
			stopErr := runV2Stop(v2StopOptions{
				InstallDir:  opts.InstallDir,
				Force:       opts.Force,
				StopTimeout: 5 * time.Second,
			})
			if stopErr != nil {
				fmt.Fprintf(os.Stderr, "v2-stop warning: %v\n", stopErr)
			}
		}
	}

	if opts.RemoveLocal {
		dirsToRemove := map[string]bool{}
		filesToRemove := map[string]bool{
			".ncc-api-token":              true,
			".ncc-api-schedule.json":      true,
			".ncc-api-notifications.json": true,
			"ncc-orchestrator.new.exe":    true,
			"apply-ncc-update.cmd":        true,
			".ncc-preflight-check":        true,
			".ncc-prefight-check":         true,
		}
		if opts.RemoveV2Runtime {
			dirsToRemove[opts.InstallDir] = true
		}

		// Defaults used by runner. Cover both layouts:
		//   * legacy v2.0.0/v2.0.1: paths relative to CWD
		//   * v2.0.2+: paths relative to install-dir (auto-detected when
		//     running from inside a bootstrapped stack)
		// In both modes RemoveV2Runtime=true nukes the install-dir wholesale
		// so the install-dir-relative entries below are belt-and-braces for
		// the rare --remove-v2-runtime=false invocation.
		dirsToRemove["nccfiles"] = true
		dirsToRemove["outputfiles"] = true
		dirsToRemove["promfiles"] = true
		dirsToRemove["logs"] = true
		if installDirAbs := strings.TrimSpace(opts.InstallDir); installDirAbs != "" {
			dirsToRemove[filepath.Join(installDirAbs, "nccfiles")] = true
			dirsToRemove[filepath.Join(installDirAbs, "outputfiles")] = true
			dirsToRemove[filepath.Join(installDirAbs, "promfiles")] = true
			dirsToRemove[filepath.Join(installDirAbs, "logs")] = true
			filesToRemove[filepath.Join(installDirAbs, ".ncc-api-token")] = true
			filesToRemove[filepath.Join(installDirAbs, ".ncc-api-schedule.json")] = true
			filesToRemove[filepath.Join(installDirAbs, ".ncc-api-notifications.json")] = true
		}

		if opts.ConfigPath != "" {
			cfg, err := loadConfigForValidation(opts.ConfigPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "config parse warning (%s): %v\n", opts.ConfigPath, err)
			} else {
				if strings.TrimSpace(cfg.OutputDirLogs) != "" {
					dirsToRemove[cfg.OutputDirLogs] = true
				}
				if strings.TrimSpace(cfg.OutputDirFiltered) != "" {
					dirsToRemove[cfg.OutputDirFiltered] = true
				}
				if strings.TrimSpace(cfg.PromDir) != "" {
					dirsToRemove[cfg.PromDir] = true
				}
				if strings.TrimSpace(cfg.RunHistoryDir) != "" {
					dirsToRemove[cfg.RunHistoryDir] = true
				}
				if strings.TrimSpace(cfg.LogFile) != "" {
					dirsToRemove[filepath.Dir(cfg.LogFile)] = true
				}
			}
		}

		for f := range filesToRemove {
			if opts.DryRun {
				fmt.Fprintf(os.Stderr, "[dry-run] would remove file %s\n", f)
				continue
			}
			if err := os.Remove(f); err != nil && !errors.Is(err, os.ErrNotExist) {
				fmt.Fprintf(os.Stderr, "remove file warning (%s): %v\n", f, err)
			}
		}
		for d := range dirsToRemove {
			clean := strings.TrimSpace(d)
			if clean == "" || clean == "." || clean == "/" {
				continue
			}
			if opts.DryRun {
				fmt.Fprintf(os.Stderr, "[dry-run] would remove directory %s\n", clean)
				continue
			}
			if err := os.RemoveAll(clean); err != nil {
				fmt.Fprintf(os.Stderr, "remove directory warning (%s): %v\n", clean, err)
			}
		}
	}

	fmt.Fprintln(os.Stderr, "Uninstall completed.")
	return nil
}

// systemdStackIsActive reports whether the installed stack service is
// currently active. v2-start/v2-restart must not launch a second detached
// runtime beside the native systemd supervisor: both copies would race for
// :8080/:8081 and the supervisor would only report opaque exit status 1s.
func systemdStackIsActive() bool {
	if runtime.GOOS == "windows" {
		return false
	}
	if _, err := exec.LookPath("systemctl"); err != nil {
		return false
	}
	return exec.Command("systemctl", "is-active", "--quiet", "ncc-orchestrator.service").Run() == nil
}

func restartSystemdStack() error {
	out, err := exec.Command("systemctl", "restart", "ncc-orchestrator.service").CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg == "" {
			msg = err.Error()
		}
		return fmt.Errorf("restart ncc-orchestrator.service: %s", msg)
	}
	return nil
}

// ensureV2StartSlotsAvailable prevents a second detached runtime from being
// created after a stale/partial stop. Stale pid files are safe to remove; a
// live PID or an occupied listen address is never killed automatically because
// it may belong to an unrelated service.
func ensureV2StartSlotsAvailable(installDir string, opts v2StartOptions) error {
	runDir := filepath.Join(installDir, "run")
	pids := []struct {
		name     string
		path     string
		identity []string
	}{
		{"supervisor", filepath.Join(runDir, "v2-supervisor.pid"), []string{"ncc-orchestrator", "v2-supervise"}},
		{"api", filepath.Join(runDir, "v2-api.pid"), []string{"ncc-api-server"}},
	}
	if !opts.APIOnly {
		pids = append(pids, struct {
			name     string
			path     string
			identity []string
		}{"ui", filepath.Join(runDir, "v2-ui.pid"), []string{"ncc-ui-server"}})
	}
	for _, item := range pids {
		pid, err := readPIDFromFile(item.path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("cannot inspect %s pid file: %w", item.name, err)
		}
		if processIsExpected(pid, item.identity...) {
			return fmt.Errorf("%s is already running (pid %d); stop it with `ncc-orchestrator v2-stop --install-dir %s` before starting another stack", item.name, pid, installDir)
		}
		// A live PID with a known non-NCC command is a reused PID. Do not
		// signal it; remove only the stale metadata and continue.
		_ = os.Remove(item.path)
	}
	if err := canBindListenAddress(opts.APIListen); err != nil {
		return fmt.Errorf("api listen address %s is unavailable: %w (another process may already own the port)", opts.APIListen, err)
	}
	if !opts.APIOnly {
		if err := canBindListenAddress(opts.UIListen); err != nil {
			return fmt.Errorf("ui listen address %s is unavailable: %w (another process may already own the port)", opts.UIListen, err)
		}
	}
	return nil
}

func runV2Start(opts v2StartOptions) error {
	installDir := strings.TrimSpace(opts.InstallDir)
	if installDir == "" {
		installDir = defaultV2InstallDir()
	}
	if absInstallDir, err := filepath.Abs(installDir); err == nil {
		installDir = absInstallDir
	}
	if opts.Detach && !opts.Supervise && systemdStackIsActive() {
		return fmt.Errorf("ncc-orchestrator.service is already active; use `systemctl restart ncc-orchestrator.service` or `ncc-orchestrator v2-restart` instead of starting a detached duplicate")
	}
	// Record the portable start settings so backups carry them and an
	// orchestrator-managed restart (after a restore, or via v2-restart) reuses
	// the same CORS/listen/session configuration. Best-effort; never block the
	// start on a state-write failure.
	if err := writeV2StartState(installDir, opts); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not persist v2-start settings: %v\n", err)
	}
	apiOnly := opts.APIOnly
	var (
		apiBin      string
		uiBin       string
		frontendDir string
		layout      string
	)
	if apiOnly {
		apiBin, layout = resolveV2APIBinary(installDir)
		if apiBin == "" {
			return fmt.Errorf("could not locate API runtime binary. expected either %s (from v2-bootstrap), or local release asset ncc-api-server-%s-%s (run `ncc-orchestrator v2-bootstrap` first)",
				binaryPathInInstallDir(installDir, "ncc-api-server"), runtime.GOOS, runtime.GOARCH,
			)
		}
	} else {
		apiBin, uiBin, frontendDir, layout = resolveV2RuntimeLayout(installDir)
		if apiBin == "" || uiBin == "" || frontendDir == "" {
			return fmt.Errorf("could not locate v2 runtime assets. expected either %s + %s + %s (from v2-bootstrap), or local release assets like ncc-api-server-%s-%s, ncc-ui-server-%s-%s, and frontend-dist/ (run `ncc-orchestrator v2-bootstrap` first)",
				binaryPathInInstallDir(installDir, "ncc-api-server"),
				binaryPathInInstallDir(installDir, "ncc-ui-server"),
				filepath.Join(installDir, "frontend-dist"),
				runtime.GOOS, runtime.GOARCH, runtime.GOOS, runtime.GOARCH,
			)
		}
	}

	// Default the runtime paths relative to the resolved install-dir (so
	// `bin/ncc-orchestrator v2-start` from inside a stack picks colocated
	// config/outputs/logs instead of paths under bin/).
	if strings.TrimSpace(opts.ConfigPath) == "" {
		opts.ConfigPath = filepath.Join(installDir, "config.yaml")
	}
	if strings.TrimSpace(opts.OutputDir) == "" {
		opts.OutputDir = filepath.Join(installDir, "outputfiles")
	}
	if strings.TrimSpace(opts.LogDir) == "" {
		opts.LogDir = filepath.Join(installDir, "nccfiles")
	}
	if strings.TrimSpace(opts.OrchestratorBin) == "" {
		opts.OrchestratorBin = "./ncc-orchestrator"
	}
	opts.OrchestratorBin = resolveV2OrchestratorBin(opts.OrchestratorBin)
	if strings.TrimSpace(opts.APIListen) == "" {
		opts.APIListen = ":8081"
	}
	if strings.TrimSpace(opts.UIListen) == "" {
		opts.UIListen = ":8080"
	}
	if strings.TrimSpace(opts.TokenFile) == "" {
		opts.TokenFile = filepath.Join(installDir, ".ncc-api-token")
	}
	if absConfigPath, err := filepath.Abs(opts.ConfigPath); err == nil {
		opts.ConfigPath = absConfigPath
	}
	if absOutputDir, err := filepath.Abs(opts.OutputDir); err == nil {
		opts.OutputDir = absOutputDir
	}
	if absLogDir, err := filepath.Abs(opts.LogDir); err == nil {
		opts.LogDir = absLogDir
	}
	if absTokenPath, err := filepath.Abs(opts.TokenFile); err == nil {
		opts.TokenFile = absTokenPath
	}
	// Pin the writable user database to the install dir so local accounts,
	// roles, and SSO config survive v2-stop / v2-start regardless of the
	// process's working directory (without this the api-server falls back to a
	// CWD-relative default that can differ between launches).
	if strings.TrimSpace(opts.UsersDB) == "" {
		opts.UsersDB = filepath.Join(installDir, ".ncc-api-users.json")
	}
	if absUsersDB, err := filepath.Abs(opts.UsersDB); err == nil {
		opts.UsersDB = absUsersDB
	}
	// Same example_config.yaml fallback as v2-check: if config.yaml is
	// missing but the install ships an example, prefer that with a
	// stderr warning rather than failing on a fresh extraction.
	if _, err := os.Stat(opts.ConfigPath); err != nil {
		exampleCfg := filepath.Join(installDir, "example_config.yaml")
		if _, exErr := os.Stat(exampleCfg); exErr == nil {
			fmt.Fprintf(os.Stderr, "warning: config-path %s not found; falling back to %s (replace with your own config before production use)\n", opts.ConfigPath, exampleCfg)
			opts.ConfigPath = exampleCfg
		}
	}
	if cfg, err := loadConfigForValidation(opts.ConfigPath); err == nil {
		pwd := strings.TrimSpace(cfg.Password)
		if pwd != "" && !strings.HasPrefix(strings.ToLower(pwd), "secret://") {
			fmt.Fprintln(os.Stderr, "warning: config contains plaintext password; prefer NCC_PASSWORD env or secret:// source")
		}
	}
	if strings.TrimSpace(opts.APIAuthMode) == "" {
		opts.APIAuthMode = "token"
	}
	if opts.APISessionTTL <= 0 {
		opts.APISessionTTL = 10 * time.Minute
	}
	if opts.APIRunTimeout <= 0 {
		opts.APIRunTimeout = 90 * time.Minute
	}
	if opts.APIRateLimitPerMinute < 0 {
		return fmt.Errorf("api-rate-limit-per-minute must be >= 0")
	}
	if opts.APIReadTimeout <= 0 {
		opts.APIReadTimeout = 15 * time.Second
	}
	if opts.APIWriteTimeout <= 0 {
		opts.APIWriteTimeout = 60 * time.Second
	}
	if opts.APIIdleTimeout <= 0 {
		opts.APIIdleTimeout = 60 * time.Second
	}
	if opts.ReadyTimeout <= 0 {
		opts.ReadyTimeout = 20 * time.Second
	}
	if opts.SelfHealMaxRestarts <= 0 {
		opts.SelfHealMaxRestarts = 3
	}
	if opts.SelfHealWindow <= 0 {
		opts.SelfHealWindow = 10 * time.Minute
	}
	if opts.SelfHealProbeInterval <= 0 {
		opts.SelfHealProbeInterval = 10 * time.Second
	}
	if opts.SelfHealUnhealthyThreshold <= 0 {
		opts.SelfHealUnhealthyThreshold = 3
	}
	if opts.Detach && !opts.Supervise {
		if err := ensureV2StartSlotsAvailable(installDir, opts); err != nil {
			return err
		}
	}
	if opts.SelfHeal && !opts.Detach {
		fmt.Fprintln(os.Stderr, "warning: --self-heal is effective only with --detach; continuing without detached self-heal monitor")
	}
	if strings.TrimSpace(opts.APISessionSecret) != "" && strings.TrimSpace(opts.APISessionSecretFile) != "" {
		return fmt.Errorf("only one of api-session-secret or api-session-secret-file may be set")
	}
	if strings.TrimSpace(opts.APISessionSecretFile) != "" {
		secret, err := readTrimmedFile(opts.APISessionSecretFile)
		if err != nil {
			return fmt.Errorf("read api-session-secret-file: %w", err)
		}
		if secret == "" {
			return fmt.Errorf("api-session-secret-file is empty")
		}
		opts.APISessionSecret = secret
	}
	if opts.APIAuthMode != "token" && opts.APIAuthMode != "session" && opts.APIAuthMode != "hybrid" {
		return fmt.Errorf("api-auth-mode must be one of token, session, hybrid")
	}
	cwd, _ := os.Getwd()
	// repoRoot must contain config-path, output-dir, log-dir, token-file
	// (api-server enforces this as a path-traversal sandbox). Use the
	// install-dir-or-CWD ancestor and pre-resolve symlinks so /tmp vs
	// /private/tmp on macOS doesn't break the check.
	repoRoot := resolveV2RepoRoot(installDir, cwd)
	// Also EvalSymlinks the secondary paths so they share the prefix the
	// api-server compares against (it EvalSymlinks rootAbs but not abs).
	if real := resolveV2PathToReal(opts.ConfigPath); real != "" {
		opts.ConfigPath = real
	}
	if real := resolveV2PathToReal(opts.OutputDir); real != "" {
		opts.OutputDir = real
	}
	if real := resolveV2PathToReal(opts.LogDir); real != "" {
		opts.LogDir = real
	}
	if real := resolveV2PathToReal(opts.TokenFile); real != "" {
		opts.TokenFile = real
	}
	if real := resolveV2PathToReal(installDir); real != "" {
		installDir = real
	}

	// Secure-by-default: when serving the UI and the operator did not bring
	// their own certificate (or explicitly opt into plain HTTP with
	// --ui-insecure-http), mint/reuse a self-signed certificate and bind the
	// UI server to TLS. The ui-server additionally 308-redirects any plain-HTTP
	// client on the same port, so "http://…" still lands the user on HTTPS.
	// Browsers show a one-time self-signed warning, which is expected for an
	// internal IP-addressed tool. Operators can install a real cert any time
	// from Settings → Access (or with --ui-tls-cert-file/--ui-tls-key-file).
	if !opts.APIOnly &&
		strings.TrimSpace(opts.UITLSCertFile) == "" &&
		strings.TrimSpace(opts.UITLSKeyFile) == "" &&
		!opts.UIInsecureHTTP {
		certPath, keyPath, gerr := ensureDefaultUISelfSignedCert(installDir, opts)
		if gerr != nil {
			fmt.Fprintf(os.Stderr, "warning: could not generate the default self-signed UI certificate (%v); falling back to plain HTTP. Pass --ui-tls-cert-file/--ui-tls-key-file to provide your own, or --ui-insecure-http to silence this.\n", gerr)
		} else {
			opts.UITLSCertFile = certPath
			opts.UITLSKeyFile = keyPath
			fmt.Fprintf(os.Stderr, "[tls] no UI certificate provided; serving self-signed HTTPS by default (cert: %s). Use --ui-insecure-http for plain HTTP.\n", certPath)
		}
	}
	uiTLSActive := strings.TrimSpace(opts.UITLSCertFile) != "" && strings.TrimSpace(opts.UITLSKeyFile) != ""

	backendURL := strings.TrimSpace(opts.UIBackendURL)
	if backendURL == "" {
		if strings.TrimSpace(opts.APITLSCertFile) != "" && strings.TrimSpace(opts.APITLSKeyFile) != "" {
			backendURL = strings.Replace(localHTTPURLFromListen(opts.APIListen, "8081"), "http://", "https://", 1)
		} else {
			backendURL = localHTTPURLFromListen(opts.APIListen, "8081")
		}
	}
	uiOrigin := localHTTPURLFromListen(opts.UIListen, "8080")
	// When the UI listens on a loopback IP, browsers may reach it via
	// either form (http://localhost:port OR http://127.0.0.1:port). Add
	// the alternate form so CORS doesn't reject whichever the user types.
	corsBase := uiOrigin
	if alt := loopbackAltOriginFromListen(opts.UIListen, "8080"); alt != "" && alt != uiOrigin {
		corsBase = uiOrigin + "," + alt
	}
	// When the UI serves TLS, the browser's origin is https://; reflect that in
	// the derived origins so CORS matches. (Same-origin requests are accepted
	// regardless, but keep the advertised/printed URL on the right scheme.)
	if uiTLSActive {
		uiOrigin = strings.Replace(uiOrigin, "http://", "https://", 1)
		corsBase = strings.ReplaceAll(corsBase, "http://", "https://")
	}
	allowedOrigins := mergeAllowedOriginsCSV(corsBase, opts.UIAllowedOrigins)
	apiCORSOrigins := strings.TrimSpace(opts.APICORSOrigins)
	if apiCORSOrigins == "" {
		apiCORSOrigins = allowedOrigins
	}
	displayAPIURL := strings.TrimSpace(opts.APIAdvertiseURL)
	if displayAPIURL == "" {
		displayAPIURL = backendURL
	}
	displayUIURL := strings.TrimSpace(opts.UIAdvertiseURL)
	if displayUIURL == "" {
		displayUIURL = uiOrigin
	}

	apiArgs := []string{
		"--listen", opts.APIListen,
		"--repo-root", repoRoot,
		"--config-path", opts.ConfigPath,
		"--output-dir", opts.OutputDir,
		"--log-dir", opts.LogDir,
		"--orchestrator-bin", opts.OrchestratorBin,
		"--token-file-path", opts.TokenFile,
		"--users-db", opts.UsersDB,
		"--cors-origin", apiCORSOrigins,
		"--auth-mode", opts.APIAuthMode,
		"--session-ttl", opts.APISessionTTL.String(),
		"--run-timeout", opts.APIRunTimeout.String(),
		"--rate-limit-per-minute", strconv.Itoa(opts.APIRateLimitPerMinute),
		"--read-timeout", opts.APIReadTimeout.String(),
		"--write-timeout", opts.APIWriteTimeout.String(),
		"--idle-timeout", opts.APIIdleTimeout.String(),
	}
	if strings.TrimSpace(opts.APISessionSecret) != "" {
		apiArgs = append(apiArgs, "--session-secret", opts.APISessionSecret)
	}
	if strings.TrimSpace(opts.APITLSCertFile) != "" || strings.TrimSpace(opts.APITLSKeyFile) != "" {
		apiArgs = append(apiArgs, "--tls-cert-file", opts.APITLSCertFile, "--tls-key-file", opts.APITLSKeyFile)
	}
	if strings.TrimSpace(opts.APITLSClientCAFile) != "" {
		apiArgs = append(apiArgs, "--tls-client-ca-file", opts.APITLSClientCAFile)
	}
	// Drop the Secure attribute on session cookies when serving the stack over
	// plain HTTP from a non-localhost address; otherwise browsers silently
	// refuse to store the session cookie and every login bounces back to the
	// login screen. TLS is still the right answer for anything exposed.
	if opts.APICookieInsecure {
		apiArgs = append(apiArgs, "--cookie-insecure")
	} else if uiTLSActive {
		// The browser reaches the stack over HTTPS, so session cookies can (and
		// should) carry the Secure attribute. The api-server sits on loopback
		// behind the UI server and cannot infer this itself, so tell it here.
		apiArgs = append(apiArgs, "--cookie-secure")
	}

	apiCmd := exec.Command(apiBin, apiArgs...)
	var uiCmd *exec.Cmd
	var uiArgs []string
	if !apiOnly {
		uiAuthMode := "token"
		if opts.APIAuthMode == "session" {
			uiAuthMode = "session"
		}
		uiArgs = []string{
			"--listen", opts.UIListen,
			"--dir", frontendDir,
			"--backend-url", backendURL,
			"--api-token-file", opts.TokenFile,
			"--api-auth-mode", uiAuthMode,
			"--allowed-origins", allowedOrigins,
		}
		if strings.TrimSpace(opts.UITLSCertFile) != "" || strings.TrimSpace(opts.UITLSKeyFile) != "" {
			uiArgs = append(uiArgs, "--tls-cert-file", opts.UITLSCertFile, "--tls-key-file", opts.UITLSKeyFile)
		}
		if strings.TrimSpace(opts.UIBackendCAFile) != "" {
			uiArgs = append(uiArgs, "--backend-ca-file", opts.UIBackendCAFile)
		}
		if strings.TrimSpace(opts.UIBackendClientCertFile) != "" {
			uiArgs = append(uiArgs, "--backend-client-cert-file", opts.UIBackendClientCertFile)
		}
		if strings.TrimSpace(opts.UIBackendClientKeyFile) != "" {
			uiArgs = append(uiArgs, "--backend-client-key-file", opts.UIBackendClientKeyFile)
		}
		if opts.UIBackendInsecureSkipVerify {
			uiArgs = append(uiArgs, "--backend-insecure-skip-verify")
		}
		uiCmd = exec.Command(uiBin, uiArgs...)
	}
	if opts.Supervise {
		// Native foreground supervisor: this process owns the children and
		// keeps them alive (crash + hang recovery) for as long as it runs.
		// Run it as a Type=simple systemd service so the OS keeps the
		// supervisor alive across reboots. apiCmd/uiCmd built above are not
		// used here; the supervisor builds its own commands on every restart.
		runDir := filepath.Join(installDir, "run")
		logDir := filepath.Join(installDir, "logs")
		apiLogPath := strings.TrimSpace(opts.APILogFile)
		if apiLogPath == "" {
			apiLogPath = filepath.Join(logDir, "v2-api.log")
		}
		apiPIDPath := strings.TrimSpace(opts.APIPIDFile)
		if apiPIDPath == "" {
			apiPIDPath = filepath.Join(runDir, "v2-api.pid")
		}
		// Health-probe restarts catch a hung-but-alive api-server. The probe
		// reuses the api-server's built-in --health-check (loopback HTTP), so
		// it only applies when the api-server is not serving its own TLS.
		var apiHealthArgs []string
		if strings.TrimSpace(opts.APITLSCertFile) == "" {
			apiHealthArgs = buildAPIHealthProbeArgs(apiBin, opts.APIListen, opts.TokenFile, repoRoot)
		}
		children := []*superviseChild{{
			name:       "api",
			bin:        apiBin,
			args:       apiArgs,
			listen:     opts.APIListen,
			pidPath:    apiPIDPath,
			logPath:    apiLogPath,
			healthArgs: apiHealthArgs,
		}}
		if uiCmd != nil {
			uiLogPath := strings.TrimSpace(opts.UILogFile)
			if uiLogPath == "" {
				uiLogPath = filepath.Join(logDir, "v2-ui.log")
			}
			uiPIDPath := strings.TrimSpace(opts.UIPIDFile)
			if uiPIDPath == "" {
				uiPIDPath = filepath.Join(runDir, "v2-ui.pid")
			}
			children = append(children, &superviseChild{
				name:       "ui",
				bin:        uiBin,
				args:       uiArgs,
				listen:     opts.UIListen,
				pidPath:    uiPIDPath,
				logPath:    uiLogPath,
				healthArgs: buildUIHealthProbeArgs(uiBin, opts.UIListen, uiTLSActive),
				// Gate the UI's first launch on the API having written the
				// shared token file (same ordering as the detached path).
				waitToken: opts.TokenFile,
			})
		}
		fmt.Fprintf(os.Stderr, "v2 stack supervisor starting (foreground)\n")
		if strings.TrimSpace(layout) != "" {
			fmt.Fprintf(os.Stderr, "Asset layout: %s\n", layout)
		}
		fmt.Fprintf(os.Stderr, "API binary: %s\n", apiBin)
		if uiCmd != nil {
			fmt.Fprintf(os.Stderr, "UI binary : %s\n", uiBin)
			fmt.Fprintf(os.Stderr, "Frontend  : %s\n", frontendDir)
		}
		fmt.Fprintf(os.Stderr, "Config path: %s\n", opts.ConfigPath)
		fmt.Fprintf(os.Stderr, "Token file: %s\n", opts.TokenFile)
		fmt.Fprintf(os.Stderr, "API: %s\n", displayAPIURL)
		if uiCmd != nil {
			fmt.Fprintf(os.Stderr, "UI : %s\n", displayUIURL)
			fmt.Fprintf(os.Stderr, "UI allowed origins: %s\n", allowedOrigins)
		}
		fmt.Fprintf(os.Stderr, "Supervisor: max_restarts=%d window=%s, health-probe every %s after %d failures, cooldown-and-resume\n",
			opts.SelfHealMaxRestarts, opts.SelfHealWindow, opts.SelfHealProbeInterval, opts.SelfHealUnhealthyThreshold)
		fmt.Fprintf(os.Stderr, "Stop with SIGTERM/SIGINT (or `systemctl stop ncc-orchestrator`).\n")
		return runV2Supervise(superviseConfig{
			installDir:         installDir,
			children:           children,
			maxRestarts:        opts.SelfHealMaxRestarts,
			window:             opts.SelfHealWindow,
			probeInterval:      opts.SelfHealProbeInterval,
			unhealthyThreshold: opts.SelfHealUnhealthyThreshold,
		})
	}
	if opts.Detach {
		runDir := filepath.Join(installDir, "run")
		detachedLogDir := filepath.Join(installDir, "logs")
		if err := os.MkdirAll(runDir, 0755); err != nil {
			return fmt.Errorf("prepare detached run dir: %w", err)
		}
		if err := os.MkdirAll(detachedLogDir, 0755); err != nil {
			return fmt.Errorf("prepare detached log dir: %w", err)
		}
		apiLogPath := strings.TrimSpace(opts.APILogFile)
		if apiLogPath == "" {
			apiLogPath = filepath.Join(detachedLogDir, "v2-api.log")
		}
		if absPath, err := filepath.Abs(apiLogPath); err == nil {
			apiLogPath = absPath
		}
		opts.APILogFile = apiLogPath
		if err := os.MkdirAll(filepath.Dir(apiLogPath), 0755); err != nil {
			return fmt.Errorf("prepare api detached log parent dir: %w", err)
		}
		apiLogFile, err := os.OpenFile(apiLogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("open api detached log: %w", err)
		}
		defer apiLogFile.Close()
		devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
		if err != nil {
			return fmt.Errorf("open devnull: %w", err)
		}
		defer devNull.Close()
		apiCmd.Stdin = devNull
		apiCmd.Stdout = apiLogFile
		apiCmd.Stderr = apiLogFile
		if uiCmd != nil {
			uiLogPath := strings.TrimSpace(opts.UILogFile)
			if uiLogPath == "" {
				uiLogPath = filepath.Join(detachedLogDir, "v2-ui.log")
			}
			if absPath, err := filepath.Abs(uiLogPath); err == nil {
				uiLogPath = absPath
			}
			opts.UILogFile = uiLogPath
			if err := os.MkdirAll(filepath.Dir(uiLogPath), 0755); err != nil {
				return fmt.Errorf("prepare ui detached log parent dir: %w", err)
			}
			uiLogFile, err := os.OpenFile(uiLogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				return fmt.Errorf("open ui detached log: %w", err)
			}
			defer uiLogFile.Close()
			uiCmd.Stdin = devNull
			uiCmd.Stdout = uiLogFile
			uiCmd.Stderr = uiLogFile
		}
	} else {
		apiCmd.Stdout = os.Stdout
		apiCmd.Stderr = os.Stderr
		if uiCmd != nil {
			uiCmd.Stdout = os.Stdout
			uiCmd.Stderr = os.Stderr
		}
	}
	if err := apiCmd.Start(); err != nil {
		return fmt.Errorf("start api server: %w", err)
	}

	if strings.TrimSpace(opts.TokenFile) != "" {
		waitForFile(opts.TokenFile, 5*time.Second)
	}

	if uiCmd != nil {
		if err := uiCmd.Start(); err != nil {
			signalProcessStop(apiCmd)
			return fmt.Errorf("start ui server: %w", err)
		}
	}

	if uiCmd != nil {
		fmt.Fprintf(os.Stderr, "v2 services started (api pid=%d, ui pid=%d)\n", apiCmd.Process.Pid, uiCmd.Process.Pid)
	} else {
		fmt.Fprintf(os.Stderr, "v2 api service started (api pid=%d)\n", apiCmd.Process.Pid)
	}
	if strings.TrimSpace(layout) != "" {
		fmt.Fprintf(os.Stderr, "Asset layout: %s\n", layout)
	}
	fmt.Fprintf(os.Stderr, "API binary: %s\n", apiBin)
	if uiCmd != nil {
		fmt.Fprintf(os.Stderr, "UI binary : %s\n", uiBin)
		fmt.Fprintf(os.Stderr, "Frontend  : %s\n", frontendDir)
	}
	fmt.Fprintf(os.Stderr, "Orchestrator binary for API runs: %s\n", opts.OrchestratorBin)
	fmt.Fprintf(os.Stderr, "Config path: %s\n", opts.ConfigPath)
	fmt.Fprintf(os.Stderr, "Token file: %s\n", opts.TokenFile)
	fmt.Fprintf(os.Stderr, "API: %s\n", displayAPIURL)
	if uiCmd != nil {
		fmt.Fprintf(os.Stderr, "UI : %s\n", displayUIURL)
		fmt.Fprintf(os.Stderr, "UI allowed origins: %s\n", allowedOrigins)
	} else {
		fmt.Fprintf(os.Stderr, "API docs: %s/\n", displayAPIURL)
	}
	if opts.WaitReady {
		healthURL := strings.TrimRight(backendURL, "/") + "/api/v1/health"
		if err := waitForHTTPReady(healthURL, opts.ReadyTimeout); err != nil {
			signalProcessStop(apiCmd)
			if uiCmd != nil {
				signalProcessStop(uiCmd)
			}
			return fmt.Errorf("wait-ready api health check failed: %w", err)
		}
		if uiCmd != nil {
			if err := waitForHTTPReady(uiOrigin, opts.ReadyTimeout); err != nil {
				signalProcessStop(apiCmd)
				signalProcessStop(uiCmd)
				return fmt.Errorf("wait-ready ui check failed: %w", err)
			}
		}
		fmt.Fprintf(os.Stderr, "Readiness checks passed (timeout=%s).\n", opts.ReadyTimeout)
	}
	if opts.Detach {
		runDir := filepath.Join(installDir, "run")
		apiPIDPath := strings.TrimSpace(opts.APIPIDFile)
		if apiPIDPath == "" {
			apiPIDPath = filepath.Join(runDir, "v2-api.pid")
		}
		if absPath, err := filepath.Abs(apiPIDPath); err == nil {
			apiPIDPath = absPath
		}
		if err := os.MkdirAll(filepath.Dir(apiPIDPath), 0755); err != nil {
			signalProcessStop(apiCmd)
			if uiCmd != nil {
				signalProcessStop(uiCmd)
			}
			return fmt.Errorf("prepare api pid parent dir: %w", err)
		}
		if err := os.WriteFile(apiPIDPath, []byte(fmt.Sprintf("%d\n", apiCmd.Process.Pid)), 0644); err != nil {
			signalProcessStop(apiCmd)
			return fmt.Errorf("write api pid file: %w", err)
		}
		uiPIDPath := ""
		if uiCmd != nil {
			uiPIDPath = strings.TrimSpace(opts.UIPIDFile)
			if uiPIDPath == "" {
				uiPIDPath = filepath.Join(runDir, "v2-ui.pid")
			}
			if absPath, err := filepath.Abs(uiPIDPath); err == nil {
				uiPIDPath = absPath
			}
			if err := os.MkdirAll(filepath.Dir(uiPIDPath), 0755); err != nil {
				signalProcessStop(apiCmd)
				signalProcessStop(uiCmd)
				return fmt.Errorf("prepare ui pid parent dir: %w", err)
			}
			if err := os.WriteFile(uiPIDPath, []byte(fmt.Sprintf("%d\n", uiCmd.Process.Pid)), 0644); err != nil {
				signalProcessStop(apiCmd)
				signalProcessStop(uiCmd)
				return fmt.Errorf("write ui pid file: %w", err)
			}
		}
		var apiSupervisorPID int
		var uiSupervisorPID int
		if opts.SelfHeal {
			if runtime.GOOS == "windows" {
				return fmt.Errorf("self-heal is currently unsupported on windows detached mode")
			}
			apiLogPath := strings.TrimSpace(opts.APILogFile)
			if apiLogPath == "" {
				apiLogPath = filepath.Join(installDir, "logs", "v2-api.log")
			}
			// Health-probe restarts catch a hung-but-alive api-server (deadlock,
			// stuck handler) that a liveness-only check would miss. The probe
			// reuses the api-server's built-in `--health-check` mode (no curl
			// dependency), which hits /api/v1/health over loopback HTTP — so it
			// only applies when the api-server is not serving its own TLS.
			apiHealthCmd := ""
			if strings.TrimSpace(opts.APITLSCertFile) == "" {
				apiHealthCmd = buildAPIHealthProbeCmd(apiBin, opts.APIListen, opts.TokenFile, repoRoot)
			}
			pid, err := startSelfHealSupervisor("api", apiBin, apiArgs, apiPIDPath, apiLogPath, opts.SelfHealMaxRestarts, opts.SelfHealWindow, apiHealthCmd, opts.SelfHealProbeInterval, opts.SelfHealUnhealthyThreshold)
			if err != nil {
				return fmt.Errorf("start api self-heal supervisor: %w", err)
			}
			apiSupervisorPID = pid
			apiSupPIDPath := filepath.Join(runDir, "v2-api-supervisor.pid")
			_ = os.WriteFile(apiSupPIDPath, []byte(fmt.Sprintf("%d\n", apiSupervisorPID)), 0644)
			if uiCmd != nil {
				uiLogPath := strings.TrimSpace(opts.UILogFile)
				if uiLogPath == "" {
					uiLogPath = filepath.Join(installDir, "logs", "v2-ui.log")
				}
				// The UI server's built-in --health-check probe lets the
				// supervisor catch a hung (alive-but-unresponsive) UI process,
				// not just a crashed one.
				uiHealthCmd := buildUIHealthProbeCmd(uiBin, opts.UIListen, strings.TrimSpace(opts.UITLSCertFile) != "")
				pid, err := startSelfHealSupervisor("ui", uiBin, uiArgs, uiPIDPath, uiLogPath, opts.SelfHealMaxRestarts, opts.SelfHealWindow, uiHealthCmd, opts.SelfHealProbeInterval, opts.SelfHealUnhealthyThreshold)
				if err != nil {
					return fmt.Errorf("start ui self-heal supervisor: %w", err)
				}
				uiSupervisorPID = pid
				uiSupPIDPath := filepath.Join(runDir, "v2-ui-supervisor.pid")
				_ = os.WriteFile(uiSupPIDPath, []byte(fmt.Sprintf("%d\n", uiSupervisorPID)), 0644)
			}
		}
		_ = apiCmd.Process.Release()
		if uiCmd != nil {
			_ = uiCmd.Process.Release()
		}
		fmt.Fprintf(os.Stderr, "Detached mode enabled.\n")
		if uiCmd != nil {
			fmt.Fprintf(os.Stderr, "PID files: %s, %s\n", apiPIDPath, uiPIDPath)
			apiLogPath := strings.TrimSpace(opts.APILogFile)
			if apiLogPath == "" {
				apiLogPath = filepath.Join(installDir, "logs", "v2-api.log")
			}
			uiLogPath := strings.TrimSpace(opts.UILogFile)
			if uiLogPath == "" {
				uiLogPath = filepath.Join(installDir, "logs", "v2-ui.log")
			}
			fmt.Fprintf(os.Stderr, "Logs: %s, %s\n", apiLogPath, uiLogPath)
			if runtime.GOOS == "windows" {
				fmt.Fprintf(os.Stderr, "Kill cmd: taskkill /PID %d /PID %d /T\n", apiCmd.Process.Pid, uiCmd.Process.Pid)
			} else {
				fmt.Fprintf(os.Stderr, "Kill cmd: kill \"$(cat %s)\" \"$(cat %s)\"\n", shellQuote(apiPIDPath), shellQuote(uiPIDPath))
			}
			if opts.SelfHeal {
				fmt.Fprintf(os.Stderr, "Self-heal: enabled (max_restarts=%d window=%s, health-probe every %s after %d failures, cooldown-and-resume)\n", opts.SelfHealMaxRestarts, opts.SelfHealWindow, opts.SelfHealProbeInterval, opts.SelfHealUnhealthyThreshold)
				fmt.Fprintf(os.Stderr, "Self-heal supervisor pids: api=%d ui=%d\n", apiSupervisorPID, uiSupervisorPID)
			}
		} else {
			fmt.Fprintf(os.Stderr, "PID file: %s\n", apiPIDPath)
			apiLogPath := strings.TrimSpace(opts.APILogFile)
			if apiLogPath == "" {
				apiLogPath = filepath.Join(installDir, "logs", "v2-api.log")
			}
			fmt.Fprintf(os.Stderr, "Log: %s\n", apiLogPath)
			if runtime.GOOS == "windows" {
				fmt.Fprintf(os.Stderr, "Kill cmd: taskkill /PID %d /T\n", apiCmd.Process.Pid)
			} else {
				fmt.Fprintf(os.Stderr, "Kill cmd: kill \"$(cat %s)\"\n", shellQuote(apiPIDPath))
			}
			if opts.SelfHeal {
				fmt.Fprintf(os.Stderr, "Self-heal: enabled (max_restarts=%d window=%s, health-probe every %s after %d failures, cooldown-and-resume)\n", opts.SelfHealMaxRestarts, opts.SelfHealWindow, opts.SelfHealProbeInterval, opts.SelfHealUnhealthyThreshold)
				fmt.Fprintf(os.Stderr, "Self-heal supervisor pid: api=%d\n", apiSupervisorPID)
			}
		}
		fmt.Fprintf(os.Stderr, "Recommended stop cmd: ncc-orchestrator v2-stop --install-dir %s\n", shellQuote(installDir))
		return nil
	}
	if uiCmd != nil {
		fmt.Fprintln(os.Stderr, "Press Ctrl+C to stop both services.")
	} else {
		fmt.Fprintln(os.Stderr, "Press Ctrl+C to stop API service.")
	}

	type procExit struct {
		name string
		err  error
	}
	exitCh := make(chan procExit, 2)
	go func() { exitCh <- procExit{name: "api", err: apiCmd.Wait()} }()
	if uiCmd != nil {
		go func() { exitCh <- procExit{name: "ui", err: uiCmd.Wait()} }()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case sig := <-sigCh:
		if uiCmd != nil {
			fmt.Fprintf(os.Stderr, "received %s, stopping services...\n", sig.String())
		} else {
			fmt.Fprintf(os.Stderr, "received %s, stopping API service...\n", sig.String())
		}
		signalProcessStop(apiCmd)
		if uiCmd != nil {
			signalProcessStop(uiCmd)
		}
		<-exitCh
		if uiCmd != nil {
			<-exitCh
		}
		return nil
	case first := <-exitCh:
		signalProcessStop(apiCmd)
		if uiCmd != nil {
			signalProcessStop(uiCmd)
		}
		if first.err != nil {
			return fmt.Errorf("%s server exited with error: %w", first.name, first.err)
		}
		if uiCmd != nil {
			second := <-exitCh
			if second.err != nil {
				return fmt.Errorf("%s server exited with error: %w", second.name, second.err)
			}
		}
		return fmt.Errorf("%s server exited", first.name)
	}
}

func extractZipArchive(archive []byte, destDir string) error {
	r, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		return fmt.Errorf("open zip: %w", err)
	}
	destClean := filepath.Clean(destDir) + string(os.PathSeparator)
	for _, f := range r.File {
		name := filepath.Clean(f.Name)
		target := filepath.Join(destDir, name)
		if !strings.HasPrefix(target, destClean) {
			return fmt.Errorf("zip contains unsafe path %q", f.Name)
		}
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return err
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		data, err := io.ReadAll(io.LimitReader(rc, 200*1024*1024))
		_ = rc.Close()
		if err != nil {
			return err
		}
		// Preserve archive entry mode bits (esp. the executable bit). The
		// stack archives we ship contain executable binaries under bin/; if
		// we wrote them with a hardcoded 0644 mode the post-extract
		// isExecutableFile checks (and any attempt to exec the binary) would
		// fail. Defaults to 0644 only if the archive entry has no mode set.
		mode := f.FileInfo().Mode().Perm()
		if mode == 0 {
			mode = 0o644
		}
		if err := os.WriteFile(target, data, mode); err != nil {
			return err
		}
	}
	return nil
}

func extractTarGzArchive(archive []byte, destDir string) error {
	gzr, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		return fmt.Errorf("open gzip: %w", err)
	}
	defer gzr.Close()
	tr := tar.NewReader(gzr)
	destClean := filepath.Clean(destDir) + string(os.PathSeparator)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		name := filepath.Clean(hdr.Name)
		target := filepath.Join(destDir, name)
		if !strings.HasPrefix(target, destClean) {
			return fmt.Errorf("tar contains unsafe path %q", hdr.Name)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			data, err := io.ReadAll(io.LimitReader(tr, 200*1024*1024))
			if err != nil {
				return err
			}
			// Preserve the tar entry mode (esp. the +x bit for binaries
			// under bin/). Without this, v2-bootstrap drops the executable
			// bit during extraction, causing v2-check / v2-start failures
			// with "binary not executable under install dir" errors.
			mode := os.FileMode(hdr.Mode).Perm()
			if mode == 0 {
				mode = 0o644
			}
			if err := os.WriteFile(target, data, mode); err != nil {
				return err
			}
		}
	}
	return nil
}

func listAssetNames(rel githubRelease) []string {
	out := make([]string, 0, len(rel.Assets))
	for _, a := range rel.Assets {
		out = append(out, a.Name)
	}
	sort.Strings(out)
	return out
}

func extractArchiveByAssetName(archive []byte, assetName string, destDir string) error {
	lower := strings.ToLower(strings.TrimSpace(assetName))
	switch {
	case strings.HasSuffix(lower, ".zip"):
		return extractZipArchive(archive, destDir)
	case strings.HasSuffix(lower, ".tar.gz"):
		return extractTarGzArchive(archive, destDir)
	default:
		return fmt.Errorf("unsupported archive format for %s", assetName)
	}
}

func hasBootstrappedV2Layout(installDir string) bool {
	if existingBinaryInInstallDir(installDir, "ncc-api-server") == "" {
		return false
	}
	if existingBinaryInInstallDir(installDir, "ncc-ui-server") == "" {
		return false
	}
	return existingDir(filepath.Join(installDir, "frontend-dist"))
}

func releaseHasRequiredV2Assets(rel githubRelease, goos, goarch string) bool {
	_, okStack := findAsset(rel, func(n string) bool {
		if !(strings.HasSuffix(n, ".zip") || strings.HasSuffix(n, ".tar.gz")) {
			return false
		}
		return strings.Contains(n, "ncc-v2-stack") && strings.Contains(n, goos) && strings.Contains(n, goarch)
	})
	if okStack {
		return true
	}
	_, okAPI := findAsset(rel, func(n string) bool {
		return strings.Contains(n, "ncc-api-server") && strings.Contains(n, goos) && strings.Contains(n, goarch) && !strings.HasSuffix(n, ".zip") && !strings.HasSuffix(n, ".tar.gz")
	})
	_, okUI := findAsset(rel, func(n string) bool {
		return strings.Contains(n, "ncc-ui-server") && strings.Contains(n, goos) && strings.Contains(n, goarch) && !strings.HasSuffix(n, ".zip") && !strings.HasSuffix(n, ".tar.gz")
	})
	_, okFrontend := findAsset(rel, func(n string) bool {
		if !(strings.HasSuffix(n, ".zip") || strings.HasSuffix(n, ".tar.gz")) {
			return false
		}
		return strings.Contains(n, "frontend") || strings.Contains(n, "ui-dist")
	})
	return okAPI && okUI && okFrontend
}

func pickBestReleaseWithV2Assets(releases []githubRelease, goos, goarch string, majorFilter int64) *githubRelease {
	var best *githubRelease
	for i := range releases {
		rel := &releases[i]
		if rel.Draft || rel.Prerelease {
			continue
		}
		if !releaseHasRequiredV2Assets(*rel, goos, goarch) {
			continue
		}
		if majorFilter > 0 {
			maj, err := parseVersionMajor(rel.TagName)
			if err != nil || maj != majorFilter {
				continue
			}
		}
		if best == nil || versionLess(best.TagName, rel.TagName) {
			best = rel
		}
	}
	return best
}

func runV2Bootstrap(opts v2BootstrapOptions) error {
	repo := strings.TrimSpace(opts.Repo)
	if repo == "" {
		repo = defaultGitHubRepo
	}
	repo, err := normalizeGitHubRepo(repo)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 30 * time.Second}
	releases, err := fetchGitHubReleases(repo, client)
	if err != nil {
		return err
	}
	if len(releases) == 0 {
		return fmt.Errorf("no releases found for %s", repo)
	}

	goos := runtime.GOOS
	goarch := runtime.GOARCH

	var rel *githubRelease
	if strings.TrimSpace(opts.Version) != "" {
		want := strings.TrimPrefix(strings.TrimSpace(opts.Version), "v")
		for i := range releases {
			tag := strings.TrimPrefix(strings.TrimSpace(releases[i].TagName), "v")
			if strings.EqualFold(tag, want) {
				rel = &releases[i]
				break
			}
		}
		if rel == nil {
			return fmt.Errorf("release version %q not found", opts.Version)
		}
		if !releaseHasRequiredV2Assets(*rel, goos, goarch) {
			return fmt.Errorf("release %s does not contain required v2 assets for %s/%s. available assets: %s", rel.TagName, goos, goarch, strings.Join(listAssetNames(*rel), ", "))
		}
	} else {
		rel = pickBestReleaseWithV2Assets(releases, goos, goarch, 2)
		if rel == nil {
			rel = pickBestReleaseWithV2Assets(releases, goos, goarch, 0)
		}
		if rel == nil {
			latestOverall := pickLatestSemverRelease(releases, 0)
			if latestOverall != nil {
				return fmt.Errorf("no stable release in %s contains required v2 assets for %s/%s. latest release is %s with assets: %s", repo, goos, goarch, latestOverall.TagName, strings.Join(listAssetNames(*latestOverall), ", "))
			}
			return errors.New("no stable releases available")
		}
	}
	stackAsset, okStack := findAsset(*rel, func(n string) bool {
		if !(strings.HasSuffix(n, ".zip") || strings.HasSuffix(n, ".tar.gz")) {
			return false
		}
		return strings.Contains(n, "ncc-v2-stack") && strings.Contains(n, goos) && strings.Contains(n, goarch)
	})
	apiAsset, okAPI := findAsset(*rel, func(n string) bool {
		return strings.Contains(n, "ncc-api-server") && strings.Contains(n, goos) && strings.Contains(n, goarch) && !strings.HasSuffix(n, ".zip") && !strings.HasSuffix(n, ".tar.gz")
	})
	uiAsset, okUI := findAsset(*rel, func(n string) bool {
		return strings.Contains(n, "ncc-ui-server") && strings.Contains(n, goos) && strings.Contains(n, goarch) && !strings.HasSuffix(n, ".zip") && !strings.HasSuffix(n, ".tar.gz")
	})
	frontendAsset, okFrontend := findAsset(*rel, func(n string) bool {
		if !(strings.HasSuffix(n, ".zip") || strings.HasSuffix(n, ".tar.gz")) {
			return false
		}
		return strings.Contains(n, "frontend") || strings.Contains(n, "ui-dist")
	})
	if !okStack && (!okAPI || !okUI || !okFrontend) {
		return fmt.Errorf("release %s does not contain all required v2 assets for %s/%s (need api binary, ui binary, frontend archive). available assets: %s",
			rel.TagName, goos, goarch, strings.Join(listAssetNames(*rel), ", "))
	}

	fmt.Fprintf(os.Stderr, "Selected release: %s\n", rel.TagName)
	if okStack {
		fmt.Fprintf(os.Stderr, "v2 stack bundle: %s\n", stackAsset.Name)
	} else {
		fmt.Fprintf(os.Stderr, "API binary: %s\n", apiAsset.Name)
		fmt.Fprintf(os.Stderr, "UI binary: %s\n", uiAsset.Name)
		fmt.Fprintf(os.Stderr, "Frontend bundle: %s\n", frontendAsset.Name)
	}
	if opts.CheckOnly {
		fmt.Fprintln(os.Stderr, "Check-only mode: no files downloaded.")
		return nil
	}

	installDir := strings.TrimSpace(opts.InstallDir)
	if installDir == "" {
		installDir = ".ncc-v2"
	}
	binDir := filepath.Join(installDir, "bin")
	frontendDir := filepath.Join(installDir, "frontend-dist")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(frontendDir, 0755); err != nil {
		return err
	}

	apiBin := filepath.Join(binDir, "ncc-api-server")
	uiBin := filepath.Join(binDir, "ncc-ui-server")
	if runtime.GOOS == "windows" {
		apiBin += ".exe"
		uiBin += ".exe"
	}

	if okStack {
		fmt.Fprintf(os.Stderr, "Downloading %s\n", stackAsset.BrowserDownloadURL)
		stackArchive, err := downloadBinaryURL(client, stackAsset.BrowserDownloadURL)
		if err != nil {
			return fmt.Errorf("download v2 stack bundle: %w", err)
		}
		if err := verifyDownloadedAsset(rel, stackAsset.Name, stackArchive, client, opts.SkipChecksumVerify); err != nil {
			return fmt.Errorf("verify v2 stack bundle: %w", err)
		}
		if err := extractArchiveByAssetName(stackArchive, stackAsset.Name, installDir); err != nil {
			return fmt.Errorf("extract v2 stack bundle: %w", err)
		}
		if !hasBootstrappedV2Layout(installDir) {
			return fmt.Errorf("v2 stack bundle %s extracted but required layout was not found in %s (expected bin/ncc-api-server, bin/ncc-ui-server, frontend-dist/)", stackAsset.Name, installDir)
		}
	} else {
		fmt.Fprintf(os.Stderr, "Downloading %s\n", apiAsset.BrowserDownloadURL)
		apiBody, err := downloadBinaryURL(client, apiAsset.BrowserDownloadURL)
		if err != nil {
			return fmt.Errorf("download api binary: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Downloading %s\n", uiAsset.BrowserDownloadURL)
		uiBody, err := downloadBinaryURL(client, uiAsset.BrowserDownloadURL)
		if err != nil {
			return fmt.Errorf("download ui binary: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Downloading %s\n", frontendAsset.BrowserDownloadURL)
		frontendArchive, err := downloadBinaryURL(client, frontendAsset.BrowserDownloadURL)
		if err != nil {
			return fmt.Errorf("download frontend archive: %w", err)
		}
		if err := verifyDownloadedAsset(rel, apiAsset.Name, apiBody, client, opts.SkipChecksumVerify); err != nil {
			return fmt.Errorf("verify api binary: %w", err)
		}
		if err := verifyDownloadedAsset(rel, uiAsset.Name, uiBody, client, opts.SkipChecksumVerify); err != nil {
			return fmt.Errorf("verify ui binary: %w", err)
		}
		if err := verifyDownloadedAsset(rel, frontendAsset.Name, frontendArchive, client, opts.SkipChecksumVerify); err != nil {
			return fmt.Errorf("verify frontend archive: %w", err)
		}

		if err := writeExecutable(apiBin, apiBody); err != nil {
			return fmt.Errorf("write api binary: %w", err)
		}
		if err := writeExecutable(uiBin, uiBody); err != nil {
			return fmt.Errorf("write ui binary: %w", err)
		}
		if err := extractArchiveByAssetName(frontendArchive, frontendAsset.Name, frontendDir); err != nil {
			return fmt.Errorf("extract frontend bundle: %w", err)
		}
	}

	cwd, _ := os.Getwd()
	// Same repo-root resolution as runV2Start: must contain all the paths
	// the api-server might touch, and must be EvalSymlinks-resolved so the
	// api-server's normalize check is consistent on /tmp-vs-/private/tmp.
	repoRoot := resolveV2RepoRoot(installDir, cwd)
	if strings.TrimSpace(opts.OrchestratorBin) == "" {
		opts.OrchestratorBin = "./ncc-orchestrator"
	}
	if strings.TrimSpace(opts.ConfigPath) == "" {
		opts.ConfigPath = "config.yaml"
	}
	if strings.TrimSpace(opts.OutputDir) == "" {
		opts.OutputDir = "outputfiles"
	}
	if strings.TrimSpace(opts.LogDir) == "" {
		opts.LogDir = "nccfiles"
	}
	if strings.TrimSpace(opts.APIListen) == "" {
		opts.APIListen = ":8081"
	}
	if strings.TrimSpace(opts.UIListen) == "" {
		opts.UIListen = ":8080"
	}
	if strings.TrimSpace(opts.TokenFile) == "" {
		opts.TokenFile = ".ncc-api-token"
	}
	backendURL := "http://localhost:8081"
	if strings.HasPrefix(opts.APIListen, ":") {
		backendURL = "http://localhost" + opts.APIListen
	}
	uiOrigin := "http://localhost:8080"
	if strings.HasPrefix(opts.UIListen, ":") {
		uiOrigin = "http://localhost" + opts.UIListen
	}

	apiScript := fmt.Sprintf(`#!/usr/bin/env sh
set -eu
DIR="$(cd "$(dirname "$0")" && pwd)"
"%s" --listen %s --repo-root %s --config-path %s --output-dir %s --log-dir %s --orchestrator-bin %s
`, apiBin, shellQuote(opts.APIListen), shellQuote(repoRoot), shellQuote(opts.ConfigPath), shellQuote(opts.OutputDir), shellQuote(opts.LogDir), shellQuote(opts.OrchestratorBin))
	uiScript := fmt.Sprintf(`#!/usr/bin/env sh
set -eu
DIR="$(cd "$(dirname "$0")" && pwd)"
"%s" --listen %s --dir %s --backend-url %s --api-token-file %s --api-auth-mode token --allowed-origins %s
`, uiBin, shellQuote(opts.UIListen), shellQuote(frontendDir), shellQuote(backendURL), shellQuote(opts.TokenFile), shellQuote(uiOrigin))

	apiScriptPath := filepath.Join(installDir, "start-api.sh")
	uiScriptPath := filepath.Join(installDir, "start-ui.sh")
	if runtime.GOOS == "windows" {
		apiScriptPath = filepath.Join(installDir, "start-api.cmd")
		uiScriptPath = filepath.Join(installDir, "start-ui.cmd")
		apiScript = fmt.Sprintf("@echo off\r\n\"%s\" --listen %s --repo-root %s --config-path %s --output-dir %s --log-dir %s --orchestrator-bin %s\r\n",
			apiBin, opts.APIListen, repoRoot, opts.ConfigPath, opts.OutputDir, opts.LogDir, opts.OrchestratorBin)
		uiScript = fmt.Sprintf("@echo off\r\n\"%s\" --listen %s --dir %s --backend-url %s --api-token-file %s --api-auth-mode token --allowed-origins %s\r\n",
			uiBin, opts.UIListen, frontendDir, backendURL, opts.TokenFile, uiOrigin)
	}
	if err := os.WriteFile(apiScriptPath, []byte(apiScript), 0755); err != nil {
		return err
	}
	if err := os.WriteFile(uiScriptPath, []byte(uiScript), 0755); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "v2 bootstrap completed in %s\n", installDir)
	fmt.Fprintf(os.Stderr, "Start API: %s\n", apiScriptPath)
	fmt.Fprintf(os.Stderr, "Start UI : %s\n", uiScriptPath)
	fmt.Fprintf(os.Stderr, "Open UI  : %s\n", uiOrigin)
	return nil
}

// runUpdate fetches release metadata and updates or checks binary availability.
func runUpdate(opts updateOptions) error {
	currentVer := stripGoBuildGitSuffix(Version)
	client := &http.Client{Timeout: 20 * time.Second}
	providedSHA256 := strings.TrimSpace(opts.BinarySHA256)
	if providedSHA256 != "" {
		normalized, err := normalizeSHA256Hex(providedSHA256)
		if err != nil {
			return fmt.Errorf("invalid --binary-sha256: %w", err)
		}
		providedSHA256 = normalized
	}

	if strings.TrimSpace(opts.BinaryURL) != "" {
		if isArchiveAssetURL(opts.BinaryURL) {
			return fmt.Errorf("binary URL points to archive; provide direct binary URL: %s", opts.BinaryURL)
		}
		if strings.TrimSpace(opts.TargetVersion) != "" {
			if err := enforceMajorUpgradePolicy(currentVer, opts.TargetVersion, opts.AllowMajorUpgrade); err != nil {
				return err
			}
		}
		if opts.CheckOnly {
			req, err := http.NewRequest(http.MethodHead, opts.BinaryURL, nil)
			if err != nil {
				return fmt.Errorf("create HEAD request: %w", err)
			}
			resp, err := client.Do(req)
			if err != nil {
				return fmt.Errorf("check binary URL: %w", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return fmt.Errorf("binary URL check failed: HTTP %d", resp.StatusCode)
			}
			fmt.Fprintf(os.Stderr, "Binary URL is reachable: %s (status %d)\n", opts.BinaryURL, resp.StatusCode)
			if strings.TrimSpace(opts.TargetVersion) != "" {
				if versionLess(currentVer, opts.TargetVersion) {
					fmt.Fprintf(os.Stderr, "Update available: current=%s target=%s\n", currentVer, opts.TargetVersion)
				} else {
					fmt.Fprintf(os.Stderr, "No upgrade needed: current=%s target=%s\n", currentVer, opts.TargetVersion)
				}
			}
			return nil
		}
		if providedSHA256 == "" {
			return errors.New("--binary-sha256 is required when using --binary-url")
		}
		fmt.Fprintf(os.Stderr, "Downloading %s ...\n", opts.BinaryURL)
		body, err := downloadBinaryURL(client, opts.BinaryURL)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(body)
		got := hex.EncodeToString(sum[:])
		if !strings.EqualFold(got, providedSHA256) {
			return fmt.Errorf("checksum mismatch: expected %s, got %s", providedSHA256, got)
		}
		fmt.Fprintln(os.Stderr, "Checksum verified.")
		targetVer := strings.TrimSpace(opts.TargetVersion)
		if targetVer == "" {
			targetVer = "custom"
		}
		return installDownloadedBinary(body, targetVer)
	}

	repo := strings.TrimSpace(opts.Repo)
	if repo == "" {
		repo = defaultGitHubRepo
	}
	repo, err := normalizeGitHubRepo(repo)
	if err != nil {
		return err
	}
	releases, err := fetchGitHubReleases(repo, client)
	if err != nil {
		return err
	}
	if len(releases) == 0 {
		return fmt.Errorf("no releases found for %s", repo)
	}

	currentMajor, _ := parseVersionMajor(currentVer)
	latestOverall := pickLatestSemverRelease(releases, 0)
	if latestOverall == nil {
		return fmt.Errorf("no stable releases found for %s", repo)
	}
	targetRelease := latestOverall
	if !opts.AllowMajorUpgrade && currentMajor > 0 {
		if sameMajor := pickLatestSemverRelease(releases, currentMajor); sameMajor != nil {
			targetRelease = sameMajor
		}
	}
	if strings.TrimSpace(opts.TargetVersion) != "" {
		want := strings.TrimPrefix(strings.TrimSpace(opts.TargetVersion), "v")
		var match *githubRelease
		for i := range releases {
			tag := strings.TrimPrefix(strings.TrimSpace(releases[i].TagName), "v")
			if strings.EqualFold(tag, want) {
				match = &releases[i]
				break
			}
		}
		if match == nil {
			return fmt.Errorf("target version %q not found in repo %s", opts.TargetVersion, repo)
		}
		targetRelease = match
	}
	targetVer := strings.TrimPrefix(strings.TrimSpace(targetRelease.TagName), "v")
	if err := enforceMajorUpgradePolicy(currentVer, targetVer, opts.AllowMajorUpgrade); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "Current version: %s\n", currentVer)
	fmt.Fprintf(os.Stderr, "Selected release: %s\n", targetRelease.TagName)
	if targetRelease.TagName != latestOverall.TagName {
		fmt.Fprintf(os.Stderr, "Latest overall release: %s (major upgrade not auto-applied)\n", latestOverall.TagName)
		if currentMajor == 1 {
			fmt.Fprintln(os.Stderr, "Use --allow-major-upgrade to move from v1.x to v2.x after migration review.")
		}
	}
	// Prefer the stack archive when available: this is the canonical
	// "update the whole package" path. Falls back to single-binary update
	// for legacy releases (v1.x) that don't ship a stack archive.
	stackURL, stackName := pickStackAssetForCurrentPlatform(*targetRelease)

	if opts.CheckOnly {
		if opts.JSONOut {
			result := map[string]interface{}{
				"current_version":  currentVer,
				"latest_version":   targetVer,
				"latest_overall":   strings.TrimPrefix(strings.TrimSpace(latestOverall.TagName), "v"),
				"update_available": versionLess(currentVer, targetVer),
				"has_package":      stackURL != "",
			}
			if b, err := json.Marshal(result); err == nil {
				fmt.Fprintf(os.Stdout, "%s%s\n", updateCheckJSONPrefix, string(b))
			}
		}
		if versionLess(currentVer, targetVer) {
			fmt.Fprintf(os.Stderr, "Update available in track: %s -> %s\n", currentVer, targetVer)
		} else if versionLess(targetVer, currentVer) {
			fmt.Fprintln(os.Stderr, "You have a newer version than the selected release track (dev build).")
		} else {
			fmt.Fprintln(os.Stderr, "You are already on the latest version for the selected track.")
		}
		if stackURL != "" {
			fmt.Fprintf(os.Stderr, "Package candidate for %s/%s: %s (%s)\n", runtime.GOOS, runtime.GOARCH, stackName, stackURL)
		} else {
			downloadURL, assetName := pickAssetForCurrentPlatform(*targetRelease)
			if downloadURL != "" {
				fmt.Fprintf(os.Stderr, "Binary candidate for %s/%s: %s (%s)\n", runtime.GOOS, runtime.GOARCH, assetName, downloadURL)
			}
		}
		return nil
	}
	if !versionLess(currentVer, targetVer) {
		if versionLess(targetVer, currentVer) {
			fmt.Fprintln(os.Stderr, "You have a newer version than the selected release (dev build).")
		} else {
			fmt.Fprintln(os.Stderr, "You are already on the latest version for the selected track.")
		}
		return nil
	}

	if stackURL != "" {
		return installPackageUpdate(stackURL, stackName, targetRelease, targetVer, client, opts.SkipChecksumVerify)
	}

	// Legacy single-binary update path (v1.x releases without a stack archive).
	downloadURL, chosenAssetName := pickAssetForCurrentPlatform(*targetRelease)
	if downloadURL == "" {
		fmt.Fprintf(os.Stderr, "No binary found for %s/%s. Download manually: %s\n", runtime.GOOS, runtime.GOARCH, targetRelease.HTMLURL)
		return nil
	}
	if isArchiveAssetURL(downloadURL) {
		fmt.Fprintf(os.Stderr, "Binary for %s/%s is only available as archive. Download and extract: %s\n", runtime.GOOS, runtime.GOARCH, downloadURL)
		return nil
	}

	fmt.Fprintf(os.Stderr, "Downloading %s ...\n", downloadURL)
	body, err := downloadBinaryURL(client, downloadURL)
	if err != nil {
		return err
	}
	if err := verifyDownloadedAsset(targetRelease, chosenAssetName, body, client, opts.SkipChecksumVerify); err != nil {
		return err
	}
	return installDownloadedBinary(body, targetVer)
}

func fetchURL(url string, client *http.Client) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if token := os.Getenv("GITHUB_TOKEN"); token != "" && strings.Contains(url, "github") {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
}

// parseChecksumFile finds a line in checksum body that matches filename (or *filename) and returns the hex hash.
func parseChecksumFile(body []byte, filename string) string {
	lines := strings.Split(string(body), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// Format: "hash  filename" or "hash *filename"
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		hashStr := fields[0]
		name := fields[1]
		if strings.TrimPrefix(name, "*") == filename || name == filename {
			hashStr = strings.TrimPrefix(hashStr, "0x")
			if len(hashStr) == 64 {
				return hashStr
			}
			return ""
		}
	}
	return ""
}

// pcListClustersResponse represents a minimal Prism Central v3 clusters/list response.
type pcListClustersResponse struct {
	Entities []map[string]interface{} `json:"entities"`
}

// errDiscoverV4Unavailable signals that the v4 clustermgmt API is not available (e.g. HTTP 404).
type errDiscoverV4Unavailable struct {
	status int
	body   string
}

func (e errDiscoverV4Unavailable) Error() string {
	return fmt.Sprintf("Prism Central clustermgmt v4 returned HTTP %d", e.status)
}

func discoverHTTPClient(insecure bool) *http.Client {
	return &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
		},
	}
}

func normalizePCBaseURL(raw string, insecureSkipVerify bool) (string, error) {
	target := strings.TrimSpace(raw)
	if target == "" {
		return "", errors.New("pc target is empty")
	}
	if !strings.HasPrefix(target, "https://") && !strings.HasPrefix(target, "http://") {
		target = "https://" + target
	}
	u, err := url.Parse(target)
	if err != nil {
		return "", fmt.Errorf("parse PC target %q: %w", raw, err)
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return "", fmt.Errorf("PC target must use http or https scheme, got %q", u.Scheme)
	}
	if u.Scheme == "http" && !insecureSkipVerify {
		return "", errors.New("PC target with http:// requires --insecure-skip-verify")
	}
	if strings.TrimSpace(u.Host) == "" {
		return "", fmt.Errorf("PC target %q has no host", raw)
	}
	hostname := u.Hostname()
	if err := validateClusterAddress(hostname); err != nil {
		return "", fmt.Errorf("PC host %q: %w", hostname, err)
	}
	port := u.Port()
	if strings.TrimSpace(port) == "" {
		port = strconv.Itoa(prismGatewayPort)
	}
	clean := url.URL{
		Scheme: u.Scheme,
		Host:   net.JoinHostPort(hostname, port),
	}
	return strings.TrimSuffix(clean.String(), "/"), nil
}

func discoverClustersFromPCTargets(cfg Config) ([]string, error) {
	apiVersion := strings.ToLower(strings.TrimSpace(cfg.DiscoverAPIVersion))
	if apiVersion == "" {
		apiVersion = defaultDiscoverAPIVersion
	}
	if apiVersion != "v3" && apiVersion != "v4" {
		return nil, fmt.Errorf("discover-api-version must be v3 or v4, got %q", apiVersion)
	}

	seen := make(map[string]bool)
	out := make([]string, 0)
	for _, pc := range cfg.PCs {
		pcURL, err := normalizePCBaseURL(pc, cfg.InsecureSkipVerify)
		if err != nil {
			return nil, err
		}
		var discovered []string
		if apiVersion == "v4" {
			discovered, err = fetchPCClustersV4(pcURL, cfg.Username, cfg.Password, cfg.InsecureSkipVerify, cfg.NutanixV4APIVersion)
			if err != nil {
				var v4Unavailable errDiscoverV4Unavailable
				if errors.As(err, &v4Unavailable) {
					discovered, err = fetchPCClustersV3(pcURL, cfg.Username, cfg.Password, cfg.InsecureSkipVerify)
				}
			}
		} else {
			discovered, err = fetchPCClustersV3(pcURL, cfg.Username, cfg.Password, cfg.InsecureSkipVerify)
		}
		if err != nil {
			return nil, fmt.Errorf("discover clusters from PC %s: %w", pcURL, err)
		}
		for _, addr := range discovered {
			addr = strings.TrimSpace(addr)
			if addr == "" || seen[addr] {
				continue
			}
			seen[addr] = true
			out = append(out, addr)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no clusters discovered from provided PCs")
	}
	return out, nil
}

// fetchPCClustersV3 lists clusters via legacy POST /api/nutanix/v3/clusters/list.
func fetchPCClustersV3(pcURL, username, password string, insecure bool) ([]string, error) {
	u := pcURL + "/api/nutanix/v3/clusters/list"
	body := []byte(`{}`)
	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(username, password)

	client := discoverHTTPClient(insecure)
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request to Prism Central: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Prism Central returned %d: %s", resp.StatusCode, string(bytes.TrimSpace(data)))
	}

	var list pcListClustersResponse
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	var addresses []string
	for _, e := range list.Entities {
		addr := extractClusterAddressV3(e)
		if addr != "" {
			addresses = append(addresses, addr)
		}
	}
	return addresses, nil
}

// fetchPCClustersV4 lists clusters via GET /api/clustermgmt/{ver}/config/clusters with $page / $limit pagination.
func fetchPCClustersV4(pcURL, username, password string, insecure bool, v4APIVer string) ([]string, error) {
	ver := nutanixV4PathSegment(v4APIVer)
	base := strings.TrimSuffix(pcURL, "/") + "/api/clustermgmt/" + ver + "/config/clusters"
	client := discoverHTTPClient(insecure)
	const pageSize = 100
	seen := make(map[string]bool)
	var out []string

	for page := 0; page < 1000; page++ {
		u, err := url.Parse(base)
		if err != nil {
			return nil, fmt.Errorf("parse URL: %w", err)
		}
		q := url.Values{}
		q.Set("$limit", strconv.Itoa(pageSize))
		q.Set("$page", strconv.Itoa(page))
		u.RawQuery = q.Encode()

		req, err := http.NewRequest(http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		req.SetBasicAuth(username, password)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("request to Prism Central: %w", err)
		}
		data, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("read response: %w", err)
		}
		if resp.StatusCode == http.StatusNotFound {
			return nil, errDiscoverV4Unavailable{status: resp.StatusCode, body: string(data)}
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("Prism Central returned %d: %s", resp.StatusCode, string(bytes.TrimSpace(data)))
		}

		var payload struct {
			Metadata struct {
				TotalAvailableResults int `json:"totalAvailableResults"`
			} `json:"metadata"`
			Data []map[string]interface{} `json:"data"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, fmt.Errorf("parse response: %w", err)
		}
		if len(payload.Data) == 0 {
			break
		}
		for _, e := range payload.Data {
			addr := extractClusterAddressV4(e)
			if addr != "" && !seen[addr] {
				seen[addr] = true
				out = append(out, addr)
			}
		}
		total := payload.Metadata.TotalAvailableResults
		if total > 0 && len(out) >= total {
			break
		}
		// Next page until empty; do not stop on len(data) < $limit (server may cap below our limit).
	}
	return out, nil
}

// DiscoverClusterRow is one registered cluster from discover-clusters (table/json output).
type DiscoverClusterRow struct {
	Name    string `json:"name"`
	ExtID   string `json:"ext_id"`
	Address string `json:"address"`
	API     string `json:"api"`
}

func discoverRowFromV4Entity(e map[string]interface{}) DiscoverClusterRow {
	if e == nil {
		return DiscoverClusterRow{}
	}
	name, _ := e["name"].(string)
	return DiscoverClusterRow{
		Name:    strings.TrimSpace(name),
		ExtID:   extractClusterExtIDV4(e),
		Address: extractClusterAddressV4(e),
		API:     "v4",
	}
}

func extractClusterExtIDV3(e map[string]interface{}) string {
	if m, _ := e["metadata"].(map[string]interface{}); m != nil {
		if u, _ := m["uuid"].(string); strings.TrimSpace(u) != "" {
			return strings.TrimSpace(u)
		}
	}
	return ""
}

func discoverRowFromV3Entity(e map[string]interface{}) DiscoverClusterRow {
	if e == nil {
		return DiscoverClusterRow{}
	}
	name := ""
	if m, _ := e["metadata"].(map[string]interface{}); m != nil {
		if n, _ := m["name"].(string); strings.TrimSpace(n) != "" {
			name = strings.TrimSpace(n)
		}
	}
	if name == "" {
		if spec, _ := e["spec"].(map[string]interface{}); spec != nil {
			if n, _ := spec["name"].(string); strings.TrimSpace(n) != "" {
				name = strings.TrimSpace(n)
			}
		}
	}
	return DiscoverClusterRow{
		Name:    name,
		ExtID:   extractClusterExtIDV3(e),
		Address: extractClusterAddressV3(e),
		API:     "v3",
	}
}

// fetchDiscoverClusterRowsV4 returns cluster rows from GET clustermgmt v4 (paginated).
func fetchDiscoverClusterRowsV4(pcURL, username, password string, insecure bool, v4APIVer string) ([]DiscoverClusterRow, error) {
	ver := nutanixV4PathSegment(v4APIVer)
	base := strings.TrimSuffix(pcURL, "/") + "/api/clustermgmt/" + ver + "/config/clusters"
	client := discoverHTTPClient(insecure)
	const pageSize = 100
	seen := make(map[string]bool)
	var out []DiscoverClusterRow

	for page := 0; page < 1000; page++ {
		u, err := url.Parse(base)
		if err != nil {
			return nil, fmt.Errorf("parse URL: %w", err)
		}
		q := url.Values{}
		q.Set("$limit", strconv.Itoa(pageSize))
		q.Set("$page", strconv.Itoa(page))
		u.RawQuery = q.Encode()

		req, err := http.NewRequest(http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		req.SetBasicAuth(username, password)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("request to Prism Central: %w", err)
		}
		data, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("read response: %w", err)
		}
		if resp.StatusCode == http.StatusNotFound {
			return nil, errDiscoverV4Unavailable{status: resp.StatusCode, body: string(data)}
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("Prism Central returned %d: %s", resp.StatusCode, string(bytes.TrimSpace(data)))
		}

		var payload struct {
			Metadata struct {
				TotalAvailableResults int `json:"totalAvailableResults"`
			} `json:"metadata"`
			Data []map[string]interface{} `json:"data"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, fmt.Errorf("parse response: %w", err)
		}
		if len(payload.Data) == 0 {
			break
		}
		for _, e := range payload.Data {
			row := discoverRowFromV4Entity(e)
			key := row.ExtID
			if key == "" {
				key = row.Address + row.Name
			}
			if key != "" && !seen[key] {
				seen[key] = true
				out = append(out, row)
			}
		}
		total := payload.Metadata.TotalAvailableResults
		if total > 0 && len(out) >= total {
			break
		}
	}
	return out, nil
}

// fetchDiscoverClusterRowsV3 returns cluster rows from POST v3 clusters/list.
func fetchDiscoverClusterRowsV3(pcURL, username, password string, insecure bool) ([]DiscoverClusterRow, error) {
	u := pcURL + "/api/nutanix/v3/clusters/list"
	body := []byte(`{}`)
	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(username, password)

	client := discoverHTTPClient(insecure)
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request to Prism Central: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Prism Central returned %d: %s", resp.StatusCode, string(bytes.TrimSpace(data)))
	}

	var list pcListClustersResponse
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	var out []DiscoverClusterRow
	for _, e := range list.Entities {
		out = append(out, discoverRowFromV3Entity(e))
	}
	return out, nil
}

// runDiscoverClusters lists clusters from Prism Central (default v4 GET clustermgmt; optional v3 or v4→v3 fallback).
func runDiscoverClusters(cmd *cobra.Command, args []string) error {
	// Load config file if set (so prism-central-url, username, etc. can come from config)
	if cfgFile := viper.GetString("config"); cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		_ = viper.ReadInConfig()
	}
	pcURL := strings.TrimSuffix(strings.TrimSpace(viper.GetString("prism-central-url")), "/")
	if pcURL == "" {
		return exitConfig(errors.New("prism-central-url is required (flag or config)"))
	}
	if !strings.HasPrefix(pcURL, "https://") && !strings.HasPrefix(pcURL, "http://") {
		pcURL = "https://" + pcURL
	}
	// Discover defines its own username/password/insecure flags; those must NOT be
	// viper.BindPFlag'd to the same keys as the root command (the last bind wins and
	// breaks the main ncc-orchestrator run). Merge: flag when set, else viper/config.
	username := viper.GetString("username")
	if cmd.Flags().Changed("username") {
		username, _ = cmd.Flags().GetString("username")
	}
	if strings.TrimSpace(username) == "" {
		username = "admin"
	}
	password := viper.GetString("password")
	if cmd.Flags().Changed("password") {
		password, _ = cmd.Flags().GetString("password")
	}
	if password == "" {
		password = os.Getenv("NCC_PASSWORD")
	}
	if password == "" {
		var err error
		password, err = promptPasswordIfEmpty("", username)
		if err != nil {
			return err
		}
	}
	insecure := viper.GetBool("insecure-skip-verify")
	if cmd.Flags().Changed("insecure-skip-verify") {
		insecure, _ = cmd.Flags().GetBool("insecure-skip-verify")
	}

	discoverVer, _ := cmd.Flags().GetString("discover-api-version")
	discoverVer = strings.ToLower(strings.TrimSpace(discoverVer))
	if discoverVer == "" {
		discoverVer = strings.ToLower(strings.TrimSpace(viper.GetString("discover-api-version")))
	}
	if discoverVer == "" {
		discoverVer = "v4"
	}
	if discoverVer != "v3" && discoverVer != "v4" {
		return exitConfig(fmt.Errorf("discover-api-version must be v3 or v4, got %q", discoverVer))
	}
	if discoverVer == "v4" {
		if err := validateNutanixV4APIVersion(viper.GetString("nutanix-v4-api-version")); err != nil {
			return exitConfig(fmt.Errorf("nutanix-v4-api-version: %w", err))
		}
	}

	format, _ := cmd.Flags().GetString("format")
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		format = "lines"
	}
	switch format {
	case "lines", "table", "json":
	default:
		return exitConfig(fmt.Errorf("format must be lines, table, or json, got %q", format))
	}

	var rows []DiscoverClusterRow
	var err error
	if discoverVer == "v4" {
		rows, err = fetchDiscoverClusterRowsV4(pcURL, username, password, insecure, viper.GetString("nutanix-v4-api-version"))
		if err != nil {
			var v4un errDiscoverV4Unavailable
			if errors.As(err, &v4un) && v4un.status == http.StatusNotFound {
				fmt.Fprintf(os.Stderr, "discover-clusters: v4 API not found (%d), falling back to v3\n", v4un.status)
				rows, err = fetchDiscoverClusterRowsV3(pcURL, username, password, insecure)
			}
		}
	} else {
		rows, err = fetchDiscoverClusterRowsV3(pcURL, username, password, insecure)
	}
	if err != nil {
		return err
	}

	outPath, _ := cmd.Flags().GetString("output")
	writeOut := func(content string) error {
		if outPath != "" {
			if err := os.WriteFile(outPath, []byte(content), 0644); err != nil {
				return fmt.Errorf("write %s: %w", outPath, err)
			}
			fmt.Fprintf(os.Stderr, "Wrote %d cluster(s) to %s\n", len(rows), outPath)
			return nil
		}
		fmt.Print(content)
		return nil
	}

	switch format {
	case "json":
		b, err := json.MarshalIndent(rows, "", "  ")
		if err != nil {
			return err
		}
		return writeOut(string(b) + "\n")
	case "table":
		var buf strings.Builder
		tw := tabwriter.NewWriter(&buf, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "NAME\tEXT_ID\tADDRESS\tAPI")
		for _, r := range rows {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", r.Name, r.ExtID, r.Address, r.API)
		}
		_ = tw.Flush()
		return writeOut(buf.String())
	default: // lines
		var lines []string
		for _, r := range rows {
			if r.Address != "" {
				lines = append(lines, r.Address)
			} else if r.Name != "" {
				lines = append(lines, r.Name)
			}
		}
		return writeOut(strings.Join(lines, "\n") + "\n")
	}
}

func normalizeScheduleType(s string) (string, error) {
	v := strings.ToLower(strings.TrimSpace(s))
	switch v {
	case "", "auto":
		if runtime.GOOS == "windows" {
			return "windows", nil
		}
		return "cron", nil
	case "cron":
		return "cron", nil
	case "systemd", "systemd-timer", "timer":
		return "systemd", nil
	case "windows", "windows-task":
		return "windows", nil
	default:
		return "", fmt.Errorf("type must be auto, cron, systemd, or windows (got %q)", s)
	}
}

func cronExprFromInterval(every time.Duration) (string, error) {
	if every <= 0 {
		return "", errors.New("every must be > 0")
	}
	if every%time.Minute != 0 {
		return "", errors.New("every must be a whole number of minutes for cron")
	}
	minutes := int(every / time.Minute)
	switch {
	case minutes < 60:
		return fmt.Sprintf("*/%d * * * *", minutes), nil
	case minutes%60 == 0 && minutes < 24*60:
		return fmt.Sprintf("0 */%d * * *", minutes/60), nil
	case minutes%(24*60) == 0:
		days := minutes / (24 * 60)
		return fmt.Sprintf("0 0 */%d * *", days), nil
	default:
		return "", errors.New("every must be an even minute/hour/day interval for cron (or provide --cron)")
	}
}

func shellQuotePOSIX(s string) string {
	if s == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func validateScheduleTaskName(taskName string) error {
	name := strings.TrimSpace(taskName)
	if name == "" {
		return errors.New("task-name must not be empty")
	}
	if len(name) > 128 {
		return errors.New("task-name too long")
	}
	for _, ch := range name {
		if !(ch == '.' || ch == '_' || ch == '-' || ch == ':' ||
			(ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')) {
			return errors.New("task-name contains invalid characters")
		}
	}
	return nil
}

func sanitizeScheduleCommand(runCmd string) (string, error) {
	v := strings.TrimSpace(runCmd)
	if v == "" {
		return "", errors.New("command cannot be empty")
	}
	if strings.ContainsAny(v, "&;|`$><\n\r\t") {
		return "", errors.New("command contains unsafe shell metacharacters")
	}
	return v, nil
}

func parseCommandLineStrict(raw string) (string, []string, error) {
	clean, err := sanitizeScheduleCommand(raw)
	if err != nil {
		return "", nil, err
	}
	parts := []string{}
	var current strings.Builder
	var quote rune
	flush := func() {
		if current.Len() == 0 {
			return
		}
		parts = append(parts, current.String())
		current.Reset()
	}
	for _, ch := range clean {
		if quote != 0 {
			if ch == quote {
				quote = 0
				continue
			}
			current.WriteRune(ch)
			continue
		}
		if ch == '"' || ch == '\'' {
			quote = ch
			continue
		}
		if ch == ' ' {
			flush()
			continue
		}
		current.WriteRune(ch)
	}
	if quote != 0 {
		return "", nil, errors.New("unterminated quotes in command")
	}
	flush()
	if len(parts) == 0 {
		return "", nil, errors.New("command cannot be empty")
	}
	name := strings.TrimSpace(parts[0])
	if name == "" {
		return "", nil, errors.New("command executable cannot be empty")
	}
	args := make([]string, 0, len(parts)-1)
	for _, p := range parts[1:] {
		args = append(args, strings.TrimSpace(p))
	}
	return name, args, nil
}

func scheduleMarker(taskName string) string {
	name := strings.TrimSpace(taskName)
	if name == "" {
		name = "default"
	}
	return "ncc-orchestrator:" + name
}

// scheduleRunHistoryFlags are appended to generated schedule commands so a
// scheduled run records a per-run snapshot under <output-dir-filtered>/runs.
// Without this, scheduled scans only overwrite the single in-place
// run-summary.json and never accumulate, so the dashboard "recent runs" history
// omits them entirely (only API/UI-triggered runs, which the api-server archives
// itself, would appear). The flag is passed explicitly rather than relying on
// `run-history: true` in config because it is the authoritative source for the
// run and is honored regardless of config-key precedence. Retention is bounded
// so a frequent schedule can't grow the history dir without limit.
const scheduleRunHistoryFlags = "--run-history --retain-last 500 --retain-days 30"

func defaultScheduleCommand(configPath string) (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve executable: %w", err)
	}
	exe = filepath.Clean(exe)
	if runtime.GOOS == "windows" {
		if strings.TrimSpace(configPath) == "" {
			return fmt.Sprintf("\"%s\" %s", exe, scheduleRunHistoryFlags), nil
		}
		return fmt.Sprintf("\"%s\" --config \"%s\" %s", exe, configPath, scheduleRunHistoryFlags), nil
	}
	if strings.TrimSpace(configPath) == "" {
		return fmt.Sprintf("%s %s", shellQuotePOSIX(exe), scheduleRunHistoryFlags), nil
	}
	return fmt.Sprintf("%s --config %s %s", shellQuotePOSIX(exe), shellQuotePOSIX(configPath), scheduleRunHistoryFlags), nil
}

func upsertScheduleLine(content, marker, line string) string {
	lines := splitLines(content)
	out := make([]string, 0, len(lines)+1)
	for _, ln := range lines {
		trim := strings.TrimSpace(ln)
		if trim == "" {
			continue
		}
		if strings.Contains(ln, marker) {
			continue
		}
		out = append(out, ln)
	}
	out = append(out, line)
	return strings.Join(out, "\n") + "\n"
}

func removeScheduleLine(content, marker string) (string, bool) {
	lines := splitLines(content)
	out := make([]string, 0, len(lines))
	removed := false
	for _, ln := range lines {
		trim := strings.TrimSpace(ln)
		if trim == "" {
			continue
		}
		if strings.Contains(ln, marker) {
			removed = true
			continue
		}
		out = append(out, ln)
	}
	if len(out) == 0 {
		return "", removed
	}
	return strings.Join(out, "\n") + "\n", removed
}

func listCronSchedules(taskName string) error {
	marker := scheduleMarker(taskName)
	out, err := exec.Command("crontab", "-l").CombinedOutput()
	if err != nil {
		low := strings.ToLower(string(out))
		if strings.Contains(low, "no crontab") {
			fmt.Printf("No cron entries found for marker %q\n", marker)
			return nil
		}
		return fmt.Errorf("read current crontab: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	lines := splitLines(string(out))
	found := false
	for _, ln := range lines {
		if strings.Contains(ln, marker) {
			fmt.Println(ln)
			found = true
		}
	}
	if !found {
		fmt.Printf("No cron entries found for marker %q\n", marker)
	}
	return nil
}

func removeCronSchedule(taskName string) error {
	marker := scheduleMarker(taskName)
	existingBytes, err := exec.Command("crontab", "-l").CombinedOutput()
	existing := string(existingBytes)
	if err != nil {
		low := strings.ToLower(existing)
		if strings.Contains(low, "no crontab") {
			fmt.Printf("No cron entries found for marker %q\n", marker)
			return nil
		}
		return fmt.Errorf("read current crontab: %v (%s)", err, strings.TrimSpace(existing))
	}
	updated, removed := removeScheduleLine(existing, marker)
	if !removed {
		fmt.Printf("No cron entries found for marker %q\n", marker)
		return nil
	}
	c := exec.Command("crontab", "-")
	c.Stdin = strings.NewReader(updated)
	if out, err := c.CombinedOutput(); err != nil {
		return fmt.Errorf("write updated crontab: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("Removed cron entry for marker %q\n", marker)
	return nil
}

func listWindowsSchedule(taskName string) error {
	if runtime.GOOS != "windows" {
		return errors.New("windows schedule can only be listed on Windows")
	}
	out, err := exec.Command("schtasks", "/Query", "/TN", taskName, "/FO", "LIST", "/V").CombinedOutput()
	if err != nil {
		return fmt.Errorf("query scheduled task: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	fmt.Print(string(out))
	return nil
}

func removeWindowsSchedule(taskName string) error {
	if runtime.GOOS != "windows" {
		return errors.New("windows schedule can only be removed on Windows")
	}
	out, err := exec.Command("schtasks", "/Delete", "/TN", taskName, "/F").CombinedOutput()
	if err != nil {
		return fmt.Errorf("delete scheduled task: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("Removed Windows Scheduled Task %q\n%s\n", taskName, strings.TrimSpace(string(out)))
	return nil
}

func runScheduleCommandNow(runCmd string) error {
	name, args, err := parseCommandLineStrict(runCmd)
	if err != nil {
		return err
	}
	c := exec.Command(name, args...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	c.Stdin = os.Stdin
	return c.Run()
}

func buildCronScheduleLine(cronSpec, runCmd, logPath, marker string, withLock bool) string {
	if withLock {
		lockFile := filepath.Join(filepath.Dir(logPath), ".ncc-scheduler.lock")
		return fmt.Sprintf("%s flock -n %s -c %s >> %s 2>&1 # %s",
			cronSpec,
			shellQuotePOSIX(lockFile),
			shellQuotePOSIX(runCmd),
			shellQuotePOSIX(logPath),
			marker,
		)
	}
	return fmt.Sprintf("%s %s >> %s 2>&1 # %s", cronSpec, runCmd, shellQuotePOSIX(logPath), marker)
}

func installCronSchedule(taskName, cronSpec, runCmd, logPath string, withLock bool) error {
	marker := scheduleMarker(taskName)
	line := buildCronScheduleLine(cronSpec, runCmd, logPath, marker, withLock)

	existingBytes, err := exec.Command("crontab", "-l").CombinedOutput()
	existing := string(existingBytes)
	if err != nil {
		// "no crontab for <user>" should be treated as empty content.
		low := strings.ToLower(existing)
		if !strings.Contains(low, "no crontab") {
			return fmt.Errorf("read current crontab: %v (%s)", err, strings.TrimSpace(existing))
		}
		existing = ""
	}

	updated := upsertScheduleLine(existing, marker, line)
	c := exec.Command("crontab", "-")
	c.Stdin = strings.NewReader(updated)
	out, err := c.CombinedOutput()
	if err != nil {
		return fmt.Errorf("install crontab entry: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("Installed cron schedule with marker %q\n", marker)
	fmt.Printf("%s\n", line)
	return nil
}

func installWindowsSchedule(taskName, runCmd string, every time.Duration) error {
	if runtime.GOOS != "windows" {
		return errors.New("windows schedule can only be created on Windows")
	}
	if every <= 0 {
		return errors.New("every must be > 0")
	}
	if every%time.Minute != 0 {
		return errors.New("every must be a whole number of minutes")
	}
	minutes := int(every / time.Minute)
	if minutes < 1 {
		return errors.New("every must be at least 1 minute")
	}

	args := []string{
		"/Create",
		"/TN", taskName,
		"/TR", runCmd,
		"/SC", "MINUTE",
		"/MO", strconv.Itoa(minutes),
		"/F",
	}
	out, err := exec.Command("schtasks", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("create scheduled task: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("Created/updated Windows Scheduled Task %q (every %d minute(s))\n", taskName, minutes)
	return nil
}

func runCreateSchedule(cmd *cobra.Command, args []string) error {
	scheduleTypeRaw, _ := cmd.Flags().GetString("type")
	scheduleType, err := normalizeScheduleType(scheduleTypeRaw)
	if err != nil {
		return exitConfig(err)
	}
	rawLower := strings.ToLower(strings.TrimSpace(scheduleTypeRaw))
	isAuto := rawLower == "" || rawLower == "auto"

	taskName, _ := cmd.Flags().GetString("task-name")
	if err := validateScheduleTaskName(taskName); err != nil {
		return exitConfig(err)
	}
	logPath, _ := cmd.Flags().GetString("log-path")
	if strings.TrimSpace(logPath) == "" {
		logPath = "logs/ncc-scheduler.log"
	}
	logPath = strings.TrimSpace(logPath)
	if !filepath.IsAbs(logPath) {
		if absLogPath, absErr := filepath.Abs(logPath); absErr == nil {
			logPath = absLogPath
		}
	}

	configPath, _ := cmd.Flags().GetString("config")
	configPath = strings.TrimSpace(configPath)
	if configPath != "" && !filepath.IsAbs(configPath) {
		if absConfigPath, absErr := filepath.Abs(configPath); absErr == nil {
			configPath = absConfigPath
		}
	}
	runCmd, _ := cmd.Flags().GetString("command")
	if strings.TrimSpace(runCmd) == "" {
		runCmd, err = defaultScheduleCommand(configPath)
		if err != nil {
			return fmt.Errorf("build schedule command: %w", err)
		}
	}
	// systemd timers run with an explicit WorkingDirectory; anchor it to the
	// config file's directory so a relative output-dir resolves the same way an
	// interactive run from there would (the cron cwd footgun).
	systemdWorkDir := ""
	if configPath != "" {
		systemdWorkDir = filepath.Dir(configPath)
	}
	runCmd, err = sanitizeScheduleCommand(runCmd)
	if err != nil {
		return exitConfig(err)
	}

	printOnly, _ := cmd.Flags().GetBool("print-only")
	withLock, _ := cmd.Flags().GetBool("with-lock")
	every, _ := cmd.Flags().GetDuration("every")
	action, _ := cmd.Flags().GetString("action")
	action = strings.ToLower(strings.TrimSpace(action))
	if action == "" {
		action = "create"
	}

	switch scheduleType {
	case "cron":
		switch action {
		case "list":
			if err := listCronSchedules(taskName); err != nil {
				return err
			}
			// With --type auto on a systemd host, also surface (and below,
			// also remove) a systemd timer so detection/cleanup is uniform
			// regardless of which backend installed the schedule.
			if isAuto && systemctlAvailable() {
				_ = listSystemdSchedules(taskName)
			}
			return nil
		case "remove":
			if printOnly {
				fmt.Printf("Cron remove preview: marker=%q\n", scheduleMarker(taskName))
				return nil
			}
			if err := removeCronSchedule(taskName); err != nil {
				return err
			}
			if isAuto && systemctlAvailable() {
				if _, rmErr := removeSystemdSchedule(taskName, logPath); rmErr != nil {
					fmt.Fprintf(os.Stderr, "warning: remove systemd timer: %v\n", rmErr)
				}
			}
			return nil
		case "run-now":
			fmt.Printf("Running now: %s\n", runCmd)
			return runScheduleCommandNow(runCmd)
		case "create":
		default:
			return exitConfig(fmt.Errorf("action must be create, list, remove, or run-now (got %q)", action))
		}

		cronSpec, _ := cmd.Flags().GetString("cron")
		cronSpec = strings.TrimSpace(cronSpec)
		if cronSpec == "" {
			cronSpec, err = cronExprFromInterval(every)
			if err != nil {
				return exitConfig(fmt.Errorf("derive cron from --every: %w", err))
			}
		}
		line := buildCronScheduleLine(cronSpec, runCmd, logPath, scheduleMarker(taskName), withLock)
		if printOnly {
			fmt.Printf("Cron entry preview:\n%s\n", line)
			return nil
		}
		// Coexistence guard: a systemd timer for the same task would double-run
		// the scan. Remove it when switching to cron.
		if schedulerHasSystemdTimer(taskName) {
			fmt.Fprintf(os.Stderr, "note: removing existing systemd timer for %q to avoid duplicate runs\n", scheduleMarker(taskName))
			if _, rmErr := removeSystemdSchedule(taskName, logPath); rmErr != nil {
				fmt.Fprintf(os.Stderr, "warning: could not remove conflicting systemd timer: %v\n", rmErr)
			}
		}
		return installCronSchedule(taskName, cronSpec, runCmd, logPath, withLock)
	case "systemd":
		switch action {
		case "list":
			return listSystemdSchedules(taskName)
		case "remove":
			if printOnly {
				fmt.Printf("systemd remove preview: timer=%s.timer marker=%q\n", systemdUnitBase(taskName), scheduleMarker(taskName))
				return nil
			}
			removed, rmErr := removeSystemdSchedule(taskName, logPath)
			if rmErr != nil {
				return rmErr
			}
			if !removed {
				fmt.Printf("No systemd timer found for marker %q\n", scheduleMarker(taskName))
			}
			return nil
		case "run-now":
			fmt.Printf("Running now: %s\n", runCmd)
			return runScheduleCommandNow(runCmd)
		case "create":
		default:
			return exitConfig(fmt.Errorf("action must be create, list, remove, or run-now (got %q)", action))
		}

		// Defense-in-depth: a systemd timer runs the scan from an explicit
		// WorkingDirectory that is NOT the operator's interactive cwd, so a
		// config-less run cannot reliably locate config.yaml and fails with
		// "at least one cluster must be provided". Require an explicit --config
		// (anchored absolute above) rather than installing a timer that is
		// guaranteed to fail on its first activation.
		if strings.TrimSpace(configPath) == "" {
			return exitConfig(errors.New("systemd timers require an explicit --config (preferably an absolute path): without it the timer runs from systemd's working directory and cannot locate config.yaml"))
		}

		cronSpec, _ := cmd.Flags().GetString("cron")
		cronSpec = strings.TrimSpace(cronSpec)
		onCalendar, ocErr := onCalendarFromSchedule(cronSpec, every)
		if ocErr != nil {
			return exitConfig(fmt.Errorf("derive OnCalendar: %w", ocErr))
		}
		if printOnly {
			marker := scheduleMarker(taskName)
			workDir := systemdWorkDir
			if workDir == "" {
				workDir = filepath.Dir(logPath)
			}
			svc, tmr := buildSystemdScheduleUnits(taskName, onCalendar, scheduleRunnerScriptPath(taskName, logPath), workDir, marker)
			fmt.Printf("systemd unit preview (%s.service):\n%s\nsystemd unit preview (%s.timer):\n%s\n",
				systemdUnitBase(taskName), svc, systemdUnitBase(taskName), tmr)
			return nil
		}
		// Coexistence guard: drop any cron entry for the same task so the scan
		// is not scheduled twice.
		if schedulerHasCronEntry(taskName) {
			fmt.Fprintf(os.Stderr, "note: removing existing cron entry for %q to avoid duplicate runs\n", scheduleMarker(taskName))
			if rmErr := removeCronSchedule(taskName); rmErr != nil {
				fmt.Fprintf(os.Stderr, "warning: could not remove conflicting cron entry: %v\n", rmErr)
			}
		}
		return installSystemdSchedule(taskName, cronSpec, every, runCmd, logPath, systemdWorkDir)
	case "windows":
		switch action {
		case "list":
			return listWindowsSchedule(taskName)
		case "remove":
			if printOnly {
				fmt.Printf("Windows remove preview: schtasks /Delete /TN %q /F\n", taskName)
				return nil
			}
			return removeWindowsSchedule(taskName)
		case "run-now":
			fmt.Printf("Running now: %s\n", runCmd)
			return runScheduleCommandNow(runCmd)
		case "create":
		default:
			return exitConfig(fmt.Errorf("action must be create, list, remove, or run-now (got %q)", action))
		}
		if printOnly {
			if every <= 0 {
				return exitConfig(errors.New("every must be > 0"))
			}
			fmt.Printf("Windows task preview:\nschtasks /Create /TN %q /TR %q /SC MINUTE /MO %d /F\n",
				taskName, runCmd, int(every/time.Minute))
			return nil
		}
		return installWindowsSchedule(taskName, runCmd, every)
	default:
		return exitConfig(fmt.Errorf("unsupported type %q", scheduleType))
	}
}

func configJSONSchema() map[string]interface{} {
	props := map[string]interface{}{
		"cluster-source-mode":       map[string]interface{}{"type": "string", "enum": []string{"clusters", "pc"}},
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
		"max-parallel":              map[string]interface{}{"type": "integer", "minimum": 1},
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
		"notify-on-regression":      map[string]interface{}{"type": "boolean"},
		"adaptive-parallelism":      map[string]interface{}{"type": "boolean"},
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

type preflightCheck struct {
	ID              string `json:"id"`
	Status          string `json:"status"`
	Title           string `json:"title"`
	Message         string `json:"message"`
	RemediationCode string `json:"remediation_code,omitempty"`
	Hint            string `json:"hint,omitempty"`
	Output          string `json:"output,omitempty"`
}

type preflightReport struct {
	OK              bool             `json:"ok"`
	Failed          int              `json:"failed"`
	Warn            int              `json:"warn"`
	ConfigPath      string           `json:"config_path"`
	Checks          []preflightCheck `json:"checks"`
	ActionableHints []string         `json:"actionableHints"`
}

func defaultPreflightRemediationCode(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return "NCC_PREFLIGHT_GENERIC"
	}
	replacer := strings.NewReplacer(".", "_", "-", "_", "/", "_", " ", "_")
	return "NCC_PREFLIGHT_" + strings.ToUpper(replacer.Replace(id))
}

func preflightProbePath(path string) error {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return err
	}
	// Keep a persistent probe sentinel and validate RW access on each preflight run.
	probe := filepath.Join(path, ".ncc-preflight-check")
	f, err := os.OpenFile(probe, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	// Write one byte at offset 0 (fixed-size probe), then read it back to verify RW.
	if _, err := f.WriteAt([]byte{'1'}, 0); err != nil {
		return err
	}
	buf := make([]byte, 1)
	if _, err := f.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if buf[0] != '1' {
		return fmt.Errorf("preflight probe verification failed for %s", probe)
	}
	// Backward-compat cleanup for older typo probe file name.
	legacyProbe := filepath.Join(path, ".ncc-prefight-check")
	if err := os.Remove(legacyProbe); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func preflightResolveClusterTarget(cluster string) error {
	target, err := normalizeClusterAddress(cluster)
	if err != nil {
		return err
	}
	if net.ParseIP(target) != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, target)
	if err != nil {
		return err
	}
	if len(addrs) == 0 {
		return errors.New("resolver returned no addresses")
	}
	return nil
}

func buildPreflightReport(cfgPath string) preflightReport {
	report := preflightReport{
		OK:         true,
		ConfigPath: cfgPath,
		Checks:     []preflightCheck{},
	}
	hints := []string{}
	add := func(c preflightCheck) {
		if c.Status != "pass" && strings.TrimSpace(c.RemediationCode) == "" {
			c.RemediationCode = defaultPreflightRemediationCode(c.ID)
		}
		report.Checks = append(report.Checks, c)
		switch c.Status {
		case "fail":
			report.Failed++
			report.OK = false
			if strings.TrimSpace(c.Hint) != "" {
				hints = append(hints, c.Hint)
			}
		case "warn":
			report.Warn++
		}
	}

	if strings.TrimSpace(cfgPath) == "" {
		add(preflightCheck{
			ID:      "validate-config",
			Status:  "warn",
			Title:   "validate-config",
			Message: "config path not provided; file-based preflight checks skipped",
			Hint:    "Run with --config to enable full preflight checks.",
		})
		report.ActionableHints = dedupeStringsKeepOrder(hints)
		return report
	}

	var loadedCfg Config
	cfgLoaded := false
	if cfg, err := loadConfigForValidation(cfgPath); err != nil {
		add(preflightCheck{
			ID:      "validate-config",
			Status:  "fail",
			Title:   "validate-config",
			Message: err.Error(),
			Hint:    "Fix configuration schema/values and rerun preflight.",
		})
	} else {
		loadedCfg = cfg
		cfgLoaded = true
		add(preflightCheck{ID: "validate-config", Status: "pass", Title: "validate-config", Message: "config is valid"})
		dirs := []struct {
			id    string
			path  string
			title string
			hint  string
		}{
			{"path.output-dir-logs", cfg.OutputDirLogs, "Output logs path permission", "Grant write permission to output-dir-logs."},
			{"path.output-dir-filtered", cfg.OutputDirFiltered, "Output filtered path permission", "Grant write permission to output-dir-filtered."},
			{"path.log-file-dir", filepath.Dir(cfg.LogFile), "Log file directory permission", "Grant write permission to log-file directory."},
		}
		if cfg.PromEnabled {
			dirs = append(dirs, struct {
				id    string
				path  string
				title string
				hint  string
			}{"path.prom-dir", cfg.PromDir, "Prometheus path permission", "Grant write permission to prom-dir."})
		}
		if cfg.RunHistoryEnabled {
			dirs = append(dirs, struct {
				id    string
				path  string
				title string
				hint  string
			}{"path.run-history-dir", cfg.RunHistoryDir, "Run history path permission", "Grant write permission to run-history-dir."})
		}
		for _, d := range dirs {
			if err := preflightProbePath(d.path); err != nil {
				add(preflightCheck{ID: d.id, Status: "fail", Title: d.title, Message: err.Error(), Hint: d.hint})
			} else {
				add(preflightCheck{ID: d.id, Status: "pass", Title: d.title, Message: d.path})
			}
		}
		// Additional file-level checks for optional inputs when configured.
		checkFile := func(id string, title string, p string, required bool) {
			path := strings.TrimSpace(p)
			if path == "" {
				if required {
					add(preflightCheck{
						ID:      id,
						Status:  "fail",
						Title:   title,
						Message: "required file path is empty",
						Hint:    "Set this file path in config.",
					})
				} else {
					add(preflightCheck{ID: id, Status: "warn", Title: title, Message: "not set"})
				}
				return
			}
			st, err := os.Stat(path)
			if err != nil {
				add(preflightCheck{
					ID:      id,
					Status:  "fail",
					Title:   title,
					Message: err.Error(),
					Hint:    "Create the file or fix the configured path.",
				})
				return
			}
			if st.IsDir() {
				add(preflightCheck{
					ID:      id,
					Status:  "fail",
					Title:   title,
					Message: "path points to a directory",
					Hint:    "Use a regular file path.",
				})
				return
			}
			add(preflightCheck{ID: id, Status: "pass", Title: title, Message: path})
		}
		checkFile("file.clusters-file", "Clusters file", cfg.ClustersFile, false)
		checkFile("file.exclude-alert-titles-file", "Exclude alert titles file", cfg.ExcludeAlertTitlesFile, false)
		checkFile("file.secrets-file", "Secrets file", cfg.SecretsFile, strings.EqualFold(strings.TrimSpace(cfg.SecretsProvider), "file"))
		for i, cluster := range cfg.Clusters {
			id := fmt.Sprintf("cluster.target.%d", i+1)
			title := fmt.Sprintf("Cluster target %d resolution", i+1)
			if err := preflightResolveClusterTarget(cluster); err != nil {
				add(preflightCheck{
					ID:      id,
					Status:  "fail",
					Title:   title,
					Message: fmt.Sprintf("%s: %v", cluster, err),
					Hint:    "Use a reachable IP/FQDN, or verify DNS/network from this runtime.",
				})
			} else {
				add(preflightCheck{ID: id, Status: "pass", Title: title, Message: cluster})
			}
		}
		// Security posture advisories.
		if cfg.InsecureSkipVerify {
			add(preflightCheck{
				ID:      "safety.insecure-skip-verify",
				Status:  "warn",
				Title:   "TLS verification disabled",
				Message: "insecure-skip-verify=true",
				Hint:    "Use false in production and install valid certificates.",
			})
		}
		if cfg.LogHTTP {
			add(preflightCheck{
				ID:      "safety.log-http",
				Status:  "warn",
				Title:   "HTTP request/response logging enabled",
				Message: "log-http=true",
				Hint:    "Disable log-http for production runs to reduce sensitive output exposure.",
			})
		}
		if cfg.MaxParallel > 20 {
			add(preflightCheck{
				ID:      "safety.max-parallel",
				Status:  "warn",
				Title:   "High max-parallel setting",
				Message: fmt.Sprintf("max-parallel=%d", cfg.MaxParallel),
				Hint:    "Lower max-parallel if you hit API rate limits or network instability.",
			})
		}
	}

	var (
		secretRefs int
		provider   string
		secErr     error
	)
	if cfgLoaded {
		secretRefs, provider, secErr = validateSecretsWithConfig(cfgPath, loadedCfg)
	} else {
		secretRefs, provider, secErr = validateSecretsForPath(cfgPath)
	}
	if secErr != nil {
		add(preflightCheck{
			ID:      "validate-secrets",
			Status:  "fail",
			Title:   "validate-secrets",
			Message: secErr.Error(),
			Hint:    "Set secrets-provider and ensure secret sources are accessible.",
		})
	} else if secretRefs == 0 {
		add(preflightCheck{ID: "validate-secrets", Status: "warn", Title: "validate-secrets", Message: "no secret:// references found"})
	} else {
		add(preflightCheck{ID: "validate-secrets", Status: "pass", Title: "validate-secrets", Message: fmt.Sprintf("refs=%d provider=%s", secretRefs, provider)})
	}

	report.ActionableHints = dedupeStringsKeepOrder(hints)
	return report
}

func runPreflightCheckCommand(cmd *cobra.Command, args []string) error {
	zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	cfgPath, _ := cmd.Flags().GetString("config")
	format, _ := cmd.Flags().GetString("format")
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		format = "json"
	}

	report := buildPreflightReport(cfgPath)
	data, _ := json.MarshalIndent(report, "", "  ")
	if format == "json" {
		fmt.Println(string(data))
		return nil
	}
	return exitConfig(fmt.Errorf("unsupported format: %s (supported: json)", format))
}

type automationLevel string

const (
	automationLevelAdvisory automationLevel = "advisory"
	automationLevelSafeFix  automationLevel = "safe-fix"
	automationLevelFullAuto automationLevel = "full-auto"
)

func parseAutomationLevel(raw string) (automationLevel, error) {
	v := automationLevel(strings.ToLower(strings.TrimSpace(raw)))
	if v == "" {
		return automationLevelSafeFix, nil
	}
	switch v {
	case automationLevelAdvisory, automationLevelSafeFix, automationLevelFullAuto:
		return v, nil
	default:
		return "", fmt.Errorf("invalid automation-level %q (valid: advisory, safe-fix, full-auto)", raw)
	}
}

func recommendationForPreflightCheck(c preflightCheck) string {
	msg := strings.TrimSpace(c.Message)
	switch c.ID {
	case "validate-config":
		return "Fix config schema/values, then rerun with --auto for guided retries."
	case "validate-secrets":
		return "Set secrets-provider and verify secret sources are reachable."
	case "path.output-dir-logs", "path.output-dir-filtered", "path.log-file-dir", "path.prom-dir", "path.run-history-dir":
		return "Grant write permissions or let safe automation create/repair directory permissions."
	case "file.clusters-file", "file.exclude-alert-titles-file", "file.secrets-file":
		return "Create the configured file or update config path; safe automation can create a starter file."
	}
	if strings.Contains(strings.ToLower(msg), "permission") {
		return "Check file ownership and writable permissions for configured paths."
	}
	return "Review check output and apply the listed hint."
}

func printAutomationRunbook(report preflightReport, level automationLevel) {
	if report.Failed == 0 {
		return
	}
	fmt.Fprintln(os.Stderr, "Automation runbook:")
	for _, c := range report.Checks {
		if c.Status != "fail" {
			continue
		}
		code := c.RemediationCode
		if strings.TrimSpace(code) == "" {
			code = defaultPreflightRemediationCode(c.ID)
		}
		fmt.Fprintf(os.Stderr, "- [%s] %s\n", code, recommendationForPreflightCheck(c))
	}
	if level == automationLevelAdvisory {
		fmt.Fprintln(os.Stderr, "Next step: re-run with --auto --automation-level safe-fix to allow automatic safe repairs.")
	}
}

func applySafePreflightFixes(cfg Config, cfgPath string, report preflightReport, level automationLevel) (int, []string) {
	if level == automationLevelAdvisory {
		return 0, nil
	}
	fixed := 0
	actions := []string{}
	fixPath := func(id string, path string) {
		path = strings.TrimSpace(path)
		if path == "" {
			return
		}
		if err := os.MkdirAll(path, 0o755); err == nil {
			fixed++
			actions = append(actions, fmt.Sprintf("%s -> ensured dir %s", id, path))
		}
	}
	fixFile := func(id string, path string, content string) {
		path = strings.TrimSpace(path)
		if path == "" {
			return
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return
		}
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			if err := os.WriteFile(path, []byte(content), 0o600); err == nil {
				fixed++
				actions = append(actions, fmt.Sprintf("%s -> created file %s", id, path))
			}
		}
	}
	for _, c := range report.Checks {
		if c.Status != "fail" {
			continue
		}
		switch c.ID {
		case "validate-config":
			if strings.TrimSpace(cfgPath) != "" {
				if _, err := os.Stat(cfgPath); errors.Is(err, os.ErrNotExist) {
					if err := writeDummyConfig(cfgPath); err == nil {
						fixed++
						actions = append(actions, fmt.Sprintf("%s -> generated starter config %s", c.ID, cfgPath))
					}
				}
			}
		case "path.output-dir-logs":
			fixPath(c.ID, cfg.OutputDirLogs)
		case "path.output-dir-filtered":
			fixPath(c.ID, cfg.OutputDirFiltered)
		case "path.log-file-dir":
			fixPath(c.ID, filepath.Dir(cfg.LogFile))
		case "path.prom-dir":
			fixPath(c.ID, cfg.PromDir)
		case "path.run-history-dir":
			fixPath(c.ID, cfg.RunHistoryDir)
		case "file.clusters-file":
			fixFile(c.ID, cfg.ClustersFile, "# one cluster per line\n")
		case "file.exclude-alert-titles-file":
			fixFile(c.ID, cfg.ExcludeAlertTitlesFile, "# one alert title per line\n")
		case "file.secrets-file":
			fixFile(c.ID, cfg.SecretsFile, "{}\n")
		case "validate-secrets":
			if strings.TrimSpace(cfgPath) != "" {
				b, err := os.ReadFile(cfgPath)
				if err == nil {
					content := string(b)
					updated := upsertYAMLScalar(content, "secrets-provider", "env")
					if updated != content {
						if err := os.WriteFile(cfgPath, []byte(updated), 0644); err == nil {
							fixed++
							actions = append(actions, fmt.Sprintf("%s -> set secrets-provider to %q in %s", c.ID, "env", cfgPath))
						}
					}
				}
			}
		}
	}
	return fixed, actions
}

func applyFullAutoRuntimeTuning(cfg *Config) []string {
	if cfg == nil {
		return nil
	}
	changes := []string{}
	if cfg.MaxParallel > 12 {
		old := cfg.MaxParallel
		cfg.MaxParallel = 12
		changes = append(changes, fmt.Sprintf("max-parallel %d -> %d", old, cfg.MaxParallel))
	}
	if cfg.Timeout < 20*time.Minute {
		old := cfg.Timeout
		cfg.Timeout = 20 * time.Minute
		changes = append(changes, fmt.Sprintf("timeout %s -> %s", old, cfg.Timeout))
	}
	if cfg.RequestTimeout < 30*time.Second {
		old := cfg.RequestTimeout
		cfg.RequestTimeout = 30 * time.Second
		changes = append(changes, fmt.Sprintf("request-timeout %s -> %s", old, cfg.RequestTimeout))
	}
	if cfg.RetryMaxAttempts < 8 {
		old := cfg.RetryMaxAttempts
		cfg.RetryMaxAttempts = 8
		changes = append(changes, fmt.Sprintf("retry-max-attempts %d -> %d", old, cfg.RetryMaxAttempts))
	}
	if cfg.RetryCircuitBreaker < 4 {
		old := cfg.RetryCircuitBreaker
		cfg.RetryCircuitBreaker = 4
		changes = append(changes, fmt.Sprintf("retry-circuit-breaker %d -> %d", old, cfg.RetryCircuitBreaker))
	}
	if !cfg.AdaptiveParallelism {
		cfg.AdaptiveParallelism = true
		changes = append(changes, "adaptive-parallelism false -> true")
	}
	return changes
}

func quickstartPrompt(reader *bufio.Reader, prompt string, defaultValue string) (string, error) {
	renderPrompt := func() {
		if strings.TrimSpace(defaultValue) != "" {
			fmt.Printf("%s [%s]: ", prompt, defaultValue)
		} else {
			fmt.Printf("%s: ", prompt)
		}
	}
	renderPrompt()
	raw, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	v := strings.TrimSpace(raw)
	if v != "" {
		return v, nil
	}
	fmt.Println("Input is empty. Press Enter again to continue, or type a value.")
	renderPrompt()
	raw, err = reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	v = strings.TrimSpace(raw)
	if v == "" {
		return strings.TrimSpace(defaultValue), nil
	}
	return v, nil
}

func quickstartWarnOnEmptyTargets(mode, clusters, pcs string) {
	if mode == "clusters" && strings.TrimSpace(clusters) == "" {
		fmt.Println("Warning: cluster targets are empty; preflight will fail until clusters (or clusters-file) is set.")
	}
	if mode == "pc" && strings.TrimSpace(pcs) == "" {
		fmt.Println("Warning: Prism Central targets are empty; discovery will rely on prism-central-url.")
	}
}

func quickstartPromptChoice(reader *bufio.Reader, prompt string, defaultValue string, allowed []string) (string, error) {
	allowSet := map[string]bool{}
	for _, a := range allowed {
		allowSet[strings.ToLower(strings.TrimSpace(a))] = true
	}
	v, err := quickstartPrompt(reader, prompt, defaultValue)
	if err != nil {
		return "", err
	}
	normalized := strings.ToLower(strings.TrimSpace(v))
	if allowSet[normalized] {
		return normalized, nil
	}
	fmt.Printf("Invalid value %q. Allowed values: %s\n", v, strings.Join(allowed, ", "))
	v, err = quickstartPrompt(reader, prompt, defaultValue)
	if err != nil {
		return "", err
	}
	normalized = strings.ToLower(strings.TrimSpace(v))
	if !allowSet[normalized] {
		return strings.ToLower(strings.TrimSpace(defaultValue)), nil
	}
	return normalized, nil
}

func upsertYAMLScalar(content string, key string, value string) string {
	quoted := fmt.Sprintf("%s: %q", key, value)
	pattern := fmt.Sprintf(`(?m)^\s*%s:\s*.*$`, regexp.QuoteMeta(key))
	re := regexp.MustCompile(pattern)
	if re.MatchString(content) {
		return re.ReplaceAllString(content, quoted)
	}
	if !strings.HasSuffix(content, "\n") {
		content += "\n"
	}
	return content + quoted + "\n"
}

func yamlScalarValue(content, key string) string {
	pattern := fmt.Sprintf(`(?m)^\s*%s:\s*(.*)$`, regexp.QuoteMeta(key))
	re := regexp.MustCompile(pattern)
	m := re.FindStringSubmatch(content)
	if len(m) != 2 {
		return ""
	}
	v := strings.TrimSpace(m[1])
	if i := strings.Index(v, "#"); i >= 0 {
		v = strings.TrimSpace(v[:i])
	}
	v = strings.TrimSpace(strings.Trim(v, `"'`))
	return v
}

func repairConfigInlineCommentValues(cfgPath string) (bool, error) {
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		return false, err
	}
	lines := strings.Split(string(raw), "\n")
	changed := false
	repairableKey := func(k string) bool {
		switch strings.TrimSpace(k) {
		case "timeout", "request-timeout", "poll-interval", "poll-jitter", "retry-base-delay", "retry-max-delay", "idle-conn-timeout":
			return true
		default:
			return false
		}
	}
	stripCommentTailForKey := func(k string) bool {
		switch strings.TrimSpace(k) {
		case "username", "cluster-source-mode", "discover-api-version", "nutanix-v4-api-version", "insecure-skip-verify", "timeout", "request-timeout":
			return true
		default:
			return false
		}
	}
	for i, ln := range lines {
		trim := strings.TrimSpace(ln)
		if trim == "" || strings.HasPrefix(trim, "#") {
			continue
		}
		colon := strings.Index(ln, ":")
		if colon <= 0 {
			continue
		}
		key := strings.TrimSpace(ln[:colon])
		prefix := ln[:colon+1]
		rest := strings.TrimSpace(ln[colon+1:])
		if len(rest) < 2 || rest[0] != '"' || rest[len(rest)-1] != '"' {
			continue
		}
		if stripCommentTailForKey(key) {
			body := strings.TrimPrefix(rest, `"`)
			body = strings.TrimSuffix(body, `"`)
			if hash := strings.Index(body, "#"); hash >= 0 {
				body = body[:hash]
			}
			body = strings.TrimSpace(body)
			for strings.HasSuffix(body, `\"`) {
				body = strings.TrimSpace(strings.TrimSuffix(body, `\"`))
			}
			body = strings.TrimRight(body, `\`)
			fixedLine := prefix + " " + strconv.Quote(body)
			if strings.TrimSpace(fixedLine) != strings.TrimSpace(ln) {
				lines[i] = fixedLine
				changed = true
			}
			continue
		}
		inner := rest[1 : len(rest)-1]
		orig := inner
		legacyIdx := strings.Index(inner, `\"`)
		if legacyIdx >= 0 {
			legacyTail := inner[legacyIdx+2:]
			if strings.Contains(legacyTail, "#") {
				inner = strings.TrimSpace(inner[:legacyIdx])
			}
		}
		if repairableKey(key) {
			inner = strings.TrimRight(inner, `\`)
		}
		inner = strings.TrimSpace(inner)
		if inner != strings.TrimSpace(orig) {
			lines[i] = prefix + " " + strconv.Quote(inner)
			changed = true
		}
	}
	if !changed {
		return false, nil
	}
	out := strings.Join(lines, "\n")
	if err := os.WriteFile(cfgPath, []byte(out), 0644); err != nil {
		return false, err
	}
	return true, nil
}

func maskQuickstartValue(key, value string) string {
	if strings.EqualFold(strings.TrimSpace(key), "password") {
		if strings.TrimSpace(value) == "" {
			return `""`
		}
		return `"********"`
	}
	return strconv.Quote(value)
}

func runQuickstartInteractive(cfgPath string) error {
	reader := bufio.NewReader(os.Stdin)
	contentBytes, err := os.ReadFile(cfgPath)
	if err != nil {
		return err
	}
	content := string(contentBytes)
	original := content
	updates := map[string]string{}
	defaultMode := yamlScalarValue(original, "cluster-source-mode")
	if defaultMode == "" {
		defaultMode = "clusters"
	}
	mode, err := quickstartPromptChoice(reader, "Cluster source mode (clusters|pc)", defaultMode, []string{"clusters", "pc"})
	if err != nil {
		return err
	}
	updates["cluster-source-mode"] = mode
	content = upsertYAMLScalar(content, "cluster-source-mode", mode)
	if mode == "pc" {
		pcURLDefault := yamlScalarValue(original, "prism-central-url")
		pcURL, err := quickstartPrompt(reader, "Prism Central URL (optional, e.g. https://pc:9440)", pcURLDefault)
		if err != nil {
			return err
		}
		updates["prism-central-url"] = pcURL
		content = upsertYAMLScalar(content, "prism-central-url", pcURL)
		pcs := ""
		if strings.TrimSpace(pcURL) == "" {
			pcsDefault := yamlScalarValue(original, "pcs")
			pcs, err = quickstartPrompt(reader, "Prism Central targets (comma-separated)", pcsDefault)
			if err != nil {
				return err
			}
			updates["pcs"] = pcs
			content = upsertYAMLScalar(content, "pcs", pcs)
			quickstartWarnOnEmptyTargets(mode, "", pcs)
		} else {
			updates["pcs"] = yamlScalarValue(original, "pcs")
		}
		updates["clusters"] = ""
		content = upsertYAMLScalar(content, "clusters", "")
	} else {
		clusterDefault := yamlScalarValue(original, "clusters")
		clusters, err := quickstartPrompt(reader, "Cluster targets (comma-separated)", clusterDefault)
		if err != nil {
			return err
		}
		updates["clusters"] = clusters
		updates["pcs"] = ""
		content = upsertYAMLScalar(content, "clusters", clusters)
		content = upsertYAMLScalar(content, "pcs", "")
		quickstartWarnOnEmptyTargets(mode, clusters, "")
	}
	defaultUser := yamlScalarValue(original, "username")
	if strings.TrimSpace(defaultUser) == "" {
		defaultUser = "admin"
	}
	username, err := quickstartPrompt(reader, "Prism username", defaultUser)
	if err != nil {
		return err
	}
	updates["username"] = username
	content = upsertYAMLScalar(content, "username", username)
	insecureDefault := strings.ToLower(strings.TrimSpace(yamlScalarValue(original, "insecure-skip-verify")))
	if insecureDefault != "true" && insecureDefault != "false" {
		insecureDefault = "false"
	}
	insecureValue, err := quickstartPromptChoice(reader, "Skip TLS certificate verification? (true|false)", insecureDefault, []string{"true", "false"})
	if err != nil {
		return err
	}
	updates["insecure-skip-verify"] = insecureValue
	content = upsertYAMLScalar(content, "insecure-skip-verify", insecureValue)
	advanced, err := quickstartConfirm(reader, "Configure advanced network/runtime options (timeouts/api version)?", false)
	if err != nil {
		return err
	}
	if advanced {
		timeoutDefault := yamlScalarValue(original, "timeout")
		if timeoutDefault == "" {
			timeoutDefault = "15m"
		}
		timeoutValue, err := quickstartPrompt(reader, "Overall cluster timeout (e.g. 15m)", timeoutDefault)
		if err != nil {
			return err
		}
		updates["timeout"] = timeoutValue
		content = upsertYAMLScalar(content, "timeout", timeoutValue)
		reqTimeoutDefault := yamlScalarValue(original, "request-timeout")
		if reqTimeoutDefault == "" {
			reqTimeoutDefault = "20s"
		}
		reqTimeoutValue, err := quickstartPrompt(reader, "Per-request timeout (e.g. 20s)", reqTimeoutDefault)
		if err != nil {
			return err
		}
		updates["request-timeout"] = reqTimeoutValue
		content = upsertYAMLScalar(content, "request-timeout", reqTimeoutValue)
		if mode == "pc" {
			discoverDefault := yamlScalarValue(original, "discover-api-version")
			if discoverDefault == "" {
				discoverDefault = defaultDiscoverAPIVersion
			}
			discoverVer, err := quickstartPromptChoice(reader, "Discover API version (v4|v3)", discoverDefault, []string{"v4", "v3"})
			if err != nil {
				return err
			}
			updates["discover-api-version"] = discoverVer
			content = upsertYAMLScalar(content, "discover-api-version", discoverVer)

			v4PathDefault := yamlScalarValue(original, "nutanix-v4-api-version")
			if v4PathDefault == "" {
				v4PathDefault = defaultNutanixV4APIVersion
			}
			v4PathValue, err := quickstartPrompt(reader, "Nutanix v4 API path version (e.g. v4.2)", v4PathDefault)
			if err != nil {
				return err
			}
			updates["nutanix-v4-api-version"] = v4PathValue
			content = upsertYAMLScalar(content, "nutanix-v4-api-version", v4PathValue)
		}
	}
	if term.IsTerminal(int(os.Stdin.Fd())) {
		setPasswordNow, err := quickstartConfirm(reader, "Set Prism password now?", false)
		if err != nil {
			return err
		}
		if setPasswordNow {
			fmt.Print("Prism password (input hidden): ")
			bytePw, err := term.ReadPassword(int(os.Stdin.Fd()))
			fmt.Println()
			if err != nil {
				return fmt.Errorf("read quickstart password: %w", err)
			}
			pw := strings.TrimSpace(string(bytePw))
			updates["password"] = pw
			content = upsertYAMLScalar(content, "password", pw)
		}
	}
	fmt.Println("Planned config updates:")
	orderedKeys := []string{
		"cluster-source-mode", "clusters", "pcs", "prism-central-url",
		"username", "password", "insecure-skip-verify",
		"timeout", "request-timeout", "discover-api-version", "nutanix-v4-api-version",
	}
	changed := 0
	for _, key := range orderedKeys {
		newVal, ok := updates[key]
		if !ok {
			continue
		}
		oldVal := yamlScalarValue(original, key)
		if oldVal == newVal {
			continue
		}
		changed++
		fmt.Printf("- %s: %s -> %s\n", key, maskQuickstartValue(key, oldVal), maskQuickstartValue(key, newVal))
	}
	if changed == 0 {
		fmt.Println("- No effective value changes detected.")
	}
	applyChanges, err := quickstartConfirm(reader, "Apply these changes to config?", true)
	if err != nil {
		return err
	}
	if !applyChanges {
		fmt.Printf("Interactive quickstart did not modify %s\n", cfgPath)
		return nil
	}
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		return err
	}
	fmt.Printf("Interactive quickstart saved updates to %s\n", cfgPath)
	return nil
}

func quickstartConfirm(reader *bufio.Reader, prompt string, defaultYes bool) (bool, error) {
	suffix := "[y/N]"
	if defaultYes {
		suffix = "[Y/n]"
	}
	renderPrompt := func() {
		fmt.Printf("%s %s ", prompt, suffix)
	}
	renderPrompt()
	raw, err := reader.ReadString('\n')
	if err != nil {
		return false, err
	}
	v := strings.ToLower(strings.TrimSpace(raw))
	if v == "" {
		fmt.Println("Input is empty. Press Enter again to continue, or type y/n.")
		renderPrompt()
		raw, err = reader.ReadString('\n')
		if err != nil {
			return false, err
		}
		v = strings.ToLower(strings.TrimSpace(raw))
		if v == "" {
			return defaultYes, nil
		}
	}
	if v == "y" || v == "yes" {
		return true, nil
	}
	if v == "n" || v == "no" {
		return false, nil
	}
	return defaultYes, nil
}

func quickstartV2Status(installDir string) (bool, string, string, string, string) {
	apiBin, uiBin, frontendDir, layout := resolveV2RuntimeLayout(installDir)
	if strings.TrimSpace(apiBin) == "" || strings.TrimSpace(uiBin) == "" || strings.TrimSpace(frontendDir) == "" {
		return false, "", "", "", ""
	}
	return true, apiBin, uiBin, frontendDir, layout
}

func quickstartStatusSummary(report preflightReport) (pass, warn, fail int) {
	for _, c := range report.Checks {
		switch strings.ToLower(strings.TrimSpace(c.Status)) {
		case "pass":
			pass++
		case "warn":
			warn++
		case "fail":
			fail++
		}
	}
	return pass, warn, fail
}

func printQuickstartWarningHighlights(report preflightReport, limit int) {
	if limit <= 0 {
		limit = 3
	}
	printed := 0
	for _, c := range report.Checks {
		if strings.ToLower(strings.TrimSpace(c.Status)) != "warn" {
			continue
		}
		msg := strings.TrimSpace(c.Message)
		title := strings.TrimSpace(c.Title)
		switch strings.ToLower(msg) {
		case "", "not set":
			if title != "" {
				msg = fmt.Sprintf("%s: %s", title, strings.TrimSpace(c.Message))
			}
		}
		if strings.TrimSpace(msg) == "" {
			msg = c.ID
		}
		fmt.Printf("- [warn] %s", strings.TrimSpace(msg))
		if h := strings.TrimSpace(c.Hint); h != "" {
			fmt.Printf(" | hint: %s", h)
		}
		fmt.Println()
		printed++
		if printed >= limit {
			break
		}
	}
	if report.Warn > printed {
		fmt.Printf("- ... %d more warnings (run `ncc-orchestrator preflight-check --config %s` for full details)\n", report.Warn-printed, report.ConfigPath)
	}
}

func runQuickstartCommand(cmd *cobra.Command, args []string) error {
	cfgPath, _ := cmd.Flags().GetString("config")
	if strings.TrimSpace(cfgPath) == "" {
		cfgPath = "config.yaml"
	}
	autoFix, _ := cmd.Flags().GetBool("auto-fix")
	interactive, _ := cmd.Flags().GetBool("interactive")
	assumeYes, _ := cmd.Flags().GetBool("assume-yes")
	setupV2, _ := cmd.Flags().GetString("setup-v2")
	installDir, _ := cmd.Flags().GetString("install-dir")
	repo, _ := cmd.Flags().GetString("repo")
	levelRaw, _ := cmd.Flags().GetString("automation-level")
	level, err := parseAutomationLevel(levelRaw)
	if err != nil {
		return err
	}
	setupV2 = strings.ToLower(strings.TrimSpace(setupV2))
	if setupV2 == "" {
		setupV2 = "ask"
	}
	switch setupV2 {
	case "ask", "download", "skip":
	default:
		return fmt.Errorf("invalid --setup-v2 %q (valid: ask, download, skip)", setupV2)
	}
	if strings.TrimSpace(installDir) == "" {
		installDir = ".ncc-v2"
	}
	if strings.TrimSpace(repo) == "" {
		repo = defaultGitHubRepo
	}
	cfgPath = strings.TrimSpace(cfgPath)
	if cfgPath == "" {
		cfgPath = "config.yaml"
	}
	fmt.Println("Quickstart mode: beginner-friendly setup")
	fmt.Println("Step 1/3: checking config")
	if _, err := os.Stat(cfgPath); errors.Is(err, os.ErrNotExist) {
		if err := writeDummyConfig(cfgPath); err != nil {
			return fmt.Errorf("quickstart: create dummy config: %w", err)
		}
		fmt.Printf("Created starter config: %s\n", cfgPath)
	}
	if autoFix {
		repairedAny := false
		for i := 0; i < 3; i++ {
			fixed, err := repairConfigInlineCommentValues(cfgPath)
			if err != nil {
				break
			}
			if !fixed {
				break
			}
			repairedAny = true
		}
		if repairedAny {
			fmt.Println("Auto-fix: repaired malformed quoted config values.")
		}
	}
	if interactive {
		if err := runQuickstartInteractive(cfgPath); err != nil {
			return fmt.Errorf("quickstart interactive: %w", err)
		}
	}
	fmt.Println("Step 2/3: running safety checks")
	report := buildPreflightReport(cfgPath)
	pass, warn, fail := quickstartStatusSummary(report)
	fmt.Printf("Quickstart preflight: pass=%d warn=%d failed=%d\n", pass, warn, fail)
	if warn > 0 {
		fmt.Println("Preflight warnings:")
		printQuickstartWarningHighlights(report, 3)
	}
	if report.Failed > 0 && autoFix {
		cfg, cfgErr := loadConfigForValidation(cfgPath)
		if cfgErr == nil {
			fixed, actions := applySafePreflightFixes(cfg, cfgPath, report, level)
			for _, a := range actions {
				fmt.Printf("Auto-fix: %s\n", a)
			}
			if fixed > 0 {
				report = buildPreflightReport(cfgPath)
				pass, warn, fail = quickstartStatusSummary(report)
				fmt.Printf("After auto-fix: pass=%d warn=%d failed=%d\n", pass, warn, fail)
				if warn > 0 {
					fmt.Println("Remaining warnings:")
					printQuickstartWarningHighlights(report, 3)
				}
			}
		}
	}
	if report.Failed > 0 {
		printAutomationRunbook(report, level)
		return fmt.Errorf("quickstart preflight failed (%d failures)", report.Failed)
	}
	fmt.Println("Step 3/3: checking optional v2 API/UI components")
	v2Ready, apiBin, uiBin, frontendDir, layout := quickstartV2Status(installDir)
	webComponentsInstalled := v2Ready
	if v2Ready {
		fmt.Println("v2 components are ready.")
		fmt.Printf("- API binary : %s\n", apiBin)
		fmt.Printf("- UI binary  : %s\n", uiBin)
		fmt.Printf("- Frontend   : %s\n", frontendDir)
		fmt.Printf("- Layout     : %s\n", layout)
	} else {
		downloadV2 := false
		switch setupV2 {
		case "download":
			downloadV2 = true
		case "ask":
			if assumeYes {
				downloadV2 = true
			} else if term.IsTerminal(int(os.Stdin.Fd())) {
				reader := bufio.NewReader(os.Stdin)
				ok, err := quickstartConfirm(reader, "Install optional API/UI components now?", true)
				if err == nil {
					downloadV2 = ok
				}
			}
		case "skip":
			downloadV2 = false
		}
		if downloadV2 {
			fmt.Println("Downloading v2 components...")
			err := runV2Bootstrap(v2BootstrapOptions{
				Repo:       repo,
				InstallDir: installDir,
			})
			if err != nil {
				fmt.Printf("Could not auto-download v2 components: %v\n", err)
				fmt.Printf("You can download manually from: https://github.com/%s/releases/latest\n", repo)
				fmt.Printf("Command: ncc-orchestrator v2-bootstrap --repo %s --install-dir %s\n", repo, installDir)
			} else {
				fmt.Println("v2 components downloaded successfully.")
				webComponentsInstalled = true
			}
		} else {
			fmt.Println("v2 components are not installed yet (this is okay for CLI-only use).")
			fmt.Printf("Download link: https://github.com/%s/releases/latest\n", repo)
			fmt.Printf("Command: ncc-orchestrator v2-bootstrap --repo %s --install-dir %s\n", repo, installDir)
		}
	}
	fmt.Println("Quickstart complete.")
	fmt.Println("Next:")
	fmt.Printf("1) Edit config if needed: %s\n", cfgPath)
	fmt.Printf("2) Run checks: ncc-orchestrator --auto --config %s\n", cfgPath)
	if webComponentsInstalled {
		fmt.Println("3) (Optional) Start web UI: ncc-orchestrator v2-start")
	} else {
		fmt.Printf("3) (Optional) Install API/UI components: ncc-orchestrator v2-bootstrap --repo %s --install-dir %s\n", repo, installDir)
	}
	return nil
}

// extractClusterAddressV4 returns a reachable cluster address from clustermgmt v4 config cluster JSON.
func extractClusterAddressV4(entity map[string]interface{}) string {
	if netw, _ := entity["network"].(map[string]interface{}); netw != nil {
		if ext, _ := netw["externalAddress"].(map[string]interface{}); ext != nil {
			if ipv4, _ := ext["ipv4"].(map[string]interface{}); ipv4 != nil {
				if v, _ := ipv4["value"].(string); strings.TrimSpace(v) != "" {
					return strings.TrimSpace(v)
				}
			}
			if ipv6, _ := ext["ipv6"].(map[string]interface{}); ipv6 != nil {
				if v, _ := ipv6["value"].(string); strings.TrimSpace(v) != "" {
					return strings.TrimSpace(v)
				}
			}
		}
	}
	if nodes, _ := entity["nodes"].(map[string]interface{}); nodes != nil {
		if list, _ := nodes["nodeList"].([]interface{}); len(list) > 0 {
			if first, _ := list[0].(map[string]interface{}); first != nil {
				if cvm, _ := first["controllerVmIp"].(map[string]interface{}); cvm != nil {
					if ipv4, _ := cvm["ipv4"].(map[string]interface{}); ipv4 != nil {
						if v, _ := ipv4["value"].(string); strings.TrimSpace(v) != "" {
							return strings.TrimSpace(v)
						}
					}
				}
				if host, _ := first["hostIp"].(map[string]interface{}); host != nil {
					if ipv4, _ := host["ipv4"].(map[string]interface{}); ipv4 != nil {
						if v, _ := ipv4["value"].(string); strings.TrimSpace(v) != "" {
							return strings.TrimSpace(v)
						}
					}
				}
			}
		}
	}
	if name, _ := entity["name"].(string); strings.TrimSpace(name) != "" {
		return strings.TrimSpace(name)
	}
	return ""
}

// extractCVMIPv4sFromClusterEntity returns controller VM IPv4 addresses from a clustermgmt v4 cluster entity.
func extractCVMIPv4sFromClusterEntity(entity map[string]interface{}) []string {
	nodes, _ := entity["nodes"].(map[string]interface{})
	if nodes == nil {
		return nil
	}
	list, _ := nodes["nodeList"].([]interface{})
	var out []string
	for _, raw := range list {
		node, _ := raw.(map[string]interface{})
		if node == nil {
			continue
		}
		cvm, _ := node["controllerVmIp"].(map[string]interface{})
		if cvm == nil {
			continue
		}
		if ipv4, _ := cvm["ipv4"].(map[string]interface{}); ipv4 != nil {
			if v, _ := ipv4["value"].(string); strings.TrimSpace(v) != "" {
				out = append(out, strings.TrimSpace(v))
			}
		}
	}
	return out
}

func extractClusterExtIDV4(entity map[string]interface{}) string {
	if extID, _ := entity["extId"].(string); strings.TrimSpace(extID) != "" {
		return strings.TrimSpace(extID)
	}
	return ""
}

func dedupeStringsKeepOrder(in []string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// clusterEntityMatchesUserRef reports whether the user's --clusters value refers to this
// registered cluster (name, extId, external address, or any CVM IP). Used so Prism Central
// does not always use data[0] (wrong cluster when multiple are registered).
func clusterEntityMatchesUserRef(ref string, entity map[string]interface{}) bool {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return false
	}
	if name, _ := entity["name"].(string); strings.TrimSpace(name) != "" && strings.EqualFold(strings.TrimSpace(name), ref) {
		return true
	}
	if extID, _ := entity["extId"].(string); strings.TrimSpace(extID) != "" && strings.EqualFold(strings.TrimSpace(extID), ref) {
		return true
	}
	if a := extractClusterAddressV4(entity); a != "" && strings.EqualFold(a, ref) {
		return true
	}
	for _, ip := range extractCVMIPv4sFromClusterEntity(entity) {
		if strings.EqualFold(ip, ref) {
			return true
		}
	}
	return false
}

// extractClusterAddressV3 extracts external IP or name from a Prism Central cluster entity (v3).
func extractClusterAddressV3(entity map[string]interface{}) string {
	// spec.resources.network.external_ip or external_ip_address
	if spec, _ := entity["spec"].(map[string]interface{}); spec != nil {
		if res, _ := spec["resources"].(map[string]interface{}); res != nil {
			if netw, _ := res["network"].(map[string]interface{}); netw != nil {
				if ip, _ := netw["external_ip"].(string); ip != "" {
					return ip
				}
				if ip, _ := netw["external_ip_address"].(string); ip != "" {
					return ip
				}
			}
		}
	}
	// status.resources.network.external_ip
	if status, _ := entity["status"].(map[string]interface{}); status != nil {
		if res, _ := status["resources"].(map[string]interface{}); res != nil {
			if netw, _ := res["network"].(map[string]interface{}); netw != nil {
				if ip, _ := netw["external_ip"].(string); ip != "" {
					return ip
				}
			}
		}
		if name, _ := status["name"].(string); name != "" {
			return name
		}
	}
	return ""
}

func versionInfoString() string {
	return fmt.Sprintf(
		"Version: %s\nCommit: %s\nStream: %s\nBuild Date: %s\nGo Version: %s\nOS: %s\nArch: %s",
		Version, GitRevision, Stream, BuildDate, GoVersion, runtime.GOOS, runtime.GOARCH,
	)
}

// runVerifyCommand backs the `ncc-orchestrator verify` subcommand.
// Renders the trust-relevant metadata in a stable, scriptable format
// (single-key-per-line) so support tickets and CI checks can pin
// fields with simple grep. The hash is computed over the running
// executable's bytes on disk; if the file is unreadable (chroot,
// procfs-only systems, etc.) we degrade gracefully rather than
// failing the entire command.
//
// Wraps an io.Writer rather than printing directly so unit tests can
// drive the function with a bytes.Buffer.
//
// Note on attribution: this is an independent open-source project
// (MIT licensed) and is not affiliated with or endorsed by Nutanix,
// Inc. The "project_url" field below is the source of truth for
// where the binary came from.
// verifyOptions controls the `verify` subcommand. The default (zero value) is
// the historical offline behavior — print the self-hash and a compare URL. With
// Online set, verify fetches the matching release's checksums.txt from GitHub
// and reports MATCH / MISMATCH / NOT FOUND, returning a non-zero error on
// mismatch so it is usable in CI / health checks.
type verifyOptions struct {
	Online           bool
	ReleaseTag       string // override the release tag to check against ("" = stamped version)
	Repo             string // override the GitHub repo ("" = default)
	RequireSignature bool   // fail unless the Ed25519 signature over checksums.txt verifies
}

func runVerifyCommand(out io.Writer, opts verifyOptions) error {
	exe, err := os.Executable()
	if err != nil {
		exe = "(unknown)"
	}
	exeReal := exe
	if r, errReal := filepath.EvalSymlinks(exe); errReal == nil {
		exeReal = r
	}
	hash, hashErr := sha256OfFile(exeReal)
	gitRev, gitDirty, _ := goBuildInfoVCS()
	fmt.Fprintln(out, "ncc-orchestrator verify")
	fmt.Fprintln(out, "-----------------------")
	fmt.Fprintf(out, "version:           %s\n", Version)
	fmt.Fprintf(out, "stream:            %s\n", Stream)
	fmt.Fprintf(out, "build_date:        %s\n", BuildDate)
	fmt.Fprintf(out, "go_version:        %s\n", GoVersion)
	fmt.Fprintf(out, "os_arch:           %s/%s\n", runtime.GOOS, runtime.GOARCH)
	fmt.Fprintf(out, "git_revision:      %s\n", strDefault(gitRev, "(unknown)"))
	fmt.Fprintf(out, "git_dirty:         %t\n", gitDirty)
	fmt.Fprintf(out, "executable_path:   %s\n", exeReal)
	if hashErr != nil {
		fmt.Fprintf(out, "executable_sha256: (unavailable: %v)\n", hashErr)
	} else {
		fmt.Fprintf(out, "executable_sha256: %s\n", hash)
	}
	fmt.Fprintf(out, "license:           MIT\n")
	fmt.Fprintf(out, "project_url:       https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator\n")
	fmt.Fprintf(out, "affiliation:       independent open-source project; not affiliated with or endorsed by Nutanix, Inc.\n")

	// Strip git-rev / dirty suffixes (e.g. "2.0.2-<sha>" or "2.0.2-dirty")
	// so the URL / release lookup points at the canonical release tag.
	tagVersion := Version
	if i := strings.IndexAny(tagVersion, "-+"); i >= 0 {
		tagVersion = tagVersion[:i]
	}
	releaseTag := tagVersion
	if t := strings.TrimSpace(opts.ReleaseTag); t != "" {
		releaseTag = strings.TrimPrefix(t, "v")
	}

	if !opts.Online {
		fmt.Fprintf(out, "verify:            compare executable_sha256 against checksums.txt at\n")
		fmt.Fprintf(out, "                   https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v%s\n", releaseTag)
		fmt.Fprintf(out, "                   (run `verify --online` to fetch and compare automatically)\n")
		return nil
	}

	return runVerifyOnline(out, exeReal, hash, hashErr, gitDirty, releaseTag, opts.Repo, opts.RequireSignature)
}

// runVerifyOnline fetches the matching release's checksums.txt from GitHub and
// compares the running executable's SHA-256 against the published value for the
// platform's ncc-orchestrator-<os>-<arch> asset. It prints a verify_result line
// and returns a non-zero error on MISMATCH / NOT FOUND so callers (and CI) can
// branch on the exit code.
//
// Trust note: the checksum is fetched from the same repository that serves the
// binary, so this authenticates an *accidentally corrupted, truncated, or
// MITM'd* download and pins the *version* — it is not proof against a
// compromised release. Cryptographic provenance (signed checksums / GitHub
// artifact attestations) is tracked as future work.
func runVerifyOnline(out io.Writer, exePath, localHash string, hashErr error, gitDirty bool, releaseTag, repoOverride string, requireSignature bool) error {
	if hashErr != nil {
		return fmt.Errorf("verify --online: cannot hash executable: %w", hashErr)
	}
	if gitDirty {
		fmt.Fprintf(out, "verify_result:     SKIPPED — locally-modified build (git_dirty=true) will not match a published release\n")
		return nil
	}
	repo := strings.TrimSpace(repoOverride)
	if repo == "" {
		repo = defaultGitHubRepo
	}
	repo, err := normalizeGitHubRepo(repo)
	if err != nil {
		return fmt.Errorf("verify --online: %w", err)
	}
	client := &http.Client{Timeout: 20 * time.Second}
	fmt.Fprintf(out, "verify:            checking %s against %s release v%s checksums.txt …\n", filepath.Base(exePath), repo, releaseTag)
	releases, err := fetchGitHubReleases(repo, client)
	if err != nil {
		return fmt.Errorf("verify --online: fetch releases: %w", err)
	}
	var rel *githubRelease
	for i := range releases {
		if strings.EqualFold(strings.TrimPrefix(strings.TrimSpace(releases[i].TagName), "v"), releaseTag) {
			rel = &releases[i]
			break
		}
	}
	if rel == nil {
		fmt.Fprintf(out, "verify_result:     NOT FOUND — no release tagged v%s in %s (dev build or unpublished version?)\n", releaseTag, repo)
		return fmt.Errorf("release v%s not found in %s", releaseTag, repo)
	}
	_, assetName := pickAssetForCurrentPlatform(*rel)
	if assetName == "" {
		fmt.Fprintf(out, "verify_result:     NOT FOUND — release v%s has no asset for %s/%s\n", releaseTag, runtime.GOOS, runtime.GOARCH)
		return fmt.Errorf("no asset for %s/%s in release v%s", runtime.GOOS, runtime.GOARCH, releaseTag)
	}
	csBody, csAsset, err := fetchReleaseChecksumBody(rel, client)
	if err != nil {
		return fmt.Errorf("verify --online: %w", err)
	}
	expected := parseChecksumFile(csBody, assetName)
	if expected == "" {
		return fmt.Errorf("verify --online: checksum entry for %s not found in %s", assetName, csAsset)
	}
	fmt.Fprintf(out, "release_asset:     %s\n", assetName)
	fmt.Fprintf(out, "checksum_source:   %s (%s)\n", csAsset, rel.TagName)
	fmt.Fprintf(out, "expected_sha256:   %s\n", expected)

	// Verify the Ed25519 signature over checksums.txt before trusting the hash.
	// INVALID always fails; MISSING/SKIPPED fail only with --require-signature.
	sigStatus, sigErr := verifyChecksumSignature(rel, csBody, client, requireSignature)
	fmt.Fprintf(out, "signature_result:  %s\n", sigStatus)
	if sigErr != nil {
		fmt.Fprintf(out, "verify_result:     FAILED — checksum signature %s: %v\n", sigStatus, sigErr)
		return fmt.Errorf("checksum signature %s: %w", sigStatus, sigErr)
	}
	if strings.EqualFold(expected, localHash) {
		fmt.Fprintf(out, "verify_result:     MATCH — executable matches the published checksum for v%s\n", releaseTag)
		return nil
	}
	fmt.Fprintf(out, "verify_result:     MISMATCH — executable does NOT match the published checksum for v%s\n", releaseTag)
	return fmt.Errorf("checksum mismatch for v%s: expected %s, got %s", releaseTag, expected, localHash)
}

// fetchReleaseChecksum finds the release's checksums.txt (or sha256/.sha256)
// asset, downloads it, and returns the published SHA-256 for assetName plus the
// checksum asset's name. Mirrors the asset selection in
// verifyAssetAgainstReleaseChecksum but returns the hash for display/comparison
// rather than verifying a downloaded body.
func fetchReleaseChecksum(rel *githubRelease, assetName string, client *http.Client) (expected, checksumAsset string, err error) {
	csBody, csAsset, err := fetchReleaseChecksumBody(rel, client)
	if err != nil {
		return "", csAsset, err
	}
	h := parseChecksumFile(csBody, assetName)
	if h == "" {
		return "", csAsset, fmt.Errorf("checksum entry for %s not found in %s", assetName, csAsset)
	}
	return h, csAsset, nil
}

// fetchReleaseChecksumBody downloads the release's checksums.txt (or
// sha256/.sha256) asset and returns its raw bytes plus the asset name. The raw
// body is what release-signature verification authenticates, so callers that
// verify signatures must use the exact bytes returned here.
func fetchReleaseChecksumBody(rel *githubRelease, client *http.Client) (csBody []byte, checksumAsset string, err error) {
	if rel == nil {
		return nil, "", errors.New("nil release passed to checksum fetcher")
	}
	for _, a := range rel.Assets {
		an := strings.ToLower(a.Name)
		// The signature asset (checksums.txt.sig) also contains "checksum"; skip it.
		if strings.HasSuffix(an, ".sig") {
			continue
		}
		if !(strings.Contains(an, "checksum") || strings.Contains(an, "sha256") || strings.HasSuffix(an, ".sha256")) {
			continue
		}
		body, ferr := fetchURL(a.BrowserDownloadURL, client)
		if ferr != nil {
			return nil, a.Name, fmt.Errorf("fetch checksum asset %s: %w", a.Name, ferr)
		}
		return body, a.Name, nil
	}
	return nil, "", fmt.Errorf("no checksum asset found for release %s", rel.TagName)
}

// sha256OfFile streams a file through SHA-256. Streamed (rather than
// reading into memory) so the verify command stays fast even on a
// 50MB binary built with -trimpath (no debug info).
func sha256OfFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// goBuildInfoVCS pulls the (commit, dirty, build_time) triple Go's
// linker embeds via -buildvcs=true. Returns ("", false, false) on
// binaries built with -buildvcs=false. Stable across Go releases —
// the keys "vcs.revision", "vcs.modified", "vcs.time" have been
// part of debug.ReadBuildInfo since Go 1.18.
func goBuildInfoVCS() (revision string, dirty bool, ok bool) {
	info, infoOK := debug.ReadBuildInfo()
	if !infoOK {
		return "", false, false
	}
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	return revision, dirty, revision != ""
}

func strDefault(s, dflt string) string {
	if strings.TrimSpace(s) == "" {
		return dflt
	}
	return s
}

func printEnvInfo() {
	fmt.Println("Possible Environment Variables (prefix: NCC_) and Current Values:")
	envKeys := []string{
		"CONFIG",
		"SKIP_PREFLIGHT_CHECK",
		"CLUSTER_SOURCE_MODE",
		"CLUSTERS",
		"CLUSTERS_FILE",
		"PCS",
		"PCS_FILE",
		"PRISM_CENTRAL_URL",
		"DISCOVER_API_VERSION",
		"USERNAME",
		"PASSWORD",
		"INSECURE_SKIP_VERIFY",
		"TIMEOUT",
		"REQUEST_TIMEOUT",
		"POLL_INTERVAL",
		"POLL_JITTER",
		"MAX_PARALLEL",
		"OUTPUTS",
		"OUTPUT_DIR_LOGS",
		"OUTPUT_DIR_FILTERED",
		"LOG_FILE",
		"LOG_LEVEL",
		"LOG_HTTP",
		"RETRY_MAX_ATTEMPTS",
		"RETRY_BASE_DELAY",
		"RETRY_MAX_DELAY",
		"RETRY_CIRCUIT_BREAKER",
		"PROM_DIR",
		"RUN_HISTORY",
		"RUN_HISTORY_DIR",
		"RETAIN_LAST",
		"RETAIN_DAYS",
		"ARTIFACT_RETAIN_DAYS",
		"ARTIFACT_RETAIN_MAX_FILES",
		"SINGLE_REPORT",
		"NOTIFY_ON_REGRESSION",
		"ADAPTIVE_PARALLELISM",
		"SEVERITY_FILTER",
		"EXCLUDE_ALERT_TITLES",
		"EXCLUDE_ALERT_TITLES_FILE",
		"EXCLUDE_ALERT_MATCH_MODE",
		"DRY_RUN",
		"REPLAY",
		"MAX_IDLE_CONNS",
		"MAX_IDLE_CONNS_PER_HOST",
		"MAX_CONNS_PER_HOST",
		"IDLE_CONN_TIMEOUT",
		"EMAIL_ENABLED",
		"EMAIL_ATTACH_HTML",
		"NOTIFY_DIGEST",
		"SMTP_SERVER",
		"SMTP_PORT",
		"SMTP_USER",
		"SMTP_PASSWORD",
		"EMAIL_FROM",
		"EMAIL_TO",
		"EMAIL_USE_TLS",
		"WEBHOOK_ENABLED",
		"WEBHOOK_INCLUDE_HTML",
		"WEBHOOK_URL",
		"WEBHOOK_HEADERS",
		"SLACK_ENABLED",
		"SLACK_WEBHOOK_URL",
		"SLACK_CHANNEL",
	}
	for _, key := range envKeys {
		envVar := "NCC_" + key
		val := os.Getenv(envVar)
		if val != "" {
			if key == "PASSWORD" || key == "SMTP_PASSWORD" {
				fmt.Printf("%s = %s\n", envVar, maskPassword(val))
			} else {
				fmt.Printf("%s = %s\n", envVar, val)
			}
		} else {
			fmt.Printf("%s = (not set)\n", envVar)
		}
	}
}

func newRootCmd() *cobra.Command {

	cmd := &cobra.Command{
		Use:   "ncc-orchestrator",
		Short: "Nutanix NCC Orchestrator",
		Long: `A tool to run NCC checks on multiple clusters, aggregate results, and generate reports.
Use --config for setup.

Examples:

  # Basic usage with configuration file
  ncc-orchestrator --config config.yaml

  # Specify clusters and username via flags
  ncc-orchestrator --clusters 10.0.1.1,10.0.2.1 --username admin

  # Show all available environment variables
  ncc-orchestrator env-info

Run 'ncc-orchestrator --help' for a full list of options.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Setup console logger first for early error visibility
			consoleWriter := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
			consoleLogger := zerolog.New(consoleWriter).With().Timestamp().Logger()
			zerolog.SetGlobalLevel(zerolog.InfoLevel)

			// Backward-compatible aliases for old root flags.
			if update, _ := cmd.Flags().GetBool("update"); update {
				if err := runUpdate(updateOptions{Repo: defaultGitHubRepo}); err != nil {
					consoleLogger.Error().Err(err).Msg("update failed")
					return fmt.Errorf("update: %w", err)
				}
				return nil
			}
			if genN, _ := cmd.Flags().GetInt("gen-test-agg"); genN > 0 {
				outDir := viper.GetString("output-dir-filtered")
				if outDir == "" {
					outDir = "outputfiles"
				}
				if err := generateTestAgg(genN, outDir); err != nil {
					consoleLogger.Error().Err(err).Int("clusters", genN).Msg("gen-test-agg failed")
					return fmt.Errorf("gen-test-agg: %w", err)
				}
				fmt.Printf("Generated test aggregated report: %d clusters, output in %s/index.html\n", genN, outDir)
				return nil
			}
			if showTC, _ := cmd.Flags().GetBool("tc"); showTC {
				fmt.Print(termsText)
				return nil
			}
			if showEnvInfo, _ := cmd.Flags().GetBool("env-info"); showEnvInfo {
				printEnvInfo()
				return nil
			}
			if showVersion, _ := cmd.Flags().GetBool("version"); showVersion {
				fmt.Println(versionInfoString())
				return nil
			}

			cfg, err := bindConfig()
			if err != nil {
				return exitConfig(fmt.Errorf("configuration: %w", err))
			}
			autoMode, _ := cmd.Flags().GetBool("auto")
			autoLevelRaw, _ := cmd.Flags().GetString("automation-level")
			autoLevel, err := parseAutomationLevel(autoLevelRaw)
			if err != nil {
				return exitConfig(err)
			}
			if autoMode && autoLevel == automationLevelFullAuto {
				changes := applyFullAutoRuntimeTuning(&cfg)
				for _, change := range changes {
					consoleLogger.Warn().Str("auto_tuning", change).Msg("full-auto runtime tuning applied")
				}
			}

			lvl := parseLogLevel(cfg.LogLevel)
			// Validate log level
			if lvl > zerolog.FatalLevel {
				consoleLogger.Warn().Int("level", int(lvl)).Msg("invalid log level, using info level")
				lvl = zerolog.InfoLevel
			}

			if err := checkOutputPermissions(&cfg); err != nil {
				consoleLogger.Error().Err(err).Msg("output permissions check failed (cannot open/create required files)")
				return exitConfig(fmt.Errorf("output permissions: %w", err))
			}

			if err := setupFileLogger(cfg.LogFile, lvl); err != nil {
				consoleLogger.Error().Err(err).Str("logFile", cfg.LogFile).Msg("failed to setup file logger")
				return exitConfig(fmt.Errorf("setup logger: %w", err))
			}

			// Set global log level after file logger is set up
			zerolog.SetGlobalLevel(lvl)
			log.Info().
				Str("clusterSourceMode", cfg.ClusterSourceMode).
				Strs("clusters", cfg.Clusters).
				Strs("pcs", cfg.PCs).
				Str("username", cfg.Username).
				Str("password", maskPassword(cfg.Password)).
				Bool("insecureSkipVerify", cfg.InsecureSkipVerify).
				Dur("timeout", cfg.Timeout).
				Dur("requestTimeout", cfg.RequestTimeout).
				Dur("pollInterval", cfg.PollInterval).
				Dur("pollJitter", cfg.PollJitter).
				Int("maxParallel", cfg.MaxParallel).
				Strs("outputs", cfg.OutputFormats).
				Str("logsDir", cfg.OutputDirLogs).
				Str("filteredDir", cfg.OutputDirFiltered).
				Str("logFile", cfg.LogFile).
				Str("logLevel", lvl.String()).
				Bool("logHTTP", cfg.LogHTTP || os.Getenv("LOG_HTTP") == "1").
				Int("retryMaxAttempts", cfg.RetryMaxAttempts).
				Dur("retryBaseDelay", cfg.RetryBaseDelay).
				Dur("retryMaxDelay", cfg.RetryMaxDelay).
				Msg("starting NCC orchestrator")

			skipPreflight, _ := cmd.Flags().GetBool("skip-preflight-check")
			if !skipPreflight {
				preflightCfgPath := strings.TrimSpace(viper.GetString("config"))
				report := buildPreflightReport(preflightCfgPath)
				if report.Failed > 0 && autoMode && autoLevel != automationLevelAdvisory {
					fixed, actions := applySafePreflightFixes(cfg, preflightCfgPath, report, autoLevel)
					for _, action := range actions {
						log.Info().Str("auto_fix", action).Msg("automation applied safe fix")
					}
					if fixed > 0 {
						report = buildPreflightReport(preflightCfgPath)
					}
				}
				if report.Failed > 0 {
					printAutomationRunbook(report, autoLevel)
					hintMsg := ""
					if len(report.ActionableHints) > 0 {
						hintMsg = " hints: " + strings.Join(report.ActionableHints, "; ")
					}
					return exitConfig(fmt.Errorf("preflight-check failed (%d failures, %d warnings).%s", report.Failed, report.Warn, hintMsg))
				}
				log.Info().Int("preflight_failed", report.Failed).Int("preflight_warn", report.Warn).Msg("preflight-check passed")
			} else {
				log.Warn().Msg("preflight-check skipped by flag")
			}

			// Resolve targets by source mode.
			if cfg.ClusterSourceMode == "pc" {
				if strings.TrimSpace(cfg.Password) == "" {
					cfg.Password, err = promptPasswordIfEmpty("", cfg.Username)
					if err != nil {
						return err
					}
				}
				resolvedClusters, discoverErr := discoverClustersFromPCTargets(cfg)
				if discoverErr != nil {
					log.Error().Err(discoverErr).Msg("failed to discover clusters from PC targets")
					return exitConfig(discoverErr)
				}
				cfg.Clusters = resolvedClusters
				cfg.ClusterCredentials = map[string]ClusterCredential{}
				log.Info().
					Int("pcs", len(cfg.PCs)).
					Int("discovered_clusters", len(cfg.Clusters)).
					Str("discover_api_version", cfg.DiscoverAPIVersion).
					Msg("resolved clusters from pc mode")
			}

			// Validate required fields after target resolution.
			if len(cfg.Clusters) == 0 {
				err := errors.New("no clusters provided (--clusters, --clusters-file, env, or config)")
				log.Error().Msg(err.Error())
				return exitConfig(err)
			}
			if err := validateClusterCredentialCoverage(cfg); err != nil {
				log.Error().Msg(err.Error())
				return exitConfig(err)
			}

			// Dry-run mode: perform full validation and exit
			if cfg.DryRun {
				// Perform comprehensive validation for dry-run
				if err := validateConfig(cfg); err != nil {
					return fmt.Errorf("dry-run validation failed: %w", err)
				}

				log.Info().Msg("DRY-RUN MODE: Configuration validated, no checks will be executed")
				fmt.Println("✓ Configuration is valid")
				fmt.Printf("  Clusters: %d configured\n", len(cfg.Clusters))
				fmt.Printf("  Cluster source mode: %s\n", cfg.ClusterSourceMode)
				if cfg.ClusterSourceMode == "pc" {
					fmt.Printf("  Prism Central targets: %d\n", len(cfg.PCs))
				}
				if strings.TrimSpace(cfg.Username) != "" {
					fmt.Printf("  Username: %s\n", cfg.Username)
				} else {
					fmt.Println("  Username: per-cluster (clusters-file)")
				}
				fmt.Printf("  Output formats: %v\n", cfg.OutputFormats)
				if len(cfg.SeverityFilter) > 0 {
					fmt.Printf("  Severity filter: %v\n", cfg.SeverityFilter)
				}
				if len(cfg.ExcludeAlertTitles) > 0 {
					fmt.Printf("  Excluded alert titles: %v\n", cfg.ExcludeAlertTitles)
				}
				fmt.Printf("  Exclude alert match mode: %s\n", cfg.ExcludeAlertMatchMode)
				if cfg.ExcludeAlertTitlesFile != "" {
					fmt.Printf("  Exclude alert titles file: %s\n", cfg.ExcludeAlertTitlesFile)
				}
				if cfg.ArtifactRetainDays > 0 || cfg.ArtifactRetainMaxFiles > 0 {
					fmt.Printf("  Artifact retention: days=%d max_files=%d\n", cfg.ArtifactRetainDays, cfg.ArtifactRetainMaxFiles)
				}
				if cfg.InsecureSkipVerify {
					fmt.Println("  ⚠️  WARNING: TLS verification is disabled")
				}
				fmt.Println("  All settings validated successfully")
				return nil
			}

			if needPrompt, promptUser := needsPasswordPrompt(cfg); needPrompt {
				cfg.Password, err = promptPasswordIfEmpty(cfg.Password, promptUser)
				if err != nil {
					return err
				}
			}

			fs := OSFS{}
			httpc := NewHTTPClient(cfg)
			if err := fs.MkdirAll(cfg.OutputDirLogs, 0755); err != nil {
				return err
			}
			if err := fs.MkdirAll(cfg.OutputDirFiltered, 0755); err != nil {
				return err
			}
			if cfg.PromEnabled {
				if err := fs.MkdirAll(cfg.PromDir, 0755); err != nil {
					return err
				}
			}

			previousSummaryPath := filepath.Join(cfg.OutputDirFiltered, "run-summary.json")
			previousSummary, hasPreviousSummary, err := loadRunSummaryJSON(previousSummaryPath)
			if err != nil {
				log.Warn().Err(err).Str("path", previousSummaryPath).Msg("failed to read previous run-summary; regression baseline disabled")
			}
			previousChecksSnapshotPath := filepath.Join(cfg.OutputDirFiltered, "checks-snapshot.json")
			previousChecksSnapshot, hasPreviousChecksSnapshot, err := loadChecksSnapshotJSON(previousChecksSnapshotPath)
			if err != nil {
				log.Warn().Err(err).Str("path", previousChecksSnapshotPath).Msg("failed to read previous checks snapshot; drill-down baseline disabled")
			}
			if hasPreviousSummary {
				cfg.PreviousClusterFailCount = failCountByCluster(previousSummary)
				log.Info().
					Str("previous_timestamp", previousSummary.Timestamp).
					Int("previous_clusters", len(previousSummary.Clusters)).
					Msg("loaded previous run-summary baseline")
			} else {
				cfg.PreviousClusterFailCount = map[string]int{}
			}

			// Fast replay mode: skip API, parse existing logs and render everything
			if cmd.Flags().Changed("replay") && viper.GetBool("replay") {
				var agg []AggBlock
				var clusterFiles []struct{ Cluster, HTML, CSV string }
				excludedByCluster := make(map[string][]ExcludedAlert)

				for _, cluster := range cfg.Clusters {
					// Ensure filtered log exists
					filtered := filepath.Join(cfg.OutputDirFiltered, fmt.Sprintf("%s.log", cluster))
					if _, err := os.Stat(filtered); err != nil {
						// Try to build it from raw ncc log
						raw := filepath.Join(cfg.OutputDirLogs, fmt.Sprintf("%s.log", cluster))
						if _, err2 := os.Stat(raw); err2 == nil {
							if _, err3 := filterBlocksToFile(OSFS{}, raw, filtered); err3 != nil {
								log.Error().Str("cluster", cluster).Err(err3).Msg("replay: build filtered failed")
								continue
							}
							log.Info().Str("cluster", cluster).Str("filtered", filtered).Msg("replay: built filtered")
						} else {
							log.Warn().Str("cluster", cluster).Msg("replay: no filtered or raw log, skipping")
							continue
						}
					}
					// Parse filtered
					data, err := os.ReadFile(filtered)
					if err != nil {
						log.Error().Str("cluster", cluster).Err(err).Msg("replay: read filtered failed")
						continue
					}
					blocks, err := ParseSummary(string(data))
					if err != nil {
						log.Error().Str("cluster", cluster).Err(err).Msg("replay: parse filtered failed")
						continue
					}
					if len(cfg.SeverityFilter) > 0 {
						originalCount := len(blocks)
						blocks = filterBlocksBySeverity(blocks, cfg.SeverityFilter)
						log.Info().
							Str("cluster", cluster).
							Int("original", originalCount).
							Int("filtered", len(blocks)).
							Strs("severities", cfg.SeverityFilter).
							Msg("replay: applied severity filter")
					}
					if len(cfg.ExcludeAlertTitles) > 0 {
						originalCount := len(blocks)
						filteredBlocks, excludedAlerts, err := filterBlocksByTitle(blocks, cfg.ExcludeAlertTitles, cfg.ExcludeAlertMatchMode)
						if err != nil {
							log.Error().Str("cluster", cluster).Err(err).Msg("replay: apply alert title exclusion filter failed")
							continue
						}
						blocks = filteredBlocks
						excludedByCluster[cluster] = excludedAlerts
						log.Info().
							Str("cluster", cluster).
							Int("original", originalCount).
							Int("filtered", len(blocks)).
							Int("excluded", len(excludedAlerts)).
							Str("mode", cfg.ExcludeAlertMatchMode).
							Strs("titles", cfg.ExcludeAlertTitles).
							Msg("replay: applied alert title exclusion filter")
					}

					counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
					for _, b := range blocks {
						sev := b.Severity
						if sev == "" {
							sev = "INFO"
						}
						counts[sev]++
					}
					overviewReplay := fmt.Sprintf("NCC replay for cluster %s. FAIL: %d, WARN: %d, ERR: %d, INFO: %d. Total: %d checks (from existing log).",
						cluster, counts["FAIL"], counts["WARN"], counts["ERR"], counts["INFO"], len(blocks))
					replaySummary := NotificationSummary{
						Cluster:     cluster,                           // from replay filename or param
						StartedAt:   time.Now().Add(-10 * time.Minute), // estimate
						FinishedAt:  time.Now(),
						FailCount:   counts["FAIL"],
						WarnCount:   counts["WARN"],
						ErrCount:    counts["ERR"],
						InfoCount:   counts["INFO"],
						TotalChecks: len(blocks),
						OutputFiles: []string{filtered},
						Overview:    overviewReplay,
					}

					subj := fmt.Sprintf("NCC REPLAY %s: FAIL=%d WARN=%d",
						replaySummary.Cluster, replaySummary.FailCount, replaySummary.WarnCount)
					body := overviewReplay + "\n\n" + fmt.Sprintf("REPLAY MODE - From existing log:\nFAIL: %d | WARN: %d | Total: %d\nLog: %s",
						replaySummary.FailCount, replaySummary.WarnCount, len(blocks), filtered)
					subj, body = applyEmailTemplates(cfg, subj, body, replaySummary, log.Logger)

					// Per-cluster outputs: generate HTML and CSV before notifications so we can attach HTML to email
					base := filtered
					var replayHTMLPath string
					rawPath := filepath.Join(cfg.OutputDirLogs, fmt.Sprintf("%s.log", cluster))
					var meta HTMLMeta
					metaLoaded := false
					loadMeta := func() HTMLMeta {
						if !metaLoaded {
							m, mErr := parseNCCHeader(rawPath)
							if mErr != nil {
								log.Warn().Err(mErr).Str("rawPath", rawPath).Msg("replay: parseNCCHeader failed, using empty meta")
								meta = HTMLMeta{}
							} else {
								meta = m
							}
							metaLoaded = true
						}
						return meta
					}
					if cfg.PromEnabled {
						if err := writePrometheusFile(OSFS{}, cfg.PromDir, cluster, blocks); err != nil {
							log.Error().Str("cluster", cluster).Err(err).Msg("replay write Prometheus .prom failed")
						} else {
							log.Info().Str("cluster", cluster).Str("prom_dir", cfg.PromDir).Msg("replay: Prometheus .prom written")
						}
					}
					for _, f := range cfg.OutputFormats {
						switch strings.ToLower(strings.TrimSpace(f)) {
						case "html":
							htmlFile := base + ".html"
							replayHTMLPath = htmlFile
							if err := generateHTML(OSFS{}, rowsFromBlocks(blocks), htmlFile, loadMeta()); err != nil {
								log.Error().Err(err).Str("file", htmlFile).Msg("replay: write HTML failed")
								replayHTMLPath = ""
							}
						case "csv":
							_ = generateCSV(OSFS{}, blocks, base+".csv")
						case "json":
							_ = generateJSON(OSFS{}, blocks, base+".json", loadMeta())
						case "markdown":
							_ = generateMarkdown(OSFS{}, blocks, base+".md", loadMeta())
						case "sarif":
							_ = generateSARIF(OSFS{}, blocks, base+".sarif")
						}
					}

					attachPath := ""
					if cfg.EmailAttachHTML && replayHTMLPath != "" {
						attachPath = replayHTMLPath
					}
					if cfg.WebhookIncludeHTML && replayHTMLPath != "" {
						if b, err := os.ReadFile(replayHTMLPath); err == nil {
							replaySummary.ReportHTMLBase64 = base64.StdEncoding.EncodeToString(b)
						}
					}
					ctx := context.Background()
					httpc := NewHTTPClient(cfg)
					if suppressed, reason := notificationsSuppressedNow(cfg, time.Now()); suppressed {
						log.Info().Str("cluster", cluster).Str("reason", reason).Msg("replay notifications suppressed")
					} else {
						if err := sendEmailWithRetry(cfg, subj, body, attachPath); err != nil {
							log.Error().Err(err).Str("cluster", cluster).Msg("replay email failed")
						}
						if err := sendWebhookWithRetry(ctx, httpc, cfg, replaySummary); err != nil {
							log.Error().Err(err).Str("cluster", cluster).Msg("replay webhook failed")
						}
						log.Info().Int("fail", replaySummary.FailCount).Int("warn", replaySummary.WarnCount).
							Str("cluster", cluster).Msg("replay notifications sent")
					}

					clusterFiles = append(clusterFiles, struct{ Cluster, HTML, CSV string }{
						Cluster: cluster,
						HTML:    filepath.Base(base + ".html"),
						CSV:     filepath.Base(base + ".csv"),
					})
					meta = loadMeta()

					for _, b := range blocks {
						agg = append(agg, AggBlock{
							Cluster:        cluster,
							Severity:       b.Severity,
							Check:          b.CheckName,
							Detail:         b.DetailRaw,
							ClusterName:    meta.ClusterName,
							ClusterVersion: meta.ClusterVersion,
							NCCVersion:     meta.NCCVersion,
						})
					}
				}

				if err := writeAggregatedHTMLSingle(OSFS{}, cfg.OutputDirFiltered, agg, clusterFiles, cfg); err != nil {
					log.Error().Err(err).Msg("replay: write aggregated HTML failed")
					return err
				}
				if len(cfg.ExcludeAlertTitles) > 0 {
					if err := writeExcludedAlertsAuditJSON(OSFS{}, cfg.OutputDirFiltered, cfg.ExcludeAlertMatchMode, cfg.ExcludeAlertTitles, excludedByCluster); err != nil {
						log.Error().Err(err).Msg("replay: write excluded-alerts.json failed (non-fatal)")
					}
				}
				if deleted, err := applyArtifactRetentionPolicies(cfg.OutputDirFiltered, cfg.ArtifactRetainDays, cfg.ArtifactRetainMaxFiles, time.Now()); err != nil {
					log.Error().Err(err).Msg("replay: artifact retention failed (non-fatal)")
				} else if deleted > 0 {
					log.Info().Int("deleted", deleted).Int("retain_days", cfg.ArtifactRetainDays).Int("retain_max_files", cfg.ArtifactRetainMaxFiles).Msg("replay: artifact retention applied")
				}
				log.Info().Int("clusters", len(clusterFiles)).Int("rows", len(agg)).Msg("replay: aggregated page generated")
				return nil
			}

			// Inside RunE, after setting up cfg, fs, httpc...
			fmt.Println("You have accepted T&C, Check using --tc flag")

			// Create root context with graceful shutdown support
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Setup graceful shutdown signal handling
			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
			defer signal.Stop(sigChan)
			go func() {
				sig := <-sigChan
				log.Warn().Str("signal", sig.String()).Msg("received shutdown signal, initiating graceful shutdown")
				fmt.Fprintf(os.Stderr, "\n⚠️  Received %s signal. Initiating graceful shutdown...\n", sig.String())
				cancel() // Cancel root context to stop all operations
			}()

			// Live progress bars are for an interactive terminal only. When
			// stdout is redirected to a file (the systemd-timer scheduler and
			// the api-server both do `... >> ncc-runner.log 2>&1`), mpb's ANSI
			// cursor-control frames (\x1b[<n>A, \x1b[J) pile up and a viewer
			// collapses them to a single ~18-line final frame — making the
			// runner log look truncated. Discard the bar output off-TTY; the
			// structured zerolog lines below (phase change / cluster run
			// completed / cluster run failed) still record per-cluster progress.
			progressOpts := []mpb.ContainerOption{mpb.WithWidth(80)}
			if !term.IsTerminal(int(os.Stdout.Fd())) {
				progressOpts = append(progressOpts, mpb.WithOutput(io.Discard))
			}
			p := mpb.New(progressOpts...)
			defer func() {
				// Ensure progress bars are cleaned up on exit
				p.Wait()
			}()
			resetAdaptiveParallelism(cfg.MaxParallel)
			gateChanged := make(chan struct{}, 1)
			setAdaptiveParallelismNotify(gateChanged)
			defer setAdaptiveParallelismNotify(nil)
			notifyGateChange := func() {
				select {
				case gateChanged <- struct{}{}:
				default:
				}
			}
			sem := make(chan struct{}, cfg.MaxParallel)
			var activeWorkers int32
			var wg sync.WaitGroup
			results := make(chan ClusterResult, len(cfg.Clusters))
			runStart := time.Now()
			notify.ResetMetrics()

			// Run-activity heartbeat: write a tiny per-pid marker into the output
			// dir for the lifetime of this run. The api-server reads these to
			// surface runs it did NOT spawn — chiefly systemd-timer/cron
			// *scheduled* runs — in its Active Runs view (it otherwise only knows
			// about runs it launched itself). Removed on exit; stale markers from a
			// crash are cleaned up by the reader once the pid is gone.
			if hbDir := strings.TrimSpace(cfg.OutputDirFiltered); hbDir != "" {
				hbSource := strings.TrimSpace(os.Getenv("NCC_RUN_SOURCE"))
				if hbSource == "" {
					hbSource = "manual"
				}
				hbPath := filepath.Join(hbDir, fmt.Sprintf(".ncc-run-active-%d.json", os.Getpid()))
				if hb, err := json.Marshal(map[string]interface{}{
					"pid":        os.Getpid(),
					"started_at": runStart.UTC().Format(time.RFC3339),
					"clusters":   cfg.Clusters,
					"source":     hbSource,
				}); err == nil {
					_ = os.WriteFile(hbPath, hb, 0o644)
					defer func() { _ = os.Remove(hbPath) }()
				}
			}

			// Opt-in OpenTelemetry tracing (no-op unless an OTLP endpoint is
			// configured). Spans are emitted per cluster below.
			otelShutdown, otelErr := trace.Init(ctx, Version)
			if otelErr != nil {
				log.Warn().Err(otelErr).Msg("opentelemetry tracing init failed; continuing without tracing")
			}
			defer func() {
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = otelShutdown(shutdownCtx)
			}()

			for _, cluster := range cfg.Clusters {
				clusterUser, clusterPass := credentialsForCluster(cfg, cluster)
				wg.Add(1)
			waitForWorkerSlot:
				for {
					limit := currentAdaptiveParallel(cfg.MaxParallel)
					curActive := int(atomic.LoadInt32(&activeWorkers))
					if curActive < limit {
						break
					}
					select {
					case <-ctx.Done():
						break waitForWorkerSlot
					case <-gateChanged:
					}
				}
				if ctx.Err() != nil {
					wg.Done()
					break
				}
				sem <- struct{}{}
				atomic.AddInt32(&activeWorkers, 1)

				mainBar := p.New(
					100,
					mpb.BarStyle().Rbound("|"),
					mpb.PrependDecorators(
						decor.Name(fmt.Sprintf("%-18s", cluster), decor.WC{W: 20, C: decor.DidentRight}),
					),
					mpb.AppendDecorators(
						decor.Percentage(decor.WC{W: 4}),
						decor.Name(" • "),
						decor.Elapsed(decor.ET_STYLE_GO, decor.WC{W: 4}),
					),
				)

				phaseProxy := &proxyDecorator{text: "starting"}

				phaseBar := p.New(
					1,
					mpb.NopStyle(),
					mpb.PrependDecorators(decor.Name(strings.Repeat(" ", 20))),
					mpb.AppendDecorators(phaseProxy),
				)

				go func(cl string, user string, pass string, b *mpb.Bar, phase *proxyDecorator, phaseBar *mpb.Bar) {
					defer wg.Done()
					defer func() {
						<-sem
						atomic.AddInt32(&activeWorkers, -1)
						notifyGateChange()
					}()
					defer func() {
						if r := recover(); r != nil {
							b.Abort(false)
							b.SetTotal(b.Current(), true)
							phaseBar.SetCurrent(1)     // Set current to match total
							phaseBar.SetTotal(1, true) // Complete phaseBar on panic
							log.Error().Interface("panic", r).Stack().Str("cluster", cl).Msg("cluster goroutine panic")
							panicErr := fmt.Errorf("panic: %v", r)
							results <- ClusterResult{Cluster: cl, Blocks: nil, Err: panicErr, ErrorClass: classifyClusterError(panicErr)}
						}
					}()

					reqCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
					defer cancel()

					reqCtx, span := trace.StartCluster(reqCtx, cl)
					defer span.End()

					onPct := func(pct int) { b.SetCurrent(int64(pct)) }
					setPhase := func(text string) {
						phase.SetText(text)
						log.Info().Str("cluster", cl).Str("phase", text).Msg("phase change")
					}

					runOut, err := runClusterWithBars(reqCtx, cfg, fs, httpc, cl, user, pass, onPct, setPhase)
					if err != nil {
						trace.RecordError(span, err)
						b.Abort(false)
						b.SetTotal(b.Current(), true)
						setPhase("failed")
						phaseBar.SetCurrent(1)     // Set current to match total
						phaseBar.SetTotal(1, true) // Complete phaseBar on error
						// Log with detailed error information
						log.Error().
							Str("cluster", cl).
							Err(err).
							Str("error_type", fmt.Sprintf("%T", err)).
							Msg("cluster run failed")
						// Also print to console for visibility
						fmt.Fprintf(os.Stderr, "\n❌ Cluster %s failed: %v\n", cl, err)
						results <- ClusterResult{Cluster: cl, Blocks: nil, Err: err, ErrorClass: classifyClusterError(err)}
						return
					}

					b.SetCurrent(100)
					b.SetTotal(100, true)
					setPhase("done")
					phaseBar.SetCurrent(1)     // Set current to match total
					phaseBar.SetTotal(1, true) // Complete phaseBar on success
					log.Info().Str("cluster", cl).Msg("cluster run completed")
					results <- ClusterResult{Cluster: cl, Blocks: runOut.Blocks, ExcludedByTitle: runOut.ExcludedByTitle, Err: nil, ErrorClass: ""}
				}(cluster, clusterUser, clusterPass, mainBar, phaseProxy, phaseBar) // Pass phaseBar and per-cluster credentials
			}

			// Wait for workers with context cancellation support
			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			select {
			case <-done:
				// All workers completed normally
			case <-ctx.Done():
				// Context cancelled (shutdown signal received)
				log.Warn().Msg("context cancelled, waiting for workers to finish")
				fmt.Fprintln(os.Stderr, "⏳ Waiting for in-progress operations to complete...")
				// Wait with timeout for workers to finish
				waitCtx, waitCancel := context.WithTimeout(context.Background(), shutdownTimeout)
				defer waitCancel()
				select {
				case <-done:
					log.Info().Msg("all workers completed after cancellation")
				case <-waitCtx.Done():
					log.Error().Msg("timeout waiting for workers after cancellation")
					fmt.Fprintln(os.Stderr, "❌ Timeout waiting for operations to complete")
					return fmt.Errorf("graceful shutdown timeout: operations did not complete in time")
				}
			}

			close(results)

			var failed []string
			var agg []AggBlock
			var clusterFiles []struct{ Cluster, HTML, CSV string }
			perCluster := make([]RunClusterSummary, 0, len(cfg.Clusters))
			checksSnapshot := ChecksSnapshotJSON{
				Timestamp: time.Now().UTC().Format(time.RFC3339),
				Clusters:  make([]ClusterChecksSnapshot, 0, len(cfg.Clusters)),
			}
			failureCounts := newFailureClassCounts()
			excludedByCluster := make(map[string][]ExcludedAlert)

			for r := range results {
				perCluster = append(perCluster, buildRunClusterSummary(r))
				checksSnapshot.Clusters = append(checksSnapshot.Clusters, buildClusterChecksSnapshotFromResult(r))
				incrementFailureClassCount(failureCounts, r)
				if r.Err != nil {
					failed = append(failed, r.Cluster)
					// Surface the failure as a real, visible UNKNOWN-severity row
					// rather than letting the cluster silently disappear from the
					// aggregated report (see clusterRunFailedCheckName doc comment).
					agg = append(agg, AggBlock{
						Cluster:  r.Cluster,
						Severity: "UNKNOWN",
						Check:    clusterRunFailedCheckName,
						Detail:   r.Err.Error() + " — " + runFailedRemediation(r.Err, r.ErrorClass),
					})
					continue
				}
				if len(r.ExcludedByTitle) > 0 {
					excludedByCluster[r.Cluster] = append(excludedByCluster[r.Cluster], r.ExcludedByTitle...)
				}

				rawPath := filepath.Join(cfg.OutputDirLogs, fmt.Sprintf("%s.log", r.Cluster))
				meta, _ := parseNCCHeader(rawPath) // ignore error

				for _, b := range r.Blocks {
					agg = append(agg, AggBlock{
						Cluster:        r.Cluster,
						Severity:       b.Severity,
						Check:          b.CheckName,
						Detail:         b.DetailRaw,
						ClusterName:    meta.ClusterName,
						ClusterVersion: meta.ClusterVersion,
						NCCVersion:     meta.NCCVersion,
					})
				}
				basePath := filepath.Join(cfg.OutputDirFiltered, fmt.Sprintf("%s.log", r.Cluster))
				htmlPath := basePath + ".html"
				csvPath := basePath + ".csv"
				clusterFiles = append(clusterFiles, struct{ Cluster, HTML, CSV string }{
					Cluster: r.Cluster,
					HTML:    filepath.Base(htmlPath),
					CSV:     filepath.Base(csvPath),
				})
				// Release per-cluster parsed blocks after aggregation to limit peak memory.
				r.Blocks = nil
			}
			sort.Slice(checksSnapshot.Clusters, func(i, j int) bool { return checksSnapshot.Clusters[i].Address < checksSnapshot.Clusters[j].Address })

			runDuration := time.Since(runStart)
			indexPath := filepath.Join(cfg.OutputDirFiltered, "index.html")
			log.Info().
				Int("clusters_ok", len(clusterFiles)).
				Int("clusters_failed", len(failed)).
				Float64("duration_s", runDuration.Seconds()).
				Str("index_html", indexPath).
				Msg("run summary")

			exitCode := 0
			if len(failed) > 0 {
				if len(clusterFiles) > 0 {
					exitCode = 3
				} else {
					exitCode = 1
				}
			}
			avgHealth := 0
			minHealth := 100
			healthClusters := 0
			for _, c := range perCluster {
				if !c.OK {
					continue
				}
				avgHealth += c.HealthScore
				healthClusters++
				if c.HealthScore < minHealth {
					minHealth = c.HealthScore
				}
			}
			if healthClusters > 0 {
				avgHealth = int(math.Round(float64(avgHealth) / float64(healthClusters)))
			} else {
				minHealth = 0
			}

			runSummary := RunSummaryJSON{
				Timestamp:      time.Now().UTC().Format(time.RFC3339),
				DurationS:      runDuration.Seconds(),
				ClustersOK:     len(clusterFiles),
				ClustersFailed: len(failed),
				FailedClusters: failed,
				Clusters:       perCluster,
				ExitCode:       exitCode,
				IndexHTML:      indexPath,
				TotalChecks:    len(agg),
				AvgHealthScore: avgHealth,
				MinHealthScore: minHealth,
				FailureClasses: failureCounts,
				Source:         strings.TrimSpace(os.Getenv("NCC_RUN_SOURCE")),
			}
			if err := writeRunSummaryJSON(fs, cfg.OutputDirFiltered, runSummary); err != nil {
				log.Error().Err(err).Msg("write run-summary.json failed (non-fatal)")
			}
			if err := writeChecksSnapshotJSON(fs, cfg.OutputDirFiltered, checksSnapshot); err != nil {
				log.Error().Err(err).Msg("write checks-snapshot.json failed (non-fatal)")
			}
			if err := writeNCCRunRecordJSON(fs, cfg.OutputDirFiltered, runSummary); err != nil {
				log.Error().Err(err).Msg("write ncc-run-record.json failed (non-fatal)")
			}
			regression := computeRegressionSummary(previousSummary, hasPreviousSummary, runSummary)
			if err := writeRegressionSummaryJSON(fs, cfg.OutputDirFiltered, regression); err != nil {
				log.Error().Err(err).Msg("write regression-summary.json failed (non-fatal)")
			} else {
				log.Info().
					Bool("has_regression", regression.HasRegression).
					Int("delta_fail_total", regression.DeltaFailTotal).
					Msg("regression summary generated")
			}
			drillDownDiff := computeDrillDownDiff(previousChecksSnapshot, hasPreviousChecksSnapshot, checksSnapshot)
			if err := writeDrillDownDiffJSON(fs, cfg.OutputDirFiltered, drillDownDiff); err != nil {
				log.Error().Err(err).Msg("write drilldown-diff.json failed (non-fatal)")
			}
			if cfg.PromEnabled {
				if err := writeNotificationMetricsFile(fs, cfg.PromDir); err != nil {
					log.Error().Err(err).Msg("write notifications.prom failed (non-fatal)")
				}
			}
			historySnapshots, err := loadRecentCheckSnapshots(cfg.RunHistoryDir, cfg.FlakyLookbackRuns-1)
			if err != nil {
				log.Warn().Err(err).Str("history_dir", cfg.RunHistoryDir).Msg("load check snapshot history failed (non-fatal)")
				historySnapshots = nil
			}
			historySnapshots = append(historySnapshots, checksSnapshot)
			flaky := computeFlakyChecks(historySnapshots, cfg.FlakyMinTransitions)
			if err := writeFlakyChecksJSON(fs, cfg.OutputDirFiltered, flaky); err != nil {
				log.Error().Err(err).Msg("write flaky-checks.json failed (non-fatal)")
			}
			slo := buildSLODashboard(runSummary)
			if err := writeSLODashboardJSON(fs, cfg.OutputDirFiltered, slo); err != nil {
				log.Error().Err(err).Msg("write slo-dashboard.json failed (non-fatal)")
			}
			// Write index.html after run-summary/artifacts are written, so embedded RUN_SUMMARY
			// and artifact links in the page always represent the current run.
			if len(agg) > 0 {
				if err := writeAggregatedHTMLSingle(fs, cfg.OutputDirFiltered, agg, clusterFiles, cfg); err != nil {
					log.Error().Err(err).Msg("write aggregated HTML failed")
					return fmt.Errorf("write aggregated HTML: %w", err)
				}
			} else if len(failed) > 0 {
				if err := writeAllClustersFailedHTML(fs, cfg.OutputDirFiltered, failed); err != nil {
					log.Error().Err(err).Msg("write all-failed HTML failed (non-fatal)")
				}
			}
			if len(cfg.ExcludeAlertTitles) > 0 {
				if err := writeExcludedAlertsAuditJSON(fs, cfg.OutputDirFiltered, cfg.ExcludeAlertMatchMode, cfg.ExcludeAlertTitles, excludedByCluster); err != nil {
					log.Error().Err(err).Msg("write excluded-alerts.json failed (non-fatal)")
				}
			}
			if cfg.SingleReport {
				src := filepath.Join(cfg.OutputDirFiltered, "index.html")
				dst := filepath.Join(cfg.OutputDirFiltered, "ncc-report-single.html")
				if err := copyFile(src, dst); err != nil {
					log.Error().Err(err).Str("src", src).Str("dst", dst).Msg("single-report copy failed (non-fatal)")
				} else {
					log.Info().Str("file", dst).Msg("single-file report generated")
				}
			}
			if cfg.RunHistoryEnabled {
				if runDir, err := writeRunHistorySnapshot(cfg); err != nil {
					log.Error().Err(err).Str("history_dir", cfg.RunHistoryDir).Msg("write run history snapshot failed (non-fatal)")
				} else {
					log.Info().
						Str("history_dir", runDir).
						Int("retain_last", cfg.RetainLastRuns).
						Int("retain_days", cfg.RetainDays).
						Msg("run history snapshot created")
				}
			}
			if deleted, err := applyArtifactRetentionPolicies(cfg.OutputDirFiltered, cfg.ArtifactRetainDays, cfg.ArtifactRetainMaxFiles, time.Now()); err != nil {
				log.Error().Err(err).Msg("artifact retention failed (non-fatal)")
			} else if deleted > 0 {
				log.Info().Int("deleted", deleted).Int("retain_days", cfg.ArtifactRetainDays).Int("retain_max_files", cfg.ArtifactRetainMaxFiles).Msg("artifact retention applied")
			}

			if cfg.NotifyDigest {
				if cfg.NotifyOnRegression && !regression.HasRegression {
					log.Info().Msg("notify-on-regression enabled: skipping digest notification (no regression detected)")
				} else if suppressed, reason := notificationsSuppressedNow(cfg, time.Now()); suppressed {
					log.Info().Str("reason", reason).Msg("digest notifications suppressed by quiet-hours/maintenance-window")
				} else {
					digestCounts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
					for _, row := range agg {
						sev := strings.ToUpper(strings.TrimSpace(row.Severity))
						if sev == "" {
							sev = "INFO"
						}
						if _, ok := digestCounts[sev]; ok {
							digestCounts[sev]++
						} else {
							digestCounts["INFO"]++
						}
					}
					overview := fmt.Sprintf("Run completed in %s. Clusters OK: %d, Failed: %d. Alerts (filtered): FAIL=%d WARN=%d ERR=%d INFO=%d Total=%d.",
						runDuration.Round(time.Second), len(clusterFiles), len(failed),
						digestCounts["FAIL"], digestCounts["WARN"], digestCounts["ERR"], digestCounts["INFO"], len(agg))
					if len(failed) > 0 {
						overview += fmt.Sprintf(" Failed: %v.", failed)
					}
					subj := fmt.Sprintf("NCC Run: OK=%d FAIL=%d", len(clusterFiles), len(failed))
					bodyEmail := overview + "\n\nIndex report: " + indexPath
					attachPath := ""
					if cfg.EmailAttachHTML {
						if _, err := os.Stat(indexPath); err == nil {
							attachPath = indexPath
						}
					}
					digestSummary := NotificationSummary{
						Cluster:     "run",
						StartedAt:   runStart,
						FinishedAt:  time.Now(),
						FailCount:   digestCounts["FAIL"],
						WarnCount:   digestCounts["WARN"],
						ErrCount:    digestCounts["ERR"],
						InfoCount:   digestCounts["INFO"],
						TotalChecks: len(agg),
						Overview:    overview,
					}
					subj, bodyEmail = applyEmailTemplates(cfg, subj, bodyEmail, digestSummary, log.Logger)
					if err := sendEmailWithRetry(cfg, subj, bodyEmail, attachPath); err != nil {
						log.Error().Err(err).Msg("digest email failed")
					}
					if cfg.WebhookIncludeHTML {
						if b, err := os.ReadFile(indexPath); err == nil {
							digestSummary.ReportHTMLBase64 = base64.StdEncoding.EncodeToString(b)
						}
					}
					if err := sendWebhookWithRetry(ctx, httpc, cfg, digestSummary); err != nil {
						log.Error().Err(err).Msg("digest webhook failed")
					}
					if err := sendSlackWithRetry(ctx, httpc, cfg, digestSummary); err != nil {
						log.Error().Err(err).Msg("digest slack failed")
					}
				}
			}

			// Check if context was cancelled during execution
			if ctx.Err() != nil {
				log.Warn().Err(ctx.Err()).Msg("operation cancelled during execution")
				if len(failed) > 0 {
					return fmt.Errorf("operation cancelled: %d clusters failed: %v", len(failed), failed)
				}
				return fmt.Errorf("operation cancelled: %w", ctx.Err())
			}
			policyMetrics := map[string]float64{
				"new-fails":            float64(drillDownDiff.NewFailCount),
				"resolved-fails":       float64(drillDownDiff.ResolvedFailCount),
				"fail-rate":            0,
				"clusters-failed":      float64(len(failed)),
				"max-cluster-failures": float64(len(failed)),
				"regressions":          0,
				"flaky-checks":         float64(flaky.TotalFlakyChecks),
				"min-health-score":     float64(runSummary.MinHealthScore),
				"avg-health-score":     float64(runSummary.AvgHealthScore),
				"timeout-clusters":     float64(failureCounts["timeout"]),
				"auth-failures":        float64(failureCounts["auth"]),
				"network-failures":     float64(failureCounts["network"]),
				"api-failures":         float64(failureCounts["api"]),
				"parser-failures":      float64(failureCounts["parser"]),
				"rate-limit-failures":  float64(failureCounts["rate_limit"]),
				"unknown-failures":     float64(failureCounts["unknown"]),
			}
			if regression.HasRegression {
				policyMetrics["regressions"] = 1
			}
			if runSummary.TotalChecks > 0 {
				policyMetrics["fail-rate"] = (float64(regression.CurrentFailTotal) * 100.0) / float64(runSummary.TotalChecks)
			}
			if len(cfg.PolicyGates) > 0 {
				violations, err := evaluatePolicyGates(cfg.PolicyGates, policyMetrics)
				if err != nil {
					return exitConfig(fmt.Errorf("policy-gates: %w", err))
				}
				if len(violations) > 0 {
					_ = fs.WriteFile(filepath.Join(cfg.OutputDirFiltered, "policy-gates.txt"), []byte(strings.Join(violations, "\n")+"\n"), 0644)
					return fmt.Errorf("policy gate violations: %s", strings.Join(violations, "; "))
				}
			}

			if len(failed) > 0 {
				printFailureResolutionHints(failureCounts)
				if len(clusterFiles) > 0 {
					log.Warn().Strs("failedClusters", failed).Int("succeeded", len(clusterFiles)).Msg("some clusters failed; aggregated report written for successful clusters")
					fmt.Fprintf(os.Stderr, "Some clusters failed: %v (report written for %d successful cluster(s)). Exit code 3.\n", failed, len(clusterFiles))
					return exitPartial(fmt.Errorf("some clusters failed: %v", failed))
				}
				log.Error().Strs("failedClusters", failed).Msg("all clusters failed")
				return fmt.Errorf("all clusters failed: %v", failed)
			}

			log.Info().Msg("all clusters processed successfully")
			fmt.Printf("All clusters processed successfully\n")
			return nil
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SuggestionsMinimumDistance = 2

	cmd.PersistentFlags().String("nutanix-v4-api-version", defaultNutanixV4APIVersion, "Nutanix v4 REST API path revision for clustermgmt and monitoring (e.g. v4.2, v4.1, v4.0.a1)")
	_ = viper.BindPFlag("nutanix-v4-api-version", cmd.PersistentFlags().Lookup("nutanix-v4-api-version"))
	cmd.PersistentFlags().String("config", "", "Config file path (yaml/json)")
	_ = viper.BindPFlag("config", cmd.PersistentFlags().Lookup("config"))
	cmd.PersistentFlags().Bool("insecure-skip-verify", false, "Skip TLS verify (only for trusted labs)")
	_ = viper.BindPFlag("insecure-skip-verify", cmd.PersistentFlags().Lookup("insecure-skip-verify"))
	cmd.PersistentFlags().String("ca-bundle", "", "Path to a PEM file of extra trusted CA certs (safer than --insecure-skip-verify)")
	_ = viper.BindPFlag("ca-bundle", cmd.PersistentFlags().Lookup("ca-bundle"))
	cmd.PersistentFlags().String("pin-sha256", "", "Comma-separated allowed server cert SHA-256 fingerprints (cert pinning; overrides system trust)")
	_ = viper.BindPFlag("pin-sha256", cmd.PersistentFlags().Lookup("pin-sha256"))
	cmd.PersistentFlags().String("request-timeout", "20s", "Per-request timeout")
	_ = viper.BindPFlag("request-timeout", cmd.PersistentFlags().Lookup("request-timeout"))

	// Deprecated root aliases (kept hidden for backward compatibility).
	cmd.Flags().BoolP("update", "u", false, "Fetch latest release from GitHub and update this binary if a matching asset exists")
	cmd.Flags().Bool("env-info", false, "Display possible environment variables and their current values")
	cmd.Flags().Bool("tc", false, "Display terms and conditions")
	cmd.Flags().BoolP("version", "v", false, "Display version/build metadata")
	_ = cmd.Flags().MarkDeprecated("update", "use `ncc-orchestrator update`")
	_ = cmd.Flags().MarkDeprecated("env-info", "use `ncc-orchestrator env-info`")
	_ = cmd.Flags().MarkDeprecated("tc", "use `ncc-orchestrator terms`")
	_ = cmd.Flags().MarkDeprecated("version", "use `ncc-orchestrator version`")
	_ = cmd.Flags().MarkHidden("update")
	_ = cmd.Flags().MarkHidden("env-info")
	_ = cmd.Flags().MarkHidden("tc")
	_ = cmd.Flags().MarkHidden("version")

	// flags
	cmd.Flags().Bool("skip-preflight-check", false, "Skip default preflight-check before run (not recommended)")
	cmd.Flags().Bool("auto", false, "Enable guided automation: apply safe self-healing fixes before failing")
	cmd.Flags().String("automation-level", "safe-fix", "Automation policy: advisory, safe-fix, full-auto")
	cmd.Flags().String("cluster-source-mode", defaultClusterSourceMode, "Cluster source mode: clusters (direct PE list) or pc (discover PEs from Prism Central targets)")
	cmd.Flags().String("clusters", "", "Comma-separated cluster IPs or FQDNs")
	cmd.Flags().String("clusters-file", "", "Path to cluster file (cluster or cluster,username[,password] per line; overrides clusters when set)")
	cmd.Flags().String("pcs", "", "Comma-separated Prism Central IPs/FQDNs/URLs (used when --cluster-source-mode=pc)")
	cmd.Flags().String("pcs-file", "", "Path to file with one Prism Central IP/FQDN/URL per line (used when --cluster-source-mode=pc)")
	cmd.Flags().String("prism-central-url", "", "Single Prism Central URL/IP/FQDN fallback target (used when --cluster-source-mode=pc and no pcs/pcs-file)")
	cmd.Flags().String("discover-api-version", defaultDiscoverAPIVersion, "Cluster discovery API for pc mode: v4 (GET clustermgmt) or v3 (legacy POST)")
	cmd.Flags().String("username", "admin", "Username for Prism Gateway")
	cmd.Flags().String("password", "", "Password (omit to be prompted)")
	cmd.Flags().String("ncc-api-version", "v4", "NCC API mode: v4 (default) or Legacy (Prism Gateway v1 start-checks only; v1 accepted as alias); use --nutanix-v4-api-version for v4.2 vs v4.0.a1 etc.")
	cmd.Flags().String("timeout", "15m", "Overall per-cluster timeout")
	cmd.Flags().String("poll-interval", "15s", "Polling interval for task status")
	cmd.Flags().String("poll-jitter", "2s", "Additive jitter to polling interval")
	cmd.Flags().Int("max-parallel", 4, "Max concurrent clusters")
	cmd.Flags().String("outputs", "html,csv", "Comma-separated outputs: html,csv,json,markdown,sarif for per-cluster files")
	cmd.Flags().String("output-dir-logs", "nccfiles", "Directory for raw logs")
	cmd.Flags().String("output-dir-filtered", "outputfiles", "Directory for filtered and aggregated results")
	cmd.Flags().Bool("single-report", false, "Also write a single-file report copy at output-dir-filtered/ncc-report-single.html")
	cmd.Flags().String("severity-filter", "", "Comma-separated severities to include (FAIL,WARN,ERR,INFO). Empty = all")
	cmd.Flags().String("exclude-alert-titles", "", "Comma-separated NCC alert titles to exclude from generated reports/HTML")
	cmd.Flags().String("exclude-alert-titles-file", "", "Path to file containing alert titles to exclude (one title per line)")
	cmd.Flags().String("exclude-alert-match-mode", defaultExcludeMatchMode, "Alert title exclusion match mode: exact, contains, regex")
	cmd.Flags().Bool("dry-run", false, "Validate configuration without running checks")
	cmd.Flags().String("log-file", "logs/ncc-runner.log", "Path to log file (rotated)")
	cmd.Flags().String("log-level", "", "Log level (trace/debug/info/warn/error or 0..5)")
	cmd.Flags().Bool("log-http", false, "Enable HTTP request/response dump logs")
	cmd.Flags().Int("retry-max-attempts", 6, "Max retry attempts for HTTP calls")
	cmd.Flags().String("retry-base-delay", "400ms", "Base retry delay (with jitter, exponential)")
	cmd.Flags().String("retry-max-delay", "8s", "Max retry delay cap")
	cmd.Flags().Int("retry-circuit-breaker", defaultRetryCircuitBreaker, "Fail fast after N consecutive retryable failures")
	cmd.Flags().Bool("replay", false, "Replay from existing logs without running NCC")
	cmd.Flags().Int("gen-test-agg", 0, "Generate a test index.html with N clusters for scalability testing (no API calls)")
	_ = cmd.Flags().MarkDeprecated("gen-test-agg", "use `ncc-orchestrator gen-test-agg --clusters <N>`")
	_ = cmd.Flags().MarkHidden("gen-test-agg")
	cmd.Flags().Bool("prom-enabled", true, "Enable writing Prometheus textfile metrics")
	cmd.Flags().String("prom-dir", "promfiles", "Directory for Prometheus metrics (used when prom-enabled=true)")
	cmd.Flags().Bool("run-history", false, "Store each run snapshot in run-history-dir")
	cmd.Flags().String("run-history-dir", "", "Run history directory (default: <output-dir-filtered>/runs)")
	cmd.Flags().Int("retain-last", 0, "When run-history is enabled, keep only the last N runs (0 = unlimited)")
	cmd.Flags().Int("retain-days", 0, "When run-history is enabled, keep runs newer than N days (0 = unlimited)")
	cmd.Flags().Int("artifact-retain-days", 0, "Remove generated artifacts older than N days from output-dir-filtered (0 = unlimited)")
	cmd.Flags().Int("artifact-retain-max-files", 0, "Keep only N newest generated artifacts in output-dir-filtered (0 = unlimited)")
	cmd.Flags().Bool("notify-on-regression", false, "Only send notifications when FAIL count increases vs previous run-summary")
	cmd.Flags().Bool("adaptive-parallelism", true, "Dynamically reduce/increase effective concurrency based on HTTP 429 responses")
	cmd.Flags().String("policy-gates", "", "Comma-separated policy gates (e.g. new-fails>0,fail-rate>2,min-health-score<90,flaky-checks>0,timeout-clusters>0,auth-failures>0)")
	cmd.Flags().String("quiet-hours", "", "Suppress notifications during local quiet hours, format HH:MM-HH:MM")
	cmd.Flags().String("maintenance-windows", "", "Suppress notifications during RFC3339 windows: start/end[,start/end...]")
	cmd.Flags().Int("flaky-lookback-runs", defaultFlakyLookbackRuns, "Number of recent runs to inspect for flaky check detection")
	cmd.Flags().Int("flaky-min-transitions", defaultFlakyTransitions, "Minimum severity transitions to mark a check as flaky")
	cmd.Flags().Bool("email-enabled", false, "Enable email notifications")
	cmd.Flags().Bool("email-attach-html", false, "Attach per-cluster (or digest) HTML report to notification email")
	cmd.Flags().Bool("notify-digest", false, "Send one email/webhook/slack per run with run overview (and optional index.html attach) instead of per-cluster")
	cmd.Flags().String("smtp-server", "", "SMTP server (smtp.gmail.com)")
	cmd.Flags().String("smtp-port", "587", "SMTP port (587=STARTTLS, 465=SSL)")
	cmd.Flags().String("smtp-user", "", "SMTP username")
	cmd.Flags().String("smtp-password", "", "SMTP password (use env NCC_SMTP_PASSWORD)")
	cmd.Flags().String("email-from", "", "From email address")
	cmd.Flags().String("email-to", "", "Comma-separated recipient emails")
	cmd.Flags().Bool("email-use-tls", true, "Use STARTTLS (recommended)")
	cmd.Flags().Bool("smtp-insecure-skip-verify", false, "Skip SMTP STARTTLS certificate verification (independent of --insecure-skip-verify)")
	cmd.Flags().Bool("webhook-enabled", false, "Enable webhook notifications")
	cmd.Flags().Bool("webhook-include-html", false, "Include per-cluster HTML report as base64 in webhook JSON payload")
	cmd.Flags().String("webhook-url", "", "Webhook endpoint URL")
	cmd.Flags().StringToString("webhook-headers", map[string]string{}, "Webhook headers (key=value)")
	cmd.Flags().String("notification-deadletter-dir", "", "Directory to persist notification payloads that fail to deliver after retries")
	cmd.Flags().Bool("slack-enabled", false, "Enable Slack notifications")
	cmd.Flags().String("slack-webhook-url", "", "Slack webhook URL")
	cmd.Flags().String("slack-channel", "", "Slack channel (optional, uses webhook default if empty)")
	cmd.Flags().String("secrets-provider", "", "Secret source for secret:// refs: env or file")
	cmd.Flags().String("secrets-file", "", "Path to YAML/JSON secrets map when secrets-provider=file")

	// viper bindings
	_ = viper.BindPFlag("skip-preflight-check", cmd.Flags().Lookup("skip-preflight-check"))
	_ = viper.BindPFlag("auto", cmd.Flags().Lookup("auto"))
	_ = viper.BindPFlag("automation-level", cmd.Flags().Lookup("automation-level"))
	_ = viper.BindPFlag("config", cmd.Flags().Lookup("config"))
	_ = viper.BindPFlag("cluster-source-mode", cmd.Flags().Lookup("cluster-source-mode"))
	_ = viper.BindPFlag("clusters", cmd.Flags().Lookup("clusters"))
	_ = viper.BindPFlag("clusters-file", cmd.Flags().Lookup("clusters-file"))
	_ = viper.BindPFlag("pcs", cmd.Flags().Lookup("pcs"))
	_ = viper.BindPFlag("pcs-file", cmd.Flags().Lookup("pcs-file"))
	_ = viper.BindPFlag("prism-central-url", cmd.Flags().Lookup("prism-central-url"))
	_ = viper.BindPFlag("discover-api-version", cmd.Flags().Lookup("discover-api-version"))
	_ = viper.BindPFlag("username", cmd.Flags().Lookup("username"))
	_ = viper.BindPFlag("password", cmd.Flags().Lookup("password"))
	_ = viper.BindPFlag("ncc-api-version", cmd.Flags().Lookup("ncc-api-version"))
	_ = viper.BindPFlag("insecure-skip-verify", cmd.Flags().Lookup("insecure-skip-verify"))
	_ = viper.BindPFlag("timeout", cmd.Flags().Lookup("timeout"))
	_ = viper.BindPFlag("request-timeout", cmd.Flags().Lookup("request-timeout"))
	_ = viper.BindPFlag("poll-interval", cmd.Flags().Lookup("poll-interval"))
	_ = viper.BindPFlag("poll-jitter", cmd.Flags().Lookup("poll-jitter"))
	_ = viper.BindPFlag("max-parallel", cmd.Flags().Lookup("max-parallel"))
	_ = viper.BindPFlag("outputs", cmd.Flags().Lookup("outputs"))
	_ = viper.BindPFlag("output-dir-logs", cmd.Flags().Lookup("output-dir-logs"))
	_ = viper.BindPFlag("output-dir-filtered", cmd.Flags().Lookup("output-dir-filtered"))
	_ = viper.BindPFlag("single-report", cmd.Flags().Lookup("single-report"))
	_ = viper.BindPFlag("log-file", cmd.Flags().Lookup("log-file"))
	_ = viper.BindPFlag("log-level", cmd.Flags().Lookup("log-level"))
	_ = viper.BindPFlag("log-http", cmd.Flags().Lookup("log-http"))
	_ = viper.BindPFlag("retry-max-attempts", cmd.Flags().Lookup("retry-max-attempts"))
	_ = viper.BindPFlag("retry-base-delay", cmd.Flags().Lookup("retry-base-delay"))
	_ = viper.BindPFlag("retry-max-delay", cmd.Flags().Lookup("retry-max-delay"))
	_ = viper.BindPFlag("retry-circuit-breaker", cmd.Flags().Lookup("retry-circuit-breaker"))
	_ = viper.BindPFlag("replay", cmd.Flags().Lookup("replay"))
	_ = viper.BindPFlag("prom-enabled", cmd.Flags().Lookup("prom-enabled"))
	_ = viper.BindPFlag("prom-dir", cmd.Flags().Lookup("prom-dir"))
	_ = viper.BindPFlag("run-history", cmd.Flags().Lookup("run-history"))
	_ = viper.BindPFlag("run-history-dir", cmd.Flags().Lookup("run-history-dir"))
	_ = viper.BindPFlag("retain-last", cmd.Flags().Lookup("retain-last"))
	_ = viper.BindPFlag("retain-days", cmd.Flags().Lookup("retain-days"))
	_ = viper.BindPFlag("artifact-retain-days", cmd.Flags().Lookup("artifact-retain-days"))
	_ = viper.BindPFlag("artifact-retain-max-files", cmd.Flags().Lookup("artifact-retain-max-files"))
	_ = viper.BindPFlag("notify-on-regression", cmd.Flags().Lookup("notify-on-regression"))
	_ = viper.BindPFlag("adaptive-parallelism", cmd.Flags().Lookup("adaptive-parallelism"))
	_ = viper.BindPFlag("policy-gates", cmd.Flags().Lookup("policy-gates"))
	_ = viper.BindPFlag("quiet-hours", cmd.Flags().Lookup("quiet-hours"))
	_ = viper.BindPFlag("maintenance-windows", cmd.Flags().Lookup("maintenance-windows"))
	_ = viper.BindPFlag("flaky-lookback-runs", cmd.Flags().Lookup("flaky-lookback-runs"))
	_ = viper.BindPFlag("flaky-min-transitions", cmd.Flags().Lookup("flaky-min-transitions"))
	_ = viper.BindPFlag("email-enabled", cmd.Flags().Lookup("email-enabled"))
	_ = viper.BindPFlag("email-attach-html", cmd.Flags().Lookup("email-attach-html"))
	_ = viper.BindPFlag("notify-digest", cmd.Flags().Lookup("notify-digest"))
	_ = viper.BindPFlag("smtp-server", cmd.Flags().Lookup("smtp-server"))
	_ = viper.BindPFlag("smtp-port", cmd.Flags().Lookup("smtp-port"))
	_ = viper.BindPFlag("smtp-user", cmd.Flags().Lookup("smtp-user"))
	_ = viper.BindPFlag("smtp-password", cmd.Flags().Lookup("smtp-password"))
	_ = viper.BindPFlag("email-from", cmd.Flags().Lookup("email-from"))
	_ = viper.BindPFlag("email-to", cmd.Flags().Lookup("email-to"))
	_ = viper.BindPFlag("email-use-tls", cmd.Flags().Lookup("email-use-tls"))
	_ = viper.BindPFlag("smtp-insecure-skip-verify", cmd.Flags().Lookup("smtp-insecure-skip-verify"))
	_ = viper.BindPFlag("webhook-enabled", cmd.Flags().Lookup("webhook-enabled"))
	_ = viper.BindPFlag("webhook-include-html", cmd.Flags().Lookup("webhook-include-html"))
	_ = viper.BindPFlag("webhook-url", cmd.Flags().Lookup("webhook-url"))
	_ = viper.BindPFlag("webhook-headers", cmd.Flags().Lookup("webhook-headers"))
	_ = viper.BindPFlag("notification-deadletter-dir", cmd.Flags().Lookup("notification-deadletter-dir"))
	_ = viper.BindPFlag("severity-filter", cmd.Flags().Lookup("severity-filter"))
	_ = viper.BindPFlag("exclude-alert-titles", cmd.Flags().Lookup("exclude-alert-titles"))
	_ = viper.BindPFlag("exclude-alert-titles-file", cmd.Flags().Lookup("exclude-alert-titles-file"))
	_ = viper.BindPFlag("exclude-alert-match-mode", cmd.Flags().Lookup("exclude-alert-match-mode"))
	_ = viper.BindPFlag("dry-run", cmd.Flags().Lookup("dry-run"))
	_ = viper.BindPFlag("slack-enabled", cmd.Flags().Lookup("slack-enabled"))
	_ = viper.BindPFlag("slack-webhook-url", cmd.Flags().Lookup("slack-webhook-url"))
	_ = viper.BindPFlag("slack-channel", cmd.Flags().Lookup("slack-channel"))
	_ = viper.BindPFlag("secrets-provider", cmd.Flags().Lookup("secrets-provider"))
	_ = viper.BindPFlag("secrets-file", cmd.Flags().Lookup("secrets-file"))

	termsCmd := &cobra.Command{
		Use:   "terms",
		Short: "Display terms and conditions",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Print(termsText)
			return nil
		},
	}
	cmd.AddCommand(termsCmd)

	envInfoCmd := &cobra.Command{
		Use:   "env-info",
		Short: "Display supported NCC environment variables and current values",
		RunE: func(cmd *cobra.Command, args []string) error {
			printEnvInfo()
			return nil
		},
	}
	cmd.AddCommand(envInfoCmd)

	updateCmd := &cobra.Command{
		Use:   "update",
		Short: "Check or update binary (track-aware, source configurable)",
		Long: `Checks or updates ncc-orchestrator binaries.

Default behavior follows the current major track (v1.x stays on latest v1.x).
To cross major versions (for example v1 -> v2), pass --allow-major-upgrade.

Use --check to only validate availability/version without downloading.
Use --binary-url to work with non-GitHub/custom binary repositories.
When using --binary-url for install, --binary-sha256 is required.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			checkOnly, _ := cmd.Flags().GetBool("check")
			allowMajorUpgrade, _ := cmd.Flags().GetBool("allow-major-upgrade")
			repo, _ := cmd.Flags().GetString("repo")
			binaryURL, _ := cmd.Flags().GetString("binary-url")
			binarySHA256, _ := cmd.Flags().GetString("binary-sha256")
			targetVersion, _ := cmd.Flags().GetString("target-version")
			skipChecksumVerify, _ := cmd.Flags().GetBool("skip-checksum-verify")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runUpdate(updateOptions{
				CheckOnly:          checkOnly,
				AllowMajorUpgrade:  allowMajorUpgrade,
				Repo:               repo,
				BinaryURL:          binaryURL,
				BinarySHA256:       binarySHA256,
				TargetVersion:      targetVersion,
				SkipChecksumVerify: skipChecksumVerify,
				JSONOut:            jsonOut,
			})
		},
	}
	updateCmd.Flags().Bool("check", false, "Check update availability without downloading or replacing")
	updateCmd.Flags().Bool("json", false, "With --check, also emit a machine-readable result line (NCC_UPDATE_JSON {...}) on stdout")
	updateCmd.Flags().Bool("allow-major-upgrade", false, "Allow major upgrades (for example v1.x to v2.x)")
	updateCmd.Flags().String("repo", defaultGitHubRepo, "GitHub repo in owner/repo or GitHub URL format")
	updateCmd.Flags().String("binary-url", "", "Direct binary URL for non-GitHub/custom repositories")
	updateCmd.Flags().String("binary-sha256", "", "Expected SHA256 for --binary-url (required for install)")
	updateCmd.Flags().String("target-version", "", "Target version hint (recommended with --binary-url)")
	updateCmd.Flags().Bool("skip-checksum-verify", false, "Skip SHA-256 verification of the downloaded asset against the release checksums.txt (not recommended; for air-gapped/mirrored installs)")
	cmd.AddCommand(updateCmd)

	v2BootstrapCmd := &cobra.Command{
		Use:   "v2-bootstrap",
		Short: "Download and prepare v2 API/UI/frontend binaries",
		Long: `Automates v2 stack setup using release artifacts (binary-first workflow).

Preferred artifact:
- ncc-v2-stack-<os>-<arch>.zip|tar.gz (single bundle)

Fallback (legacy layout):
- ncc-api-server binary (for current OS/arch)
- ncc-ui-server binary (for current OS/arch)
- frontend bundle archive

Then it writes startup scripts under --install-dir.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			version, _ := cmd.Flags().GetString("version")
			installDir, _ := cmd.Flags().GetString("install-dir")
			configPath, _ := cmd.Flags().GetString("config-path")
			outputDir, _ := cmd.Flags().GetString("output-dir")
			logDir, _ := cmd.Flags().GetString("log-dir")
			orchestratorBin, _ := cmd.Flags().GetString("orchestrator-bin")
			apiListen, _ := cmd.Flags().GetString("api-listen")
			uiListen, _ := cmd.Flags().GetString("ui-listen")
			tokenFile, _ := cmd.Flags().GetString("token-file")
			checkOnly, _ := cmd.Flags().GetBool("check")
			skipChecksumVerify, _ := cmd.Flags().GetBool("skip-checksum-verify")
			return runV2Bootstrap(v2BootstrapOptions{
				Repo:               repo,
				Version:            version,
				InstallDir:         installDir,
				ConfigPath:         configPath,
				OutputDir:          outputDir,
				LogDir:             logDir,
				OrchestratorBin:    orchestratorBin,
				APIListen:          apiListen,
				UIListen:           uiListen,
				TokenFile:          tokenFile,
				CheckOnly:          checkOnly,
				SkipChecksumVerify: skipChecksumVerify,
			})
		},
	}
	v2BootstrapCmd.Flags().String("repo", defaultGitHubRepo, "GitHub repo in owner/repo or URL format")
	v2BootstrapCmd.Flags().String("version", "", "Release version (default: latest v2 stable)")
	v2BootstrapCmd.Flags().String("install-dir", ".ncc-v2", "Installation directory for downloaded binaries and scripts")
	v2BootstrapCmd.Flags().String("config-path", "config.yaml", "Config file path passed to API server")
	v2BootstrapCmd.Flags().String("output-dir", "outputfiles", "Output directory passed to API server")
	v2BootstrapCmd.Flags().String("log-dir", "nccfiles", "Log directory passed to API server")
	v2BootstrapCmd.Flags().String("orchestrator-bin", "./ncc-orchestrator", "Path to ncc-orchestrator binary used by API server")
	v2BootstrapCmd.Flags().String("api-listen", ":8081", "Listen address for API server")
	v2BootstrapCmd.Flags().String("ui-listen", ":8080", "Listen address for UI server")
	v2BootstrapCmd.Flags().String("token-file", ".ncc-api-token", "Token file path used by UI/API servers")
	v2BootstrapCmd.Flags().Bool("check", false, "Check required assets only; do not download")
	v2BootstrapCmd.Flags().Bool("skip-checksum-verify", false, "Skip SHA-256 verification of downloaded assets against the release checksums.txt (not recommended; for air-gapped/mirrored installs)")
	cmd.AddCommand(v2BootstrapCmd)

	v2StartCmd := &cobra.Command{
		Use:     "v2-start",
		Aliases: []string{"v2-supervise"},
		Short:   "Start v2 services (API + optional UI)",
		Long: `Starts ncc-api-server and, by default, ncc-ui-server from bootstrapped binaries.

Run "ncc-orchestrator v2-bootstrap" once before using this command.

Use --api-only to start only the backend API service (for custom UI integrations).

Use --detach to run services in the background.

Use --self-heal with --detach to auto-restart services on unexpected exits.

Use --supervise (or the "v2-supervise" alias) to run a single long-lived
foreground supervisor that owns and keeps the API/UI children alive (liveness +
health-probe restarts, exponential backoff, and cooldown-and-resume). Run it as
a Type=simple systemd service (Restart=always) so the OS keeps the supervisor
alive across reboots and the supervisor keeps the stack alive across crashes.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			configPath, _ := cmd.Flags().GetString("config-path")
			outputDir, _ := cmd.Flags().GetString("output-dir")
			logDir, _ := cmd.Flags().GetString("log-dir")
			orchestratorBin, _ := cmd.Flags().GetString("orchestrator-bin")
			apiListen, _ := cmd.Flags().GetString("api-listen")
			uiListen, _ := cmd.Flags().GetString("ui-listen")
			apiAdvertiseURL, _ := cmd.Flags().GetString("api-advertise-url")
			uiAdvertiseURL, _ := cmd.Flags().GetString("ui-advertise-url")
			uiBackendURL, _ := cmd.Flags().GetString("ui-backend-url")
			apiCORSOrigins, _ := cmd.Flags().GetString("api-cors-origins")
			uiAllowedOrigins, _ := cmd.Flags().GetString("ui-allowed-origins")
			tokenFile, _ := cmd.Flags().GetString("token-file")
			usersDB, _ := cmd.Flags().GetString("users-db")
			apiAuthMode, _ := cmd.Flags().GetString("api-auth-mode")
			apiSessionTTL, _ := cmd.Flags().GetDuration("api-session-ttl")
			apiSessionSecret, _ := cmd.Flags().GetString("api-session-secret")
			apiSessionSecretFile, _ := cmd.Flags().GetString("api-session-secret-file")
			apiRunTimeout, _ := cmd.Flags().GetDuration("api-run-timeout")
			apiRateLimitPerMinute, _ := cmd.Flags().GetInt("api-rate-limit-per-minute")
			apiReadTimeout, _ := cmd.Flags().GetDuration("api-read-timeout")
			apiWriteTimeout, _ := cmd.Flags().GetDuration("api-write-timeout")
			apiIdleTimeout, _ := cmd.Flags().GetDuration("api-idle-timeout")
			apiTLSCertFile, _ := cmd.Flags().GetString("api-tls-cert-file")
			apiTLSKeyFile, _ := cmd.Flags().GetString("api-tls-key-file")
			apiTLSClientCAFile, _ := cmd.Flags().GetString("api-tls-client-ca-file")
			apiCookieInsecure, _ := cmd.Flags().GetBool("api-cookie-insecure")
			uiTLSCertFile, _ := cmd.Flags().GetString("ui-tls-cert-file")
			uiTLSKeyFile, _ := cmd.Flags().GetString("ui-tls-key-file")
			uiInsecureHTTP, _ := cmd.Flags().GetBool("ui-insecure-http")
			uiBackendCAFile, _ := cmd.Flags().GetString("ui-backend-ca-file")
			uiBackendClientCertFile, _ := cmd.Flags().GetString("ui-backend-client-cert-file")
			uiBackendClientKeyFile, _ := cmd.Flags().GetString("ui-backend-client-key-file")
			uiBackendInsecureSkipVerify, _ := cmd.Flags().GetBool("ui-backend-insecure-skip-verify")
			waitReady, _ := cmd.Flags().GetBool("wait-ready")
			readyTimeout, _ := cmd.Flags().GetDuration("ready-timeout")
			detach, _ := cmd.Flags().GetBool("detach")
			apiOnly, _ := cmd.Flags().GetBool("api-only")
			apiLogFile, _ := cmd.Flags().GetString("api-log-file")
			uiLogFile, _ := cmd.Flags().GetString("ui-log-file")
			apiPIDFile, _ := cmd.Flags().GetString("api-pid-file")
			uiPIDFile, _ := cmd.Flags().GetString("ui-pid-file")
			selfHeal, _ := cmd.Flags().GetBool("self-heal")
			selfHealMaxRestarts, _ := cmd.Flags().GetInt("self-heal-max-restarts")
			selfHealWindow, _ := cmd.Flags().GetDuration("self-heal-window")
			selfHealProbeInterval, _ := cmd.Flags().GetDuration("self-heal-probe-interval")
			selfHealUnhealthyThreshold, _ := cmd.Flags().GetInt("self-heal-unhealthy-threshold")
			supervise, _ := cmd.Flags().GetBool("supervise")
			// Invoking the command via its "v2-supervise" alias implies the
			// foreground supervisor without needing the flag.
			if cmd.CalledAs() == "v2-supervise" {
				supervise = true
			}
			if supervise && detach {
				fmt.Fprintln(os.Stderr, "note: --supervise runs in the foreground; ignoring --detach")
				detach = false
			}
			// Boot persistence: when run as the (systemd-managed) supervisor,
			// replay the persisted start settings for any flag the operator did
			// not explicitly pass. This keeps a reboot honoring settings that
			// were changed at runtime (e.g. TLS enabled from Settings → Access
			// rewrites .ncc-v2-start.json) without baking them into the unit.
			if supervise {
				effInstall := strings.TrimSpace(installDir)
				if effInstall == "" {
					effInstall = defaultV2InstallDir()
				}
				if st, ok := loadV2StartState(effInstall); ok {
					ovStr := func(name string, dst *string, src string) {
						if !cmd.Flags().Changed(name) && strings.TrimSpace(src) != "" {
							*dst = src
						}
					}
					ovDur := func(name string, dst *time.Duration, src time.Duration) {
						if !cmd.Flags().Changed(name) && src > 0 {
							*dst = src
						}
					}
					ovBool := func(name string, dst *bool, src bool) {
						if !cmd.Flags().Changed(name) {
							*dst = src
						}
					}
					ovStr("api-listen", &apiListen, st.APIListen)
					ovStr("ui-listen", &uiListen, st.UIListen)
					ovStr("api-advertise-url", &apiAdvertiseURL, st.APIAdvertiseURL)
					ovStr("ui-advertise-url", &uiAdvertiseURL, st.UIAdvertiseURL)
					ovStr("ui-backend-url", &uiBackendURL, st.UIBackendURL)
					ovStr("api-cors-origins", &apiCORSOrigins, st.APICORSOrigins)
					ovStr("ui-allowed-origins", &uiAllowedOrigins, st.UIAllowedOrigins)
					ovStr("api-auth-mode", &apiAuthMode, st.APIAuthMode)
					ovStr("api-session-secret", &apiSessionSecret, st.APISessionSecret)
					ovDur("api-session-ttl", &apiSessionTTL, st.APISessionTTL)
					ovDur("api-run-timeout", &apiRunTimeout, st.APIRunTimeout)
					ovDur("api-read-timeout", &apiReadTimeout, st.APIReadTimeout)
					ovDur("api-write-timeout", &apiWriteTimeout, st.APIWriteTimeout)
					ovDur("api-idle-timeout", &apiIdleTimeout, st.APIIdleTimeout)
					if !cmd.Flags().Changed("api-rate-limit-per-minute") {
						apiRateLimitPerMinute = st.APIRateLimitPerMinute
					}
					ovBool("ui-backend-insecure-skip-verify", &uiBackendInsecureSkipVerify, st.UIBackendInsecureSkipVerify)
					ovBool("api-cookie-insecure", &apiCookieInsecure, st.APICookieInsecure)
					ovStr("ui-tls-cert-file", &uiTLSCertFile, st.UITLSCertFile)
					ovStr("ui-tls-key-file", &uiTLSKeyFile, st.UITLSKeyFile)
					ovBool("ui-insecure-http", &uiInsecureHTTP, st.UIInsecureHTTP)
					ovBool("api-only", &apiOnly, st.APIOnly)
					if st.SelfHealMaxRestarts > 0 && !cmd.Flags().Changed("self-heal-max-restarts") {
						selfHealMaxRestarts = st.SelfHealMaxRestarts
					}
					ovDur("self-heal-window", &selfHealWindow, st.SelfHealWindow)
					ovDur("self-heal-probe-interval", &selfHealProbeInterval, st.SelfHealProbeInterval)
					if st.SelfHealUnhealthyThreshold > 0 && !cmd.Flags().Changed("self-heal-unhealthy-threshold") {
						selfHealUnhealthyThreshold = st.SelfHealUnhealthyThreshold
					}
				}
			}
			return runV2Start(v2StartOptions{
				InstallDir:                  installDir,
				ConfigPath:                  configPath,
				OutputDir:                   outputDir,
				LogDir:                      logDir,
				OrchestratorBin:             orchestratorBin,
				APIListen:                   apiListen,
				UIListen:                    uiListen,
				APIAdvertiseURL:             apiAdvertiseURL,
				UIAdvertiseURL:              uiAdvertiseURL,
				UIBackendURL:                uiBackendURL,
				APICORSOrigins:              apiCORSOrigins,
				UIAllowedOrigins:            uiAllowedOrigins,
				TokenFile:                   tokenFile,
				UsersDB:                     usersDB,
				APIAuthMode:                 apiAuthMode,
				APISessionTTL:               apiSessionTTL,
				APISessionSecret:            apiSessionSecret,
				APISessionSecretFile:        apiSessionSecretFile,
				APIRunTimeout:               apiRunTimeout,
				APIRateLimitPerMinute:       apiRateLimitPerMinute,
				APIReadTimeout:              apiReadTimeout,
				APIWriteTimeout:             apiWriteTimeout,
				APIIdleTimeout:              apiIdleTimeout,
				APITLSCertFile:              apiTLSCertFile,
				APITLSKeyFile:               apiTLSKeyFile,
				APITLSClientCAFile:          apiTLSClientCAFile,
				APICookieInsecure:           apiCookieInsecure,
				UITLSCertFile:               uiTLSCertFile,
				UITLSKeyFile:                uiTLSKeyFile,
				UIInsecureHTTP:              uiInsecureHTTP,
				UIBackendCAFile:             uiBackendCAFile,
				UIBackendClientCertFile:     uiBackendClientCertFile,
				UIBackendClientKeyFile:      uiBackendClientKeyFile,
				UIBackendInsecureSkipVerify: uiBackendInsecureSkipVerify,
				WaitReady:                   waitReady,
				ReadyTimeout:                readyTimeout,
				Detach:                      detach,
				APIOnly:                     apiOnly,
				APILogFile:                  apiLogFile,
				UILogFile:                   uiLogFile,
				APIPIDFile:                  apiPIDFile,
				UIPIDFile:                   uiPIDFile,
				SelfHeal:                    selfHeal,
				SelfHealMaxRestarts:         selfHealMaxRestarts,
				SelfHealWindow:              selfHealWindow,
				SelfHealProbeInterval:       selfHealProbeInterval,
				SelfHealUnhealthyThreshold:  selfHealUnhealthyThreshold,
				Supervise:                   supervise,
			})
		},
	}
	v2StartCmd.Flags().String("install-dir", "", "Installation directory used by v2-bootstrap (default: auto-detect from running binary's stack layout, fallback .ncc-v2)")
	v2StartCmd.Flags().String("config-path", "", "Config file path passed to API server (default <install-dir>/config.yaml; falls back to <install-dir>/example_config.yaml with a warning when missing)")
	v2StartCmd.Flags().String("output-dir", "", "Output directory passed to API server (default <install-dir>/outputfiles)")
	v2StartCmd.Flags().String("log-dir", "", "Log directory passed to API server (default <install-dir>/nccfiles)")
	v2StartCmd.Flags().String("orchestrator-bin", "", "Path to ncc-orchestrator binary used by API server (default: running executable, then <install-dir>/bin/ncc-orchestrator)")
	v2StartCmd.Flags().String("api-listen", ":8081", "Listen address for API server")
	v2StartCmd.Flags().String("ui-listen", ":8080", "Listen address for UI server")
	v2StartCmd.Flags().String("api-advertise-url", "", "Optional externally reachable API URL printed in startup output")
	v2StartCmd.Flags().String("ui-advertise-url", "", "Optional externally reachable UI URL printed in startup output")
	v2StartCmd.Flags().String("ui-backend-url", "", "Backend URL for ncc-ui-server to proxy to (default derived from --api-listen)")
	v2StartCmd.Flags().String("api-cors-origins", "", "Comma-separated API CORS origins (default derived from UI origin + --ui-allowed-origins)")
	v2StartCmd.Flags().String("ui-allowed-origins", "", "Additional comma-separated UI origins to allow (e.g. http://192.168.1.50:8080); default localhost origin is always included")
	v2StartCmd.Flags().String("token-file", "", "Token file path used by UI/API servers (default <install-dir>/.ncc-api-token)")
	v2StartCmd.Flags().String("users-db", "", "Writable user-database path passed to ncc-api-server; persists local accounts, roles, and SSO config across restarts (default <install-dir>/.ncc-api-users.json)")
	v2StartCmd.Flags().String("api-auth-mode", "token", "API auth mode passed to ncc-api-server: token, session, hybrid")
	v2StartCmd.Flags().Duration("api-session-ttl", 6*time.Hour, "Session TTL passed to ncc-api-server")
	v2StartCmd.Flags().String("api-session-secret", "", "Session HMAC secret passed to ncc-api-server (use file flag in production)")
	v2StartCmd.Flags().String("api-session-secret-file", "", "Read API session secret from file and pass to ncc-api-server")
	v2StartCmd.Flags().Duration("api-run-timeout", 90*time.Minute, "Run timeout passed to ncc-api-server")
	v2StartCmd.Flags().Int("api-rate-limit-per-minute", 60, "Per-client API rate limit passed to ncc-api-server (0 disables)")
	v2StartCmd.Flags().Duration("api-read-timeout", 15*time.Second, "HTTP read timeout passed to ncc-api-server")
	v2StartCmd.Flags().Duration("api-write-timeout", 60*time.Second, "HTTP write timeout passed to ncc-api-server")
	v2StartCmd.Flags().Duration("api-idle-timeout", 60*time.Second, "HTTP idle timeout passed to ncc-api-server")
	v2StartCmd.Flags().String("api-tls-cert-file", "", "TLS cert file for ncc-api-server")
	v2StartCmd.Flags().String("api-tls-key-file", "", "TLS key file for ncc-api-server")
	v2StartCmd.Flags().String("api-tls-client-ca-file", "", "mTLS client CA bundle for ncc-api-server")
	v2StartCmd.Flags().Bool("api-cookie-insecure", false, "Drop the Secure attribute on session cookies so logins work when serving over plain HTTP from a non-localhost address (use TLS instead for anything exposed)")
	v2StartCmd.Flags().String("ui-tls-cert-file", "", "TLS cert file for ncc-ui-server (overrides the default self-signed certificate)")
	v2StartCmd.Flags().String("ui-tls-key-file", "", "TLS key file for ncc-ui-server (overrides the default self-signed certificate)")
	v2StartCmd.Flags().Bool("ui-insecure-http", false, "Serve the UI over plain HTTP instead of the default self-signed HTTPS (not recommended; only for trusted-loopback or TLS-terminating-proxy deployments)")
	v2StartCmd.Flags().String("ui-backend-ca-file", "", "Custom CA bundle for ncc-ui-server backend TLS")
	v2StartCmd.Flags().String("ui-backend-client-cert-file", "", "Client cert for ncc-ui-server backend mTLS")
	v2StartCmd.Flags().String("ui-backend-client-key-file", "", "Client key for ncc-ui-server backend mTLS")
	v2StartCmd.Flags().Bool("ui-backend-insecure-skip-verify", false, "Skip TLS verification for ncc-ui-server backend connection")
	v2StartCmd.Flags().Bool("wait-ready", false, "Wait for API health (and UI root when enabled) before returning")
	v2StartCmd.Flags().Duration("ready-timeout", 20*time.Second, "Timeout for wait-ready checks")
	v2StartCmd.Flags().Bool("api-only", false, "Start only ncc-api-server (skip ncc-ui-server/frontend); useful when building a custom frontend")
	v2StartCmd.Flags().Bool("detach", false, "Run started services in background; writes PID/log files under <install-dir>/run and <install-dir>/logs")
	v2StartCmd.Flags().String("api-log-file", "", "Detached mode API log file path (default <install-dir>/logs/v2-api.log)")
	v2StartCmd.Flags().String("ui-log-file", "", "Detached mode UI log file path (default <install-dir>/logs/v2-ui.log)")
	v2StartCmd.Flags().String("api-pid-file", "", "Detached mode API PID file path (default <install-dir>/run/v2-api.pid)")
	v2StartCmd.Flags().String("ui-pid-file", "", "Detached mode UI PID file path (default <install-dir>/run/v2-ui.pid)")
	v2StartCmd.Flags().Bool("self-heal", false, "Detached mode only: monitor and auto-restart API/UI if they exit unexpectedly (also restarts a hung-but-alive API via health probes, with exponential backoff and cooldown-and-resume after the restart budget is exhausted)")
	v2StartCmd.Flags().Int("self-heal-max-restarts", 3, "Maximum auto-restarts allowed within self-heal window before a cooldown")
	v2StartCmd.Flags().Duration("self-heal-window", 10*time.Minute, "Rolling window used for self-heal restart budget (also the cooldown duration after the budget is exhausted)")
	v2StartCmd.Flags().Duration("self-heal-probe-interval", 10*time.Second, "How often the self-heal supervisor health-probes a still-alive API server to detect hangs (api-server only; uses its built-in --health-check)")
	v2StartCmd.Flags().Int("self-heal-unhealthy-threshold", 3, "Consecutive failed health probes before the self-heal supervisor restarts a hung-but-alive API server")
	v2StartCmd.Flags().Bool("supervise", false, "Run a single long-lived foreground supervisor that owns and keeps the API/UI children alive (liveness + health-probe restarts, backoff, cooldown-and-resume). Run as a Type=simple systemd service for reboot persistence. Implied by the v2-supervise alias; reuses --self-heal-* tuning")
	cmd.AddCommand(v2StartCmd)

	v2InstallServiceCmd := &cobra.Command{
		Use:   "v2-install-service",
		Short: "Install the stack supervisor as a boot-persistent OS service",
		Long: `Registers "ncc-orchestrator v2-supervise" with the platform service manager so
the v2 stack starts automatically after an OS reboot and is kept alive by the
native supervisor:

  Linux   -> a Type=simple systemd service (Restart=always) under /etc/systemd/system
  Windows -> a Task Scheduler task triggered at system startup (run as SYSTEM)
  macOS   -> a launchd LaunchDaemon (RunAtLoad + KeepAlive) under /Library/LaunchDaemons

Run with --print-only to preview the unit/task without applying it. Typically
requires root/Administrator.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			serviceName, _ := cmd.Flags().GetString("service-name")
			orchestratorBin, _ := cmd.Flags().GetString("orchestrator-bin")
			now, _ := cmd.Flags().GetBool("now")
			printOnly, _ := cmd.Flags().GetBool("print-only")
			return runV2InstallService(installServiceOptions{
				InstallDir:      installDir,
				ServiceName:     serviceName,
				OrchestratorBin: orchestratorBin,
				Now:             now,
				PrintOnly:       printOnly,
			})
		},
	}
	v2InstallServiceCmd.Flags().String("install-dir", "", "Installation directory the supervisor manages (default: auto-detect, fallback .ncc-v2)")
	v2InstallServiceCmd.Flags().String("service-name", "ncc-orchestrator", "Service/task name to create")
	v2InstallServiceCmd.Flags().String("orchestrator-bin", "", "Path to the ncc-orchestrator binary the service runs (default: this executable)")
	v2InstallServiceCmd.Flags().Bool("now", true, "Also enable+start the service immediately (Linux: enable --now; Windows: schtasks /Run; macOS: launchctl load -w)")
	v2InstallServiceCmd.Flags().Bool("print-only", false, "Preview the unit/task and the commands without applying them")
	cmd.AddCommand(v2InstallServiceCmd)

	v2UninstallServiceCmd := &cobra.Command{
		Use:   "v2-uninstall-service",
		Short: "Remove the boot-persistent supervisor service installed by v2-install-service",
		RunE: func(cmd *cobra.Command, args []string) error {
			serviceName, _ := cmd.Flags().GetString("service-name")
			installDir, _ := cmd.Flags().GetString("install-dir")
			printOnly, _ := cmd.Flags().GetBool("print-only")
			return runV2UninstallService(installServiceOptions{
				InstallDir:  installDir,
				ServiceName: serviceName,
				PrintOnly:   printOnly,
			})
		},
	}
	v2UninstallServiceCmd.Flags().String("service-name", "ncc-orchestrator", "Service/task name to remove")
	v2UninstallServiceCmd.Flags().String("install-dir", "", "Installation directory (used to locate runner artifacts; default: auto-detect)")
	v2UninstallServiceCmd.Flags().Bool("print-only", false, "Preview the removal commands without applying them")
	cmd.AddCommand(v2UninstallServiceCmd)

	v2CheckCmd := &cobra.Command{
		Use:   "v2-check",
		Short: "Run lightweight self-check for v2 paths and ports",
		Long:  `Validates v2 runtime prerequisites (binaries, config, dirs, and listen port availability) before running v2-start.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			configPath, _ := cmd.Flags().GetString("config-path")
			outputDir, _ := cmd.Flags().GetString("output-dir")
			logDir, _ := cmd.Flags().GetString("log-dir")
			orchestratorBin, _ := cmd.Flags().GetString("orchestrator-bin")
			apiListen, _ := cmd.Flags().GetString("api-listen")
			uiListen, _ := cmd.Flags().GetString("ui-listen")
			tokenFile, _ := cmd.Flags().GetString("token-file")
			apiOnly, _ := cmd.Flags().GetBool("api-only")
			return runV2Check(v2StartOptions{
				InstallDir:      installDir,
				ConfigPath:      configPath,
				OutputDir:       outputDir,
				LogDir:          logDir,
				OrchestratorBin: orchestratorBin,
				APIListen:       apiListen,
				UIListen:        uiListen,
				TokenFile:       tokenFile,
				APIOnly:         apiOnly,
			})
		},
	}
	v2CheckCmd.Flags().String("install-dir", "", "Installation directory used by v2-bootstrap (default: auto-detect from running binary's stack layout, fallback .ncc-v2)")
	v2CheckCmd.Flags().String("config-path", "", "Config file path passed to API server (default <install-dir>/config.yaml; falls back to <install-dir>/example_config.yaml with a warning when missing)")
	v2CheckCmd.Flags().String("output-dir", "", "Output directory passed to API server (default <install-dir>/outputfiles)")
	v2CheckCmd.Flags().String("log-dir", "", "Log directory passed to API server (default <install-dir>/nccfiles)")
	v2CheckCmd.Flags().String("orchestrator-bin", "", "Path to ncc-orchestrator binary used by API server (default: running executable, then <install-dir>/bin/ncc-orchestrator)")
	v2CheckCmd.Flags().String("api-listen", ":8081", "Listen address for API server")
	v2CheckCmd.Flags().String("ui-listen", ":8080", "Listen address for UI server")
	v2CheckCmd.Flags().String("token-file", "", "Token file path used by UI/API servers (default <install-dir>/.ncc-api-token)")
	v2CheckCmd.Flags().Bool("api-only", false, "Validate only API prerequisites (skip UI/frontend checks)")
	cmd.AddCommand(v2CheckCmd)

	v2StopCmd := &cobra.Command{
		Use:   "v2-stop",
		Short: "Stop detached v2 API/UI services",
		Long: `Stops v2 services started with "v2-start --detach" using PID files under <install-dir>/run.

Use --force to send a hard kill signal.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			apiPIDFile, _ := cmd.Flags().GetString("api-pid-file")
			uiPIDFile, _ := cmd.Flags().GetString("ui-pid-file")
			force, _ := cmd.Flags().GetBool("force")
			stopTimeout, _ := cmd.Flags().GetDuration("stop-timeout")
			return runV2Stop(v2StopOptions{
				InstallDir:  installDir,
				APIPIDFile:  apiPIDFile,
				UIPIDFile:   uiPIDFile,
				Force:       force,
				StopTimeout: stopTimeout,
			})
		},
	}
	v2StopCmd.Flags().String("install-dir", "", "Installation directory used by v2-start --detach (default: auto-detect from running binary's stack layout, fallback .ncc-v2)")
	v2StopCmd.Flags().String("api-pid-file", "", "Detached API PID file path override")
	v2StopCmd.Flags().String("ui-pid-file", "", "Detached UI PID file path override")
	v2StopCmd.Flags().Bool("force", false, "Force kill processes instead of graceful stop")
	v2StopCmd.Flags().Duration("stop-timeout", 5*time.Second, "Graceful stop timeout before force-kill (ignored when --force)")
	cmd.AddCommand(v2StopCmd)

	// status subcommand: report the live state of a v2 stack
	// (PIDs alive? api healthy? log paths?). Designed for support
	// triage and shell scripts — never modifies state, always
	// prints something useful even when no service is up.
	v2StatusCmd := &cobra.Command{
		Use:   "v2-status",
		Short: "Show live status of a running v2 stack (PIDs, listeners, health)",
		Long: `Reads <install-dir>/run/v2-{api,ui,api-supervisor,ui-supervisor}.pid,
checks each PID is still alive (signal 0), and probes
/api/v1/health on the API listener using the on-disk token.

Exits 0 even when services are down; the printed table (or --json
output) is the source of truth. Pair with awk/jq for scripted
liveness checks.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			apiListen, _ := cmd.Flags().GetString("api-listen")
			uiListen, _ := cmd.Flags().GetString("ui-listen")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runV2Status(v2StatusOptions{
				InstallDir: installDir,
				APIListen:  apiListen,
				UIListen:   uiListen,
				JSON:       jsonOut,
			})
		},
	}
	v2StatusCmd.Flags().String("install-dir", "", "Installation directory (default: auto-detect from running binary's stack layout, fallback .ncc-v2)")
	v2StatusCmd.Flags().String("api-listen", ":8081", "API listen address used to derive the health-probe URL (host:port; same value passed to v2-start)")
	v2StatusCmd.Flags().String("ui-listen", ":8080", "UI listen address used in the printed status row (host:port)")
	v2StatusCmd.Flags().Bool("json", false, "Emit JSON object {install_dir, services:[...]} instead of the text table")
	cmd.AddCommand(v2StatusCmd)

	// backup subcommand: capture the stateful parts of an install dir
	// (config + referenced files, user database, API token, scheduler /
	// notifications state) into one secrets-bearing tar.gz.
	v2BackupCmd := &cobra.Command{
		Use:   "v2-backup",
		Short: "Back up v2 config, local users/roles, tokens, and state into a tar.gz",
		Long: `Captures the stateful contents of a v2 install dir into a single
tar.gz so an install can be moved or recovered:

  - config.yaml and its referenced files (clusters-file,
    exclude-alert-titles-file, secrets-file)
  - the local user database (.ncc-api-users.json): accounts, bcrypt
    hashes, roles, runtime SAML config, and session policy
  - the API auth token (.ncc-api-token) and, if still present, the
    first-run admin password (.ncc-initial-admin-password)
  - scheduler and notifications state, plus any other .ncc-api-* state
    files at the install-dir root
  - the JSONL audit log (logs/ncc-audit.log) when present

The manifest records the exact ncc-orchestrator version (and stream /
build date) that created the backup, which 'v2-restore' reports and
checks. Regenerable artifacts (binaries, frontend bundle, run/ pid
files, output/ncc files) are excluded. The archive contains secrets, so
it is written with 0600 permissions — store it securely. Restore it with
'v2-restore'.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			outputFile, _ := cmd.Flags().GetString("output-file")
			outputDir, _ := cmd.Flags().GetString("output-dir")
			retain, _ := cmd.Flags().GetInt("retain")
			encrypt, _ := cmd.Flags().GetBool("encrypt")
			keyFile, _ := cmd.Flags().GetString("key-file")
			passphrase, _ := cmd.Flags().GetString("passphrase")
			// A supplied passphrase or key-file implies intent to encrypt.
			if strings.TrimSpace(keyFile) != "" || strings.TrimSpace(passphrase) != "" {
				encrypt = true
			}
			return runV2Backup(v2BackupOptions{
				InstallDir: installDir,
				OutputFile: outputFile,
				OutputDir:  outputDir,
				Retain:     retain,
				Encrypt:    encrypt,
				KeyFile:    keyFile,
				Passphrase: passphrase,
			})
		},
	}
	v2BackupCmd.Flags().String("install-dir", "", "Installation directory to back up (default: auto-detect, fallback .ncc-v2)")
	v2BackupCmd.Flags().String("output-file", "", "Output archive path (default ./ncc-backup-<UTC-timestamp>.tar.gz)")
	v2BackupCmd.Flags().String("output-dir", "", "Directory to write the default timestamped archive into (used when --output-file is empty; e.g. <install>/backups for scheduled backups)")
	v2BackupCmd.Flags().Int("retain", 0, "Keep at most N newest ncc-backup-*.tar.gz files in the output directory, pruning older ones (0 = keep all)")
	v2BackupCmd.Flags().Bool("encrypt", false, "Encrypt the archive at rest with AES-256-GCM (key from --passphrase/NCC_BACKUP_PASSPHRASE or --key-file/NCC_BACKUP_KEY_FILE/NCC_BACKUP_KEY)")
	v2BackupCmd.Flags().String("passphrase", "", "Passphrase to derive the encryption key (scrypt); prefer NCC_BACKUP_PASSPHRASE to keep it out of the process list")
	v2BackupCmd.Flags().String("key-file", "", "File holding a 32-byte encryption key (base64/hex); alternative to --passphrase")
	cmd.AddCommand(v2BackupCmd)

	// restore subcommand: extract a v2-backup archive back into an install
	// dir. Confined to the install dir; refuses to overwrite a running
	// stack or pre-existing files unless --force.
	v2RestoreCmd := &cobra.Command{
		Use:   "v2-restore",
		Short: "Restore a v2 backup archive (config, users/roles, tokens, state) into an install dir",
		Long: `Restores an archive produced by 'v2-backup' into an install dir,
recreating config.yaml + referenced files, the local user database,
the API token, and scheduler/notifications state.

Safety:
  - refuses to run while the stack appears to be up (live pid files)
    unless --force; stop it first with 'v2-stop'
  - refuses to overwrite existing files unless --force
  - extraction is confined to the install dir (unsafe archive paths
    are rejected)

The restore reports the ncc-orchestrator version that created the
backup and warns if this binary is older than that.

Portability: the restore is OS- and version-agnostic. Backups taken on
Windows (drive letters / backslash paths) restore cleanly onto Linux/macOS —
file-reference paths under the backup's original install dir are rebased to
this install dir and separators are normalized automatically. Restoring a
backup from a different ncc-orchestrator version is allowed (a newer-than-this
binary backup only prints a warning).

Restart is automatic: when the stack is currently running it is stopped and
re-started for you (v2-stop then v2-start --detach, performed by this binary)
so the restored config/accounts/token load with no manual step. Pass --restart
to force a restart/start even if the stack looks stopped, or --no-restart to
suppress it (e.g. staging a host for later). The input archive may be given
with --input-file or as the first positional argument.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			inputFile, _ := cmd.Flags().GetString("input-file")
			force, _ := cmd.Flags().GetBool("force")
			restart, _ := cmd.Flags().GetBool("restart")
			noRestart, _ := cmd.Flags().GetBool("no-restart")
			verifyOnly, _ := cmd.Flags().GetBool("verify-only")
			keyFile, _ := cmd.Flags().GetString("key-file")
			passphrase, _ := cmd.Flags().GetString("passphrase")
			if strings.TrimSpace(inputFile) == "" && len(args) > 0 {
				inputFile = args[0]
			}
			return runV2Restore(v2RestoreOptions{
				InstallDir: installDir,
				InputFile:  inputFile,
				Force:      force,
				Restart:    restart,
				NoRestart:  noRestart,
				VerifyOnly: verifyOnly,
				KeyFile:    keyFile,
				Passphrase: passphrase,
			})
		},
	}
	v2RestoreCmd.Flags().String("install-dir", "", "Installation directory to restore into (default: auto-detect, fallback .ncc-v2)")
	v2RestoreCmd.Flags().String("input-file", "", "Backup archive to restore (or pass as the first positional argument)")
	v2RestoreCmd.Flags().Bool("verify-only", false, "Validate the archive (gzip+tar integrity, manifest, confined paths) and report, without restoring")
	v2RestoreCmd.Flags().String("passphrase", "", "Passphrase to decrypt an encrypted backup (or NCC_BACKUP_PASSPHRASE)")
	v2RestoreCmd.Flags().String("key-file", "", "Key file to decrypt a key-encrypted backup (or NCC_BACKUP_KEY_FILE/NCC_BACKUP_KEY)")
	v2RestoreCmd.Flags().Bool("force", false, "Overwrite existing files and proceed even if the stack appears to be running")
	v2RestoreCmd.Flags().Bool("restart", false, "Force a stack restart/start after restore even if it appears stopped (default: auto-restart only when running)")
	v2RestoreCmd.Flags().Bool("no-restart", false, "Do not restart the stack after restore (overrides the default auto-restart when running)")
	cmd.AddCommand(v2RestoreCmd)

	v2RestartCmd := &cobra.Command{
		Use:   "v2-restart",
		Short: "Stop and re-start the v2 stack (v2-stop then v2-start --detach), performed by this binary",
		Long: `Restarts the v2 stack by stopping it and starting it again detached,
entirely from this orchestrator binary. Used internally by the UI/api-server to
apply a restored backup automatically, and available directly for operators.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			if strings.TrimSpace(installDir) == "" {
				installDir = defaultV2InstallDir()
			}
			if abs, err := filepath.Abs(installDir); err == nil {
				installDir = abs
			}
			return restartV2Stack(installDir)
		},
	}
	v2RestartCmd.Flags().String("install-dir", "", "Installation directory of the stack to restart (default: auto-detect, fallback .ncc-v2)")
	cmd.AddCommand(v2RestartCmd)

	// reset-password subcommand: offline recovery for a lost account password
	// (admin or any local user). Invokes ncc-api-server --reset-password against
	// the stack's user store, prints a new temporary password, and reminds the
	// operator to restart so it loads.
	v2ResetPasswordCmd := &cobra.Command{
		Use:   "v2-reset-password",
		Short: "Recover a lost local account password offline (admin or any user)",
		Long: `Resets a local account to a new random temporary password without
needing to log in — the recovery path when the admin (or any user)
password is lost.

It runs 'ncc-api-server --reset-password <user>' against the same user
store the stack uses (the <install-dir>/.ncc-api-users.json file, or a
Kubernetes Secret with --users-db-secret), prints a new temporary
password, and invalidates that account's existing sessions. The user is
forced to change the password at next login.

Because a running api-server caches accounts in memory, restart the
stack afterward ('v2-stop' then 'v2-start') for the new password to take
effect. Defaults to the 'admin' account.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			user, _ := cmd.Flags().GetString("user")
			usersDB, _ := cmd.Flags().GetString("users-db")
			usersDBSecret, _ := cmd.Flags().GetString("users-db-secret")
			usersDBSecretNS, _ := cmd.Flags().GetString("users-db-secret-namespace")
			return runV2ResetPassword(v2ResetPasswordOptions{
				InstallDir:   installDir,
				User:         user,
				UsersDB:      usersDB,
				UsersDBSec:   usersDBSecret,
				UsersDBSecNS: usersDBSecretNS,
			})
		},
	}
	v2ResetPasswordCmd.Flags().String("install-dir", "", "Installation directory whose user store to reset (default: auto-detect, fallback .ncc-v2)")
	v2ResetPasswordCmd.Flags().String("user", "admin", "Local account to reset")
	v2ResetPasswordCmd.Flags().String("users-db", "", "Override the user-database path (default <install-dir>/.ncc-api-users.json)")
	v2ResetPasswordCmd.Flags().String("users-db-secret", "", "Reset against a Kubernetes Secret store instead of a file (Secret name)")
	v2ResetPasswordCmd.Flags().String("users-db-secret-namespace", "", "Namespace of the Kubernetes Secret (defaults to the pod's own namespace)")
	cmd.AddCommand(v2ResetPasswordCmd)

	// doctor subcommand: single-command diagnostic for support
	// tickets. Runs verify + v2-check + v2-status, prints recent
	// log tails, and (by default) writes a redacted support tarball.
	v2DoctorCmd := &cobra.Command{
		Use:   "doctor",
		Short: "Run all diagnostics + write a redacted support bundle (verify + v2-check + v2-status + log tails)",
		Long: `doctor is the "something's broken, give me everything" diagnostic.
It runs every read-only check the orchestrator ships and produces a
single tar.gz support bundle that can be attached to a support ticket.

Sections of the printed report (mirrored into report.txt inside the
bundle):
  1. verify   — embedded buildinfo + self SHA-256
  2. v2-check — install-dir layout + path readability
  3. v2-status — running services with PID/health/log-path
  4. environment summary — GOOS/GOARCH, NCC_* env var NAMES (values redacted)
  5. recent log tails — last 200 lines of each v2-*.log

The default bundle path is ./ncc-support-<UTC-timestamp>.tar.gz; pass
--output-file to override or --no-bundle to skip the tarball entirely.

Secrets in <install-dir>/config.yaml are redacted before they enter
the bundle: any key whose name contains password / secret / token /
credential / api-key / client-id (case insensitive) has its value
replaced by ***REDACTED***.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			apiListen, _ := cmd.Flags().GetString("api-listen")
			uiListen, _ := cmd.Flags().GetString("ui-listen")
			outputFile, _ := cmd.Flags().GetString("output-file")
			noBundle, _ := cmd.Flags().GetBool("no-bundle")
			configPath, _ := cmd.Flags().GetString("config")
			fix, _ := cmd.Flags().GetBool("fix")
			jsonOut, _ := cmd.Flags().GetBool("json")
			onlyChecks, _ := cmd.Flags().GetString("only-checks")
			noDisruptive, _ := cmd.Flags().GetBool("no-disruptive")
			return runV2Doctor(v2DoctorOptions{
				InstallDir:   installDir,
				APIListen:    apiListen,
				UIListen:     uiListen,
				OutputFile:   outputFile,
				NoBundle:     noBundle,
				ConfigPath:   configPath,
				Fix:          fix,
				JSON:         jsonOut,
				OnlyChecks:   onlyChecks,
				NoDisruptive: noDisruptive,
			})
		},
	}
	v2DoctorCmd.Flags().String("install-dir", "", "Installation directory (default: auto-detect, fallback .ncc-v2)")
	v2DoctorCmd.Flags().String("api-listen", ":8081", "API listen address used to derive the health-probe URL")
	v2DoctorCmd.Flags().String("ui-listen", ":8080", "UI listen address used in the printed status row")
	v2DoctorCmd.Flags().String("output-file", "", "Bundle path (default ./ncc-support-<UTC-timestamp>.tar.gz)")
	v2DoctorCmd.Flags().Bool("no-bundle", false, "Print the report only; do not write a tarball")
	v2DoctorCmd.Flags().String("config", "", "Config file for self-heal checks (default <install-dir>/config.yaml)")
	v2DoctorCmd.Flags().Bool("fix", false, "Apply safe self-heal remediations (create missing output dirs, anchor relative paths, tighten secret-file perms, repair config)")
	v2DoctorCmd.Flags().Bool("json", false, "Emit the self-heal report as JSON (skips the human report and support bundle)")
	v2DoctorCmd.Flags().String("only-checks", "", "Comma-separated self-heal check IDs to run (e.g. stale-pids,runtime-mode-drift)")
	v2DoctorCmd.Flags().Bool("no-disruptive", false, "Skip disruptive checks/fixes (e.g. service restarts)")
	cmd.AddCommand(v2DoctorCmd)

	genTestAggCmd := &cobra.Command{
		Use:   "gen-test-agg",
		Short: "Generate synthetic aggregated index.html for load testing",
		RunE: func(cmd *cobra.Command, args []string) error {
			genN, _ := cmd.Flags().GetInt("clusters")
			if genN <= 0 {
				return errors.New("--clusters must be greater than 0")
			}
			outDir, _ := cmd.Flags().GetString("output-dir")
			if strings.TrimSpace(outDir) == "" {
				outDir = "outputfiles"
			}
			if err := generateTestAgg(genN, outDir); err != nil {
				return fmt.Errorf("gen-test-agg: %w", err)
			}
			fmt.Printf("Generated test aggregated report: %d clusters, output in %s/index.html\n", genN, outDir)
			return nil
		},
	}
	genTestAggCmd.Flags().Int("clusters", 0, "Number of synthetic clusters to generate")
	genTestAggCmd.Flags().String("output-dir", "outputfiles", "Directory for generated index.html")
	cmd.AddCommand(genTestAggCmd)

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Display version information",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println(versionInfoString())
			return nil
		},
	}
	cmd.AddCommand(versionCmd)

	// verify subcommand: integrity / provenance check usable by anyone
	// who downloaded the binary without a code-signing certificate to
	// verify against. Prints:
	//   - injected ldflags (Version / BuildDate / Stream / GoVersion)
	//   - debug.ReadBuildInfo() output (commit, dirty, modules) — only
	//     the keys we care about, not the full module graph
	//   - SHA256 of the running executable — useful as the value an
	//     operator copy/pastes into checksums.txt comparison
	//   - a hint pointing at the published checksums.txt URL for the
	//     same release tag
	// Together this lets a user answer "is the file I'm running really
	// the one Nutanix published as v2.0.2?" using only the binary
	// itself and a hex string from the GitHub release page.
	verifyCmd := &cobra.Command{
		Use:   "verify",
		Short: "Print integrity + provenance metadata (self-hash, buildinfo, vendor)",
		Long: `verify prints information that can be used to confirm the binary
matches a known-good release without requiring an OS-level code signature:

  - The version, stream, and build date that were embedded at link time.
  - The git revision and dirty flag from Go's debug build info.
  - The SHA-256 of this executable on disk.
  - A reference URL where the matching SHA-256 is published.

Compare the printed SHA-256 against the value next to the same
ncc-orchestrator-<os>-<arch> filename in the release's checksums.txt
(or release-attestation.json) on GitHub. If both match, the binary
has not been tampered with in transit and originates from the build
that produced the release.

With --online, verify fetches the matching release's checksums.txt from
GitHub and reports MATCH / MISMATCH / NOT FOUND, exiting non-zero on a
mismatch (usable in CI / health checks). The checksum is fetched from the
same repo that serves the binary; on a build that embeds a release public
key (signed releases), --online also verifies an Ed25519 signature over
checksums.txt before trusting any hash. Add --require-signature to fail
when a valid signature is absent (e.g. an unsigned build or release).`,
		RunE: func(cmd *cobra.Command, args []string) error {
			online, _ := cmd.Flags().GetBool("online")
			releaseTag, _ := cmd.Flags().GetString("release")
			repo, _ := cmd.Flags().GetString("repo")
			requireSig, _ := cmd.Flags().GetBool("require-signature")
			return runVerifyCommand(os.Stdout, verifyOptions{Online: online, ReleaseTag: releaseTag, Repo: repo, RequireSignature: requireSig})
		},
	}
	verifyCmd.Flags().Bool("online", false, "Fetch the matching release's checksums.txt from GitHub and compare (exits non-zero on mismatch)")
	verifyCmd.Flags().String("release", "", "Release tag to verify against (default: this binary's stamped version)")
	verifyCmd.Flags().String("repo", "", "GitHub owner/repo to fetch the release from (default: the project repo)")
	verifyCmd.Flags().Bool("require-signature", false, "With --online, fail unless the Ed25519 signature over checksums.txt verifies against the embedded release key")
	cmd.AddCommand(verifyCmd)

	// release-keygen / release-sign: maintainer-side tooling to produce signed
	// releases. Hidden from the normal command list (operators never need them).
	releaseKeygenCmd := &cobra.Command{
		Use:    "release-keygen",
		Short:  "Generate an Ed25519 release signing key (maintainers)",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			out, _ := cmd.Flags().GetString("out")
			if strings.TrimSpace(out) == "" {
				out = "ncc-release-signing.key"
			}
			pub, err := generateReleaseSigningKey(out)
			if err != nil {
				return err
			}
			fmt.Printf("Wrote private key: %s (keep OFFLINE/secret)\n", out)
			fmt.Printf("Public key (embed via -ldflags \"-X main.releaseSigningPublicKeyB64=...\"):\n%s\n", pub)
			return nil
		},
	}
	releaseKeygenCmd.Flags().String("out", "ncc-release-signing.key", "Path to write the base64 Ed25519 private key")
	cmd.AddCommand(releaseKeygenCmd)

	releaseSignCmd := &cobra.Command{
		Use:    "release-sign",
		Short:  "Sign a file (e.g. checksums.txt) with the Ed25519 release key (maintainers)",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			key, _ := cmd.Flags().GetString("key")
			in, _ := cmd.Flags().GetString("in")
			out, _ := cmd.Flags().GetString("out")
			if strings.TrimSpace(out) == "" {
				out = in + ".sig"
			}
			if err := signReleaseFile(key, in, out); err != nil {
				return err
			}
			fmt.Printf("Wrote signature: %s\n", out)
			return nil
		},
	}
	releaseSignCmd.Flags().String("key", "", "Path to the base64 Ed25519 private key (from release-keygen)")
	releaseSignCmd.Flags().String("in", "checksums.txt", "File to sign")
	releaseSignCmd.Flags().String("out", "", "Signature output path (default <in>.sig)")
	_ = releaseSignCmd.MarkFlagRequired("key")
	cmd.AddCommand(releaseSignCmd)

	// completion subcommand: emit shell-completion scripts for bash,
	// zsh, fish, and powershell. Uses cobra's built-in generators so
	// the completions stay in lockstep with the actual subcommand /
	// flag set automatically — no hand-maintained completion files
	// to drift out of sync.
	completionCmd := &cobra.Command{
		Use:                   "completion [bash|zsh|fish|powershell]",
		Short:                 "Generate shell completion script",
		DisableFlagsInUseLine: true,
		ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
		Args:                  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		Long: `Output a shell completion script for the specified shell.

To install completion permanently, append the output to the
appropriate location for your shell:

Bash (Linux):
  ncc-orchestrator completion bash | sudo tee /etc/bash_completion.d/ncc-orchestrator

Bash (macOS, Homebrew bash-completion@2):
  ncc-orchestrator completion bash > $(brew --prefix)/etc/bash_completion.d/ncc-orchestrator

Zsh (with compinit enabled):
  ncc-orchestrator completion zsh > "${fpath[1]}/_ncc-orchestrator"
  # restart your shell, or:  compinit -i

Fish:
  ncc-orchestrator completion fish > ~/.config/fish/completions/ncc-orchestrator.fish

PowerShell (current session):
  ncc-orchestrator completion powershell | Out-String | Invoke-Expression

PowerShell (persistent):
  ncc-orchestrator completion powershell > $PROFILE.ncc-orchestrator.ps1
  # then dot-source it from $PROFILE`,
		RunE: func(cmd *cobra.Command, args []string) error {
			switch args[0] {
			case "bash":
				return cmd.Root().GenBashCompletionV2(os.Stdout, true)
			case "zsh":
				return cmd.Root().GenZshCompletion(os.Stdout)
			case "fish":
				return cmd.Root().GenFishCompletion(os.Stdout, true)
			case "powershell":
				return cmd.Root().GenPowerShellCompletionWithDesc(os.Stdout)
			}
			return fmt.Errorf("unsupported shell %q", args[0])
		},
	}
	cmd.AddCommand(completionCmd)

	// discover-clusters subcommand: Prism Central cluster list (default v4 clustermgmt API)
	discoverCmd := &cobra.Command{
		Use:   "discover-clusters",
		Short: "List clusters from Prism Central (v4 API by default)",
		Long: `Lists registered clusters from Prism Central. Default output is one address per line (IPv4 preferred).

Use --format table for NAME, EXT_ID, ADDRESS, API columns; --format json for machine-readable rows.

Default: GET /api/clustermgmt/{nutanix-v4-api-version}/config/clusters with pagination ($page, $limit); use global --nutanix-v4-api-version (default v4.2) to match your PC API (e.g. v4.1, v4.0.a1).
Use --discover-api-version v3 for legacy POST /api/nutanix/v3/clusters/list.

If v4 returns HTTP 404, the command falls back to v3 automatically.

Use --output to write to a file (e.g. for --clusters-file).`,
		RunE: runDiscoverClusters,
	}
	discoverCmd.Flags().String("prism-central-url", "", "Prism Central URL (e.g. https://10.0.0.1:9440)")
	discoverCmd.Flags().String("username", "admin", "Prism username")
	discoverCmd.Flags().String("password", "", "Prism password (or NCC_PASSWORD)")
	discoverCmd.Flags().String("output", "", "Write cluster list to file (one per line)")
	discoverCmd.Flags().String("format", "lines", "Output format: lines (address per line), table (NAME EXT_ID ADDRESS API), or json")
	discoverCmd.Flags().String("discover-api-version", "v4", "Cluster list API: v4 (GET clustermgmt) or v3 (legacy POST); v4 path uses --nutanix-v4-api-version")
	_ = viper.BindPFlag("prism-central-url", discoverCmd.Flags().Lookup("prism-central-url"))
	// Do not BindPFlag username, password, or insecure-skip-verify here — they share keys
	// with the root command; the second bind would override viper and break the main run.
	_ = viper.BindPFlag("discover-api-version", discoverCmd.Flags().Lookup("discover-api-version"))
	cmd.AddCommand(discoverCmd)

	createScheduleCmd := &cobra.Command{
		Use:   "create-schedule",
		Short: "Create periodic scheduler for ncc-orchestrator",
		Long: `Creates a periodic schedule to run ncc-orchestrator.

On Linux/macOS, it installs (or replaces) a crontab entry by default, or a
systemd timer with --type systemd (better on systemd hosts: explicit working
directory/env, per-run logging, free overlap protection, and Persistent=true to
replay a run missed while the box was off).
On Windows, it creates or updates a Scheduled Task.

Examples:
  ncc-orchestrator create-schedule --type cron --cron "15 */4 * * *" --config config.yaml --print-only
  ncc-orchestrator create-schedule --type cron --every 4h --config config.yaml
  ncc-orchestrator create-schedule --type systemd --every 4h --config /root/ncc-orchestrator/config.yaml --print-only=false
  ncc-orchestrator create-schedule --type windows --every 4h --config C:\ncc\config.yaml
  ncc-orchestrator create-schedule --type auto --action list
  ncc-orchestrator create-schedule --type auto --action remove --print-only=false`,
		RunE: runCreateSchedule,
	}
	createScheduleCmd.Flags().String("type", "auto", "Scheduler type: auto, cron, systemd, or windows")
	createScheduleCmd.Flags().String("action", "create", "Action: create, list, remove, or run-now")
	createScheduleCmd.Flags().String("task-name", "ncc-orchestrator", "Schedule/task name marker")
	createScheduleCmd.Flags().String("command", "", "Override full command used by scheduler (advanced)")
	createScheduleCmd.Flags().String("cron", "", "Cron expression (for --type cron). If empty, derived from --every")
	createScheduleCmd.Flags().Duration("every", 4*time.Hour, "Periodic interval used for cron derivation or Windows schedule (e.g. 30m, 4h, 24h)")
	createScheduleCmd.Flags().String("log-path", "logs/ncc-scheduler.log", "Log file path for cron redirect")
	createScheduleCmd.Flags().Bool("with-lock", true, "Use flock lock file for cron runs to prevent overlap")
	createScheduleCmd.Flags().Bool("print-only", true, "Preview action without applying changes (used by create/remove)")
	cmd.AddCommand(createScheduleCmd)

	quickstartCmd := &cobra.Command{
		Use:   "quickstart",
		Short: "Guided setup and automated preflight",
		Long: `Quickstart bootstraps a starter config when missing, runs preflight checks,
and can apply safe self-healing actions for common setup issues.`,
		RunE: runQuickstartCommand,
	}
	quickstartCmd.Flags().Bool("auto-fix", true, "Apply safe auto-fixes during quickstart")
	quickstartCmd.Flags().Bool("interactive", false, "Prompt for common configuration values and write them to config")
	quickstartCmd.Flags().String("setup-v2", "ask", "v2 component setup mode: ask, download, skip")
	quickstartCmd.Flags().String("install-dir", ".ncc-v2", "Installation directory used by v2-bootstrap")
	quickstartCmd.Flags().String("repo", defaultGitHubRepo, "GitHub repo for v2 component downloads")
	quickstartCmd.Flags().Bool("assume-yes", false, "Auto-confirm quickstart prompts")
	quickstartCmd.Flags().String("automation-level", "safe-fix", "Automation policy: advisory, safe-fix, full-auto")
	cmd.AddCommand(quickstartCmd)

	configSchemaCmd := &cobra.Command{
		Use:   "config-schema",
		Short: "Print JSON schema for config.yaml",
		RunE:  runConfigSchema,
	}
	configSchemaCmd.Flags().String("output", "", "Write schema JSON to file (default: stdout)")
	cmd.AddCommand(configSchemaCmd)

	validateConfigCmd := &cobra.Command{
		Use:   "validate-config",
		Short: "Validate configuration file and exit (use preflight-check for full guidance)",
		RunE:  runValidateConfigCommand,
	}
	cmd.AddCommand(validateConfigCmd)

	validateSecretsCmd := &cobra.Command{
		Use:   "validate-secrets",
		Short: "Validate secret:// references and secret source accessibility (use preflight-check for full guidance)",
		RunE:  runValidateSecretsCommand,
	}
	cmd.AddCommand(validateSecretsCmd)

	preflightCmd := &cobra.Command{
		Use:   "preflight-check",
		Short: "Run combined config/secrets/path preflight checks",
		RunE:  runPreflightCheckCommand,
	}
	preflightCmd.Flags().String("format", "json", "Output format (json)")
	cmd.AddCommand(preflightCmd)

	uninstallCmd := &cobra.Command{
		Use:   "uninstall",
		Short: "Uninstall NCC-created local runtime artifacts/state",
		Long: `Uninstall removes local artifacts/state generated by ncc-orchestrator.
Kubernetes uninstall is intentionally script-only (use scripts/uninstall-v2-clean.sh).

Examples:
  ncc-orchestrator uninstall --dry-run
  ncc-orchestrator uninstall --config config.yaml --force`,
		RunE: runUninstallCommand,
	}
	uninstallCmd.Flags().String("install-dir", "", "v2 install directory created by bootstrap/start (default: auto-detect from running binary's stack layout, fallback .ncc-v2)")
	uninstallCmd.Flags().String("task-name", "ncc-orchestrator", "Scheduler task marker name to remove")
	uninstallCmd.Flags().Bool("remove-local", true, "Remove local NCC artifacts/state (outputfiles, nccfiles, promfiles, logs, token/state files)")
	uninstallCmd.Flags().Bool("remove-schedule", true, "Remove scheduler entry created by create-schedule")
	uninstallCmd.Flags().Bool("remove-v2-runtime", true, "Stop detached v2 services and remove install-dir")
	uninstallCmd.Flags().Bool("force", false, "Force stop behavior and non-interactive uninstall where applicable")
	uninstallCmd.Flags().Bool("dry-run", false, "Print actions without deleting")
	cmd.AddCommand(uninstallCmd)

	return cmd
}

func main() {
	// Ensure logs are flushed on exit
	defer func() {
		// Give logger time to flush
		time.Sleep(100 * time.Millisecond)
	}()

	root := newRootCmd()
	if err := root.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", humanizeCLIError(root, os.Args[1:], err))
		code := 1
		var exitErr *exitCodeError
		if errors.As(err, &exitErr) {
			code = exitErr.code
		}
		os.Exit(code)
	}
	os.Exit(0)
}
