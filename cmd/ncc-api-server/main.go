package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	yaml "go.yaml.in/yaml/v3"
)

type apiServer struct {
	repoRoot              string
	configPath            string
	outputDir             string
	logDir                string
	runnerLogPath         string
	scheduleStatePath     string
	notificationStatePath string
	orchestratorBin       string
	authToken             string
	tokenFilePath         string
	corsOrigin            string
	authMode              string
	sessionSecret         string
	sessionTTL            time.Duration
	sessionIssuer         string
	runTimeout            time.Duration
	debugExpose           bool
	tlsCertFile           string
	tlsKeyFile            string
	tlsClientCAFile       string

	mu      sync.Mutex
	active  bool
	started time.Time
	lastErr string
	lastOut string
	lastCfg string
	lastCmd []string
	lastCwd string
	lastEnv map[string]string
	liveOut *tailBuffer
}

type envelope struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

type routeMeta struct {
	Path        string   `json:"path"`
	Methods     []string `json:"methods"`
	Description string   `json:"description,omitempty"`
	SampleBody  string   `json:"sample_body,omitempty"`
}

type configUpdateRequest struct {
	Content string `json:"content"`
}

type configRelatedFile struct {
	Key          string `json:"key"`
	Path         string `json:"path"`
	ResolvedPath string `json:"resolved_path"`
	Exists       bool   `json:"exists"`
	Size         int64  `json:"size,omitempty"`
}

type configRelatedFileUpdateRequest struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

type scheduleState struct {
	Type      string `json:"type"`
	Action    string `json:"action"`
	Cron      string `json:"cron,omitempty"`
	Every     string `json:"every,omitempty"`
	Config    string `json:"config"`
	TaskName  string `json:"task_name,omitempty"`
	PrintOnly bool   `json:"print_only"`
	UpdatedAt string `json:"updated_at"`
}

type scheduleUpdateRequest struct {
	Type      string `json:"type"`
	Action    string `json:"action"`
	Cron      string `json:"cron,omitempty"`
	Every     string `json:"every,omitempty"`
	Config    string `json:"config,omitempty"`
	TaskName  string `json:"task_name,omitempty"`
	PrintOnly *bool  `json:"print_only,omitempty"`
	Apply     bool   `json:"apply"`
}

type runTriggerRequest struct {
	ConfigPath string   `json:"config_path,omitempty"`
	Password   string   `json:"password,omitempty"`
	ExtraArgs  []string `json:"extra_args,omitempty"`
}

type artifactInfo struct {
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	ModTime string `json:"mod_time"`
}

type runInfo struct {
	ID       string `json:"id"`
	Path     string `json:"path"`
	ModTime  string `json:"mod_time"`
	HasIndex bool   `json:"has_index"`
}

type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func newTailBuffer(max int) *tailBuffer {
	return &tailBuffer{max: max}
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if t.max > 0 && len(t.buf) > t.max {
		t.buf = t.buf[len(t.buf)-t.max:]
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(t.buf)
}

func main() {
	var s apiServer
	var listen string

	flag.StringVar(&listen, "listen", ":8081", "HTTP listen address")
	flag.StringVar(&s.repoRoot, "repo-root", ".", "Repository root path")
	flag.StringVar(&s.configPath, "config-path", "config.yaml", "Application config path")
	flag.StringVar(&s.outputDir, "output-dir", "outputfiles", "Artifacts output directory")
	flag.StringVar(&s.logDir, "log-dir", "nccfiles", "Raw logs directory")
	flag.StringVar(&s.runnerLogPath, "runner-log-path", "logs/ncc-runner.log", "Runner log file path")
	flag.StringVar(&s.scheduleStatePath, "schedule-state-path", ".ncc-api-schedule.json", "Schedule state file path")
	flag.StringVar(&s.notificationStatePath, "notifications-state-path", ".ncc-api-notifications.json", "Notifications state file path")
	flag.StringVar(&s.orchestratorBin, "orchestrator-bin", "./ncc-orchestrator", "Path to ncc-orchestrator binary")
	flag.StringVar(&s.tokenFilePath, "token-file-path", ".ncc-api-token", "Token file path for UI proxy/frontend use")
	flag.StringVar(&s.corsOrigin, "cors-origin", "http://localhost:8080", "CORS allowed origin(s), comma-separated")
	flag.StringVar(&s.authMode, "auth-mode", "token", "Auth mode: token, session, hybrid")
	flag.StringVar(&s.sessionSecret, "session-secret", "", "Session token HMAC secret (required for session/hybrid unless generated)")
	flag.DurationVar(&s.sessionTTL, "session-ttl", 10*time.Minute, "Session token TTL")
	flag.StringVar(&s.sessionIssuer, "session-issuer", "ncc-api-server", "Session token issuer")
	flag.DurationVar(&s.runTimeout, "run-timeout", 90*time.Minute, "Max runtime for trigger-run command")
	flag.BoolVar(&s.debugExpose, "debug-expose", false, "Expose debug internals in APIs (off by default)")
	flag.StringVar(&s.tlsCertFile, "tls-cert-file", "", "TLS certificate file for direct HTTPS")
	flag.StringVar(&s.tlsKeyFile, "tls-key-file", "", "TLS key file for direct HTTPS")
	flag.StringVar(&s.tlsClientCAFile, "tls-client-ca-file", "", "Optional client CA file (for mTLS verification)")
	flag.Parse()

	s.authToken = strings.TrimSpace(os.Getenv("NCC_API_TOKEN"))
	if strings.Contains(s.corsOrigin, "*") {
		log.Fatal("wildcard cors-origin is not allowed in strict mode")
	}
	if s.authMode != "token" && s.authMode != "session" && s.authMode != "hybrid" {
		log.Fatal("auth-mode must be one of: token, session, hybrid")
	}
	if s.sessionTTL <= 0 || s.sessionTTL > 24*time.Hour {
		log.Fatal("session-ttl must be > 0 and <= 24h")
	}
	if err := s.ensureAuthToken(); err != nil {
		log.Fatal(err)
	}
	if (s.authMode == "session" || s.authMode == "hybrid") && strings.TrimSpace(s.sessionSecret) == "" {
		b := make([]byte, 32)
		if _, err := crand.Read(b); err != nil {
			log.Fatalf("generate session secret: %v", err)
		}
		s.sessionSecret = base64.RawURLEncoding.EncodeToString(b)
	}
	if err := s.validatePathConfig(); err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/health", s.handleHealth)
	mux.HandleFunc("/api/v1/auth/session", s.handleAuthSession)
	mux.HandleFunc("/api/v1/auth/rotate", s.handleAuthRotate)
	mux.HandleFunc("/api/v1/settings/config", s.handleConfig)
	mux.HandleFunc("/api/v1/settings/config-files", s.handleConfigFiles)
	mux.HandleFunc("/api/v1/settings/config-file", s.handleConfigFile)
	mux.HandleFunc("/api/v1/settings/notifications", s.handleNotifications)
	mux.HandleFunc("/api/v1/settings/notifications/test", s.handleNotificationsTest)
	mux.HandleFunc("/api/v1/schedule", s.handleSchedule)
	mux.HandleFunc("/api/v1/artifacts", s.handleArtifacts)
	mux.HandleFunc("/api/v1/artifacts/", s.handleArtifactByName)
	mux.HandleFunc("/api/v1/runs", s.handleRuns)
	mux.HandleFunc("/api/v1/runs/summary", s.handleRunSummary)
	mux.HandleFunc("/api/v1/runs/active", s.handleRunActive)
	mux.HandleFunc("/api/v1/runs/trigger", s.handleRunTrigger)
	mux.HandleFunc("/api/v1/report/data", s.handleReportData)
	mux.HandleFunc("/api/v1/report/trends", s.handleReportTrends)
	mux.HandleFunc("/api/v1/logs/runner", s.handleRunnerLogs)
	mux.HandleFunc("/api/v1/openapi.json", s.handleOpenAPI)
	mux.HandleFunc("/api/v1/meta/routes", s.handleMetaRoutes)

	handler := s.withCORS(s.withAuth(mux))
	srv := &http.Server{
		Addr:         listen,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	log.Printf("ncc-api-server listening on %s (auth_mode=%s, tls=%t)", listen, s.authMode, strings.TrimSpace(s.tlsCertFile) != "" && strings.TrimSpace(s.tlsKeyFile) != "")
	if strings.TrimSpace(s.tlsCertFile) != "" || strings.TrimSpace(s.tlsKeyFile) != "" {
		if strings.TrimSpace(s.tlsCertFile) == "" || strings.TrimSpace(s.tlsKeyFile) == "" {
			log.Fatal("both tls-cert-file and tls-key-file are required together")
		}
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if strings.TrimSpace(s.tlsClientCAFile) != "" {
			b, err := os.ReadFile(s.tlsClientCAFile)
			if err != nil {
				log.Fatalf("read tls-client-ca-file: %v", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(b) {
				log.Fatal("failed to parse tls-client-ca-file")
			}
			tlsCfg.ClientCAs = pool
			tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		}
		srv.TLSConfig = tlsCfg
		if err := srv.ListenAndServeTLS(s.tlsCertFile, s.tlsKeyFile); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func (s *apiServer) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions || r.URL.Path == "/api/v1/health" {
			next.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/api/v1/auth/session" {
			next.ServeHTTP(w, r)
			return
		}
		tokenOK := false
		if s.authMode == "token" || s.authMode == "hybrid" {
			token := strings.TrimSpace(r.Header.Get("X-API-Token"))
			tokenOK = secureCompare(token, s.authToken)
		}
		sessionOK := false
		if !tokenOK && (s.authMode == "session" || s.authMode == "hybrid") {
			authz := strings.TrimSpace(r.Header.Get("Authorization"))
			if strings.HasPrefix(strings.ToLower(authz), "bearer ") {
				bearer := strings.TrimSpace(authz[len("Bearer "):])
				sessionOK = s.verifySessionToken(bearer, cleanClientIP(r)) == nil
			}
		}
		if !tokenOK && !sessionOK {
			writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *apiServer) withCORS(next http.Handler) http.Handler {
	allowedOrigins := parseAllowedOrigins(s.corsOrigin)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin != "" {
			if _, ok := allowedOrigins[origin]; !ok {
				writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: "origin not allowed"})
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
		w.Header().Set("Cache-Control", "no-store")
		if r.TLS != nil {
			w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		}
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Token, Authorization")
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *apiServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	data := map[string]interface{}{
		"status":       "ok",
		"time":         time.Now().UTC().Format(time.RFC3339),
		"auth_mode":    s.authMode,
		"token_source": tokenSource(s.authToken, os.Getenv("NCC_API_TOKEN")),
	}
	if s.debugExpose {
		data["repo_root"] = s.absPath(s.repoRoot)
		data["config_path"] = s.absPath(s.configPath)
		data["output_dir"] = s.absPath(s.outputDir)
		data["schedule_state"] = s.absPath(s.scheduleStatePath)
		data["orchestrator_bin"] = s.absPath(s.orchestratorBin)
		data["orchestrator_cmd"] = strings.Join(s.orchestratorBaseCommand(), " ")
		data["token_file"] = s.absPath(s.tokenFilePath)
	}
	writeJSON(w, http.StatusOK, envelope{
		Success: true,
		Data:    data,
	})
}

func (s *apiServer) handleAuthSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if !isLoopbackRequest(r) {
		writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: "session bootstrap allowed only from loopback"})
		return
	}
	if !secureCompare(strings.TrimSpace(r.Header.Get("X-API-Token")), s.authToken) {
		writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "unauthorized"})
		return
	}
	token, exp, err := s.issueSessionToken(cleanClientIP(r))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"token":      token,
		"expires_at": exp.Format(time.RFC3339),
		"ttl_sec":    int(s.sessionTTL.Seconds()),
	}})
}

func (s *apiServer) handleAuthRotate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if !secureCompare(strings.TrimSpace(r.Header.Get("X-API-Token")), s.authToken) {
		writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "unauthorized"})
		return
	}
	b := make([]byte, 32)
	if _, err := crand.Read(b); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: fmt.Sprintf("generate token: %v", err)})
		return
	}
	s.authToken = base64.RawURLEncoding.EncodeToString(b)
	if err := s.ensureAuthToken(); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "token rotated"})
}

func (s *apiServer) handleConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		b, err := os.ReadFile(s.absPath(s.configPath))
		if err != nil {
			writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"path":    s.absPath(s.configPath),
			"content": string(b),
		}})
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		var req configUpdateRequest
		if err := decodeJSON(r.Body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if strings.TrimSpace(req.Content) == "" {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "content is required"})
			return
		}
		cfgPath, err := s.validateConfigPath(s.configPath)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if err := os.MkdirAll(filepath.Dir(cfgPath), 0o755); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		ext := filepath.Ext(cfgPath)
		base := strings.TrimSuffix(cfgPath, ext)
		if ext == "" {
			ext = ".yaml"
			base = cfgPath
		}
		// Keep temp file extension parseable by Viper during validate-config.
		tmpPath := base + ".tmp" + ext
		if err := os.WriteFile(tmpPath, []byte(req.Content), 0o600); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		defer os.Remove(tmpPath)
		cmd := s.makeOrchestratorCommand(context.TODO(), "validate-config", "--config", tmpPath)
		cmd.Dir = s.absPath(s.repoRoot)
		out, err := cmd.CombinedOutput()
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{
				Success: false,
				Error:   fmt.Sprintf("strict config validation failed: %v", err),
				Data:    map[string]string{"output": string(out)},
			})
			return
		}
		if err := os.Rename(tmpPath, cfgPath); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		s.audit(r, "settings.config.update", true, map[string]interface{}{"config_path": cfgPath})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "config updated", Data: map[string]interface{}{
			"path":     cfgPath,
			"validate": true,
		}})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

func parseScalarConfigValue(v interface{}) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case fmt.Stringer:
		return strings.TrimSpace(t.String())
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", v))
	}
}

func extractRelatedFilePathFromConfig(content string, key string) string {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return ""
	}
	var ym map[string]interface{}
	if err := yaml.Unmarshal([]byte(content), &ym); err == nil {
		val := parseScalarConfigValue(ym[key])
		if strings.EqualFold(val, "null") || val == "" {
			return ""
		}
		return val
	}
	if strings.HasPrefix(trimmed, "{") {
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(trimmed), &m); err == nil {
			return parseScalarConfigValue(m[key])
		}
	}
	pattern := fmt.Sprintf(`(?m)^\s*%s\s*:\s*(.+?)\s*$`, regexp.QuoteMeta(key))
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(content)
	if len(matches) < 2 {
		return ""
	}
	val := strings.TrimSpace(matches[1])
	val = strings.Trim(val, `"'`)
	if strings.EqualFold(val, "null") || val == "" {
		return ""
	}
	return val
}

func validateClusterAddressForConfigFile(cluster string) error {
	cluster = strings.TrimSpace(cluster)
	if cluster == "" {
		return errors.New("cluster address cannot be empty")
	}
	if len(cluster) > 255 {
		return errors.New("cluster address too long")
	}
	if net.ParseIP(cluster) != nil {
		return nil
	}
	if strings.Contains(cluster, "..") || strings.HasPrefix(cluster, ".") || strings.HasSuffix(cluster, ".") {
		return errors.New("invalid cluster hostname format")
	}
	for _, r := range cluster {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '_' {
			continue
		}
		return fmt.Errorf("invalid character %q in cluster address", r)
	}
	return nil
}

func validateClustersFileContent(content string) error {
	for i, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		r := csv.NewReader(strings.NewReader(line))
		r.TrimLeadingSpace = true
		r.FieldsPerRecord = -1
		rec, err := r.Read()
		if err != nil {
			return fmt.Errorf("line %d: invalid CSV format: %w", i+1, err)
		}
		for idx := range rec {
			rec[idx] = strings.TrimSpace(rec[idx])
		}
		switch len(rec) {
		case 1:
			if err := validateClusterAddressForConfigFile(rec[0]); err != nil {
				return fmt.Errorf("line %d: %w", i+1, err)
			}
		case 2, 3:
			if err := validateClusterAddressForConfigFile(rec[0]); err != nil {
				return fmt.Errorf("line %d: %w", i+1, err)
			}
			if rec[1] == "" {
				return fmt.Errorf("line %d: username is empty", i+1)
			}
		default:
			return fmt.Errorf("line %d: expected cluster[,username[,password]]", i+1)
		}
	}
	return nil
}

func validateExcludeAlertTitlesFileContent(content string, matchMode string) error {
	mode := strings.ToLower(strings.TrimSpace(matchMode))
	if mode == "" {
		mode = "exact"
	}
	if mode != "exact" && mode != "contains" && mode != "regex" {
		return fmt.Errorf("unsupported exclude-alert-match-mode: %s", matchMode)
	}
	if mode != "regex" {
		return nil
	}
	for i, line := range strings.Split(content, "\n") {
		title := strings.TrimSpace(line)
		if title == "" || strings.HasPrefix(title, "#") {
			continue
		}
		if _, err := regexp.Compile(title); err != nil {
			return fmt.Errorf("line %d: invalid regex %q: %w", i+1, title, err)
		}
	}
	return nil
}

func validateSecretsFileContent(content string) error {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return nil
	}
	var jsonMap map[string]string
	if json.Unmarshal([]byte(trimmed), &jsonMap) == nil {
		return nil
	}
	var yamlMap map[string]interface{}
	if err := yaml.Unmarshal([]byte(trimmed), &yamlMap); err != nil {
		return fmt.Errorf("secrets file must be valid YAML/JSON map: %w", err)
	}
	for k, v := range yamlMap {
		if _, ok := v.(string); !ok {
			return fmt.Errorf("secret %q must be a string value", k)
		}
	}
	return nil
}

func (s *apiServer) configScalarValue(key string, def string) string {
	cfgPath, err := s.validateConfigPath(s.configPath)
	if err != nil {
		return def
	}
	content, err := os.ReadFile(cfgPath)
	if err != nil {
		return def
	}
	val := strings.TrimSpace(extractRelatedFilePathFromConfig(string(content), key))
	if val == "" {
		return def
	}
	return val
}

func (s *apiServer) validateConfigRelatedFileContent(ref *configRelatedFile, content string) error {
	switch ref.Key {
	case "clusters-file":
		return validateClustersFileContent(content)
	case "exclude-alert-titles-file":
		return validateExcludeAlertTitlesFileContent(content, s.configScalarValue("exclude-alert-match-mode", "exact"))
	case "secrets-file":
		return validateSecretsFileContent(content)
	default:
		return nil
	}
}

func (s *apiServer) discoverConfigRelatedFiles() ([]configRelatedFile, error) {
	cfgPath, err := s.validateConfigPath(s.configPath)
	if err != nil {
		return nil, err
	}
	content, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, err
	}
	keys := []string{
		"clusters-file",
		"exclude-alert-titles-file",
		"secrets-file",
	}
	out := make([]configRelatedFile, 0, len(keys))
	for _, key := range keys {
		rawPath := extractRelatedFilePathFromConfig(string(content), key)
		if rawPath == "" {
			continue
		}
		resolved, err := s.normalizeAndConfinePath(rawPath)
		if err != nil {
			out = append(out, configRelatedFile{
				Key:          key,
				Path:         rawPath,
				ResolvedPath: "",
				Exists:       false,
			})
			continue
		}
		info, statErr := os.Stat(resolved)
		item := configRelatedFile{
			Key:          key,
			Path:         rawPath,
			ResolvedPath: resolved,
			Exists:       statErr == nil,
		}
		if statErr == nil {
			item.Size = info.Size()
		}
		out = append(out, item)
	}
	return out, nil
}

func (s *apiServer) relatedConfigFileByPath(rawPath string) (*configRelatedFile, error) {
	target := strings.TrimSpace(rawPath)
	if target == "" {
		return nil, errors.New("path is required")
	}
	items, err := s.discoverConfigRelatedFiles()
	if err != nil {
		return nil, err
	}
	for i := range items {
		item := items[i]
		if strings.EqualFold(item.Path, target) || (item.ResolvedPath != "" && strings.EqualFold(item.ResolvedPath, target)) {
			return &item, nil
		}
	}
	return nil, fmt.Errorf("path is not referenced by config: %s", target)
}

func (s *apiServer) handleConfigFiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	items, err := s.discoverConfigRelatedFiles()
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"items": items,
	}})
}

func (s *apiServer) handleConfigFile(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		pathArg := strings.TrimSpace(r.URL.Query().Get("path"))
		ref, err := s.relatedConfigFileByPath(pathArg)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if ref.ResolvedPath == "" {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "path cannot be resolved inside repo root"})
			return
		}
		content, err := os.ReadFile(ref.ResolvedPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
					"key":      ref.Key,
					"path":     ref.Path,
					"resolved": ref.ResolvedPath,
					"exists":   false,
					"content":  "",
				}})
				return
			}
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"key":      ref.Key,
			"path":     ref.Path,
			"resolved": ref.ResolvedPath,
			"exists":   true,
			"content":  string(content),
		}})
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		var req configRelatedFileUpdateRequest
		if err := decodeJSON(r.Body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		ref, err := s.relatedConfigFileByPath(req.Path)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if ref.ResolvedPath == "" {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "path cannot be resolved inside repo root"})
			return
		}
		if err := s.validateConfigRelatedFileContent(ref, req.Content); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: fmt.Sprintf("validation failed for %s: %v", ref.Key, err)})
			return
		}
		if err := os.MkdirAll(filepath.Dir(ref.ResolvedPath), 0o755); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		if err := os.WriteFile(ref.ResolvedPath, []byte(req.Content), 0o600); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		s.audit(r, "settings.config_file.update", true, map[string]interface{}{"key": ref.Key, "path": ref.Path, "resolved": ref.ResolvedPath})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "config file updated", Data: map[string]interface{}{
			"key":      ref.Key,
			"path":     ref.Path,
			"resolved": ref.ResolvedPath,
			"exists":   true,
		}})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

func (s *apiServer) handleSchedule(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		st, err := s.loadSchedule()
		if err != nil {
			writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: st})
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		var req scheduleUpdateRequest
		if err := decodeJSON(r.Body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		st := scheduleState{
			Type:      defaultIfEmpty(req.Type, "auto"),
			Action:    defaultIfEmpty(req.Action, "create"),
			Cron:      strings.TrimSpace(req.Cron),
			Every:     strings.TrimSpace(req.Every),
			Config:    defaultIfEmpty(req.Config, s.configPath),
			TaskName:  strings.TrimSpace(req.TaskName),
			PrintOnly: true,
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		}
		if req.PrintOnly != nil {
			st.PrintOnly = *req.PrintOnly
		}
		if err := validateScheduleInput(st); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if _, err := s.validateConfigPath(st.Config); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if err := s.saveSchedule(st); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		resp := map[string]interface{}{"schedule": st, "applied": false}
		if req.Apply {
			args := []string{"create-schedule", "--type", st.Type, "--action", st.Action, "--config", st.Config, "--print-only=" + fmt.Sprintf("%t", st.PrintOnly)}
			if st.Cron != "" {
				args = append(args, "--cron", st.Cron)
			}
			if st.Every != "" {
				args = append(args, "--every", st.Every)
			}
			if st.TaskName != "" {
				args = append(args, "--task-name", st.TaskName)
			}
			out, err := s.runOrchestrator(args, 60*time.Second)
			resp["applied"] = err == nil
			resp["command"] = append(s.orchestratorBaseCommand(), args...)
			resp["output"] = out
			if err != nil {
				resp["apply_error"] = err.Error()
			}
		}
		s.audit(r, "schedule.update", true, map[string]interface{}{"applied": req.Apply, "type": st.Type, "action": st.Action})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "schedule updated", Data: resp})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

func (s *apiServer) handleArtifacts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	dir := s.absPath(s.outputDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: err.Error()})
		return
	}
	out := make([]artifactInfo, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, artifactInfo{
			Name:    e.Name(),
			Size:    info.Size(),
			ModTime: info.ModTime().UTC().Format(time.RFC3339),
		})
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: out})
}

func (s *apiServer) handleArtifactByName(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/api/v1/artifacts/")
	if name == "" || strings.Contains(name, "/") || strings.Contains(name, "..") {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid artifact name"})
		return
	}
	path := filepath.Join(s.absPath(s.outputDir), name)
	b, err := os.ReadFile(path)
	if err != nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: err.Error()})
		return
	}
	if r.URL.Query().Get("download") == "1" {
		ct := mime.TypeByExtension(filepath.Ext(name))
		if ct == "" {
			ct = "application/octet-stream"
		}
		w.Header().Set("Content-Type", ct)
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name))
		_, _ = w.Write(b)
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"name":    name,
		"content": string(b),
	}})
}

func (s *apiServer) handleRuns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	runsDir := filepath.Join(s.absPath(s.outputDir), "runs")
	entries, err := os.ReadDir(runsDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			writeJSON(w, http.StatusOK, envelope{Success: true, Data: []runInfo{}})
			return
		}
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	out := make([]runInfo, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		runPath := filepath.Join(runsDir, e.Name())
		_, hasIndexErr := os.Stat(filepath.Join(runPath, "index.html"))
		out = append(out, runInfo{
			ID:       e.Name(),
			Path:     runPath,
			ModTime:  info.ModTime().UTC().Format(time.RFC3339),
			HasIndex: hasIndexErr == nil,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ModTime > out[j].ModTime })
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: out})
}

func (s *apiServer) handleRunSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	path := filepath.Join(s.absPath(s.outputDir), "run-summary.json")
	b, err := os.ReadFile(path)
	if err != nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: err.Error()})
		return
	}
	var raw interface{}
	if err := json.Unmarshal(b, &raw); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: raw})
}

func (s *apiServer) handleRunActive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data := map[string]interface{}{
		"active":      s.active,
		"started_at":  s.started.UTC().Format(time.RFC3339),
		"last_error":  s.lastErr,
		"last_output": s.lastOut,
		"live_output": s.currentLiveOutput(),
		"runner_log":  s.absPath(s.runnerLogPath),
		"output_dir":  s.absPath(s.outputDir),
		"config_path": defaultIfEmpty(s.lastCfg, s.absPath(s.configPath)),
	}
	if s.debugExpose {
		data["command"] = s.lastCmd
		data["work_dir"] = s.lastCwd
		data["env"] = s.lastEnv
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
}

func (s *apiServer) handleRunTrigger(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if err := requireJSONContentType(r); err != nil {
		writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
		return
	}
	s.mu.Lock()
	if s.active {
		s.mu.Unlock()
		writeJSON(w, http.StatusConflict, envelope{Success: false, Error: "a run is already active"})
		return
	}
	s.active = true
	s.started = time.Now().UTC()
	s.lastErr = ""
	s.lastOut = ""
	s.lastCmd = nil
	s.lastCwd = ""
	s.lastEnv = nil
	s.liveOut = nil
	s.mu.Unlock()

	var req runTriggerRequest
	if err := decodeJSON(r.Body, &req); err != nil && !errors.Is(err, io.EOF) {
		s.setRunDone(err, "")
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	cfgPath := s.configPath
	if strings.TrimSpace(req.ConfigPath) != "" {
		cfgPath = strings.TrimSpace(req.ConfigPath)
	}
	resolvedCfgPath, err := s.validateConfigPath(cfgPath)
	if err != nil {
		s.setRunDone(err, "")
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	if len(req.Password) > 256 {
		s.setRunDone(errors.New("password too long"), "")
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "password too long"})
		return
	}
	cleanExtraArgs, err := sanitizeExtraArgs(req.ExtraArgs)
	if err != nil {
		s.setRunDone(err, "")
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	if st, err := os.Stat(resolvedCfgPath); err != nil || st.IsDir() {
		s.setRunDone(fmt.Errorf("config not found or invalid: %s", cfgPath), "")
		writeJSON(w, http.StatusBadRequest, envelope{
			Success: false,
			Error:   fmt.Sprintf("config file not found: %s", cfgPath),
			Data: map[string]interface{}{
				"config_path": cfgPath,
				"resolved":    resolvedCfgPath,
			},
		})
		return
	}
	args := []string{"--config", resolvedCfgPath}
	args = append(args, cleanExtraArgs...)
	s.mu.Lock()
	s.lastCfg = resolvedCfgPath
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), s.runTimeout)
	cmd := s.makeOrchestratorCommand(ctx, args...)
	cmd.Dir = s.absPath(s.repoRoot)
	injectedEnv := map[string]string{}
	if strings.TrimSpace(req.Password) != "" {
		cmd.Env = append(os.Environ(), "NCC_PASSWORD="+req.Password)
		injectedEnv["NCC_PASSWORD"] = "***"
	}
	fullCmd := append(s.orchestratorBaseCommand(), redactedArgs(args)...)
	var runOut bytes.Buffer
	liveBuf := newTailBuffer(64000)
	mw := io.MultiWriter(&runOut, liveBuf)
	cmd.Stdout = mw
	cmd.Stderr = mw
	s.mu.Lock()
	s.lastCmd = fullCmd
	s.lastCwd = cmd.Dir
	s.lastEnv = injectedEnv
	s.liveOut = liveBuf
	s.mu.Unlock()
	if err := cmd.Start(); err != nil {
		cancel()
		s.setRunDone(err, "")
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	pid := cmd.Process.Pid
	go func() {
		defer cancel()
		err := cmd.Wait()
		if ctx.Err() == context.DeadlineExceeded {
			err = fmt.Errorf("run timed out after %s", s.runTimeout)
		}
		s.setRunDone(err, runOut.String())
		go s.notifyRunFinished(err)
	}()
	s.audit(r, "runs.trigger", true, map[string]interface{}{"config_path": resolvedCfgPath, "extra_args_count": len(cleanExtraArgs)})
	writeJSON(w, http.StatusAccepted, envelope{Success: true, Message: "run triggered", Data: map[string]interface{}{
		"pid":           pid,
		"command":       append(s.orchestratorBaseCommand(), redactedArgs(args)...),
		"started_at":    s.started.Format(time.RFC3339),
		"config_path":   resolvedCfgPath,
		"used_password": strings.TrimSpace(req.Password) != "",
	}})
}

func (s *apiServer) handleRunnerLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	logPath := s.absPath(s.runnerLogPath)
	content, err := readTailFile(logPath, 64000)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
				"path":    logPath,
				"content": "",
				"exists":  false,
			}})
			return
		}
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"path":    logPath,
		"content": strings.TrimSpace(content),
		"exists":  true,
	}})
}

func (s *apiServer) handleReportData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	outDir := s.selectBestReportOutDir()
	runSummary := readJSONArtifact(filepath.Join(outDir, "run-summary.json"), map[string]interface{}{})
	checksSnapshot := readJSONArtifact(filepath.Join(outDir, "checks-snapshot.json"), []interface{}{})
	drilldownDiff := readJSONArtifact(filepath.Join(outDir, "drilldown-diff.json"), map[string]interface{}{})
	flakyChecks := readJSONArtifact(filepath.Join(outDir, "flaky-checks.json"), map[string]interface{}{})
	regressionSummary := readJSONArtifact(filepath.Join(outDir, "regression-summary.json"), map[string]interface{}{})
	sloDashboard := readJSONArtifact(filepath.Join(outDir, "slo-dashboard.json"), map[string]interface{}{})
	policyViolations := []string{}
	if b, err := os.ReadFile(filepath.Join(outDir, "policy-gates.txt")); err == nil {
		for _, ln := range strings.Split(string(b), "\n") {
			ln = strings.TrimSpace(ln)
			if ln == "" {
				continue
			}
			policyViolations = append(policyViolations, ln)
		}
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"run_summary":        runSummary,
		"checks_snapshot":    checksSnapshot,
		"drilldown_diff":     drilldownDiff,
		"flaky_checks":       flakyChecks,
		"regression_summary": regressionSummary,
		"slo_dashboard":      sloDashboard,
		"policy_violations":  policyViolations,
		"agg_rows":           readInlineJSONVar(filepath.Join(outDir, "index.html"), "AGG", []interface{}{}),
		"diff_flags":         readInlineJSONVar(filepath.Join(outDir, "index.html"), "DIFF_FLAGS", map[string]interface{}{}),
		"flaky_keys":         readInlineJSONVar(filepath.Join(outDir, "index.html"), "FLAKY_KEYS", map[string]interface{}{}),
		"cluster_links":      readInlineJSONVar(filepath.Join(outDir, "index.html"), "CLUSTER_LINKS", []interface{}{}),
		"artifact_links":     readInlineJSONVar(filepath.Join(outDir, "index.html"), "ARTIFACT_LINKS", map[string]interface{}{}),
		"report_meta":        readInlineJSONVar(filepath.Join(outDir, "index.html"), "REPORT_META", map[string]interface{}{}),
		"ncc_logs":           listNCCLogs(s.absPath(s.logDir)),
		"trends":             collectTrendPoints(outDir, 30),
		"report_source_dir":  outDir,
	}})
}

type trendClusterSummary struct {
	FailCount int `json:"fail_count"`
	WarnCount int `json:"warn_count"`
	ErrCount  int `json:"err_count"`
	InfoCount int `json:"info_count"`
}

type trendRunSummary struct {
	Timestamp      string                `json:"timestamp"`
	DurationS      float64               `json:"duration_s"`
	ClustersOK     int                   `json:"clusters_ok"`
	ClustersFailed int                   `json:"clusters_failed"`
	TotalChecks    int                   `json:"total_checks"`
	AvgHealthScore int                   `json:"avg_health_score"`
	MinHealthScore int                   `json:"min_health_score"`
	Clusters       []trendClusterSummary `json:"clusters"`
}

type trendPoint struct {
	Timestamp      string  `json:"timestamp"`
	DurationS      float64 `json:"duration_s"`
	ClustersOK     int     `json:"clusters_ok"`
	ClustersFailed int     `json:"clusters_failed"`
	TotalChecks    int     `json:"total_checks"`
	AvgHealthScore int     `json:"avg_health_score"`
	MinHealthScore int     `json:"min_health_score"`
	FailTotal      int     `json:"fail_total"`
	WarnTotal      int     `json:"warn_total"`
	ErrTotal       int     `json:"err_total"`
	InfoTotal      int     `json:"info_total"`
}

func parseTrendLimit(raw string) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 30
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 30
	}
	if n > 365 {
		return 365
	}
	return n
}

func readTrendRunSummary(path string) (trendPoint, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return trendPoint{}, false
	}
	var s trendRunSummary
	if err := json.Unmarshal(b, &s); err != nil {
		return trendPoint{}, false
	}
	failTotal, warnTotal, errTotal, infoTotal := 0, 0, 0, 0
	for _, c := range s.Clusters {
		failTotal += c.FailCount
		warnTotal += c.WarnCount
		errTotal += c.ErrCount
		infoTotal += c.InfoCount
	}
	return trendPoint{
		Timestamp:      s.Timestamp,
		DurationS:      s.DurationS,
		ClustersOK:     s.ClustersOK,
		ClustersFailed: s.ClustersFailed,
		TotalChecks:    s.TotalChecks,
		AvgHealthScore: s.AvgHealthScore,
		MinHealthScore: s.MinHealthScore,
		FailTotal:      failTotal,
		WarnTotal:      warnTotal,
		ErrTotal:       errTotal,
		InfoTotal:      infoTotal,
	}, true
}

func collectTrendPoints(outDir string, limit int) []trendPoint {
	points := make([]trendPoint, 0)
	seenTs := map[string]bool{}
	addPoint := func(p trendPoint) {
		if strings.TrimSpace(p.Timestamp) == "" {
			return
		}
		if seenTs[p.Timestamp] {
			return
		}
		seenTs[p.Timestamp] = true
		points = append(points, p)
	}
	if p, ok := readTrendRunSummary(filepath.Join(outDir, "run-summary.json")); ok {
		addPoint(p)
	}
	runsDir := filepath.Join(outDir, "runs")
	entries, err := os.ReadDir(runsDir)
	if err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			if p, ok := readTrendRunSummary(filepath.Join(runsDir, e.Name(), "run-summary.json")); ok {
				addPoint(p)
			}
		}
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Timestamp < points[j].Timestamp })
	if limit > 0 && len(points) > limit {
		points = points[len(points)-limit:]
	}
	return points
}

func (s *apiServer) handleReportTrends(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	outDir := s.selectBestReportOutDir()
	limit := parseTrendLimit(r.URL.Query().Get("limit"))
	points := collectTrendPoints(outDir, limit)
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"points":            points,
		"count":             len(points),
		"limit":             limit,
		"report_source_dir": outDir,
	}})
}

func (s *apiServer) handleMetaRoutes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	routes := []routeMeta{
		{Path: "/api/v1/health", Methods: []string{http.MethodGet}, Description: "Backend health and resolved paths"},
		{Path: "/api/v1/auth/session", Methods: []string{http.MethodPost}, Description: "Issue short-lived session token"},
		{Path: "/api/v1/auth/rotate", Methods: []string{http.MethodPost}, Description: "Rotate API token"},
		{Path: "/api/v1/settings/config", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Read/write runtime config", SampleBody: "{\n  \"content\": \"clusters: \\\"10.0.0.1\\\"\\nusername: \\\"admin\\\"\\n\"\n}"},
		{Path: "/api/v1/settings/config-files", Methods: []string{http.MethodGet}, Description: "List config-referenced files (clusters, exclusions, secrets)"},
		{Path: "/api/v1/settings/config-file", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Read/write one config-referenced file", SampleBody: "{\n  \"path\": \"alerts-exclude.txt\",\n  \"content\": \"AHV_MemoryUsage\\n\"\n}"},
		{Path: "/api/v1/settings/notifications", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Read/write notifications state", SampleBody: "{\n  \"enabled\": true,\n  \"channel\": \"webhook\"\n}"},
		{Path: "/api/v1/settings/notifications/test", Methods: []string{http.MethodPost}, Description: "Send test notification(s)", SampleBody: "{\n  \"channel\": \"all\"\n}"},
		{Path: "/api/v1/schedule", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Read/write scheduler state", SampleBody: "{\n  \"type\": \"cron\",\n  \"action\": \"create\",\n  \"cron\": \"15 */4 * * *\",\n  \"config\": \"config.yaml\",\n  \"print_only\": true,\n  \"apply\": false\n}"},
		{Path: "/api/v1/artifacts", Methods: []string{http.MethodGet}, Description: "List available artifacts"},
		{Path: "/api/v1/artifacts/{name}", Methods: []string{http.MethodGet}, Description: "Read artifact by name"},
		{Path: "/api/v1/runs", Methods: []string{http.MethodGet}, Description: "List historical runs"},
		{Path: "/api/v1/runs/summary", Methods: []string{http.MethodGet}, Description: "Read latest run summary"},
		{Path: "/api/v1/runs/active", Methods: []string{http.MethodGet}, Description: "Read active run state"},
		{Path: "/api/v1/runs/trigger", Methods: []string{http.MethodPost}, Description: "Trigger orchestrator run", SampleBody: "{\n  \"config_path\": \"config.yaml\",\n  \"password\": \"\",\n  \"extra_args\": [\"--no-html\"]\n}"},
		{Path: "/api/v1/report/data", Methods: []string{http.MethodGet}, Description: "Aggregated report payload"},
		{Path: "/api/v1/report/trends", Methods: []string{http.MethodGet}, Description: "Historical trends from run summaries"},
		{Path: "/api/v1/logs/runner", Methods: []string{http.MethodGet}, Description: "Read tail of runner log"},
		{Path: "/api/v1/openapi.json", Methods: []string{http.MethodGet}, Description: "OpenAPI 3.0 specification"},
		{Path: "/api/v1/meta/routes", Methods: []string{http.MethodGet}, Description: "List available REST routes for API explorer"},
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"routes": routes,
		"count":  len(routes),
	}})
}

func (s *apiServer) handleOpenAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	spec := s.buildOpenAPISpec()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(spec)
}

func (s *apiServer) buildOpenAPISpec() map[string]interface{} {
	return map[string]interface{}{
		"openapi": "3.0.3",
		"info": map[string]interface{}{
			"title":       "NCC Orchestrator API",
			"version":     "2.0.0",
			"description": "REST API for NCC orchestrator run control, artifacts, settings, and analytics.",
		},
		"servers": []map[string]interface{}{
			{"url": "/"},
		},
		"paths": map[string]interface{}{
			"/api/v1/health": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Backend health and resolved paths"},
			},
			"/api/v1/auth/session": map[string]interface{}{
				"post": map[string]interface{}{"summary": "Issue short-lived session token"},
			},
			"/api/v1/auth/rotate": map[string]interface{}{
				"post": map[string]interface{}{"summary": "Rotate API token"},
			},
			"/api/v1/settings/config": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read runtime config"},
				"put": map[string]interface{}{
					"summary": "Write runtime config",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"content": "clusters: \"10.0.0.1\"\nusername: \"admin\"\n",
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/config-files": map[string]interface{}{
				"get": map[string]interface{}{"summary": "List config-referenced files"},
			},
			"/api/v1/settings/config-file": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read one config-referenced file"},
				"put": map[string]interface{}{
					"summary": "Write one config-referenced file",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"path":    "alerts-exclude.txt",
									"content": "AHV_MemoryUsage\n",
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/notifications": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read notifications state"},
				"put": map[string]interface{}{
					"summary": "Write notifications state",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"enabled": true,
									"channel": "webhook",
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/notifications/test": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Send test notification(s)",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"channel": "all",
								},
							},
						},
					},
				},
			},
			"/api/v1/schedule": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read scheduler state"},
				"put": map[string]interface{}{
					"summary": "Write scheduler state",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"type":       "cron",
									"action":     "create",
									"cron":       "15 */4 * * *",
									"config":     "config.yaml",
									"print_only": true,
									"apply":      false,
								},
							},
						},
					},
				},
			},
			"/api/v1/artifacts": map[string]interface{}{
				"get": map[string]interface{}{"summary": "List available artifacts"},
			},
			"/api/v1/artifacts/{name}": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read artifact by name"},
			},
			"/api/v1/runs": map[string]interface{}{
				"get": map[string]interface{}{"summary": "List historical runs"},
			},
			"/api/v1/runs/summary": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read latest run summary"},
			},
			"/api/v1/runs/active": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read active run state"},
			},
			"/api/v1/runs/trigger": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Trigger orchestrator run",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"config_path": "config.yaml",
									"password":    "",
									"extra_args":  []string{"--no-html"},
								},
							},
						},
					},
				},
			},
			"/api/v1/report/data": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Aggregated report payload"},
			},
			"/api/v1/report/trends": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Historical trends from run summaries"},
			},
			"/api/v1/logs/runner": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read tail of runner log"},
			},
			"/api/v1/openapi.json": map[string]interface{}{
				"get": map[string]interface{}{"summary": "OpenAPI 3.0 specification"},
			},
			"/api/v1/meta/routes": map[string]interface{}{
				"get": map[string]interface{}{"summary": "List available REST routes for API explorer"},
			},
		},
	}
}

func (s *apiServer) setRunDone(err error, output string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active = false
	s.lastOut = tailString(strings.TrimSpace(output), 4000)
	s.liveOut = nil
	if err != nil {
		if s.lastOut != "" {
			s.lastErr = fmt.Sprintf("%s\n%s", err.Error(), s.lastOut)
			return
		}
		s.lastErr = err.Error()
		return
	}
	s.lastErr = ""
}

func (s *apiServer) runOrchestrator(args []string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := s.makeOrchestratorCommand(ctx, args...)
	cmd.Dir = s.absPath(s.repoRoot)
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return string(out), fmt.Errorf("command timed out after %s", timeout)
	}
	return string(out), err
}

func (s *apiServer) loadSchedule() (scheduleState, error) {
	path := s.absPath(s.scheduleStatePath)
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return scheduleState{
				Type:      "auto",
				Action:    "create",
				Config:    s.configPath,
				PrintOnly: true,
				UpdatedAt: "",
			}, nil
		}
		return scheduleState{}, err
	}
	var st scheduleState
	if err := json.Unmarshal(b, &st); err != nil {
		return scheduleState{}, err
	}
	return st, nil
}

func (s *apiServer) saveSchedule(st scheduleState) error {
	path := s.absPath(s.scheduleStatePath)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

func (s *apiServer) absPath(p string) string {
	if abs, err := s.normalizeAndConfinePath(p); err == nil {
		return abs
	}
	if filepath.IsAbs(p) {
		return filepath.Clean(p)
	}
	return filepath.Clean(filepath.Join(s.repoRoot, p))
}

func writeJSON(w http.ResponseWriter, status int, v envelope) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func defaultIfEmpty(v, def string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return def
	}
	return v
}

func (s *apiServer) orchestratorBaseCommand() []string {
	binPath, _ := filepath.Abs(s.absPath(s.orchestratorBin))
	if st, err := os.Stat(binPath); err == nil && !st.IsDir() {
		return []string{binPath}
	}
	// Dev-friendly fallback: run source directly when binary is not built yet.
	goNCCPath, _ := filepath.Abs(filepath.Join(s.absPath(s.repoRoot), "goNCC.go"))
	return []string{"go", "run", goNCCPath}
}

func (s *apiServer) makeOrchestratorCommand(ctx context.Context, args ...string) *exec.Cmd {
	base := s.orchestratorBaseCommand()
	full := append(append([]string{}, base...), args...)
	if len(full) == 0 {
		return exec.Command("true")
	}
	name := full[0]
	rest := full[1:]
	if ctx == nil {
		return exec.Command(name, rest...)
	}
	return exec.CommandContext(ctx, name, rest...)
}

func tailString(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[len(s)-max:]
}

func (s *apiServer) currentLiveOutput() string {
	if s.liveOut == nil {
		return ""
	}
	return strings.TrimSpace(s.liveOut.String())
}

func readTailFile(path string, maxBytes int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return "", err
	}
	var offset int64 = 0
	if st.Size() > maxBytes {
		offset = st.Size() - maxBytes
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return "", err
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func readJSONArtifact(path string, fallback interface{}) interface{} {
	b, err := os.ReadFile(path)
	if err != nil {
		return fallback
	}
	var out interface{}
	if err := json.Unmarshal(b, &out); err != nil {
		return fallback
	}
	return out
}

func readInlineJSONVar(path, varName string, fallback interface{}) interface{} {
	b, err := os.ReadFile(path)
	if err != nil {
		return fallback
	}
	pattern := `(?s)(?:const|var)\s+` + regexp.QuoteMeta(varName) + `\s*=\s*(.*?);`
	re := regexp.MustCompile(pattern)
	m := re.FindSubmatch(b)
	if len(m) < 2 {
		return fallback
	}
	raw := strings.TrimSpace(string(m[1]))
	if raw == "" {
		return fallback
	}
	var out interface{}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return fallback
	}
	return out
}

func (s *apiServer) selectBestReportOutDir() string {
	seen := map[string]bool{}
	candidates := []string{}
	add := func(p string) {
		if strings.TrimSpace(p) == "" {
			return
		}
		abs := s.absPath(p)
		if seen[abs] {
			return
		}
		seen[abs] = true
		candidates = append(candidates, abs)
	}
	add(s.outputDir)
	add(filepath.Join(filepath.Dir(s.absPath(s.configPath)), "outputfiles"))
	if strings.TrimSpace(s.lastCfg) != "" {
		add(filepath.Join(filepath.Dir(s.lastCfg), "outputfiles"))
	}
	best := s.absPath(s.outputDir)
	bestScore := -1
	for _, c := range candidates {
		agg := readInlineJSONVar(filepath.Join(c, "index.html"), "AGG", []interface{}{})
		score := 0
		if rows, ok := agg.([]interface{}); ok {
			score = len(rows)
		}
		if score > bestScore {
			best = c
			bestScore = score
		}
	}
	return best
}

func listNCCLogs(logDir string) []map[string]string {
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return []map[string]string{}
	}
	out := make([]map[string]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".log") {
			continue
		}
		out = append(out, map[string]string{
			"name": name,
			"path": filepath.Join(logDir, name),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i]["name"] < out[j]["name"] })
	return out
}

func redactedArgs(args []string) []string {
	if len(args) == 0 {
		return args
	}
	out := make([]string, len(args))
	copy(out, args)
	for i := 0; i < len(out)-1; i++ {
		if out[i] == "--password" {
			out[i+1] = "***"
		}
	}
	return out
}

func tokenSource(activeToken, envToken string) string {
	if strings.TrimSpace(envToken) != "" && strings.TrimSpace(activeToken) == strings.TrimSpace(envToken) {
		return "env"
	}
	return "generated"
}

func (s *apiServer) ensureAuthToken() error {
	if strings.TrimSpace(s.authToken) == "" {
		b := make([]byte, 32)
		if _, err := crand.Read(b); err != nil {
			return fmt.Errorf("generate auth token: %w", err)
		}
		s.authToken = base64.RawURLEncoding.EncodeToString(b)
	}
	tokenPath := s.absPath(s.tokenFilePath)
	if err := os.MkdirAll(filepath.Dir(tokenPath), 0o700); err != nil {
		return fmt.Errorf("prepare token file dir: %w", err)
	}
	if err := os.WriteFile(tokenPath, []byte(s.authToken+"\n"), 0o600); err != nil {
		return fmt.Errorf("write token file: %w", err)
	}
	log.Printf("api auth token ready (source=%s, token_file=%s)", tokenSource(s.authToken, os.Getenv("NCC_API_TOKEN")), tokenPath)
	return nil
}
