package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const (
	maxJSONBodyBytes = 1 << 20 // 1 MiB
	maxExtraArgs     = 32
)

var (
	reTaskName = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	reCronSafe = regexp.MustCompile(`^[0-9*/,\- ]{1,120}$`)
	reEvery    = regexp.MustCompile(`^[1-9][0-9]{0,3}[mhd]$`)
	reSafeArg  = regexp.MustCompile(`^[A-Za-z0-9._:/=\-,@]+$`)
)

type sessionClaims struct {
	Exp int64  `json:"exp"`
	Iat int64  `json:"iat"`
	JTI string `json:"jti"`
	CIP string `json:"cip"`
	Iss string `json:"iss"`
}

func parseAllowedOrigins(raw string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, part := range strings.Split(raw, ",") {
		v := strings.TrimSpace(part)
		if v == "" {
			continue
		}
		out[v] = struct{}{}
	}
	return out
}

func secureCompare(a, b string) bool {
	ab := []byte(strings.TrimSpace(a))
	bb := []byte(strings.TrimSpace(b))
	if len(ab) != len(bb) {
		return false
	}
	return subtle.ConstantTimeCompare(ab, bb) == 1
}

func decodeJSON(r io.Reader, out interface{}) error {
	dec := json.NewDecoder(io.LimitReader(r, maxJSONBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	var trailing interface{}
	if err := dec.Decode(&trailing); err != io.EOF {
		return errors.New("request body must contain a single JSON object")
	}
	return nil
}

func requireJSONContentType(r *http.Request) error {
	ct := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	if ct == "" {
		return errors.New("content-type is required")
	}
	if !strings.HasPrefix(ct, "application/json") {
		return fmt.Errorf("unsupported content-type: %s", ct)
	}
	return nil
}

func cleanClientIP(r *http.Request) string {
	xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For"))
	if xff != "" {
		first := strings.TrimSpace(strings.Split(xff, ",")[0])
		if ip := net.ParseIP(first); ip != nil {
			return ip.String()
		}
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err == nil {
		if ip := net.ParseIP(host); ip != nil {
			return ip.String()
		}
	}
	if ip := net.ParseIP(strings.TrimSpace(r.RemoteAddr)); ip != nil {
		return ip.String()
	}
	return ""
}

func isLoopbackRequest(r *http.Request) bool {
	ip := net.ParseIP(cleanClientIP(r))
	return ip != nil && ip.IsLoopback()
}

func (s *apiServer) normalizeAndConfinePath(p string) (string, error) {
	rootAbs, err := filepath.Abs(s.repoRoot)
	if err != nil {
		return "", fmt.Errorf("resolve repo root: %w", err)
	}
	rootAbs = filepath.Clean(rootAbs)
	var abs string
	if filepath.IsAbs(p) {
		abs = filepath.Clean(p)
	} else {
		abs = filepath.Clean(filepath.Join(rootAbs, p))
	}
	prefix := rootAbs + string(filepath.Separator)
	if abs != rootAbs && !strings.HasPrefix(abs, prefix) {
		return "", fmt.Errorf("path escapes repo root: %s", p)
	}
	return abs, nil
}

func (s *apiServer) validateConfigPath(p string) (string, error) {
	trimmed := strings.TrimSpace(p)
	if trimmed == "" {
		return "", errors.New("config path is required")
	}
	abs, err := s.normalizeAndConfinePath(trimmed)
	if err != nil {
		return "", err
	}
	ext := strings.ToLower(filepath.Ext(abs))
	if ext != ".yml" && ext != ".yaml" {
		return "", fmt.Errorf("config path must end with .yaml or .yml: %s", trimmed)
	}
	return abs, nil
}

func validateScheduleInput(st scheduleState) error {
	switch st.Type {
	case "auto", "cron", "windows":
	default:
		return fmt.Errorf("invalid schedule type: %s", st.Type)
	}
	switch st.Action {
	case "create", "list", "remove", "run-now":
	default:
		return fmt.Errorf("invalid schedule action: %s", st.Action)
	}
	if strings.TrimSpace(st.Cron) != "" && !reCronSafe.MatchString(strings.TrimSpace(st.Cron)) {
		return errors.New("cron contains invalid characters")
	}
	if strings.TrimSpace(st.Every) != "" && !reEvery.MatchString(strings.TrimSpace(st.Every)) {
		return errors.New("every must match patterns like 15m, 4h, or 1d")
	}
	if strings.TrimSpace(st.TaskName) != "" && !reTaskName.MatchString(strings.TrimSpace(st.TaskName)) {
		return errors.New("task_name contains invalid characters")
	}
	return nil
}

func sanitizeExtraArgs(args []string) ([]string, error) {
	if len(args) > maxExtraArgs {
		return nil, fmt.Errorf("too many extra args: max %d", maxExtraArgs)
	}
	allowed := map[string]bool{
		"--output-dir":         true,
		"--log-dir":            true,
		"--cluster-file":       true,
		"--clusters":           true,
		"--prom-dir":           true,
		"--compare-with":       true,
		"--gen-test-agg":       false,
		"--no-html":            false,
		"--quiet-hours":        false,
		"--maintenance-window": false,
	}
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := strings.TrimSpace(args[i])
		if a == "" {
			continue
		}
		if strings.ContainsAny(a, "&;|`$><\n\r\t") {
			return nil, fmt.Errorf("unsafe characters in arg: %s", a)
		}
		if strings.HasPrefix(a, "--") {
			expectsVal, ok := allowed[a]
			if !ok {
				return nil, fmt.Errorf("extra arg not allowed: %s", a)
			}
			out = append(out, a)
			if expectsVal {
				if i+1 >= len(args) {
					return nil, fmt.Errorf("missing value for arg: %s", a)
				}
				val := strings.TrimSpace(args[i+1])
				if val == "" || strings.ContainsAny(val, "&;|`$><\n\r\t") || !reSafeArg.MatchString(val) {
					return nil, fmt.Errorf("unsafe value for arg: %s", a)
				}
				out = append(out, val)
				i++
			}
			continue
		}
		return nil, fmt.Errorf("standalone arg values are not allowed: %s", a)
	}
	return out, nil
}

func (s *apiServer) issueSessionToken(clientIP string) (string, time.Time, error) {
	jb := make([]byte, 16)
	if _, err := rand.Read(jb); err != nil {
		return "", time.Time{}, fmt.Errorf("generate session id: %w", err)
	}
	now := time.Now().UTC()
	exp := now.Add(s.sessionTTL)
	claims := sessionClaims{
		Exp: exp.Unix(),
		Iat: now.Unix(),
		JTI: base64.RawURLEncoding.EncodeToString(jb),
		CIP: strings.TrimSpace(clientIP),
		Iss: s.sessionIssuer,
	}
	raw, err := json.Marshal(claims)
	if err != nil {
		return "", time.Time{}, err
	}
	payload := base64.RawURLEncoding.EncodeToString(raw)
	mac := hmac.New(sha256.New, []byte(s.sessionSecret))
	_, _ = mac.Write([]byte(payload))
	sig := hex.EncodeToString(mac.Sum(nil))
	return payload + "." + sig, exp, nil
}

func (s *apiServer) validatePathConfig() error {
	candidates := []string{
		s.configPath,
		s.outputDir,
		s.logDir,
		s.runnerLogPath,
		s.scheduleStatePath,
		s.tokenFilePath,
	}
	for _, p := range candidates {
		if _, err := s.normalizeAndConfinePath(p); err != nil {
			return err
		}
	}
	return nil
}

func (s *apiServer) verifySessionToken(token, clientIP string) error {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 2 {
		return errors.New("invalid session token format")
	}
	payload, sigHex := parts[0], parts[1]
	mac := hmac.New(sha256.New, []byte(s.sessionSecret))
	_, _ = mac.Write([]byte(payload))
	expectedSig := hex.EncodeToString(mac.Sum(nil))
	if !secureCompare(expectedSig, sigHex) {
		return errors.New("invalid session token signature")
	}
	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return errors.New("invalid session token payload")
	}
	var claims sessionClaims
	if err := json.Unmarshal(raw, &claims); err != nil {
		return errors.New("invalid session token claims")
	}
	now := time.Now().UTC().Unix()
	if claims.Exp <= now || claims.Iat > now+30 {
		return errors.New("session token expired or invalid time")
	}
	if strings.TrimSpace(claims.Iss) != strings.TrimSpace(s.sessionIssuer) {
		return errors.New("invalid session token issuer")
	}
	if claims.CIP != "" && clientIP != "" && claims.CIP != clientIP {
		return errors.New("session token client binding mismatch")
	}
	return nil
}

func (s *apiServer) audit(r *http.Request, action string, success bool, fields map[string]interface{}) {
	payload := map[string]interface{}{
		"ts":      time.Now().UTC().Format(time.RFC3339),
		"action":  action,
		"success": success,
		"path":    r.URL.Path,
		"method":  r.Method,
		"client":  cleanClientIP(r),
	}
	for k, v := range fields {
		payload[k] = v
	}
	b, _ := json.Marshal(payload)
	fmt.Printf("AUDIT %s\n", string(b))
}
