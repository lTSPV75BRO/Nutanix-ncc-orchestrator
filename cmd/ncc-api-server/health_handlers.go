package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

func (s *apiServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	writeJSON(w, http.StatusOK, envelope{
		Success: true,
		Data:    s.healthPayload(s.optionalPrincipal(r)),
	})
}

// healthPayload builds /api/v1/health in three tiers so the endpoint stays
// usable for unauthenticated liveness probes and the login page without
// disclosing internals:
//
//   - anonymous: liveness, version, and the login options the sign-in page needs;
//   - any authenticated role: build/runtime details and the config path
//     relative to the stack root (what the Schedule/Runs pages display);
//   - admin: absolute filesystem locations (config, outputs, logs, token file,
//     orchestrator binary) and storage-protection status.
//
// Absolute paths such as /data/auth/.ncc-api-token were previously returned
// to anonymous callers.
func (s *apiServer) healthPayload(p *principal) map[string]interface{} {
	data := map[string]interface{}{
		"status":        "ok",
		"time":          time.Now().UTC().Format(time.RFC3339),
		"version":       Version,
		"auth_mode":     s.authMode,
		"login_enabled": s.loginEnabled(),
		"local_login":   s.users != nil && s.users.count() > 0,
		"saml_enabled":  s.samlEnabled,
		"oidc_enabled":  s.oidcIsEnabled(),
		"oidc_label":    s.oidcLabel(),
		"ldap_enabled":  s.ldapIsEnabled(),
		"cookie_secure": s.cookieSecure(),
		"detail_level":  "public",
		// The sign-in page decides on the opening title before anyone signs in.
		"intro_title": s.loadFeatureFlags().IntroTitle,
	}
	if p == nil || p.mustChange {
		return data
	}
	data["detail_level"] = "authenticated"
	data["rbac_enabled"] = s.viewerToken != "" || s.loginEnabled()
	data["build_date"] = BuildDate
	data["stream"] = Stream
	data["go_version"] = GoVersion
	data["os"] = runtime.GOOS
	data["arch"] = runtime.GOARCH
	data["runtime"] = s.capabilities
	paths := s.healthPaths(time.Now())
	data["config_path"] = paths.configRel
	data["output_dir"] = paths.outputRel
	if p.role < RoleAdmin {
		return data
	}
	data["detail_level"] = "admin"
	data["git_revision"] = GitRevision
	data["token_source"] = tokenSource(s.authToken, os.Getenv("NCC_API_TOKEN"))
	data["users_store_encrypted"] = s.userStoreEncrypted
	data["users_store_protection"] = s.userStoreProtection()
	data["backup_encryption_required"] = backupEncryptionRequired()
	data["backup_server_key_configured"] = backupKeyConfigured()
	data["config_path"] = paths.configAbs
	data["output_dir"] = paths.outputAbs
	data["log_dir"] = paths.logAbs
	data["token_file"] = paths.tokenAbs
	data["orchestrator_bin"] = paths.orchBinAbs
	if s.debugExpose {
		data["repo_root"] = s.absPath(s.repoRoot)
		data["schedule_state"] = s.absPath(s.scheduleStatePath)
		data["orchestrator_cmd"] = strings.Join(s.orchestratorBaseCommand(), " ")
	}
	return data
}

// runHealthCheckProbe implements the --health-check mode. It does
// NOT bind a listening socket. Instead it:
//
//  1. Reads the on-disk token from --token-file-path (the api-server
//     wrote this on its first start; stack-aware defaults will have
//     pointed it at <root>/.ncc-api-token if applicable).
//  2. Issues GET /api/v1/health to the URL derived from --listen,
//     forcing IPv4 loopback when the bind addr is :PORT or
//     0.0.0.0:PORT (avoids the macOS IPv6/IPv4 mismatch the
//     orchestrator's wait-ready already works around).
//  3. Exits 0 on a 200 with `data.status == "ok"`, 1 on any other
//     outcome.
//
// Designed for `HEALTHCHECK CMD ["ncc-api-server", "--health-check"]`
// in Docker images and `livenessProbe.exec.command` in Kubernetes
// manifests, so operators don't have to ship curl/wget in the image.
func runHealthCheckProbe(s *apiServer, listenAddr string, timeout time.Duration) {
	tokenPath := s.tokenFilePath
	if !filepath.IsAbs(tokenPath) {
		tokenPath = filepath.Join(s.repoRoot, tokenPath)
	}
	tokenBytes, err := os.ReadFile(tokenPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "health-check: read token %s: %v\n", tokenPath, err)
		os.Exit(1)
	}
	token := strings.TrimSpace(string(tokenBytes))

	host, port, err := net.SplitHostPort(strings.TrimSpace(listenAddr))
	if err != nil {
		fmt.Fprintf(os.Stderr, "health-check: parse listen %q: %v\n", listenAddr, err)
		os.Exit(1)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	url := "http://" + net.JoinHostPort(host, port) + "/api/v1/health"

	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	client := &http.Client{Timeout: timeout}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "health-check: build request: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("X-Api-Token", token)
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "health-check: GET %s: %v\n", url, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "health-check: %s -> HTTP %d\n", url, resp.StatusCode)
		os.Exit(1)
	}
	var env struct {
		Success bool `json:"success"`
		Data    struct {
			Status string `json:"status"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		fmt.Fprintf(os.Stderr, "health-check: decode body: %v\n", err)
		os.Exit(1)
	}
	if !env.Success || env.Data.Status != "ok" {
		fmt.Fprintf(os.Stderr, "health-check: %s -> success=%v status=%q\n", url, env.Success, env.Data.Status)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "ncc-api-server health: ok (%s)\n", url)
	os.Exit(0)
}
