package main

import (
	"compress/gzip"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	pathpkg "path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"goncc/internal/v2layout"
)

// gzipResponseWriter wraps an http.ResponseWriter and transparently compresses
// the body when the client advertised `gzip` in Accept-Encoding. Compression is
// applied lazily on the first Write so handlers that produce no body (304/204)
// don't incur the overhead.
type gzipResponseWriter struct {
	http.ResponseWriter
	gz          *gzip.Writer
	wroteHeader bool
}

func (g *gzipResponseWriter) WriteHeader(status int) {
	if !g.wroteHeader {
		g.wroteHeader = true
		// Content-Length is wrong once we gzip; remove it so chunked transfer is
		// used and add the encoding/vary headers.
		g.Header().Del("Content-Length")
		g.Header().Set("Content-Encoding", "gzip")
		existingVary := g.Header().Get("Vary")
		if existingVary == "" {
			g.Header().Set("Vary", "Accept-Encoding")
		} else if !strings.Contains(existingVary, "Accept-Encoding") {
			g.Header().Set("Vary", existingVary+", Accept-Encoding")
		}
	}
	g.ResponseWriter.WriteHeader(status)
}

func (g *gzipResponseWriter) Write(b []byte) (int, error) {
	if !g.wroteHeader {
		g.WriteHeader(http.StatusOK)
	}
	return g.gz.Write(b)
}

func (g *gzipResponseWriter) Close() error {
	if g.gz == nil {
		return nil
	}
	return g.gz.Close()
}

// shouldGzipPath returns true for text-y assets that compress well. We avoid
// gzipping already-compressed payloads (images, fonts in woff2, etc.) since
// the CPU cost outweighs the byte savings.
func shouldGzipPath(p string) bool {
	ext := strings.ToLower(filepath.Ext(p))
	switch ext {
	case ".js", ".mjs", ".css", ".html", ".htm", ".json", ".map",
		".txt", ".svg", ".xml", ".wasm":
		return true
	}
	return false
}

func acceptsGzip(r *http.Request) bool {
	return strings.Contains(strings.ToLower(r.Header.Get("Accept-Encoding")), "gzip")
}

// hashedAssetRe matches Vite's content-hashed filenames such as
// `index-Dv80tUhd.css` or `SettingsPage-tCNUHCQx.js`. These files are immutable
// for the lifetime of a build, so we can cache them aggressively.
var hashedAssetRe = regexp.MustCompile(`-[A-Za-z0-9_-]{6,}\.(js|mjs|css|map|woff2?|ttf|otf|svg|png|jpg|jpeg|gif|webp|wasm)$`)

// setStaticCacheHeaders chooses a cache strategy based on the URL path:
//   - hashed asset under /assets/ → 1-year immutable
//   - index.html / SPA shell      → no-cache (so the new build is picked up)
//   - everything else             → short max-age with revalidation
func setStaticCacheHeaders(w http.ResponseWriter, urlPath string) {
	if existing := w.Header().Get("Cache-Control"); existing != "" {
		return
	}
	base := pathpkg.Base(urlPath)
	switch {
	case strings.HasPrefix(urlPath, "/assets/") && hashedAssetRe.MatchString(base):
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	case urlPath == "/" || urlPath == "/index.html" || filepath.Ext(base) == "":
		w.Header().Set("Cache-Control", "no-cache, must-revalidate")
	default:
		w.Header().Set("Cache-Control", "public, max-age=300, must-revalidate")
	}
}

type apiErrorEnvelope struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}

func writeAPIError(w http.ResponseWriter, status int, message string, isTLS bool, csp string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
	w.Header().Set("Content-Security-Policy", csp)
	if isTLS {
		w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(apiErrorEnvelope{Success: false, Error: message})
}

func fileExists(p string) bool {
	if strings.TrimSpace(p) == "" {
		return false
	}
	st, err := os.Stat(p)
	return err == nil && !st.IsDir()
}

// uiRunHealthCheck probes the running UI server at listen and returns a process
// exit code: 0 when it answers with any HTTP status (the server is up and
// serving), 1 when the connection fails or times out (crashed or hung). TLS
// verification is skipped because the stack default is a self-signed cert.
func uiRunHealthCheck(listen string, https bool) int {
	host := strings.TrimSpace(listen)
	if host == "" {
		host = ":8080"
	}
	if strings.HasPrefix(host, ":") {
		host = "127.0.0.1" + host
	} else if h, p, err := net.SplitHostPort(host); err == nil {
		if h == "" || h == "0.0.0.0" || h == "::" {
			h = "127.0.0.1"
		}
		host = net.JoinHostPort(h, p)
	}
	scheme := "http"
	if https {
		scheme = "https"
	}
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	resp, err := client.Get(fmt.Sprintf("%s://%s/", scheme, host))
	if err != nil {
		fmt.Fprintf(os.Stderr, "ui health-check failed: %v\n", err)
		return 1
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 500 {
		fmt.Fprintf(os.Stderr, "ui health-check: server returned %d\n", resp.StatusCode)
		return 1
	}
	return 0
}

// detectBackendLogin asks the backend's health endpoint whether interactive
// login (local accounts or SAML) is enabled, so the UI proxy can decide
// whether to forward user session cookies instead of injecting the admin
// token. On any error it returns false (legacy admin-token injection).
func detectBackendLogin(backendURL string, transport *http.Transport) bool {
	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(backendURL, "/")+"/api/v1/health", nil)
	if err != nil {
		return false
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second, Transport: transport}).Do(req)
	if err != nil || resp == nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return false
	}
	var payload struct {
		Data struct {
			LoginEnabled bool `json:"login_enabled"`
		} `json:"data"`
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err := json.Unmarshal(b, &payload); err != nil {
		return false
	}
	return payload.Data.LoginEnabled
}

// Build-time metadata; injected via -ldflags. Capitalized names
// (Version / Stream / BuildDate / GoVersion) match the orchestrator
// + api-server convention so binaryGO.txt's single
// `-X main.Version=… -X main.Stream=… -X main.BuildDate=…
// -X main.GoVersion=…` LDFLAGS string applies uniformly to every main
// package. Without this match the ui-server would silently keep its
// in-source defaults (e.g. `stream: dev`) even on a tagged release.
var (
	Version   = "2.1.0"
	BuildDate = "unknown"
	Stream    = "dev"
	GoVersion = "unknown"
)

// uiHandleSubcommandArgs reacts to positional args left over after
// flag.Parse. See the matching helper in cmd/ncc-api-server for the
// motivation; the ui-server gets the same treatment so users who
// type `./ncc-ui-server update --check` aren't silently surprised by
// a UI proxy starting up on :8080.
func uiHandleSubcommandArgs(args []string) {
	if len(args) == 0 {
		return
	}
	first := strings.ToLower(strings.TrimSpace(args[0]))
	switch first {
	case "version", "--version", "-version":
		fmt.Printf("ncc-ui-server\n  version: %s\n  stream:  %s\n  build:   %s\n  go:      %s\n",
			Version, Stream, BuildDate, GoVersion)
		os.Exit(0)
	case "help", "--help", "-help", "-h":
		flag.Usage()
		os.Exit(0)
	}
	fmt.Fprintf(os.Stderr,
		"ncc-ui-server: unrecognized subcommand %q.\n"+
			"This binary is a sub-component of the Nutanix NCC Orchestrator stack and only accepts --flags.\n",
		args[0])
	if root, ok := v2layout.DetectStackRootFromExe(); ok {
		if orch := v2layout.FindBinary(root, "ncc-orchestrator"); orch != "" {
			fmt.Fprintf(os.Stderr,
				"For lifecycle commands like %q, run the orchestrator instead:\n  %s %s\n",
				args[0], orch, strings.Join(args, " "))
			os.Exit(2)
		}
	}
	fmt.Fprintf(os.Stderr,
		"For lifecycle commands like %q, run `ncc-orchestrator %s` instead.\n",
		args[0], strings.Join(args, " "))
	os.Exit(2)
}

// uiApplyStackAwareDefaults rewrites --dir and --api-token-file when
// the ui-server is running from inside an extracted v2 stack and the
// user has not explicitly set those flags. See the matching helper
// in cmd/ncc-api-server for the rationale and detection model.
func uiApplyStackAwareDefaults(dir, tokenFile *string, argv []string) {
	root, ok := v2layout.DetectStackRootFromExe()
	if !ok {
		return
	}
	explicit := uiExplicitFlagSet(argv)
	rewrote := []string{}

	stackFrontend := v2layout.FrontendDir(root)
	if !explicit["dir"] && *dir == "./frontend/dist" {
		if st, err := os.Stat(stackFrontend); err == nil && st.IsDir() {
			*dir = stackFrontend
			rewrote = append(rewrote, "dir")
		}
	}
	stackToken := v2layout.TokenFile(root)
	if !explicit["api-token-file"] && *tokenFile == ".ncc-api-token" {
		*tokenFile = stackToken
		rewrote = append(rewrote, "api-token-file")
	}
	if len(rewrote) > 0 {
		fmt.Fprintf(os.Stderr,
			"[stack-aware] detected v2 stack at %s; auto-resolved %s\n",
			root, strings.Join(rewrote, ", "))
		fmt.Fprintln(os.Stderr,
			"             pass any of those flags explicitly to override.")
	}
}

func uiExplicitFlagSet(argv []string) map[string]bool {
	out := map[string]bool{}
	for _, a := range argv {
		if !strings.HasPrefix(a, "-") {
			continue
		}
		a = strings.TrimLeft(a, "-")
		if i := strings.IndexByte(a, '='); i >= 0 {
			a = a[:i]
		}
		if a == "" {
			continue
		}
		out[a] = true
	}
	return out
}

func inferRequestOrigin(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	host := strings.TrimSpace(r.Host)
	if host == "" {
		return ""
	}
	return scheme + "://" + host
}

func main() {
	var listen string
	var dir string
	var backendURL string
	var tokenFile string
	var token string
	var authMode string
	var allowedOrigins string
	var tlsCertFile string
	var tlsKeyFile string
	var backendCAFile string
	var backendInsecureSkipVerify bool
	var backendClientCertFile string
	var backendClientKeyFile string
	var healthCheck bool
	flag.BoolVar(&healthCheck, "health-check", false, "Probe the running UI server's listen address and exit 0 (healthy) / 1 (unhealthy). Used by the self-heal supervisor to detect a hung UI process")
	flag.StringVar(&listen, "listen", ":8080", "HTTP listen address")
	flag.StringVar(&dir, "dir", "./frontend/dist", "Frontend static directory")
	flag.StringVar(&backendURL, "backend-url", "http://localhost:8081", "Backend API base URL")
	flag.StringVar(&tokenFile, "api-token-file", ".ncc-api-token", "File containing backend API token")
	flag.StringVar(&token, "api-token", "", "Override backend API token (optional)")
	flag.StringVar(&authMode, "api-auth-mode", "token", "Backend auth mode: token or session")
	var loginMode string
	flag.StringVar(&loginMode, "login-mode", "auto", "Browser login handling: auto (detect from backend), on (forward user session cookies, never inject admin token), off (legacy: inject admin token)")
	flag.StringVar(&allowedOrigins, "allowed-origins", "http://localhost:8080", "Allowed browser origin(s), comma-separated")
	flag.StringVar(&tlsCertFile, "tls-cert-file", "", "TLS cert for UI server")
	flag.StringVar(&tlsKeyFile, "tls-key-file", "", "TLS key for UI server")
	flag.StringVar(&backendCAFile, "backend-ca-file", "", "Optional custom CA for HTTPS backend")
	flag.BoolVar(&backendInsecureSkipVerify, "backend-insecure-skip-verify", false, "Skip backend TLS verification (not recommended)")
	flag.StringVar(&backendClientCertFile, "backend-client-cert-file", "", "Optional client cert for backend mTLS")
	flag.StringVar(&backendClientKeyFile, "backend-client-key-file", "", "Optional client key for backend mTLS")
	flag.Parse()

	// Reject stray positional args (e.g. user typed
	// `./ncc-ui-server update`) with a redirect to the orchestrator.
	// Without this, Go's flag package silently ignores the args and
	// the UI server starts up anyway, leaving the user confused.
	uiHandleSubcommandArgs(flag.Args())

	// --health-check short-circuits: probe the live server and exit. The
	// self-heal supervisor invokes this to detect a hung (alive-but-unresponsive)
	// UI process, not just a crashed one.
	if healthCheck {
		os.Exit(uiRunHealthCheck(listen, strings.TrimSpace(tlsCertFile) != "" && strings.TrimSpace(tlsKeyFile) != ""))
	}

	// Stack-aware defaults: when the ui-server is launched from
	// inside an extracted v2 stack (`<root>/bin/<self>`) and the
	// user did not explicitly set --dir / --api-token-file, point
	// them at the stack-relative `<root>/frontend-dist` and
	// `<root>/.ncc-api-token` so the canonical
	//
	//     cd ncc-v2-stack-<os>-<arch>/bin && ./ncc-ui-server
	//
	// invocation works without forcing the user to retype every
	// path. Outside a stack layout (Docker images, dev checkouts)
	// this is a no-op and the original CWD-relative defaults stand.
	uiApplyStackAwareDefaults(&dir, &tokenFile, os.Args[1:])

	if strings.Contains(allowedOrigins, "*") {
		log.Fatal("wildcard allowed-origins is not permitted")
	}
	if authMode != "token" && authMode != "session" {
		log.Fatal("api-auth-mode must be token or session")
	}
	if loginMode != "auto" && loginMode != "on" && loginMode != "off" {
		log.Fatal("login-mode must be one of: auto, on, off")
	}

	apiToken := strings.TrimSpace(token)

	backend, err := url.Parse(backendURL)
	if err != nil {
		log.Fatal(err)
	}
	// Resolve the absolute token-file path once so log lines, error messages,
	// and the periodic warning all agree on what file the UI server is
	// actually consulting (very common bug: UI server started from a
	// different cwd than the API server → references different .ncc-api-token).
	absTokenFile, absErr := filepath.Abs(tokenFile)
	if absErr != nil {
		absTokenFile = tokenFile
	}
	transport, err := buildBackendTransport(backendCAFile, backendClientCertFile, backendClientKeyFile, backendInsecureSkipVerify)
	if err != nil {
		log.Fatal(err)
	}
	// forwardSessions: when the backend has interactive login (local accounts
	// or SAML) enabled, the browser authenticates with its own role-bearing
	// session cookie. In that mode we must NOT inject the shared admin token,
	// otherwise every UI visitor would be silently elevated to admin (which
	// also defeats the forced first-login password change and logout). We
	// forward the browser's cookies/CSRF header straight through instead.
	var forwardSessions atomic.Bool
	switch loginMode {
	case "on":
		forwardSessions.Store(true)
		log.Printf("login-mode=on: forwarding user session cookies; admin token is NOT injected for browser traffic")
	case "off":
		forwardSessions.Store(false)
		log.Printf("login-mode=off: injecting admin token for browser traffic (interactive login bypassed)")
	default: // "auto"
		// Detect with a short retry: under `v2-start` the UI and API servers
		// launch together, so the backend may not have finished bootstrapping
		// (and enabling login) on our first probe. Without the retry we would
		// latch into legacy token injection and never honor login.
		initial := false
		for attempt := 0; attempt < 10; attempt++ {
			if detectBackendLogin(backendURL, transport) {
				initial = true
				break
			}
			time.Sleep(time.Second)
		}
		forwardSessions.Store(initial)
		if initial {
			log.Printf("login-mode=auto: backend login detected; forwarding user session cookies (admin token NOT injected)")
		} else {
			log.Printf("login-mode=auto: backend login not detected yet; injecting admin token for now and re-checking in the background")
		}
		// Keep re-checking so the proxy flips correctly if the backend enables
		// (or disables) login at runtime after we started.
		go func() {
			t := time.NewTicker(15 * time.Second)
			defer t.Stop()
			for range t.C {
				on := detectBackendLogin(backendURL, transport)
				if on != forwardSessions.Load() {
					forwardSessions.Store(on)
					if on {
						log.Printf("login-mode=auto: backend login now enabled; forwarding user session cookies (admin token NOT injected)")
					} else {
						log.Printf("login-mode=auto: backend login now disabled; injecting admin token for browser traffic")
					}
				}
			}
		}()
	}
	var sessionMu sync.Mutex
	var sessionToken string
	var sessionExp time.Time
	getBackendToken := func() string {
		if apiToken != "" {
			return apiToken
		}
		if b, err := os.ReadFile(tokenFile); err == nil {
			return strings.TrimSpace(string(b))
		}
		return ""
	}
	// Loud startup check so the operator sees the misconfig before users do.
	if apiToken == "" {
		if _, sErr := os.Stat(tokenFile); sErr != nil {
			log.Printf("WARNING: API token file %q does not exist and --api-token is unset; every /api/* request will receive 401 until this is fixed.", absTokenFile)
			// Re-warn periodically so the operator notices even if they
			// missed the startup line.
			go func() {
				t := time.NewTicker(60 * time.Second)
				defer t.Stop()
				for range t.C {
					if _, err := os.Stat(tokenFile); err == nil {
						log.Printf("INFO: API token file %q now present.", absTokenFile)
						return
					}
					log.Printf("WARNING: API token file %q still missing; /api/* requests are 401.", absTokenFile)
				}
			}()
		} else if b, rErr := os.ReadFile(tokenFile); rErr != nil || strings.TrimSpace(string(b)) == "" {
			log.Printf("WARNING: API token file %q is empty or unreadable; every /api/* request will receive 401 until this is fixed.", absTokenFile)
		}
	}
	mintSession := func() string {
		sessionMu.Lock()
		defer sessionMu.Unlock()
		if sessionToken != "" && time.Now().UTC().Before(sessionExp.Add(-30*time.Second)) {
			return sessionToken
		}
		tok := getBackendToken()
		if tok == "" {
			return ""
		}
		req, _ := http.NewRequest(http.MethodPost, strings.TrimRight(backendURL, "/")+"/api/v1/auth/session", nil)
		req.Header.Set("X-API-Token", tok)
		resp, err := (&http.Client{Timeout: 10 * time.Second, Transport: transport}).Do(req)
		if err != nil || resp == nil {
			return ""
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			return ""
		}
		var payload struct {
			Success bool `json:"success"`
			Data    struct {
				Token     string `json:"token"`
				ExpiresAt string `json:"expires_at"`
			} `json:"data"`
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if err := json.Unmarshal(b, &payload); err != nil || !payload.Success || strings.TrimSpace(payload.Data.Token) == "" {
			return ""
		}
		sessionToken = strings.TrimSpace(payload.Data.Token)
		if t, err := time.Parse(time.RFC3339, strings.TrimSpace(payload.Data.ExpiresAt)); err == nil {
			sessionExp = t
		} else {
			sessionExp = time.Now().UTC().Add(5 * time.Minute)
		}
		return sessionToken
	}

	proxy := httputil.NewSingleHostReverseProxy(backend)
	proxy.Transport = transport
	uiCSP := "default-src 'self'; base-uri 'self'; frame-ancestors 'none'; object-src 'none'; form-action 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self' data:; connect-src 'self'"
	apiCSP := "default-src 'none'; frame-ancestors 'none'; object-src 'none'; base-uri 'none'"
	applyHeaders := func(w http.ResponseWriter, isTLS bool, csp string) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
		w.Header().Set("Content-Security-Policy", csp)
		if isTLS {
			w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		}
	}
	applyUIHeaders := func(w http.ResponseWriter, isTLS bool) { applyHeaders(w, isTLS, uiCSP) }
	applyAPIHeaders := func(w http.ResponseWriter, isTLS bool) { applyHeaders(w, isTLS, apiCSP) }
	proxy.ModifyResponse = func(resp *http.Response) error {
		// Normalize security headers on proxied API responses to avoid duplicate comma-joined values.
		resp.Header.Set("X-Content-Type-Options", "nosniff")
		resp.Header.Set("X-Frame-Options", "DENY")
		resp.Header.Set("Referrer-Policy", "no-referrer")
		resp.Header.Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
		resp.Header.Set("Content-Security-Policy", apiCSP)
		// Rewrite a backend 401 into a diagnostic envelope so the UI shows
		// "the UI server's token doesn't match the API server's token" instead
		// of a bare "unauthorized" message. This is the single most common
		// failure mode when the two servers start from different working
		// directories or one was rotated after the other started.
		//
		// This diagnostic is ONLY meaningful when the UI server itself attached
		// the credential the backend rejected (token-injection / minted-session
		// modes). When we forward the browser's own session cookie
		// (interactive-login mode), or the request targets an auth/SSO endpoint,
		// a 401 is the user's own credential failure (e.g. a wrong password) and
		// the backend's clear message ("invalid username or password") must pass
		// through untouched.
		isUserAuthPath := func(p string) bool {
			return strings.HasPrefix(p, "/api/v1/auth/") || strings.HasPrefix(p, "/saml/")
		}
		uiSuppliedCredential := !forwardSessions.Load()
		reqPath := ""
		if resp.Request != nil && resp.Request.URL != nil {
			reqPath = resp.Request.URL.Path
		}
		if resp.StatusCode == http.StatusUnauthorized && uiSuppliedCredential && !isUserAuthPath(reqPath) {
			cause := "token mismatch or empty token"
			if tok := getBackendToken(); tok == "" {
				cause = "UI server has no API token available (token file is empty or missing)"
			}
			body := map[string]interface{}{
				"success":    false,
				"error":      "Backend rejected the UI server's API token: " + cause + ". Confirm that " + absTokenFile + " contains the same token the ncc-api-server is using (or set NCC_API_TOKEN to the same value for both processes), then restart this UI server.",
				"error_code": "NCC_UI_TOKEN_MISMATCH",
				"data": map[string]interface{}{
					"backend_status":     401,
					"token_file":         absTokenFile,
					"token_file_present": fileExists(tokenFile),
					"token_override_set": apiToken != "",
				},
			}
			b, _ := json.Marshal(body)
			resp.Body = io.NopCloser(strings.NewReader(string(b)))
			resp.ContentLength = int64(len(b))
			resp.Header.Set("Content-Type", "application/json")
			resp.Header.Set("Content-Length", strconv.Itoa(len(b)))
			// Drop the Content-Encoding header in case backend sent gzipped
			// JSON — we wrote a plain UTF-8 body.
			resp.Header.Del("Content-Encoding")
		}
		return nil
	}
	origDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		origDirector(req)
		req.Header.Del("Authorization")
		req.Header.Del("X-API-Token")
		req.Header.Del("X-Forwarded-Host")
		req.Header.Set("X-Forwarded-Proto", backend.Scheme)
		if forwardSessions.Load() {
			// The browser's session cookie (forwarded automatically) plus its
			// X-CSRF-Token header authenticate the request at its own role.
			// We deliberately do not inject the admin token here.
			return
		}
		if authMode == "session" {
			if tok := mintSession(); tok != "" {
				req.Header.Set("Authorization", "Bearer "+tok)
			}
			return
		}
		if tok := getBackendToken(); tok != "" {
			req.Header.Set("X-API-Token", tok)
		}
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, e error) {
		writeAPIError(w, http.StatusBadGateway, "api proxy error: "+e.Error(), r.TLS != nil, apiCSP)
	}

	originSet := map[string]struct{}{}
	for _, o := range strings.Split(allowedOrigins, ",") {
		v := strings.TrimSpace(o)
		if v != "" {
			originSet[v] = struct{}{}
		}
	}
	mux := http.NewServeMux()
	mux.Handle("/api/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin != "" {
			if _, ok := originSet[origin]; !ok && origin != inferRequestOrigin(r) {
				writeAPIError(w, http.StatusForbidden, "origin not allowed", r.TLS != nil, apiCSP)
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Vary", "Origin")
		}
		if r.Method == http.MethodOptions {
			applyAPIHeaders(w, r.TLS != nil)
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-CSRF-Token, X-Requested-With")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if !(r.Method == http.MethodGet || r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodDelete) {
			writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed", r.TLS != nil, apiCSP)
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/api/v1/") {
			writeAPIError(w, http.StatusForbidden, "path not allowed", r.TLS != nil, apiCSP)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	// SAML SP endpoints (/saml/metadata, /saml/login, /saml/acs, /saml/complete)
	// live on the backend but are browser-mediated, so they must be reachable on
	// the UI origin — the post-login session cookie has to land on the same host
	// the SPA runs on. We pass these straight through to the backend (the proxy
	// Director already forwards the browser's cookies and never injects the admin
	// token in login mode); unlike /api/ we apply no CORS/method gating because
	// the IdP POSTs the assertion to /saml/acs cross-site as a top-level form.
	mux.Handle("/saml/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxy.ServeHTTP(w, r)
	}))
	staticFS := http.Dir(dir)
	fileServer := http.FileServer(staticFS)
	// serveStatic adds Cache-Control and transparent gzip compression for
	// text-y assets, then delegates to the underlying http.FileServer.
	serveStatic := func(w http.ResponseWriter, r *http.Request) {
		setStaticCacheHeaders(w, r.URL.Path)
		if shouldGzipPath(r.URL.Path) && acceptsGzip(r) {
			gz := gzip.NewWriter(w)
			grw := &gzipResponseWriter{ResponseWriter: w, gz: gz}
			defer grw.Close()
			fileServer.ServeHTTP(grw, r)
			return
		}
		fileServer.ServeHTTP(w, r)
	}
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			serveStatic(w, r)
			return
		}
		cleanPath := pathpkg.Clean("/" + r.URL.Path)
		if f, err := staticFS.Open(cleanPath); err == nil {
			if st, statErr := f.Stat(); statErr == nil && !st.IsDir() {
				_ = f.Close()
				serveStatic(w, r)
				return
			}
			_ = f.Close()
		}
		// Keep missing asset requests as 404s; only app routes fall back to index.html.
		if filepath.Ext(pathpkg.Base(cleanPath)) != "" {
			http.NotFound(w, r)
			return
		}
		clone := r.Clone(r.Context())
		clone.URL.Path = "/"
		serveStatic(w, clone)
	}))
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/saml/") {
			mux.ServeHTTP(w, r)
			return
		}
		applyUIHeaders(w, r.TLS != nil)
		mux.ServeHTTP(w, r)
	})

	srv := &http.Server{
		Addr:         listen,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	log.Printf("ncc-ui-server serving %s on %s (backend=%s, auth_mode=%s, token_file=%s, token_override=%t)", dir, listen, backendURL, authMode, filepath.Clean(tokenFile), apiToken != "")
	if strings.TrimSpace(tlsCertFile) != "" || strings.TrimSpace(tlsKeyFile) != "" {
		if strings.TrimSpace(tlsCertFile) == "" || strings.TrimSpace(tlsKeyFile) == "" {
			log.Fatal("both tls-cert-file and tls-key-file are required together")
		}
		log.Printf("TLS enabled: serving HTTPS and 308-redirecting plain HTTP to HTTPS on %s", listen)
		if err := serveTLSWithRedirect(srv, listen, tlsCertFile, tlsKeyFile); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

// --- HTTP→HTTPS redirect on the TLS port -----------------------------------
//
// The UI runs on a single port. To honor "no plain-HTTP support" while still
// being friendly to users who type http://, we serve HTTPS and transparently
// redirect any plain-HTTP client that lands on the same port to https://. We do
// this by peeking the first byte of every accepted connection: a TLS
// ClientHello always begins with 0x16 (handshake record); anything else is
// treated as plain HTTP and handed to a tiny 308-redirect server. Both logical
// servers are fed from one real listener via connChanListener.

type connChanListener struct {
	addr   net.Addr
	ch     chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func (l *connChanListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *connChanListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *connChanListener) Addr() net.Addr { return l.addr }

// prefixConn re-emits bytes already peeked off the wire before reading more, so
// the downstream server sees the complete stream (handshake or request line).
type prefixConn struct {
	net.Conn
	prefix []byte
}

func (c *prefixConn) Read(b []byte) (int, error) {
	if len(c.prefix) > 0 {
		n := copy(b, c.prefix)
		c.prefix = c.prefix[n:]
		return n, nil
	}
	return c.Conn.Read(b)
}

func redirectToHTTPSHandler(w http.ResponseWriter, r *http.Request) {
	host := strings.TrimSpace(r.Host)
	if host == "" {
		http.Error(w, "missing host", http.StatusBadRequest)
		return
	}
	w.Header().Set("Connection", "close")
	http.Redirect(w, r, "https://"+host+r.URL.RequestURI(), http.StatusPermanentRedirect)
}

// serveTLSWithRedirect serves HTTPS on addr using cert/key and 308-redirects any
// plain-HTTP connection on the same port to https://.
func serveTLSWithRedirect(srv *http.Server, addr, certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	base, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	tlsLn := &connChanListener{addr: base.Addr(), ch: make(chan net.Conn), closed: make(chan struct{})}
	redirLn := &connChanListener{addr: base.Addr(), ch: make(chan net.Conn), closed: make(chan struct{})}

	go func() {
		for {
			c, aerr := base.Accept()
			if aerr != nil {
				tlsLn.Close()
				redirLn.Close()
				return
			}
			go func(c net.Conn) {
				_ = c.SetReadDeadline(time.Now().Add(15 * time.Second))
				b := make([]byte, 1)
				n, rerr := c.Read(b)
				_ = c.SetReadDeadline(time.Time{})
				if rerr != nil || n == 0 {
					_ = c.Close()
					return
				}
				pc := &prefixConn{Conn: c, prefix: b[:n]}
				dst := redirLn
				if b[0] == 0x16 { // TLS handshake record byte
					dst = tlsLn
				}
				select {
				case dst.ch <- pc:
				case <-dst.closed:
					_ = c.Close()
				}
			}(c)
		}
	}()

	redirectSrv := &http.Server{
		Handler:      http.HandlerFunc(redirectToHTTPSHandler),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  10 * time.Second,
	}
	go func() { _ = redirectSrv.Serve(redirLn) }()

	srv.TLSConfig = &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	return srv.ServeTLS(tlsLn, "", "")
}

func buildBackendTransport(caFile, certFile, keyFile string, skipVerify bool) (*http.Transport, error) {
	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: skipVerify,
	}
	if strings.TrimSpace(caFile) != "" {
		b, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(b) {
			return nil, errors.New("failed to parse backend CA file")
		}
		tlsCfg.RootCAs = pool
	}
	if strings.TrimSpace(certFile) != "" || strings.TrimSpace(keyFile) != "" {
		if strings.TrimSpace(certFile) == "" || strings.TrimSpace(keyFile) == "" {
			return nil, errors.New("both backend-client-cert-file and backend-client-key-file are required")
		}
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		TLSClientConfig:     tlsCfg,
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        50,
		IdleConnTimeout:     30 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}, nil
}
