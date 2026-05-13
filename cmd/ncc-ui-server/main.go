package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	pathpkg "path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

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
	flag.StringVar(&listen, "listen", ":8080", "HTTP listen address")
	flag.StringVar(&dir, "dir", "./frontend/dist", "Frontend static directory")
	flag.StringVar(&backendURL, "backend-url", "http://localhost:8081", "Backend API base URL")
	flag.StringVar(&tokenFile, "api-token-file", ".ncc-api-token", "File containing backend API token")
	flag.StringVar(&token, "api-token", "", "Override backend API token (optional)")
	flag.StringVar(&authMode, "api-auth-mode", "token", "Backend auth mode: token or session")
	flag.StringVar(&allowedOrigins, "allowed-origins", "http://localhost:8080", "Allowed browser origin(s), comma-separated")
	flag.StringVar(&tlsCertFile, "tls-cert-file", "", "TLS cert for UI server")
	flag.StringVar(&tlsKeyFile, "tls-key-file", "", "TLS key for UI server")
	flag.StringVar(&backendCAFile, "backend-ca-file", "", "Optional custom CA for HTTPS backend")
	flag.BoolVar(&backendInsecureSkipVerify, "backend-insecure-skip-verify", false, "Skip backend TLS verification (not recommended)")
	flag.StringVar(&backendClientCertFile, "backend-client-cert-file", "", "Optional client cert for backend mTLS")
	flag.StringVar(&backendClientKeyFile, "backend-client-key-file", "", "Optional client key for backend mTLS")
	flag.Parse()
	if strings.Contains(allowedOrigins, "*") {
		log.Fatal("wildcard allowed-origins is not permitted")
	}
	if authMode != "token" && authMode != "session" {
		log.Fatal("api-auth-mode must be token or session")
	}

	apiToken := strings.TrimSpace(token)

	backend, err := url.Parse(backendURL)
	if err != nil {
		log.Fatal(err)
	}
	transport, err := buildBackendTransport(backendCAFile, backendClientCertFile, backendClientKeyFile, backendInsecureSkipVerify)
	if err != nil {
		log.Fatal(err)
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
	applyBaseHeaders := func(w http.ResponseWriter, isTLS bool) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
		w.Header().Set("Content-Security-Policy", uiCSP)
		if isTLS {
			w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		}
	}
	proxy.ModifyResponse = func(resp *http.Response) error {
		// Normalize security headers on proxied API responses to avoid duplicate comma-joined values.
		resp.Header.Set("X-Content-Type-Options", "nosniff")
		resp.Header.Set("X-Frame-Options", "DENY")
		resp.Header.Set("Referrer-Policy", "no-referrer")
		resp.Header.Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
		resp.Header.Set("Content-Security-Policy", apiCSP)
		return nil
	}
	origDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		origDirector(req)
		req.Header.Del("Authorization")
		req.Header.Del("X-API-Token")
		req.Header.Del("X-Forwarded-Host")
		req.Header.Set("X-Forwarded-Proto", backend.Scheme)
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
		http.Error(w, "api proxy error: "+e.Error(), http.StatusBadGateway)
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
		applyBaseHeaders(w, r.TLS != nil)
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin != "" {
			if _, ok := originSet[origin]; !ok {
				http.Error(w, "origin not allowed", http.StatusForbidden)
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
		}
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if !(r.Method == http.MethodGet || r.Method == http.MethodPost || r.Method == http.MethodPut) {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/api/v1/") {
			http.Error(w, "path not allowed", http.StatusForbidden)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	staticFS := http.Dir(dir)
	fileServer := http.FileServer(staticFS)
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			fileServer.ServeHTTP(w, r)
			return
		}
		cleanPath := pathpkg.Clean("/" + r.URL.Path)
		if f, err := staticFS.Open(cleanPath); err == nil {
			if st, statErr := f.Stat(); statErr == nil && !st.IsDir() {
				_ = f.Close()
				fileServer.ServeHTTP(w, r)
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
		fileServer.ServeHTTP(w, clone)
	}))
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		applyBaseHeaders(w, r.TLS != nil)
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
		if err := srv.ListenAndServeTLS(tlsCertFile, tlsKeyFile); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
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
