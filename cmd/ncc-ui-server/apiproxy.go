package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
)

// apiProxyConfig is everything the /api/ reverse proxy needs from main().
// The credential hooks are funcs so the proxy reads the current value on
// every request (token file rotation, login-mode re-detection, minted
// session refresh).
type apiProxyConfig struct {
	Backend   *url.URL
	Transport http.RoundTripper
	// APICSP is the Content-Security-Policy stamped on proxied responses.
	APICSP string
	// ForwardSessions reports whether the browser's own session cookie is
	// forwarded (interactive login); then no credential is injected.
	ForwardSessions func() bool
	// AuthMode "session" injects a bearer from MintSession; anything else
	// injects X-API-Token from BackendToken.
	AuthMode     string
	MintSession  func() string
	BackendToken func() string
	// Token-file details reported in the NCC_UI_TOKEN_MISMATCH envelope.
	TokenFile        string
	AbsTokenFile     string
	TokenOverrideSet bool
}

// newAPIProxy builds the reverse proxy for /api/ traffic.
//
// It uses ReverseProxy.Rewrite, which runs after hop-by-hop headers are
// removed, so a client sending "Connection: X-API-Token" (or
// Authorization) can no longer strip the credential this proxy injects.
// Rewrite also drops inbound X-Forwarded-* headers, so the X-Forwarded-For
// chain is rebuilt explicitly: the API server parses it right-to-left from
// trusted proxies and needs the ingress's entries when the UI server sits
// behind one.
func newAPIProxy(cfg apiProxyConfig) *httputil.ReverseProxy {
	backend := cfg.Backend
	proxy := &httputil.ReverseProxy{Transport: cfg.Transport}
	proxy.Rewrite = func(pr *httputil.ProxyRequest) {
		pr.SetURL(backend)
		// Keep the inbound Host, as NewSingleHostReverseProxy's Director did.
		pr.Out.Host = pr.In.Host
		// Inbound chain plus the client IP.
		if prior, ok := pr.In.Header["X-Forwarded-For"]; ok {
			pr.Out.Header["X-Forwarded-For"] = append([]string(nil), prior...)
		}
		pr.SetXForwarded()
		pr.Out.Header.Del("X-Forwarded-Host")
		pr.Out.Header.Set("X-Forwarded-Proto", backend.Scheme)

		pr.Out.Header.Del("Authorization")
		pr.Out.Header.Del("X-API-Token")
		if cfg.ForwardSessions() {
			// The browser's session cookie (forwarded automatically) plus its
			// X-CSRF-Token header authenticate the request at its own role.
			// We deliberately do not inject the admin token here.
			return
		}
		if cfg.AuthMode == "session" {
			if tok := cfg.MintSession(); tok != "" {
				pr.Out.Header.Set("Authorization", "Bearer "+tok)
			}
			return
		}
		if tok := cfg.BackendToken(); tok != "" {
			pr.Out.Header.Set("X-API-Token", tok)
		}
	}
	proxy.ModifyResponse = func(resp *http.Response) error {
		// Normalize security headers on proxied API responses to avoid duplicate comma-joined values.
		resp.Header.Set("X-Content-Type-Options", "nosniff")
		resp.Header.Set("X-Frame-Options", "DENY")
		resp.Header.Set("Referrer-Policy", "no-referrer")
		resp.Header.Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
		resp.Header.Set("Content-Security-Policy", cfg.APICSP)
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
		uiSuppliedCredential := !cfg.ForwardSessions()
		reqPath := ""
		if resp.Request != nil && resp.Request.URL != nil {
			reqPath = resp.Request.URL.Path
		}
		if resp.StatusCode == http.StatusUnauthorized && uiSuppliedCredential && !isUserAuthPath(reqPath) {
			cause := "token mismatch or empty token"
			if tok := cfg.BackendToken(); tok == "" {
				cause = "UI server has no API token available (token file is empty or missing)"
			}
			body := map[string]interface{}{
				"success":    false,
				"error":      "Backend rejected the UI server's API token: " + cause + ". Confirm that the UI server's token file (" + filepath.Base(cfg.AbsTokenFile) + ") contains the same token the ncc-api-server is using (or set NCC_API_TOKEN to the same value for both processes), then restart this UI server.",
				"error_code": "NCC_UI_TOKEN_MISMATCH",
				"data": map[string]interface{}{
					"backend_status":     401,
					"token_file":         filepath.Base(cfg.AbsTokenFile),
					"token_file_present": fileExists(cfg.TokenFile),
					"token_override_set": cfg.TokenOverrideSet,
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
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, e error) {
		// Log the detail (backend address, dial error) server-side only.
		log.Printf("api proxy error for %s (request_id=%s): %v", r.URL.Path, r.Header.Get("X-Request-ID"), e)
		w.Header().Set("X-Request-ID", r.Header.Get("X-Request-ID"))
		writeAPIError(w, http.StatusBadGateway, "the API server is unreachable; check that ncc-api-server is running", r.TLS != nil, cfg.APICSP)
	}
	return proxy
}
