// Package httpclient builds the orchestrator's *http.Client (connection
// pooling, TLS policy, and optional request/response logging with secret
// redaction). It depends only on goncc/internal/model so it can be reused
// without importing package main.
//
// Package main re-exports New via the NewHTTPClient alias so existing call
// sites are unchanged.
package httpclient

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"goncc/internal/model"
)

// normalizePin lowercases a SHA-256 fingerprint and strips colons/whitespace
// so "AA:BB:.." and "aabb.." compare equal.
func normalizePin(s string) string {
	return strings.ToLower(strings.NewReplacer(":", "", " ", "", "\t", "").Replace(strings.TrimSpace(s)))
}

// pinVerifier returns a VerifyPeerCertificate function that accepts the
// connection only if the leaf certificate's SHA-256 fingerprint matches one of
// the allowed pins. This provides certificate pinning without trusting the
// system roots, a safer alternative to fully disabling verification.
func pinVerifier(pins []string) func([][]byte, [][]*x509.Certificate) error {
	allowed := make(map[string]struct{}, len(pins))
	for _, p := range pins {
		if n := normalizePin(p); n != "" {
			allowed[n] = struct{}{}
		}
	}
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		for _, raw := range rawCerts {
			sum := sha256.Sum256(raw)
			if _, ok := allowed[hex.EncodeToString(sum[:])]; ok {
				return nil
			}
		}
		return fmt.Errorf("tls: server certificate fingerprint does not match any pin-sha256 value")
	}
}

// loadCABundle returns a cert pool seeded with the system roots plus the PEM
// certificates in path. A nil pool (with err) means the caller should fall
// back to defaults.
func loadCABundle(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ca-bundle %s: %w", path, err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("ca-bundle %s: no valid PEM certificates found", path)
	}
	return pool, nil
}

// HTTP connection pooling defaults applied when the resolved config leaves a
// value unset (<= 0).
const (
	DefaultMaxIdleConns        = 100
	DefaultMaxIdleConnsPerHost = 10
	DefaultIdleConnTimeout     = 90 * time.Second
)

// LoggingTransport wraps a RoundTripper and logs redacted request/response
// dumps at debug level. MaxBody caps the logged body size (0 = unlimited).
type LoggingTransport struct {
	Base    http.RoundTripper
	MaxBody int // bytes; 0 = unlimited
}

// redactHTTPDump masks Authorization and other sensitive headers/body in HTTP dumps for logging.
func redactHTTPDump(dump []byte, maxBody int) []byte {
	lines := bytes.SplitAfter(dump, []byte("\n"))
	var out []byte
	for _, line := range lines {
		if bytes.HasPrefix(bytes.TrimLeft(line, " \t"), []byte("Authorization:")) ||
			bytes.HasPrefix(bytes.TrimLeft(line, " \t"), []byte("authorization:")) {
			out = append(out, []byte("Authorization: [REDACTED]\r\n")...)
			continue
		}
		if bytes.HasPrefix(bytes.TrimLeft(line, " \t"), []byte("Cookie:")) ||
			bytes.HasPrefix(bytes.TrimLeft(line, " \t"), []byte("cookie:")) {
			out = append(out, []byte("Cookie: [REDACTED]\r\n")...)
			continue
		}
		out = append(out, line...)
	}
	// If body looks like JSON and contains password field, mask the value
	if bytes.Contains(out, []byte(`"password"`)) || bytes.Contains(out, []byte(`"Password"`)) {
		out = redactJSONPasswordValue(out)
	}
	if maxBody > 0 && len(out) > maxBody {
		out = append(append([]byte(nil), out[:maxBody]...), []byte("...[truncated]")...)
	}
	return out
}

func redactJSONPasswordValue(b []byte) []byte {
	// Replace "password":"<anything>" or "password": "<anything>" with "password":"[REDACTED]"
	i := 0
	for {
		idx := bytes.Index(b[i:], []byte(`"password"`))
		if idx < 0 {
			idx = bytes.Index(b[i:], []byte(`"Password"`))
		}
		if idx < 0 {
			break
		}
		start := i + idx
		i = start + 10 // past the key
		// Skip whitespace and colon
		for i < len(b) && (b[i] == ' ' || b[i] == '\t' || b[i] == ':') {
			i++
		}
		for i < len(b) && (b[i] == ' ' || b[i] == '\t') {
			i++
		}
		if i >= len(b) {
			break
		}
		// Find value end (string or number)
		valueStart := i
		if b[i] == '"' {
			i++
			for i < len(b) && b[i] != '"' {
				if b[i] == '\\' {
					i++
				}
				i++
			}
			if i < len(b) {
				i++
			}
		} else {
			for i < len(b) && b[i] != ',' && b[i] != '}' && b[i] != '\n' && b[i] != '\r' {
				i++
			}
		}
		// A trailing backslash inside the quoted value can push i one past
		// the end; clamp before slicing to avoid an out-of-range panic.
		if i > len(b) {
			i = len(b)
		}
		// Replace value with [REDACTED]
		replacement := []byte(`"[REDACTED]"`)
		b = append(append(append([]byte(nil), b[:valueStart]...), replacement...), b[i:]...)
		i = valueStart + len(replacement)
	}
	return b
}

func (t *LoggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	if d, err := httputil.DumpRequestOut(req, true); err == nil {
		dump := redactHTTPDump(d, t.MaxBody)
		log.Debug().
			Str("method", req.Method).
			Str("url", req.URL.String()).
			RawJSON("request_dump", dump).
			Msg("http request")
	}
	resp, err := base.RoundTrip(req)
	if err != nil {
		log.Error().Err(err).Str("url", req.URL.String()).Msg("http roundtrip error")
		return nil, err
	}
	if resp != nil {
		if d, err := httputil.DumpResponse(resp, true); err == nil {
			dump := redactHTTPDump(d, t.MaxBody)
			log.Debug().
				Int("status", resp.StatusCode).
				RawJSON("response_dump", dump).
				Msg("http response")
		}
	}
	return resp, nil
}

// New builds an *http.Client from the resolved config: connection pooling,
// TLS policy (including the insecure bypass for non-compliant Prism certs),
// and optional request/response logging.
func New(cfg model.Config) *http.Client {
	// Apply defaults for connection pooling
	maxIdleConns := cfg.MaxIdleConns
	if maxIdleConns <= 0 {
		maxIdleConns = DefaultMaxIdleConns
	}
	maxIdleConnsPerHost := cfg.MaxIdleConnsPerHost
	if maxIdleConnsPerHost <= 0 {
		maxIdleConnsPerHost = DefaultMaxIdleConnsPerHost
	}
	idleConnTimeout := cfg.IdleConnTimeout
	if idleConnTimeout <= 0 {
		idleConnTimeout = DefaultIdleConnTimeout
	}

	tlsCfg := &tls.Config{
		MinVersion: cfg.TLSMinVersion,
	}
	switch {
	case len(cfg.PinSHA256) > 0:
		// Certificate pinning: skip chain validation but accept only the
		// pinned leaf fingerprint(s). Safer than blanket InsecureSkipVerify
		// because a MITM with a different cert is still rejected.
		tlsCfg.InsecureSkipVerify = true
		tlsCfg.VerifyPeerCertificate = pinVerifier(cfg.PinSHA256)
	case cfg.InsecureSkipVerify:
		// When insecure mode is on, use a custom verifier that accepts any cert so we bypass
		// strict x509 "not standards compliant" checks that some Prism certs trigger.
		tlsCfg.InsecureSkipVerify = true
		tlsCfg.VerifyPeerCertificate = func([][]byte, [][]*x509.Certificate) error { return nil }
	case cfg.CABundle != "":
		// Verify against system roots plus the operator-supplied CA bundle.
		if pool, err := loadCABundle(cfg.CABundle); err != nil {
			log.Error().Err(err).Msg("ca-bundle load failed; falling back to system roots")
		} else {
			tlsCfg.RootCAs = pool
		}
	}
	tr := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       tlsCfg,
		// Production-ready connection pooling
		MaxIdleConns:        maxIdleConns,
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
		MaxConnsPerHost:     cfg.MaxConnsPerHost, // 0 = unlimited
		IdleConnTimeout:     idleConnTimeout,
		DisableKeepAlives:   false,
		ForceAttemptHTTP2:   true, // Enable HTTP/2 for better performance
	}
	rt := http.RoundTripper(tr)
	if cfg.LogHTTP || os.Getenv("LOG_HTTP") == "1" {
		rt = &LoggingTransport{Base: tr, MaxBody: 64 * 1024}
	}
	return &http.Client{
		Timeout:   cfg.RequestTimeout, // Use request timeout, not overall timeout
		Transport: rt,
	}
}
