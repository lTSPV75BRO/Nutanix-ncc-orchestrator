package main

import (
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"goncc/internal/selfsigned"
)

func writeTestCert(t *testing.T, hosts []string) (certPath string, pair tls.Certificate) {
	t.Helper()
	certPEM, keyPEM, err := selfsigned.Generate(hosts, time.Hour)
	if err != nil {
		t.Fatalf("generate cert: %v", err)
	}
	pair, err = tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("key pair: %v", err)
	}
	certPath = filepath.Join(t.TempDir(), "ui.crt")
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, pair
}

func TestUIRunHealthCheckVerifiesOwnSelfSignedCert(t *testing.T) {
	certPath, pair := writeTestCert(t, []string{"localhost", "127.0.0.1"})
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{pair}}
	srv.StartTLS()
	defer srv.Close()

	if code := uiRunHealthCheck(srv.Listener.Addr().String(), true, certPath); code != 0 {
		t.Fatalf("health check against own cert: want 0, got %d", code)
	}
}

func TestUIRunHealthCheckCertMismatchStillCountsAsUp(t *testing.T) {
	_, served := writeTestCert(t, []string{"localhost"})
	otherPath, _ := writeTestCert(t, []string{"localhost"})
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{served}}
	srv.StartTLS()
	defer srv.Close()

	if code := uiRunHealthCheck(srv.Listener.Addr().String(), true, otherPath); code != 0 {
		t.Fatalf("handshake completed, so the server is up: want 0, got %d", code)
	}
}

func TestUIRunHealthCheckFailsWhenDown(t *testing.T) {
	certPath, _ := writeTestCert(t, []string{"localhost"})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	if code := uiRunHealthCheck(addr, true, certPath); code != 1 {
		t.Fatalf("closed port: want 1, got %d", code)
	}
}

func TestHealthCheckTLSConfigNeedsCert(t *testing.T) {
	if _, err := healthCheckTLSConfig(""); err == nil {
		t.Fatal("want error for empty cert path")
	}
	cfg, err := healthCheckTLSConfig(func() string { p, _ := writeTestCert(t, []string{"ncc.example.com"}); return p }())
	if err != nil {
		t.Fatal(err)
	}
	// selfsigned.Generate always adds localhost, which the probe prefers.
	if cfg.InsecureSkipVerify || cfg.RootCAs == nil || cfg.ServerName != "localhost" {
		t.Fatalf("unexpected config: skip=%v roots=%v name=%q", cfg.InsecureSkipVerify, cfg.RootCAs != nil, cfg.ServerName)
	}
}
