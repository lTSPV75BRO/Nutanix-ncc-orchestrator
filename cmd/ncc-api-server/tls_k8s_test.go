package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"goncc/internal/selfsigned"
)

func TestKubernetesTLSIsStackManaged(t *testing.T) {
	dir := t.TempDir()
	db, err := openUserDB(filepath.Join(dir, "users.json"))
	if err != nil {
		t.Fatal(err)
	}
	s := &apiServer{
		users:     db,
		repoRoot:  dir,
		authMode:  "hybrid",
		authToken: "t",
	}
	s.capabilities.Kubernetes = true

	get := httptest.NewRequest(http.MethodGet, "/api/v1/settings/tls", nil)
	gr := httptest.NewRecorder()
	s.handleTLSSettings(gr, get)
	if gr.Code != http.StatusOK {
		t.Fatalf("GET tls: want 200, got %d: %s", gr.Code, gr.Body.String())
	}
	body := gr.Body.String()
	if strings.Contains(body, `"managed_by":"ingress"`) {
		t.Fatalf("GET tls should not be ingress-managed, got %s", body)
	}
	if !strings.Contains(body, `"mutation_supported":true`) {
		t.Fatalf("GET tls should allow mutations, got %s", body)
	}

	gen := httptest.NewRequest(http.MethodPost, "/api/v1/settings/tls/generate", bytes.NewBufferString(`{"hosts":["10.1.2.3"]}`))
	gen.Header.Set("Content-Type", "application/json")
	gn := httptest.NewRecorder()
	s.handleTLSGenerate(gn, gen)
	if gn.Code != http.StatusOK {
		t.Fatalf("POST tls/generate: want 200, got %d: %s", gn.Code, gn.Body.String())
	}
	var env envelope
	if err := json.Unmarshal(gn.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	data, _ := env.Data.(map[string]interface{})
	if data["restarting"] != false {
		t.Fatalf("k8s generate should hot-reload, got %#v", data)
	}
	certPath := filepath.Join(dir, "tls", selfsigned.BYOCertFile)
	if _, err := os.Stat(certPath); err != nil {
		t.Fatalf("expected generated cert at %s: %v", certPath, err)
	}

	del := httptest.NewRequest(http.MethodDelete, "/api/v1/settings/tls", nil)
	dr := httptest.NewRecorder()
	s.handleTLSSettings(dr, del)
	if dr.Code != http.StatusOK {
		t.Fatalf("DELETE tls: want 200, got %d: %s", dr.Code, dr.Body.String())
	}
	if _, err := os.Stat(certPath); !os.IsNotExist(err) {
		t.Fatalf("BYO cert should be removed after revert, err=%v", err)
	}
	if _, _, ok := selfsigned.ActivePaths(filepath.Join(dir, "tls")); !ok {
		t.Fatal("self-signed cert should remain after revert")
	}
}

func TestPublicTLSInfoOmitsPrivateKey(t *testing.T) {
	dir := t.TempDir()
	certPEM, keyPEM, err := selfsigned.Generate([]string{"10.1.2.3"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	tlsDir := filepath.Join(dir, "tls")
	if err := os.MkdirAll(tlsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tlsDir, selfsigned.SelfCertFile), certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tlsDir, selfsigned.SelfKeyFile), keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	s := &apiServer{repoRoot: dir}
	s.capabilities.Kubernetes = true
	rec := httptest.NewRecorder()
	s.handlePublicTLS(rec, httptest.NewRequest(http.MethodGet, "/api/v1/tls/public", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if strings.Contains(body, "PRIVATE KEY") {
		t.Fatalf("public TLS must not include the private key: %s", body)
	}
	var env envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	data, _ := env.Data.(map[string]interface{})
	if data["https_enabled"] != true {
		t.Fatalf("https_enabled: %#v", data["https_enabled"])
	}
	fp, _ := data["fingerprint_sha256"].(string)
	if !strings.Contains(fp, ":") || len(fp) < 40 {
		t.Fatalf("fingerprint_sha256 missing: %#v", data["fingerprint_sha256"])
	}
	pem, _ := data["cert_pem"].(string)
	if !strings.Contains(pem, "BEGIN CERTIFICATE") {
		t.Fatalf("cert_pem missing: %#v", data["cert_pem"])
	}
}

func TestPublicTLSInfoWithoutCert(t *testing.T) {
	s := &apiServer{repoRoot: t.TempDir()}
	s.capabilities.Kubernetes = true
	rec := httptest.NewRecorder()
	s.handlePublicTLS(rec, httptest.NewRequest(http.MethodGet, "/api/v1/tls/public", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var env envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	data, _ := env.Data.(map[string]interface{})
	if data["https_enabled"] != true {
		t.Fatalf("k8s without cert should still report https_enabled: %#v", data)
	}
	if _, ok := data["cert_pem"]; ok {
		t.Fatalf("cert_pem should be absent when no file exists: %#v", data)
	}
}
