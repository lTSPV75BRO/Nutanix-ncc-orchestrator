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
