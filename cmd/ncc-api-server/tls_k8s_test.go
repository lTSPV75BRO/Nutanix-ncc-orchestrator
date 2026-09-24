package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestKubernetesTLSIsIngressManaged(t *testing.T) {
	s := &apiServer{}
	s.capabilities.Kubernetes = true

	get := httptest.NewRequest(http.MethodGet, "/api/v1/settings/tls", nil)
	gr := httptest.NewRecorder()
	s.handleTLSSettings(gr, get)
	if gr.Code != http.StatusOK {
		t.Fatalf("GET tls: want 200, got %d: %s", gr.Code, gr.Body.String())
	}
	body := gr.Body.String()
	if !strings.Contains(body, "ingress") || !strings.Contains(body, "ncc-v2-ui-tls") {
		t.Fatalf("GET tls should describe Ingress TLS, got %s", body)
	}

	put := httptest.NewRequest(http.MethodPut, "/api/v1/settings/tls", nil)
	pr := httptest.NewRecorder()
	s.handleTLSSettings(pr, put)
	if pr.Code != http.StatusConflict {
		t.Fatalf("PUT tls: want 409, got %d: %s", pr.Code, pr.Body.String())
	}

	gen := httptest.NewRequest(http.MethodPost, "/api/v1/settings/tls/generate", nil)
	gn := httptest.NewRecorder()
	s.handleTLSGenerate(gn, gen)
	if gn.Code != http.StatusConflict {
		t.Fatalf("POST tls/generate: want 409, got %d: %s", gn.Code, gn.Body.String())
	}
}
