package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWithAuthTokenMode(t *testing.T) {
	s := &apiServer{authToken: "topsecret", authMode: "token"}
	next := s.withAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	rr := httptest.NewRecorder()
	next.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without token, got %d", rr.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req2.Header.Set("X-API-Token", "topsecret")
	rr2 := httptest.NewRecorder()
	next.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("expected 200 with token, got %d", rr2.Code)
	}
}

func TestWithAuthSessionMode(t *testing.T) {
	s := &apiServer{
		authMode:      "session",
		sessionSecret: "session-secret",
		sessionTTL:    5 * time.Minute,
		sessionIssuer: "ncc-api-server",
	}
	token, _, err := s.issueSessionToken("127.0.0.1")
	if err != nil {
		t.Fatalf("issueSessionToken failed: %v", err)
	}
	next := s.withAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Authorization", "Bearer "+token)
	rr := httptest.NewRecorder()
	next.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 with valid bearer session, got %d", rr.Code)
	}
}

func TestWithCORSDeniesUnknownOrigin(t *testing.T) {
	s := &apiServer{corsOrigin: "http://localhost:8080"}
	next := s.withCORS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	req.Header.Set("Origin", "https://evil.example")
	rr := httptest.NewRecorder()
	next.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for disallowed origin, got %d", rr.Code)
	}
}
