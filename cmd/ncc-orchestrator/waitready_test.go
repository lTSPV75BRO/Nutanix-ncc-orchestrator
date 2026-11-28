package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// v2-start --wait-ready used to stop a healthy stack that serves the default
// self-signed certificate.
func TestWaitForHTTPReadyAcceptsSelfSigned(t *testing.T) {
	srv := httptest.NewTLSServer(http.NotFoundHandler())
	defer srv.Close()
	if err := waitForHTTPReady(srv.URL, 3*time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestWaitForHTTPReadyFailsOnServerErrorAndDeadServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	if err := waitForHTTPReady(srv.URL, 300*time.Millisecond); err == nil {
		t.Error("503 counted as ready")
	}
	srv.Close()
	if err := waitForHTTPReady(srv.URL, 300*time.Millisecond); err == nil {
		t.Error("closed server counted as ready")
	}
}
