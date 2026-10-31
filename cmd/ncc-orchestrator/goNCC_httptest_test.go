package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDoWithRetry429Then200(t *testing.T) {
	var n int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		if n == 1 {
			w.Header().Set("Retry-After", "0")
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer srv.Close()

	cfg := Config{
		RetryMaxAttempts: 4,
		RetryBaseDelay:   5 * time.Millisecond,
		RetryMaxDelay:    50 * time.Millisecond,
		RequestTimeout:   5 * time.Second,
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/test", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, body, err := doWithRetry(context.Background(), srv.Client(), req, cfg, "httptest")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if string(body) != "ok" {
		t.Fatalf("got body %q", body)
	}
	if n < 2 {
		t.Fatalf("expected retry after 429, got %d requests", n)
	}
}
