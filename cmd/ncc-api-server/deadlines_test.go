package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// longHandlerServer serves one handler that sleeps past a short server-wide
// WriteTimeout, wrapped in the API's gzip middleware so the test also proves
// extendDeadlines reaches the connection through gzipAPIWriter.Unwrap.
func longHandlerServer(t *testing.T, extend bool) *httptest.Server {
	t.Helper()
	s := &apiServer{}
	h := s.withCompression(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if extend {
			extendDeadlines(w, time.Second)
		}
		time.Sleep(700 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"success":true,"data":"`+strings.Repeat("x", 2048)+`"}`)
	}))
	ts := httptest.NewUnstartedServer(h)
	ts.Config.WriteTimeout = 300 * time.Millisecond
	ts.Start()
	t.Cleanup(ts.Close)
	return ts
}

func getGzip(ts *httptest.Server) (*http.Response, error) {
	req, _ := http.NewRequest(http.MethodGet, ts.URL, nil)
	req.Header.Set("Accept-Encoding", "gzip")
	return ts.Client().Do(req)
}

func TestExtendDeadlinesOutlastsServerWriteTimeout(t *testing.T) {
	resp, err := getGzip(longHandlerServer(t, true))
	if err != nil {
		t.Fatalf("long handler with extended deadline failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatalf("read body: %v", err)
	}
}

// Control: without the extension the same handler is cut off by the server
// WriteTimeout — the failure mode preflight/backup/restore calls had at 60s.
func TestServerWriteTimeoutCutsLongHandlerWithoutExtension(t *testing.T) {
	resp, err := getGzip(longHandlerServer(t, false))
	if err == nil {
		_, err = io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	if err == nil {
		t.Fatal("expected the write timeout to break the response without extendDeadlines")
	}
}
