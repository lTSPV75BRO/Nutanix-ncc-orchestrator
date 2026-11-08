package main

import (
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// Request log for the UI server (stderr → logs/v2-ui.log in the stack).
//
// The API server logs every API call in detail, so here:
//   - errors (4xx/5xx) and slow responses are always logged,
//   - API writes (POST/PUT/PATCH/DELETE) are logged at info,
//   - everything else (static files, API reads) only with NCC_LOG_LEVEL=debug.

type accessRecorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (r *accessRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *accessRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += int64(n)
	return n, err
}

func (r *accessRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *accessRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func uiDebugLogging() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("NCC_LOG_LEVEL"))) {
	case "debug", "trace":
		return true
	}
	return false
}

func withAccessLog(next http.Handler) http.Handler {
	debug := uiDebugLogging()
	if debug {
		log.Printf("level=INFO msg=\"debug request logging on (NCC_LOG_LEVEL=debug)\"")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &accessRecorder{ResponseWriter: w}
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				log.Printf("level=ERROR msg=\"handler panic\" method=%s path=%q panic=%q", r.Method, r.URL.Path, v)
				if rec.status == 0 {
					http.Error(rec, "internal error", http.StatusInternalServerError)
				}
			}
			status := rec.status
			if status == 0 {
				status = http.StatusOK
			}
			d := time.Since(start)
			isAPI := strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/saml/")
			isWrite := r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions
			level := ""
			switch {
			case status >= 500:
				level = "ERROR"
			case status >= 400 || d >= 3*time.Second:
				level = "WARN"
			case isAPI && isWrite:
				level = "INFO"
			case debug:
				level = "DEBUG"
			}
			if level == "" {
				return
			}
			// The API server sets X-Request-ID; it ties this line to its own log.
			reqID := rec.Header().Get("X-Request-ID")
			if reqID == "" {
				reqID = r.Header.Get("X-Request-ID")
			}
			client := r.RemoteAddr
			if host, _, err := net.SplitHostPort(client); err == nil {
				client = host
			}
			log.Printf("level=%s msg=\"http request\" method=%s path=%q status=%d duration_ms=%.1f bytes=%d client_ip=%s request_id=%s tls=%t",
				level, r.Method, r.URL.Path, status, float64(d.Microseconds())/1000, rec.bytes, client, reqID, r.TLS != nil)
		}()
		next.ServeHTTP(rec, r)
	})
}
