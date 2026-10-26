package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Request IDs and per-route HTTP metrics.
//
// Before this there was no way to tie a user's failed action to a log or
// audit line, and /metrics had rich domain metrics but no request rate,
// error rate, or latency per endpoint. Every response now carries
// X-Request-ID (the caller's, when well formed, else a new one); it is
// recorded in audit entries and returned in error envelopes, and the UI
// shows it in error toasts.

const requestIDHeader = "X-Request-ID"

var reRequestID = regexp.MustCompile(`^[A-Za-z0-9._:-]{8,64}$`)

type requestIDKey struct{}

func newRequestID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("t%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// requestIDFromContext returns the request's ID, or "".
func requestIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey{}).(string)
	return v
}

// statusRecorder captures the status code for metrics.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(b)
}

// Unwrap keeps http.ResponseController working (extendDeadlines).
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// withRequestMeta assigns the request ID and records metrics. It is the
// outermost middleware so the ID is set before any handler (or error path)
// writes a response.
func (s *apiServer) withRequestMeta(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.Header.Get(requestIDHeader))
		if !reRequestID.MatchString(id) {
			id = newRequestID()
		}
		w.Header().Set(requestIDHeader, id)
		rec := &statusRecorder{ResponseWriter: w}
		start := time.Now()
		next.ServeHTTP(rec, r.WithContext(context.WithValue(r.Context(), requestIDKey{}, id)))
		status := rec.status
		if status == 0 {
			status = http.StatusOK
		}
		s.httpMetrics.observe(routeLabel(r.URL.Path), r.Method, status, time.Since(start))
	})
}

// routeLabel maps a request path onto a bounded set of metric labels: an
// exact catalog route, or the catalog route with a trailing parameter
// collapsed to "{id}" (e.g. /api/v1/alerts/ncc/{id}/acknowledge), else
// "other". Raw paths would explode label cardinality.
func routeLabel(path string) string {
	routeLabelsOnce.Do(buildRouteLabelIndex)
	p := strings.TrimSuffix(path, "/")
	if routeExact[p] {
		return p
	}
	parts := strings.Split(p, "/")
	for i := len(parts) - 1; i >= 3; i-- {
		cand := strings.Join(parts[:i], "/") + "/{id}"
		if rest := parts[i+1:]; len(rest) > 0 {
			cand += "/" + strings.Join(rest, "/")
		}
		if routePrefixes[strings.Join(parts[:i], "/")] {
			return cand
		}
	}
	if strings.HasPrefix(p, "/docs") || strings.HasPrefix(p, "/saml/") {
		return strings.SplitN(strings.TrimPrefix(p, "/"), "/", 2)[0]
	}
	return "other"
}

var (
	routeLabelsOnce sync.Once
	routeExact      map[string]bool
	routePrefixes   map[string]bool
)

func buildRouteLabelIndex() {
	routeExact = map[string]bool{"/metrics": true}
	routePrefixes = map[string]bool{}
	for _, rt := range apiRouteCatalog() {
		routeExact[strings.TrimSuffix(rt.Path, "/")] = true
	}
	// Routers that take an ID segment after these prefixes.
	for _, p := range []string{"/api/v1/runs", "/api/v1/alerts/ncc", "/api/v1/settings/users", "/api/v1/settings/tokens", "/api/v1/users/me/pats", "/api/v1/auth/tokens", "/api/v1/settings/password-resets", "/api/v1/artifacts", "/api/v1/settings/backups"} {
		routePrefixes[p] = true
	}
}

// httpMetrics is a small hand-rolled registry (the api-server emits its own
// Prometheus text; it does not depend on client_golang).
type httpMetrics struct {
	mu       sync.Mutex
	requests map[httpMetricKey]uint64
	duration map[string]*latencyHistogram // by route
}

type httpMetricKey struct {
	route  string
	method string
	code   int
}

var latencyBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 180}

type latencyHistogram struct {
	counts []uint64 // per bucket (cumulative at render time)
	sum    float64
	count  uint64
}

func (m *httpMetrics) observe(route, method string, code int, d time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.requests == nil {
		m.requests = map[httpMetricKey]uint64{}
		m.duration = map[string]*latencyHistogram{}
	}
	m.requests[httpMetricKey{route, method, code}]++
	h := m.duration[route]
	if h == nil {
		h = &latencyHistogram{counts: make([]uint64, len(latencyBuckets))}
		m.duration[route] = h
	}
	sec := d.Seconds()
	for i, b := range latencyBuckets {
		if sec <= b {
			h.counts[i]++
			break
		}
	}
	h.sum += sec
	h.count++
}

// write emits ncc_http_requests_total and ncc_http_request_duration_seconds.
func (m *httpMetrics) write(w io.Writer) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Fprintln(w, "# HELP ncc_http_requests_total API requests by route, method and status code since process start.")
	fmt.Fprintln(w, "# TYPE ncc_http_requests_total counter")
	keys := make([]httpMetricKey, 0, len(m.requests))
	for k := range m.requests {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].route != keys[j].route {
			return keys[i].route < keys[j].route
		}
		if keys[i].method != keys[j].method {
			return keys[i].method < keys[j].method
		}
		return keys[i].code < keys[j].code
	})
	for _, k := range keys {
		fmt.Fprintf(w, "ncc_http_requests_total{route=%q,method=%q,code=\"%d\"} %d\n", k.route, k.method, k.code, m.requests[k])
	}
	fmt.Fprintln(w, "# HELP ncc_http_request_duration_seconds API request latency by route.")
	fmt.Fprintln(w, "# TYPE ncc_http_request_duration_seconds histogram")
	routes := make([]string, 0, len(m.duration))
	for r := range m.duration {
		routes = append(routes, r)
	}
	sort.Strings(routes)
	for _, r := range routes {
		h := m.duration[r]
		var cum uint64
		for i, b := range latencyBuckets {
			cum += h.counts[i]
			fmt.Fprintf(w, "ncc_http_request_duration_seconds_bucket{route=%q,le=\"%g\"} %d\n", r, b, cum)
		}
		fmt.Fprintf(w, "ncc_http_request_duration_seconds_bucket{route=%q,le=\"+Inf\"} %d\n", r, h.count)
		fmt.Fprintf(w, "ncc_http_request_duration_seconds_sum{route=%q} %g\n", r, h.sum)
		fmt.Fprintf(w, "ncc_http_request_duration_seconds_count{route=%q} %d\n", r, h.count)
	}
}
