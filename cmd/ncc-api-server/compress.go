package main

import (
	"compress/gzip"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// Response compression for API JSON.
//
// Before v2.2.1 only the UI server compressed (static assets); API responses —
// /report/data, /alerts, /runs and the new alert history endpoints are the
// largest — were sent uncompressed, both through the UI proxy (which forwards
// Accept-Encoding to this server and relays the body as-is) and in API-only
// deployments. gzip is used (Go standard library; Brotli would need a
// third-party dependency for a modest extra gain on JSON).
//
// Rules: only when the client accepts gzip (q>0), only for JSON/text bodies of
// at least gzipMinBytes, never when the handler already set Content-Encoding
// (pre-compressed or binary downloads such as backup archives), never for
// HEAD/204/304 responses.
const gzipMinBytes = 1024

var gzipWriterPool = sync.Pool{New: func() interface{} {
	w, _ := gzip.NewWriterLevel(nil, gzip.DefaultCompression)
	return w
}}

// clientAcceptsGzip parses Accept-Encoding honoring q=0 exclusions.
func clientAcceptsGzip(r *http.Request) bool {
	for _, part := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
		fields := strings.Split(strings.TrimSpace(part), ";")
		coding := strings.ToLower(strings.TrimSpace(fields[0]))
		if coding != "gzip" && coding != "*" {
			continue
		}
		q := 1.0
		for _, f := range fields[1:] {
			f = strings.TrimSpace(f)
			if strings.HasPrefix(strings.ToLower(f), "q=") {
				if v, err := strconv.ParseFloat(strings.TrimSpace(f[2:]), 64); err == nil {
					q = v
				}
			}
		}
		if q > 0 {
			return true
		}
	}
	return false
}

func compressibleContentType(ct string) bool {
	ct = strings.ToLower(strings.TrimSpace(strings.Split(ct, ";")[0]))
	return ct == "application/json" || strings.HasPrefix(ct, "text/") ||
		ct == "application/javascript" || ct == "image/svg+xml" || ct == "application/problem+json"
}

type gzipAPIWriter struct {
	http.ResponseWriter
	status  int
	buf     []byte
	decided bool
	gz      *gzip.Writer
}

func (g *gzipAPIWriter) WriteHeader(code int) {
	if g.status == 0 {
		g.status = code
	}
}

func (g *gzipAPIWriter) Write(b []byte) (int, error) {
	if g.status == 0 {
		g.status = http.StatusOK
	}
	if g.decided {
		if g.gz != nil {
			return g.gz.Write(b)
		}
		return g.ResponseWriter.Write(b)
	}
	g.buf = append(g.buf, b...)
	if len(g.buf) >= gzipMinBytes {
		if err := g.decide(); err != nil {
			return 0, err
		}
	}
	return len(b), nil
}

func (g *gzipAPIWriter) decide() error {
	g.decided = true
	h := g.Header()
	compress := len(g.buf) >= gzipMinBytes &&
		h.Get("Content-Encoding") == "" &&
		compressibleContentType(h.Get("Content-Type")) &&
		g.status != http.StatusNoContent && g.status != http.StatusNotModified
	if compress {
		h.Del("Content-Length")
		h.Set("Content-Encoding", "gzip")
		g.gz = gzipWriterPool.Get().(*gzip.Writer)
		g.gz.Reset(g.ResponseWriter)
	}
	g.ResponseWriter.WriteHeader(g.status)
	buf := g.buf
	g.buf = nil
	if len(buf) == 0 {
		return nil
	}
	var err error
	if g.gz != nil {
		_, err = g.gz.Write(buf)
	} else {
		_, err = g.ResponseWriter.Write(buf)
	}
	return err
}

func (g *gzipAPIWriter) finish() {
	if !g.decided {
		if g.status == 0 {
			g.status = http.StatusOK
		}
		_ = g.decide()
	}
	if g.gz != nil {
		_ = g.gz.Close()
		gzipWriterPool.Put(g.gz)
		g.gz = nil
	}
}

// Flush supports handlers that stream (it forces the compression decision).
func (g *gzipAPIWriter) Flush() {
	if !g.decided {
		_ = g.decide()
	}
	if g.gz != nil {
		_ = g.gz.Flush()
	}
	if f, ok := g.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (s *apiServer) withCompression(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The response can differ by Accept-Encoding, so caches must vary on it.
		w.Header().Add("Vary", "Accept-Encoding")
		if r.Method == http.MethodHead || !clientAcceptsGzip(r) {
			next.ServeHTTP(w, r)
			return
		}
		gw := &gzipAPIWriter{ResponseWriter: w}
		defer gw.finish()
		next.ServeHTTP(gw, r)
	})
}
