package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newAssetsFor(t *testing.T, files map[string]string) (*gzipAssets, string) {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return newGzipAssets(http.Dir(dir)), dir
}

func get(t *testing.T, g *gzipAssets, path string, hdr map[string]string) (*httptest.ResponseRecorder, bool) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	return rec, g.serve(rec, req)
}

func gunzip(t *testing.T, b []byte) string {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func TestGzipAssetsServesCompressedAndReusesIt(t *testing.T) {
	js := strings.Repeat("function f(){return 1}\n", 500)
	g, _ := newAssetsFor(t, map[string]string{"assets/app-Abc123.js": js})
	rec, ok := get(t, g, "/assets/app-Abc123.js", map[string]string{"Accept-Encoding": "gzip, br"})
	if !ok || rec.Code != 200 {
		t.Fatalf("served=%v code=%d", ok, rec.Code)
	}
	if rec.Header().Get("Content-Encoding") != "gzip" || !strings.Contains(rec.Header().Get("Content-Type"), "javascript") {
		t.Fatalf("headers: %v", rec.Header())
	}
	if rec.Header().Get("ETag") == "" || rec.Header().Get("Vary") != "Accept-Encoding" || rec.Header().Get("Content-Length") == "" {
		t.Fatalf("missing ETag/Vary/Content-Length: %v", rec.Header())
	}
	if gunzip(t, rec.Body.Bytes()) != js {
		t.Fatal("body does not decompress to the source")
	}
	first := g.entries["/assets/app-Abc123.js"]
	if _, ok := get(t, g, "/assets/app-Abc123.js", map[string]string{"Accept-Encoding": "gzip"}); !ok {
		t.Fatal("second request not served")
	}
	if g.entries["/assets/app-Abc123.js"] != first {
		t.Fatal("the file was compressed again instead of reused")
	}
}

func TestGzipAssetsRevalidatesAndSupportsRange(t *testing.T) {
	g, _ := newAssetsFor(t, map[string]string{"a.css": strings.Repeat("a{color:red}", 400)})
	rec, _ := get(t, g, "/a.css", map[string]string{"Accept-Encoding": "gzip"})
	etag := rec.Header().Get("ETag")
	if rec2, ok := get(t, g, "/a.css", map[string]string{"Accept-Encoding": "gzip", "If-None-Match": etag}); !ok || rec2.Code != http.StatusNotModified {
		t.Fatalf("want 304, got %d", rec2.Code)
	}
	if rec3, _ := get(t, g, "/a.css", map[string]string{"Accept-Encoding": "gzip", "Range": "bytes=0-9"}); rec3.Code != http.StatusPartialContent || rec3.Body.Len() != 10 {
		t.Fatalf("want 206 with 10 bytes, got %d/%d", rec3.Code, rec3.Body.Len())
	}
}

func TestGzipAssetsNoticesAChangedFile(t *testing.T) {
	g, dir := newAssetsFor(t, map[string]string{"x.js": strings.Repeat("old;", 300)})
	rec, _ := get(t, g, "/x.js", map[string]string{"Accept-Encoding": "gzip"})
	if !strings.HasPrefix(gunzip(t, rec.Body.Bytes()), "old;") {
		t.Fatal("setup")
	}
	if err := os.WriteFile(filepath.Join(dir, "x.js"), []byte(strings.Repeat("new!", 301)), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = os.Chtimes(filepath.Join(dir, "x.js"), time.Now().Add(time.Minute), time.Now().Add(time.Minute))
	rec2, _ := get(t, g, "/x.js", map[string]string{"Accept-Encoding": "gzip"})
	if !strings.HasPrefix(gunzip(t, rec2.Body.Bytes()), "new!") {
		t.Fatal("served the stale compressed copy")
	}
}

func TestGzipAssetsLeavesOtherRequestsToTheFileServer(t *testing.T) {
	g, _ := newAssetsFor(t, map[string]string{"x.js": "var a=1;", "logo.png": "notreallyapng"})
	for name, tc := range map[string]struct {
		path string
		hdr  map[string]string
	}{
		"client does not accept gzip": {"/x.js", map[string]string{}},
		"gzip refused with q=0":       {"/x.js", map[string]string{"Accept-Encoding": "gzip;q=0, identity"}},
		"already compressed type":     {"/logo.png", map[string]string{"Accept-Encoding": "gzip"}},
		"missing file":                {"/nope.js", map[string]string{"Accept-Encoding": "gzip"}},
	} {
		if _, ok := get(t, g, tc.path, tc.hdr); ok {
			t.Errorf("%s: should fall through to the file server", name)
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/x.js", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	if g.serve(httptest.NewRecorder(), req) {
		t.Error("only GET and HEAD are served from the cache")
	}
}

func TestAcceptsGzip(t *testing.T) {
	for v, want := range map[string]bool{
		"gzip": true, "gzip, deflate, br": true, "br, GZIP": true, "gzip;q=0.8": true,
		"": false, "br": false, "gzip;q=0": false, "identity, gzip; q=0": false, "x-gzip-foo": false,
	} {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Accept-Encoding", v)
		if got := acceptsGzip(r); got != want {
			t.Errorf("%q: got %v want %v", v, got, want)
		}
	}
}

func benchAssets(b *testing.B) (*gzipAssets, string) {
	b.Helper()
	dir := b.TempDir()
	var js strings.Builder
	for i := 0; js.Len() < 1<<20; i++ {
		js.WriteString("export function handler" + strings.Repeat("x", i%7) + "(a,b){if(a>b){return a*2+b}return b-a/" + string(rune('0'+i%10)) + "}\n")
	}
	if err := os.WriteFile(filepath.Join(dir, "app.js"), []byte(js.String()), 0o644); err != nil {
		b.Fatal(err)
	}
	return newGzipAssets(http.Dir(dir)), dir
}

// BenchmarkStaticGzipCached is a repeat request for a 1 MB script.
func BenchmarkStaticGzipCached(b *testing.B) {
	g, _ := benchAssets(b)
	req := httptest.NewRequest(http.MethodGet, "/app.js", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		if !g.serve(rec, req) {
			b.Fatal("not served")
		}
	}
}

// BenchmarkStaticGzipEveryRequest is what the server did before: compress the file
// with the default level on every request.
func BenchmarkStaticGzipEveryRequest(b *testing.B) {
	_, dir := benchAssets(b)
	src, _ := os.ReadFile(filepath.Join(dir, "app.js"))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		_, _ = zw.Write(src)
		_ = zw.Close()
	}
}
