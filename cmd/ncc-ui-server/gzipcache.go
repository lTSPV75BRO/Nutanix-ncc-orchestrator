package main

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"mime"
	"net/http"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// gzipAssets serves text assets (JS, CSS, HTML, JSON, SVG) gzip-compressed from
// memory. Each file is compressed once, at the best level, and reused until it
// changes on disk, instead of being compressed again for every request at the
// default level. A first-load of the dashboard fetches several MB of JavaScript,
// so this moves that CPU cost from every visitor to the first one, and the
// smaller bodies are sent with an ETag and Content-Length, so browsers can
// revalidate with a 304 and resume with Range.
type gzipAssets struct {
	fsys     http.FileSystem
	maxFile  int64 // larger files are served by the file server as before
	maxTotal int64 // cache size ceiling (compressed bytes)

	mu      sync.Mutex
	entries map[string]*gzipEntry
	total   int64
}

type gzipEntry struct {
	body    []byte
	modTime time.Time
	size    int64 // size of the source file, to notice a change
	etag    string
	ctype   string
}

func newGzipAssets(fsys http.FileSystem) *gzipAssets {
	return &gzipAssets{fsys: fsys, maxFile: 16 << 20, maxTotal: 64 << 20, entries: map[string]*gzipEntry{}}
}

// serve answers the request from the cache and reports true, or reports false
// when the file server should handle it (not compressible, client does not accept
// gzip, too large, unreadable).
func (g *gzipAssets) serve(w http.ResponseWriter, r *http.Request) bool {
	if (r.Method != http.MethodGet && r.Method != http.MethodHead) || !shouldGzipPath(r.URL.Path) || !acceptsGzip(r) {
		return false
	}
	clean := path.Clean("/" + r.URL.Path)
	e := g.entry(clean)
	if e == nil {
		return false
	}
	h := w.Header()
	h.Set("Content-Type", e.ctype)
	h.Set("Content-Encoding", "gzip")
	h.Set("ETag", e.etag)
	if v := h.Get("Vary"); v == "" {
		h.Set("Vary", "Accept-Encoding")
	} else if !strings.Contains(v, "Accept-Encoding") {
		h.Set("Vary", v+", Accept-Encoding")
	}
	// net/http leaves Content-Length off whenever Content-Encoding is set; the
	// compressed length is known, so say it (except for a Range reply, which has
	// its own length).
	if r.Header.Get("Range") == "" {
		h.Set("Content-Length", strconv.Itoa(len(e.body)))
	}
	http.ServeContent(w, r, "", e.modTime, bytes.NewReader(e.body))
	return true
}

// entry returns the compressed file, compressing it when it is new or changed.
func (g *gzipAssets) entry(clean string) *gzipEntry {
	f, err := g.fsys.Open(clean)
	if err != nil {
		return nil
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || st.IsDir() || st.Size() > g.maxFile {
		return nil
	}
	g.mu.Lock()
	if e, ok := g.entries[clean]; ok && e.size == st.Size() && e.modTime.Equal(st.ModTime()) {
		g.mu.Unlock()
		return e
	}
	g.mu.Unlock()

	src, err := io.ReadAll(io.LimitReader(f, g.maxFile+1))
	if err != nil || int64(len(src)) > g.maxFile {
		return nil
	}
	var buf bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if _, err := zw.Write(src); err != nil || zw.Close() != nil {
		return nil
	}
	sum := sha256.Sum256(buf.Bytes())
	ctype := mime.TypeByExtension(strings.ToLower(filepath.Ext(clean)))
	if ctype == "" {
		ctype = "application/octet-stream"
	}
	e := &gzipEntry{
		body:    buf.Bytes(),
		modTime: st.ModTime(),
		size:    st.Size(),
		etag:    `"` + hex.EncodeToString(sum[:8]) + "-gz" + strconv.Itoa(len(buf.Bytes())) + `"`,
		ctype:   ctype,
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if old, ok := g.entries[clean]; ok {
		g.total -= int64(len(old.body))
	}
	if g.total+int64(len(e.body)) > g.maxTotal {
		// Over budget: serve this one without keeping it, rather than evict a hot file.
		return e
	}
	g.entries[clean] = e
	g.total += int64(len(e.body))
	return e
}
