package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// referenceLinesNewestFirst is what the audit page did before: read everything,
// split, walk backwards.
func referenceLinesNewestFirst(data []byte) []string {
	lines := bytes.Split(data, []byte{'\n'})
	var out []string
	for i := len(lines) - 1; i >= 0; i-- {
		out = append(out, string(lines[i]))
	}
	return out
}

func TestReadLinesNewestFirstMatchesTheFullRead(t *testing.T) {
	dir := t.TempDir()
	rng := rand.New(rand.NewSource(7))
	for iter := 0; iter < 200; iter++ {
		var b strings.Builder
		n := rng.Intn(40)
		for i := 0; i < n; i++ {
			// short, empty and long lines; sometimes no trailing newline
			b.WriteString(strings.Repeat(string(rune('a'+rng.Intn(26))), rng.Intn(60)))
			if i < n-1 || rng.Intn(2) == 0 {
				b.WriteByte('\n')
			}
		}
		path := filepath.Join(dir, fmt.Sprintf("f%d.log", iter))
		if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
			t.Fatal(err)
		}
		want := referenceLinesNewestFirst([]byte(b.String()))
		for _, chunk := range []int{1, 2, 3, 7, 16, 64, 1 << 10} {
			var got []string
			if err := readLinesNewestFirst(path, chunk, func(ln []byte) bool { got = append(got, string(ln)); return true }); err != nil {
				t.Fatal(err)
			}
			// The reader may omit one empty line for an empty file; compare non-empty lines in order.
			if strings.Join(nonEmpty(got), "\x00") != strings.Join(nonEmpty(want), "\x00") {
				t.Fatalf("iter %d chunk %d:\n got %q\nwant %q", iter, chunk, got, want)
			}
		}
	}
}

func nonEmpty(in []string) []string {
	var out []string
	for _, s := range in {
		if strings.TrimSpace(s) != "" {
			out = append(out, s)
		}
	}
	return out
}

func TestReadLinesNewestFirstStopsEarly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.log")
	var b strings.Builder
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&b, "line-%04d\n", i)
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	var got []string
	_ = readLinesNewestFirst(path, 64, func(ln []byte) bool {
		if len(ln) == 0 {
			return true
		}
		got = append(got, string(ln))
		return len(got) < 3
	})
	if strings.Join(got, ",") != "line-0999,line-0998,line-0997" {
		t.Fatalf("got %v", got)
	}
	if err := readLinesNewestFirst(filepath.Join(t.TempDir(), "missing.log"), 64, func([]byte) bool { return true }); !os.IsNotExist(err) {
		t.Fatalf("missing file must report not-exist, got %v", err)
	}
}

func benchAuditFile(b *testing.B) (*apiServer, auditFilter) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "audit.log")
	var buf bytes.Buffer
	for buf.Len() < 5<<20 { // the default rotation size
		fmt.Fprintf(&buf, `{"ts":"2026-11-23T10:00:00Z","action":"http.request","success":true,"user":"alice","client":"10.0.0.%d","path":"/api/v1/runs","method":"POST","request_id":"abcdef0123456789","client_kind":"ui","ui_version":"2.4.0"}`+"\n", buf.Len()%250)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		b.Fatal(err)
	}
	return &apiServer{auditLogPath: path}, auditFilter{}
}

// BenchmarkAuditEntriesNewest100 is the audit page's normal request: the newest
// 100 entries of a full (5 MiB) log.
func BenchmarkAuditEntriesNewest100(b *testing.B) {
	s, f := benchAuditFile(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := s.auditEntries(100, f)
		if err != nil || len(out) != 100 {
			b.Fatalf("got %d entries, err %v", len(out), err)
		}
	}
}

// BenchmarkAuditEntriesWholeFile is the previous implementation (read and split the
// whole file), kept so the improvement stays measurable.
func BenchmarkAuditEntriesWholeFile(b *testing.B) {
	s, f := benchAuditFile(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := os.ReadFile(s.auditLogPath)
		if err != nil {
			b.Fatal(err)
		}
		lines := bytes.Split(data, []byte{'\n'})
		n := 0
		for j := len(lines) - 1; j >= 0 && n < 100; j-- {
			ln := bytes.TrimSpace(lines[j])
			if len(ln) == 0 {
				continue
			}
			var entry map[string]interface{}
			if json.Unmarshal(ln, &entry) != nil || !f.matches(entry) {
				continue
			}
			n++
		}
		if n != 100 {
			b.Fatalf("got %d", n)
		}
	}
}
