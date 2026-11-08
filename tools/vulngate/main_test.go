package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const scanHeader = `{"config":{"scanner_version":"v1.9.0","db":"https://vuln.go.dev"}}
{"osv":{"id":"GO-1","summary":"bad thing"}}
`

const calledSAML = `{"finding":{"osv":"GO-1","trace":[{"module":"github.com/crewjam/saml","version":"v0.6.3","package":"github.com/crewjam/saml","function":"ParseResponse"},{"module":"goncc","package":"goncc/cmd/x","function":"main","position":{"filename":"cmd/x/main.go","line":3}}]}}
`

const notCalled = `{"finding":{"osv":"GO-2","fixed_version":"v0.59.0","trace":[{"module":"golang.org/x/crypto","version":"v0.58.0"}]}}
`

func accept(module string, by string) map[string]accepted {
	d, _ := time.Parse("2006-01-02", by)
	return map[string]accepted{"GO-1": {module: module, reviewBy: d, reason: "fixed upstream", line: 1}}
}

func run(t *testing.T, input string, acc map[string]accepted, now string) (int, string) {
	t.Helper()
	n, _ := time.Parse("2006-01-02", now)
	var out strings.Builder
	code, err := gate(strings.NewReader(input), &out, acc, n)
	if err != nil {
		t.Fatal(err)
	}
	return code, out.String()
}

func TestUnacceptedCalledVulnFails(t *testing.T) {
	code, out := run(t, scanHeader+calledSAML, nil, "2026-11-08")
	if code != 3 || !strings.Contains(out, "FAIL   GO-1") || !strings.Contains(out, "cmd/x/main.go:3") {
		t.Fatalf("code %d\n%s", code, out)
	}
}

func TestAcceptedVulnPasses(t *testing.T) {
	code, out := run(t, scanHeader+calledSAML+notCalled, accept("github.com/crewjam/saml", "2027-01-31"), "2026-11-08")
	if code != 0 || !strings.Contains(out, "ACCEPT GO-1") || !strings.Contains(out, "info   GO-2") {
		t.Fatalf("code %d\n%s", code, out)
	}
}

func TestAcceptedEntryExpires(t *testing.T) {
	code, out := run(t, scanHeader+calledSAML, accept("github.com/crewjam/saml", "2026-11-01"), "2026-11-08")
	if code != 3 || !strings.Contains(out, "expired") {
		t.Fatalf("code %d\n%s", code, out)
	}
	// The review-by day itself still passes.
	if code, out := run(t, scanHeader+calledSAML, accept("github.com/crewjam/saml", "2026-11-08"), "2026-11-08"); code != 0 {
		t.Fatalf("code %d\n%s", code, out)
	}
}

func TestAcceptedForOtherModuleFails(t *testing.T) {
	code, out := run(t, scanHeader+calledSAML, accept("example.com/other", "2027-01-31"), "2026-11-08")
	if code != 3 || !strings.Contains(out, "accepted for example.com/other") {
		t.Fatalf("code %d\n%s", code, out)
	}
}

func TestStaleEntryIsReported(t *testing.T) {
	code, out := run(t, scanHeader, accept("github.com/crewjam/saml", "2027-01-31"), "2026-11-08")
	if code != 0 || !strings.Contains(out, "stale  GO-1") {
		t.Fatalf("code %d\n%s", code, out)
	}
}

func TestEmptyInputIsAnError(t *testing.T) {
	var out strings.Builder
	if _, err := gate(strings.NewReader(""), &out, nil, time.Now()); err == nil {
		t.Fatal("want error for missing govulncheck output")
	}
}

func TestReadAccepted(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "a.txt")
	os.WriteFile(p, []byte("# comment\n\nGO-1  github.com/crewjam/saml  2027-01-31  # fixed upstream\n"), 0o600)
	acc, err := readAccepted(p)
	if err != nil || acc["GO-1"].module != "github.com/crewjam/saml" || acc["GO-1"].reason != "fixed upstream" {
		t.Fatalf("%v %+v", err, acc)
	}
	os.WriteFile(p, []byte("GO-1 github.com/crewjam/saml 2027-01-31\n"), 0o600)
	if _, err := readAccepted(p); err == nil {
		t.Fatal("want error for an entry without a reason")
	}
	// The real file must parse.
	if _, err := readAccepted(filepath.Join("..", "..", "scripts", "govulncheck-accepted.txt")); err != nil {
		t.Fatal(err)
	}
}
