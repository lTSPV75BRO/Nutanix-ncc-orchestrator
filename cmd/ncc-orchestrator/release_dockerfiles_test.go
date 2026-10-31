package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// releaseDockerfiles returns every Dockerfile that builds a shipped image
// (paths are relative to this package, cmd/ncc-orchestrator).
func releaseDockerfiles(t *testing.T) map[string]string {
	t.Helper()
	var paths []string
	for _, pattern := range []string{
		filepath.Join("..", "..", "build", "docker", "Dockerfile*"),
		filepath.Join("..", "..", "deploy", "docker", "Dockerfile*"),
	} {
		m, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, m...)
	}
	if len(paths) < 6 {
		t.Fatalf("expected the build/docker and deploy/docker Dockerfiles, found %v", paths)
	}
	out := map[string]string{}
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		// Join continuation lines so one RUN instruction is one string.
		s := strings.ReplaceAll(string(b), "\r\n", "\n")
		out[p] = strings.ReplaceAll(s, "\\\n", " ")
	}
	return out
}

// Regression: deploy/docker images saved the CA bundle before
// `apk add --upgrade ca-certificates` and copied it back afterwards, which
// reverted the trust-store update (eMudhra distrust) in published images.
func TestDockerfilesKeepUpgradedCABundle(t *testing.T) {
	overwrite := regexp.MustCompile(`\b(cp|mv|install)\b[^&;]*\s/etc/ssl/certs/ca-certificates\.crt\b`)
	for path, body := range releaseDockerfiles(t) {
		if m := overwrite.FindString(body); m != "" {
			t.Errorf("%s overwrites the system CA bundle (%q); that discards the ca-certificates upgrade", path, m)
		}
		for _, line := range strings.Split(body, "\n") {
			if !strings.Contains(line, "ncc-host-cas.pem >> /etc/ssl/certs/ca-certificates.crt") {
				continue
			}
			upgrade := strings.Index(line, "apk add --no-cache --upgrade ca-certificates")
			fix := strings.Index(line, "apk fix --no-cache ca-certificates-bundle")
			if upgrade < 0 || fix < upgrade {
				t.Errorf("%s appends host CAs but does not reinstall ca-certificates-bundle after upgrading ca-certificates", path)
			}
		}
	}
}

// Regression: go.mod moved to a newer patch release while the builder images
// stayed on an older one; official golang images set GOTOOLCHAIN=local, so
// `docker compose build` failed with "go.mod requires go >= …".
func TestDockerfileGoBuilderMatchesGoMod(t *testing.T) {
	mod, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	want := regexp.MustCompile(`(?m)^go (\d+\.\d+\.\d+)\s*$`).FindStringSubmatch(string(mod))
	if want == nil {
		t.Fatal("go.mod has no go directive with a patch version")
	}
	from := regexp.MustCompile(`(?m)^FROM golang:(\d+\.\d+\.\d+)`)
	seen := 0
	for path, body := range releaseDockerfiles(t) {
		for _, m := range from.FindAllStringSubmatch(body, -1) {
			seen++
			if versionLess(m[1], want[1]) {
				t.Errorf("%s builds with golang:%s but go.mod requires go %s", path, m[1], want[1])
			}
		}
	}
	if seen == 0 {
		t.Fatal("no golang builder stage found; update this test if the build moved")
	}
}
