package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSummaryIsDeterministicAndParsable(t *testing.T) {
	a := summary("127.0.1.1", 30, 5)
	if a != summary("127.0.1.1", 30, 5) {
		t.Fatal("summary should be deterministic per host")
	}
	if got := strings.Count(a, "Detailed information for "); got != 30 {
		t.Fatalf("findings = %d", got)
	}
	for _, want := range []string{"Cluster Name: cl-127-0-1-1", "| Total Plugins | 35 |", "Refer to KB"} {
		if !strings.Contains(a, want) {
			t.Fatalf("missing %q", want)
		}
	}
}

func TestMakeConfigListsLoopbackClusters(t *testing.T) {
	dir := t.TempDir()
	if err := makeConfig(dir, 501, 50); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(filepath.Join(dir, "clusters.txt"))
	lines := strings.Fields(string(b))
	if len(lines) != 501 || lines[0] != "127.0.1.1" || lines[250] != "127.0.2.1" || lines[500] != "127.0.3.1" {
		t.Fatalf("clusters = %d first=%q", len(lines), lines[0])
	}
	cfg, _ := os.ReadFile(filepath.Join(dir, "config.yaml"))
	if !strings.Contains(string(cfg), "max-parallel: 50") || strings.Contains(string(cfg), "password:") {
		t.Fatalf("config = %s", cfg)
	}
}
