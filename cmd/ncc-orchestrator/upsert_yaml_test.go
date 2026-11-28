package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const nestedConfig = `schema-version: 1
runner:
  credentials:
    username: "admin"

storage:
  output-dir: 'C:\ProgramData\NCC\v2\outputfiles'
  run-history-dir: 'C:\ProgramData\NCC\v2\outputfiles\runs'
secrets-provider: env
`

// Regression: upsertYAMLScalar replaced the whole line including its leading
// whitespace, so a restore (healRestoredConfigPaths) moved
// storage.run-history-dir out of its section, and a pattern that started at an
// earlier blank line could swallow it.
func TestUpsertYAMLScalarKeepsIndentation(t *testing.T) {
	got := upsertYAMLScalar(nestedConfig, "run-history-dir", "outputfiles/runs")
	want := "  run-history-dir: \"outputfiles/runs\"\n"
	if !strings.Contains(got, want) {
		t.Fatalf("indentation lost:\n%s", got)
	}
	if strings.Count(got, "run-history-dir:") != 1 {
		t.Fatalf("expected exactly one run-history-dir key:\n%s", got)
	}
	// Everything else is untouched, including the blank line before storage:.
	if !strings.Contains(got, "    username: \"admin\"\n\nstorage:\n") {
		t.Fatalf("surrounding lines changed:\n%s", got)
	}
}

func TestUpsertYAMLScalarTopLevelAndAppend(t *testing.T) {
	if got := upsertYAMLScalar("a: 1\nsecrets-provider: file\n", "secrets-provider", "env"); got != "a: 1\nsecrets-provider: \"env\"\n" {
		t.Fatalf("top-level replace: %q", got)
	}
	if got := upsertYAMLScalar("a: 1\n", "new-key", "x"); got != "a: 1\nnew-key: \"x\"\n" {
		t.Fatalf("append: %q", got)
	}
	// A "$" in the value must not be treated as a regexp group reference.
	if got := upsertYAMLScalar("k: old\n", "k", "a$1b"); got != "k: \"a$1b\"\n" {
		t.Fatalf("dollar in value: %q", got)
	}
}

func TestHealRestoredConfigPathsKeepsTheNestedSchema(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(nestedConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	changed, _ := healRestoredConfigPaths(dir, `C:\ProgramData\NCC\v2`)
	if len(changed) != 1 || changed[0] != "run-history-dir" {
		t.Fatalf("changed = %v", changed)
	}
	b, err := os.ReadFile(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "\n  run-history-dir: \"outputfiles/runs\"\n") {
		t.Fatalf("run-history-dir must stay under storage:\n%s", b)
	}
}
