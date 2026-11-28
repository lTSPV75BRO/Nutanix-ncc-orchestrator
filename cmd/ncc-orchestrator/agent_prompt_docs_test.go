package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// promptBlock returns the text of the first ~~~text fenced block in a doc.
func promptBlock(t *testing.T, rel string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", filepath.FromSlash(rel)))
	if err != nil {
		t.Fatal(err)
	}
	s := strings.ReplaceAll(string(raw), "\r\n", "\n")
	const open = "~~~text\n"
	a := strings.Index(s, open)
	if a < 0 {
		t.Fatalf("%s has no ~~~text block", rel)
	}
	a += len(open)
	b := strings.Index(s[a:], "\n~~~\n")
	if b < 0 {
		t.Fatalf("%s: unterminated ~~~text block", rel)
	}
	return s[a : a+b]
}

// The agent prompt lives in two docs so a person can find it from either; the
// copies must not drift apart.
func TestAgentPromptCopiesAreIdentical(t *testing.T) {
	a := promptBlock(t, "docs/AGENT_PROMPT.md")
	b := promptBlock(t, "deploy/README.md")
	if a != b {
		t.Fatal("docs/AGENT_PROMPT.md and deploy/README.md contain different agent prompts; copy one block over the other")
	}
	for _, must := range []string{"# RULES", "# COST RULES", "# FAILURE AND RETRY", "# EDGE CASES", "Never invent a flag"} {
		if !strings.Contains(a, must) {
			t.Fatalf("prompt lost a required section: %s", must)
		}
	}
	// It is sent with every agent step, so keep it compact (words, not tokens).
	if words := len(strings.Fields(a)); words > 2400 {
		t.Fatalf("agent prompt is %d words; keep it under 2400 to keep AI cost low", words)
	}
}
