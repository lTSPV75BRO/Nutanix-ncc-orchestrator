package kblinks

import (
	"strings"
	"testing"
)

func TestAnnotateMarkdown(t *testing.T) {
	in := "Refer to KB 5582 (http://portal.nutanix.com/kb/5582) for details."
	out := AnnotateMarkdown(in)
	if out == in {
		t.Fatal("expected KB to be linked")
	}
	if !strings.Contains(out, "[KB 5582]") || !strings.Contains(out, "https://portal.nutanix.com/kb/5582") {
		t.Fatalf("unexpected output: %s", out)
	}
}
