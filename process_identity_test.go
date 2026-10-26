package main

import (
	"os"
	"runtime"
	"testing"
)

func TestWindowsImageIdentity(t *testing.T) {
	self := os.Getpid()
	// Shell-hosted helper loops and subcommand-only identities stay unknown.
	if k, m := windowsImageIdentity(self, "v2-api-supervisor", "ncc-api-server"); k || !m {
		t.Fatalf("helper-loop identity: got known=%v matches=%v, want unknown", k, m)
	}
	if k, m := windowsImageIdentity(self, "v2-supervise"); k || !m {
		t.Fatalf("subcommand-only identity: got known=%v matches=%v, want unknown", k, m)
	}
	if runtime.GOOS != "windows" {
		// The image path is only queried on Windows.
		if k, _ := windowsImageIdentity(self, "ncc-api-server"); k {
			t.Fatal("non-Windows: image identity must be unknown")
		}
		return
	}
	// The test binary is not ncc-api-server: a pid file pointing here is a
	// reused pid, which Windows previously could not detect.
	if k, m := windowsImageIdentity(self, "ncc-api-server"); !k || m {
		t.Fatalf("unrelated image: got known=%v matches=%v, want known mismatch", k, m)
	}
	// Matching by executable base name (supervise.go passes os.Args[0]).
	if k, m := windowsImageIdentity(self, "ncc-orchestrator", os.Args[0]); !k || !m {
		t.Fatalf("own image: got known=%v matches=%v, want known match", k, m)
	}
}
