package v2layout

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// TestDetectStackRoot covers the three accept/reject paths:
//   - exe in <X>/bin and <X> has frontend-dist  → accept
//   - exe in <X>/bin and <X> has bin/ncc-api-server (legacy/headless)  → accept
//   - exe NOT in a directory called "bin"  → reject
//   - exe in <X>/bin but <X> has neither marker  → reject
//
// Ensures the api-server / ui-server only opt into stack-aware mode
// when the layout is unambiguous, so a binary copied to /usr/local/bin
// (where the parent dir is /usr/local, not a stack root) still falls
// back to its pre-existing CWD-relative defaults.
func TestDetectStackRoot(t *testing.T) {
	tmp := t.TempDir()

	// Layout 1: <tmp>/stack/bin + <tmp>/stack/frontend-dist.
	stackA := filepath.Join(tmp, "stack-with-frontend")
	mustMkdir(t, filepath.Join(stackA, stackBinDir))
	mustMkdir(t, filepath.Join(stackA, stackFrontendDir))

	// Layout 2: <tmp>/headless/bin + <tmp>/headless/bin/ncc-api-server (no frontend).
	stackB := filepath.Join(tmp, "headless-stack")
	mustMkdir(t, filepath.Join(stackB, stackBinDir))
	mustWriteExe(t, filepath.Join(stackB, stackBinDir, apiServerBaseName))

	// Layout 3: <tmp>/lonely/bin (no markers).
	stackC := filepath.Join(tmp, "lonely-bin")
	mustMkdir(t, filepath.Join(stackC, stackBinDir))

	// Layout 4: <tmp>/random (not called bin at all).
	stackD := filepath.Join(tmp, "not-a-bin-dir")
	mustMkdir(t, stackD)

	cases := []struct {
		name     string
		exeDir   string
		wantOK   bool
		wantRoot string
	}{
		{"frontend-dist marker", filepath.Join(stackA, stackBinDir), true, stackA},
		{"api-server marker", filepath.Join(stackB, stackBinDir), true, stackB},
		{"bin without markers", filepath.Join(stackC, stackBinDir), false, ""},
		{"not a bin dir", stackD, false, ""},
		{"empty exeDir", "", false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := DetectStackRoot(tc.exeDir)
			if ok != tc.wantOK {
				t.Fatalf("ok=%v, want %v", ok, tc.wantOK)
			}
			if ok && got != tc.wantRoot {
				t.Fatalf("root=%q, want %q", got, tc.wantRoot)
			}
		})
	}
}

// TestFindBinary pins the dual-naming behavior: stack archives ship
// canonical names (bin/ncc-api-server) but legacy v2.0.0 stacks
// shipped platform-suffixed names (bin/ncc-api-server-<os>-<arch>).
// The api-server / ui-server should resolve either form.
func TestFindBinary(t *testing.T) {
	tmp := t.TempDir()
	binDir := filepath.Join(tmp, stackBinDir)
	mustMkdir(t, binDir)
	// Only ship the canonical form here; the suffixed form is tested
	// in a sibling subdir to avoid the canonical winning the lookup.
	mustWriteExe(t, filepath.Join(binDir, apiServerBaseName))

	if got := FindBinary(tmp, apiServerBaseName); got == "" {
		t.Fatal("expected to find canonical-named binary")
	}

	tmp2 := t.TempDir()
	binDir2 := filepath.Join(tmp2, stackBinDir)
	mustMkdir(t, binDir2)
	mustWriteExe(t, filepath.Join(binDir2, apiServerBaseName+"-"+runtime.GOOS+"-"+runtime.GOARCH))
	if got := FindBinary(tmp2, apiServerBaseName); got == "" {
		t.Fatal("expected to find platform-suffixed binary")
	}

	if got := FindBinary(t.TempDir(), apiServerBaseName); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

// TestConfigPath pins the v2.0.2 fallback: prefer config.yaml, fall
// back to example_config.yaml, return empty when neither exists.
// Callers (v2-check, api-server stack-aware mode) use this to give
// users a sane default when they extract the stack and run without
// having created their own config yet.
func TestConfigPath(t *testing.T) {
	tmp := t.TempDir()
	if got := ConfigPath(tmp); got != "" {
		t.Fatalf("empty install: got %q, want \"\"", got)
	}
	example := filepath.Join(tmp, exampleConfigName)
	if err := os.WriteFile(example, []byte("# example"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := ConfigPath(tmp); got != example {
		t.Fatalf("only example present: got %q, want %q", got, example)
	}
	cfg := filepath.Join(tmp, configFileName)
	if err := os.WriteFile(cfg, []byte("# real"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := ConfigPath(tmp); got != cfg {
		t.Fatalf("real config wins: got %q, want %q", got, cfg)
	}
}

// TestHasWindowsExecutableExt validates the PATHEXT-based extension
// check directly (OS-independent: the function only inspects the path
// string + PATHEXT env var, so it runs the same on every platform).
// This is the logic that prevents Windows from rejecting ncc-*.exe as
// "not executable".
func TestHasWindowsExecutableExt(t *testing.T) {
	// Force the default PATHEXT so the test is deterministic regardless
	// of the host environment.
	t.Setenv("PATHEXT", ".COM;.EXE;.BAT;.CMD")
	cases := []struct {
		path string
		want bool
	}{
		{`C:\stack\bin\ncc-orchestrator.exe`, true},
		{`C:\stack\bin\ncc-api-server.EXE`, true}, // case-insensitive
		{"ncc-ui-server.exe", true},
		{"run.bat", true},
		{"run.cmd", true},
		{"legacy.com", true},
		{"ncc-orchestrator", false}, // no extension (Unix-style name)
		{"report.yaml", false},
		{"archive.tar.gz", false},
		{"", false},
	}
	for _, c := range cases {
		if got := HasWindowsExecutableExt(c.path); got != c.want {
			t.Errorf("HasWindowsExecutableExt(%q) = %v, want %v", c.path, got, c.want)
		}
	}
}

// TestHasWindowsExecutableExt_RespectsPATHEXT confirms a custom PATHEXT
// (e.g. one that adds .PS1) is honored and that an extension absent
// from PATHEXT is rejected.
func TestHasWindowsExecutableExt_RespectsPATHEXT(t *testing.T) {
	t.Setenv("PATHEXT", ".EXE;.PS1")
	if !HasWindowsExecutableExt("script.ps1") {
		t.Error("expected .ps1 to be executable when present in PATHEXT")
	}
	if HasWindowsExecutableExt("legacy.com") {
		t.Error("expected .com to be rejected when absent from PATHEXT")
	}
}

// TestIsExecutable_RegularFile checks the cross-platform contract: a
// shipped binary in the stack is reported executable on the host OS,
// a directory never is, and a missing path never is.
func TestIsExecutable(t *testing.T) {
	tmp := t.TempDir()

	// A directory is never executable.
	if IsExecutable(tmp) {
		t.Error("IsExecutable(dir) = true, want false")
	}
	// A missing path is never executable.
	if IsExecutable(filepath.Join(tmp, "does-not-exist")) {
		t.Error("IsExecutable(missing) = true, want false")
	}

	// A real on-disk binary named like the shipped artifact for this
	// platform should be reported executable. On Windows that requires
	// the .exe suffix (PATHEXT); on Unix it requires the +x bit, which
	// mustWriteExe sets.
	name := "ncc-orchestrator"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	exe := filepath.Join(tmp, name)
	mustWriteExe(t, exe)
	if !IsExecutable(exe) {
		t.Errorf("IsExecutable(%q) = false, want true on %s", exe, runtime.GOOS)
	}
}

func mustMkdir(t *testing.T, p string) {
	t.Helper()
	if err := os.MkdirAll(p, 0o755); err != nil {
		t.Fatal(err)
	}
}

func mustWriteExe(t *testing.T, p string) {
	t.Helper()
	if err := os.WriteFile(p, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
}
