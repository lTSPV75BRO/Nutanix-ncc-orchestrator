//go:build windows

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestUpdateHelperSleep is the child process for the native Windows test
// below; it only sleeps when asked to.
func TestUpdateHelperSleep(t *testing.T) {
	if os.Getenv("NCC_UPDATE_HELPER_SLEEP") != "1" {
		t.Skip("helper process only")
	}
	time.Sleep(60 * time.Second)
}

// Native Windows verification (runs on the windows-latest CI job): a running
// .exe cannot be overwritten by rename — the original bug — but the
// transactional installer replaces it.
func TestNativeWindowsReplaceRunningExe(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	exe := filepath.Join(dir, "bin", "ncc-api-server.exe")
	if err := os.MkdirAll(filepath.Dir(exe), 0o755); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(self)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(exe, body, 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestUpdateHelperSleep$")
	cmd.Env = append(os.Environ(), "NCC_UPDATE_HELPER_SLEEP=1")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() }()
	time.Sleep(500 * time.Millisecond)

	staged := filepath.Join(dir, "bin", "new.exe")
	if err := os.WriteFile(staged, []byte("new build"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(staged, exe); err == nil {
		t.Fatal("expected Windows to refuse replacing a running executable in place (root cause)")
	}
	src := t.TempDir()
	if err := os.MkdirAll(filepath.Join(src, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "bin", "ncc-api-server.exe"), []byte("new build"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := installStackComponents(src, dir); err != nil {
		t.Fatalf("installStackComponents with a running exe: %v", err)
	}
	got, err := os.ReadFile(exe)
	if err != nil || string(got) != "new build" {
		t.Fatalf("running exe not replaced: %q %v", got, err)
	}
}
