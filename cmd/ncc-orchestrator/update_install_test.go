package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// windowsLikeFS simulates Windows file semantics on any OS: a file that is a
// running executable image cannot be replaced by rename ("Access is denied"),
// but it can itself be renamed away (the loader holds FILE_SHARE_DELETE).
type windowsLikeFS struct {
	mu        sync.Mutex
	running   map[string]bool
	failOnce  map[string]int // dst -> number of transient failures to inject
	hardFail  map[string]bool
	callCount int
}

func (w *windowsLikeFS) rename(from, to string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.callCount++
	if w.hardFail[to] && !strings.Contains(from, updateAsideMarker) {
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: errors.New("The system cannot find the path specified.")}
	}
	if n := w.failOnce[to]; n > 0 {
		w.failOnce[to] = n - 1
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: errors.New("The process cannot access the file because it is being used by another process.")}
	}
	if w.running[to] {
		if _, err := os.Lstat(to); err == nil {
			return &os.LinkError{Op: "rename", Old: from, New: to, Err: errors.New("Access is denied.")}
		}
	}
	if err := os.Rename(from, to); err != nil {
		return err
	}
	if w.running[from] {
		delete(w.running, from)
		w.running[to] = true
	}
	return nil
}

func useWindowsLikeFS(t *testing.T) *windowsLikeFS {
	t.Helper()
	fs := &windowsLikeFS{running: map[string]bool{}, failOnce: map[string]int{}, hardFail: map[string]bool{}}
	prevRename, prevSleep := updateRename, updateRetrySleep
	updateRename = fs.rename
	updateRetrySleep = func(time.Duration) {}
	t.Cleanup(func() { updateRename, updateRetrySleep = prevRename, prevSleep })
	return fs
}

func writeTree(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for rel, content := range files {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o755); err != nil {
			t.Fatal(err)
		}
	}
}

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read %s: %v", p, err)
	}
	return string(b)
}

func stackFixture(t *testing.T) (src, dst string) {
	t.Helper()
	src, dst = t.TempDir(), t.TempDir()
	writeTree(t, src, map[string]string{
		"bin/ncc-api-server.exe":    "api v2",
		"bin/ncc-ui-server.exe":     "ui v2",
		"bin/ncc-orchestrator.exe":  "orch v2",
		"frontend-dist/index.html":  "new index",
		"frontend-dist/assets/a.js": "new js",
		"example_config.yaml":       "cfg v2",
	})
	writeTree(t, dst, map[string]string{
		"bin/ncc-api-server.exe":   "api v1",
		"bin/ncc-ui-server.exe":    "ui v1",
		"bin/ncc-orchestrator.exe": "orch v1",
		"frontend-dist/index.html": "old index",
		"example_config.yaml":      "cfg v1",
		"config.yaml":              "user config",
	})
	return src, dst
}

// The reported failure: during an in-app update on Windows every stack binary
// is running, so replacing them in place fails. The transactional installer
// moves the running images aside and succeeds.
func TestInstallStackComponentsReplacesRunningBinariesWindowsSemantics(t *testing.T) {
	fs := useWindowsLikeFS(t)
	src, dst := stackFixture(t)
	for _, b := range []string{"ncc-api-server.exe", "ncc-ui-server.exe", "ncc-orchestrator.exe"} {
		fs.running[filepath.Join(dst, "bin", b)] = true
	}
	if err := installStackComponents(src, dst); err != nil {
		t.Fatalf("install with running binaries failed: %v", err)
	}
	for rel, want := range map[string]string{
		"bin/ncc-api-server.exe":    "api v2",
		"bin/ncc-ui-server.exe":     "ui v2",
		"bin/ncc-orchestrator.exe":  "orch v2",
		"frontend-dist/index.html":  "new index",
		"frontend-dist/assets/a.js": "new js",
		"example_config.yaml":       "cfg v2",
		"config.yaml":               "user config",
	} {
		if got := readFile(t, filepath.Join(dst, filepath.FromSlash(rel))); got != want {
			t.Fatalf("%s = %q, want %q", rel, got, want)
		}
	}
	assertNoStagingLeftovers(t, dst)
}

// A failure part-way through must restore every file already replaced, so a
// failed upgrade never leaves a mixed old/new install.
func TestInstallStackComponentsRollsBackOnFailure(t *testing.T) {
	fs := useWindowsLikeFS(t)
	src, dst := stackFixture(t)
	fs.running[filepath.Join(dst, "bin", "ncc-api-server.exe")] = true
	// frontend-dist cannot be activated (hard error) after the binaries were swapped.
	fs.hardFail[filepath.Join(dst, "frontend-dist")] = true
	err := installStackComponents(src, dst)
	if err == nil {
		t.Fatal("expected the install to fail")
	}
	if !strings.Contains(err.Error(), "previous files restored") {
		t.Fatalf("error should say the install was rolled back: %v", err)
	}
	for rel, want := range map[string]string{
		"bin/ncc-api-server.exe":   "api v1",
		"bin/ncc-ui-server.exe":    "ui v1",
		"bin/ncc-orchestrator.exe": "orch v1",
		"frontend-dist/index.html": "old index",
		"example_config.yaml":      "cfg v1",
	} {
		if got := readFile(t, filepath.Join(dst, filepath.FromSlash(rel))); got != want {
			t.Fatalf("after rollback %s = %q, want %q", rel, got, want)
		}
	}
	assertNoStagingLeftovers(t, dst)
}

func TestReplaceRetriesTransientSharingViolations(t *testing.T) {
	fs := useWindowsLikeFS(t)
	dir := t.TempDir()
	dst := filepath.Join(dir, "ncc-orchestrator.exe")
	writeTree(t, dir, map[string]string{"ncc-orchestrator.exe": "old"})
	fs.running[dst] = true
	staged, err := stageBytes([]byte("new"), dst, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	fs.failOnce[dst] = 2 // antivirus holds the file briefly
	txn := newInstallTxn()
	if err := txn.replace(staged, dst); err != nil {
		t.Fatalf("transient errors should be retried: %v", err)
	}
	txn.commit()
	if readFile(t, dst) != "new" {
		t.Fatal("replacement not applied")
	}
}

func TestReplaceRunningExecutable(t *testing.T) {
	fs := useWindowsLikeFS(t)
	dir := t.TempDir()
	self := filepath.Join(dir, "ncc-orchestrator.exe")
	writeTree(t, dir, map[string]string{"ncc-orchestrator.exe": "v1"})
	fs.running[self] = true
	if err := replaceRunningExecutable(self, []byte("v2")); err != nil {
		t.Fatalf("self-replacement failed: %v", err)
	}
	if readFile(t, self) != "v2" {
		t.Fatal("self not replaced")
	}
}

func TestCleanupStaleUpdateArtifacts(t *testing.T) {
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{
		"bin/ncc-api-server.exe.ncc-old-abc": "old",
		"bin/.ncc-update-123":                "tmp",
		".ncc-stage-frontend-x/index.html":   "x",
		"frontend-dist.ncc-old-abc/i.html":   "x",
		"bin/ncc-api-server.exe":             "keep",
		"config.yaml":                        "keep",
	})
	cleanupStaleUpdateArtifacts(dir)
	assertNoStagingLeftovers(t, dir)
	if readFile(t, filepath.Join(dir, "bin", "ncc-api-server.exe")) != "keep" || readFile(t, filepath.Join(dir, "config.yaml")) != "keep" {
		t.Fatal("cleanup removed live files")
	}
}

func assertNoStagingLeftovers(t *testing.T, root string) {
	t.Helper()
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		name := info.Name()
		if strings.HasPrefix(name, ".ncc-update-") || strings.HasPrefix(name, ".ncc-stage-") || strings.Contains(name, updateAsideMarker) {
			t.Errorf("leftover update artifact: %s", p)
		}
		return nil
	})
}
