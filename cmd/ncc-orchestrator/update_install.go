package main

// Cross-platform, transactional installation of updated stack files.
//
// Root cause of "software upgrade from the UI does not work on Windows":
//
//   - installStackComponents replaced bin\ncc-api-server.exe, bin\ncc-ui-server.exe
//     and bin\ncc-orchestrator.exe with os.Rename(tmp, dst). On Windows,
//     MoveFileEx(MOVEFILE_REPLACE_EXISTING) onto an executable image that is
//     currently running fails with "Access is denied". During an in-app
//     update all three are running (the api-server is the process that
//     launched the update), so the update failed — after frontend-dist had
//     already been deleted and some files replaced, leaving a mixed install.
//   - installDownloadedBinary on Windows only wrote "<exe>.new.exe" and a
//     helper script, printed "exit this program, then run …", and returned
//     nil. Callers (including the api-server's update job) treated that as a
//     successful install.
//
// Windows does allow *renaming* a running executable (the loader opens the
// image with FILE_SHARE_DELETE), so the portable approach is: move the
// existing file aside, move the new file into place, and remove the old file
// later (it cannot be deleted while the old process still runs). Every step
// is recorded so a failure part-way rolls back to the previous files instead
// of leaving the install inconsistent. Transient sharing violations (an
// antivirus scanner or a request reading a frontend file) are retried with
// backoff.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// updateRename is the rename primitive; tests replace it to simulate Windows
// semantics (rename onto a running executable fails, renaming it away works).
var updateRename = os.Rename

// updateRetrySleep is the backoff primitive (overridden by tests).
var updateRetrySleep = time.Sleep

const (
	updateAsideMarker  = ".ncc-old-"
	updateRenameTries  = 8
	updateRetryInitial = 100 * time.Millisecond
)

// errManualSwapRequired is returned when a new binary could be staged but not
// activated automatically. It must never be reported as a successful update.
var errManualSwapRequired = errors.New("the new binary was downloaded but could not be activated automatically")

func renameWithRetry(from, to string) error {
	var err error
	delay := updateRetryInitial
	for attempt := 0; attempt < updateRenameTries; attempt++ {
		if err = updateRename(from, to); err == nil {
			return nil
		}
		if !isTransientFileError(err) {
			return err
		}
		updateRetrySleep(delay)
		if delay < 2*time.Second {
			delay *= 2
		}
	}
	return err
}

// isTransientFileError reports errors worth retrying: Windows sharing and
// lock violations (often an antivirus scanner holding a new file for a moment)
// and access-denied while another process has the file open.
func isTransientFileError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrPermission) {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, s := range []string{"being used by another process", "sharing violation", "lock violation", "access is denied", "text file busy", "resource busy"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

type updateStep struct {
	dst   string // final path
	aside string // where the previous file/dir was moved ("" = did not exist)
}

// installTxn records replaced files so a failed update can be rolled back.
type installTxn struct {
	stamp string
	steps []updateStep
}

func newInstallTxn() *installTxn {
	return &installTxn{stamp: strconv.FormatInt(time.Now().UnixNano(), 36)}
}

func (t *installTxn) asidePath(dst string) string {
	return dst + updateAsideMarker + t.stamp
}

// replace moves staged into dst, keeping the previous dst for rollback.
//
//   - Regular file on POSIX: the old file is preserved via a hard link (or a
//     copy) and the new file is renamed over dst atomically, so dst never
//     disappears even briefly.
//   - Windows, directories, or when the atomic rename is refused: the
//     existing dst is moved aside first (allowed for a running .exe on
//     Windows) and the staged file/dir is moved into place.
func (t *installTxn) replace(staged, dst string) error {
	info, statErr := os.Lstat(dst)
	exists := statErr == nil
	if exists && !info.IsDir() && runtime.GOOS != "windows" {
		aside := t.asidePath(dst)
		if err := os.Link(dst, aside); err != nil {
			if cerr := copyFilePlain(dst, aside, info.Mode().Perm()); cerr != nil {
				return fmt.Errorf("back up %s: %w", filepath.Base(dst), cerr)
			}
		}
		err := renameWithRetry(staged, dst)
		if err == nil {
			t.steps = append(t.steps, updateStep{dst: dst, aside: aside})
			return nil
		}
		_ = os.Remove(aside)
		if !isTransientFileError(err) {
			return fmt.Errorf("activate %s: %w", filepath.Base(dst), err)
		}
	}
	aside := ""
	if exists {
		aside = t.asidePath(dst)
		if err := renameWithRetry(dst, aside); err != nil {
			return fmt.Errorf("move %s aside: %w", filepath.Base(dst), err)
		}
	}
	if err := renameWithRetry(staged, dst); err != nil {
		if aside != "" {
			_ = renameWithRetry(aside, dst)
		}
		return fmt.Errorf("activate %s: %w", filepath.Base(dst), err)
	}
	t.steps = append(t.steps, updateStep{dst: dst, aside: aside})
	return nil
}

func copyFilePlain(src, dst string, mode os.FileMode) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, mode)
}

// rollback restores every replaced file in reverse order. It returns the
// paths it could not restore (so the caller can report them precisely).
func (t *installTxn) rollback() []string {
	var failed []string
	for i := len(t.steps) - 1; i >= 0; i-- {
		st := t.steps[i]
		if st.aside == "" {
			// The file did not exist before this update: remove it again
			// (best-effort; a running image cannot be removed on Windows).
			_ = os.RemoveAll(st.dst)
			continue
		}
		broken := st.dst + updateAsideMarker + "failed-" + t.stamp
		if _, err := os.Lstat(st.dst); err == nil {
			if err := renameWithRetry(st.dst, broken); err != nil {
				failed = append(failed, st.dst)
				continue
			}
		}
		if err := renameWithRetry(st.aside, st.dst); err != nil {
			failed = append(failed, st.dst)
			continue
		}
		_ = os.RemoveAll(broken)
	}
	t.steps = nil
	return failed
}

// commit removes the moved-aside originals. Files still locked by a running
// process (the old executables on Windows) are left behind and removed by
// cleanupStaleUpdateArtifacts on a later update or start.
func (t *installTxn) commit() {
	for _, st := range t.steps {
		if st.aside != "" {
			_ = os.RemoveAll(st.aside)
		}
	}
	t.steps = nil
}

// cleanupStaleUpdateArtifacts removes leftovers of earlier updates
// (moved-aside binaries and frontend bundles, staged temp files) from the
// install dir and its bin/ directory. Best-effort.
func cleanupStaleUpdateArtifacts(installDir string) {
	for _, dir := range []string{installDir, filepath.Join(installDir, "bin")} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			name := e.Name()
			if strings.Contains(name, updateAsideMarker) || strings.HasPrefix(name, ".ncc-update-") || strings.HasPrefix(name, ".ncc-stage-") {
				_ = os.RemoveAll(filepath.Join(dir, name))
			}
		}
	}
}

// stageFileCopy copies src next to dst (same directory, so the final rename
// is on one volume) and returns the staged path.
func stageFileCopy(src, dst string, mode os.FileMode) (string, error) {
	data, err := os.ReadFile(src)
	if err != nil {
		return "", err
	}
	return stageBytes(data, dst, mode)
}

func stageBytes(data []byte, dst string, mode os.FileMode) (string, error) {
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(dir, ".ncc-update-")
	if err != nil {
		return "", err
	}
	name := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return "", err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return "", err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return "", err
	}
	if err := os.Chmod(name, mode); err != nil && runtime.GOOS != "windows" {
		_ = os.Remove(name)
		return "", err
	}
	return name, nil
}

// replaceRunningExecutable atomically swaps the running executable for body.
// On Windows the running image is moved aside first. Returns
// errManualSwapRequired (wrapped) only when no automatic swap was possible.
func replaceRunningExecutable(selfPath string, body []byte) error {
	staged, err := stageBytes(body, selfPath, 0o755)
	if err != nil {
		return fmt.Errorf("stage new binary: %w", err)
	}
	txn := newInstallTxn()
	if err := txn.replace(staged, selfPath); err != nil {
		_ = os.Remove(staged)
		return err
	}
	txn.commit()
	return nil
}
