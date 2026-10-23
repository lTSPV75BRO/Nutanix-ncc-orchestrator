package main

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"
)

// errStateLockTimeout is returned when another process (or replica) holds a
// shared state lock for longer than the caller is willing to wait.
var errStateLockTimeout = errors.New("timed out waiting for a shared state lock held by another process")

var stateTmpSeq atomic.Uint64

// writeFileAtomic writes data to a unique sibling temp file, fsyncs it, and
// renames it over path. The temp name is unique per process and call so two
// replicas writing the same state file on a shared volume can never rename
// each other's half-written file into place.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp := filepath.Join(dir, "."+filepath.Base(path)+"."+strconv.Itoa(os.Getpid())+"."+
		strconv.FormatUint(stateTmpSeq.Add(1), 10)+"."+strconv.FormatInt(time.Now().UnixNano(), 36)+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
