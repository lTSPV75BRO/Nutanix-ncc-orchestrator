package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"log"
	"time"
)

// Concurrency control for the user store.
//
// Every API replica (Helm default: 2) keeps the whole user document in memory
// and used to overwrite the stored document with that copy on every change,
// refreshing from the backend at most every 2s. A change on one replica could
// therefore be silently undone by any write on another within that window —
// including the per-minute PAT last-used update — reverting deletions,
// demotions, token revocations, or "sign out everywhere".
//
// Now each mutation (1) takes a cross-process lock where the backend has one
// (file store), (2) reloads the latest stored document, (3) applies the
// change, and (4) saves conditionally where the backend supports it
// (Kubernetes Secret: resourceVersion precondition). A conflicting concurrent
// write surfaces as errStoreConflict instead of a lost update.

// errStoreConflict means another replica changed the user store between this
// replica's reload and its save. Nothing was written; the caller may retry.
var errStoreConflict = errors.New("the user store was changed by another API replica at the same time; nothing was saved — retry the action")

// versionedBackend is implemented by backends that can make a save
// conditional on the stored version being the one last loaded.
type versionedBackend interface {
	loadVersioned() (data []byte, version string, err error)
	// saveVersioned writes data only if the stored version is still
	// ifVersion (errStoreConflict otherwise) and returns the new version.
	saveVersioned(data []byte, ifVersion string) (newVersion string, err error)
}

// lockingBackend is implemented by backends that can serialize writers
// across processes.
type lockingBackend interface {
	lockForWrite() (release func(), err error)
}

const userStoreLockTimeout = 10 * time.Second

func backendLoad(be userStoreBackend) ([]byte, string, error) {
	if v, ok := be.(versionedBackend); ok {
		return v.loadVersioned()
	}
	data, err := be.load()
	return data, "", err
}

func backendSave(be userStoreBackend, data []byte, ifVersion string) (string, error) {
	if v, ok := be.(versionedBackend); ok && ifVersion != "" {
		return v.saveVersioned(data, ifVersion)
	}
	return "", be.save(data)
}

// beginWriteLocked prepares a mutation of the user store. Caller holds db.mu
// (write lock) and must call the returned release function after saving —
// typically `defer db.beginWriteLocked()()` right after `defer db.mu.Unlock()`.
func (db *userDB) beginWriteLocked() func() {
	release := func() {}
	if db == nil || db.backend == nil {
		return release
	}
	if lb, ok := db.backend.(lockingBackend); ok {
		r, err := lb.lockForWrite()
		if err != nil {
			// Proceed without the lock rather than block the action; the
			// reload below still narrows the window to milliseconds.
			log.Printf("user store: write lock unavailable: %v", err)
		} else {
			release = r
		}
	}
	db.reloadLocked()
	return release
}

// reloadLocked applies the latest stored document if it differs from the
// in-memory one. Caller holds db.mu.
func (db *userDB) reloadLocked() {
	raw, version, err := backendLoad(db.backend)
	if err != nil {
		log.Printf("user store: reload before write failed: %v", err)
		return
	}
	if len(raw) == 0 {
		return
	}
	sum := sha256.Sum256(raw)
	if db.hasHash && sum == db.loadedHash {
		db.version = version
		return
	}
	var f usersDBFile
	if err := json.Unmarshal(raw, &f); err != nil {
		log.Printf("user store: reload before write: parse: %v", err)
		return
	}
	if err := db.applyDocument(f); err != nil {
		log.Printf("user store: reload before write: %v", err)
		return
	}
	db.loadedHash = sum
	db.hasHash = true
	db.version = version
}
