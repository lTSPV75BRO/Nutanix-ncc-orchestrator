package main

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// twoReplicas opens the same store twice, as two API replicas would.
func twoReplicas(t *testing.T, open func() userStoreBackend) (*userDB, *userDB) {
	t.Helper()
	a, err := openUserDBFromBackend(open())
	if err != nil {
		t.Fatal(err)
	}
	hash, err := hashPassword("pw-admin-1234")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.upsertUser("admin", hash, RoleAdmin, false); err != nil {
		t.Fatal(err)
	}
	if err := a.addToken(personalToken{ID: "tok1", Name: "ci", Owner: "admin", OwnerLocal: true, Role: "admin", Hash: "h1", CreatedAt: time.Now().UTC().Format(time.RFC3339)}); err != nil {
		t.Fatal(err)
	}
	b, err := openUserDBFromBackend(open())
	if err != nil {
		t.Fatal(err)
	}
	return a, b
}

// Regression (DATA-01): replica B, holding a stale copy, used to overwrite
// replica A's changes on its next unrelated write. Here A revokes a token and
// creates a user; B then records the token's last use (the hot write PATs make
// once a minute). Both of A's changes must survive.
func assertNoLostUpdate(t *testing.T, a, b *userDB, reopen func() *userDB) {
	t.Helper()
	if _, removed, err := a.deleteToken("tok1", "admin", true); err != nil || !removed {
		t.Fatalf("revoke on A: removed=%v err=%v", removed, err)
	}
	hash, _ := hashPassword("pw-carol-1234")
	if err := a.upsertUser("carol", hash, RoleViewer, false); err != nil {
		t.Fatal(err)
	}
	// B never refreshed; its in-memory copy still has tok1 and no carol.
	b.touchTokenLastUsed("tok1", "192.0.2.9")
	if err := b.setRunConfigPath("admin", "config.yaml"); err != nil {
		t.Fatalf("write on B: %v", err)
	}

	fresh := reopen()
	if _, ok := fresh.findTokenByHash("h1"); ok {
		t.Fatal("revoked token came back after a write on the other replica")
	}
	if _, ok := fresh.lookup("carol"); !ok {
		t.Fatal("user created on replica A was lost by replica B's write")
	}
	if got := fresh.getRunConfigPath("admin"); got != "config.yaml" {
		t.Fatalf("replica B's own change lost: run config = %q", got)
	}
}

func TestUserStoreNoLostUpdateFileBackend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "users.json")
	open := func() userStoreBackend { return &fileStoreBackend{path: path} }
	a, b := twoReplicas(t, open)
	assertNoLostUpdate(t, a, b, func() *userDB {
		db, err := openUserDB(path)
		if err != nil {
			t.Fatal(err)
		}
		return db
	})
}

func TestUserStoreNoLostUpdateK8sBackend(t *testing.T) {
	be, _ := newTestSecretBackend(t)
	open := func() userStoreBackend {
		c := *be
		return &c
	}
	a, b := twoReplicas(t, open)
	assertNoLostUpdate(t, a, b, func() *userDB {
		db, err := openUserDBFromBackend(open())
		if err != nil {
			t.Fatal(err)
		}
		return db
	})
}

// With the reload in place a conflict needs a write landing between reload
// and save; simulate it by saving a stale copy directly. The Kubernetes
// backend must refuse it (resourceVersion precondition) instead of
// overwriting the newer document.
func TestUserStoreK8sStaleSaveConflicts(t *testing.T) {
	be, _ := newTestSecretBackend(t)
	open := func() userStoreBackend {
		c := *be
		return &c
	}
	a, b := twoReplicas(t, open)
	if err := a.revokeSessions("admin"); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	b.accounts["admin"].RunConfigPath = "stale.yaml"
	err := b.saveLocked()
	b.mu.Unlock()
	if !errors.Is(err, errStoreConflict) {
		t.Fatalf("stale save: want errStoreConflict, got %v", err)
	}
	// After the conflict a normal mutation reloads and succeeds.
	if err := b.setRunConfigPath("admin", "config.yaml"); err != nil {
		t.Fatalf("retry after conflict: %v", err)
	}
	fresh, err := openUserDBFromBackend(open())
	if err != nil {
		t.Fatal(err)
	}
	acct, _ := fresh.lookup("admin")
	if acct.TokenGen == 0 {
		t.Fatal("A's session revocation was overwritten")
	}
}

func TestUserStoreEncryptedBackendPassesVersionThrough(t *testing.T) {
	be, _ := newTestSecretBackend(t)
	key := make([]byte, 32)
	open := func() userStoreBackend {
		c := *be
		return &encryptingBackend{inner: &c, key: key}
	}
	a, b := twoReplicas(t, open)
	assertNoLostUpdate(t, a, b, func() *userDB {
		db, err := openUserDBFromBackend(open())
		if err != nil {
			t.Fatal(err)
		}
		return db
	})
	if a.version == "" {
		t.Fatal("encrypting backend should expose the inner resourceVersion")
	}
}
