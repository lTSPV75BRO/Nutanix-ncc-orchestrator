package main

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

// Low bcrypt costs keep the suite fast; the production value is asserted in
// TestPasswordHashCostDefault through this copy.
var productionPasswordHashCost = passwordHashCost

func TestMain(m *testing.M) {
	passwordHashCost = bcrypt.MinCost
	os.Exit(m.Run())
}

func TestPasswordHashCostDefault(t *testing.T) {
	if productionPasswordHashCost < 12 {
		t.Fatalf("bcrypt cost %d is below 12", productionPasswordHashCost)
	}
	if c, err := bcrypt.Cost(dummyHash(productionPasswordHashCost)); err != nil || c != productionPasswordHashCost {
		t.Fatalf("dummy hash: cost %d, err %v", c, err)
	}
}

// withPasswordHashCost runs the rest of the test at the given cost.
func withPasswordHashCost(t *testing.T, cost int) {
	t.Helper()
	old := passwordHashCost
	passwordHashCost = cost
	t.Cleanup(func() { passwordHashCost = old })
}

func TestSignInUpgradesAnOlderPasswordHash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "users.json")
	db, err := openUserDB(path)
	if err != nil {
		t.Fatal(err)
	}
	old, _ := hashPassword("carol-strong-password")
	if err := db.upsertUser("carol", old, RoleOperator, true); err != nil {
		t.Fatal(err)
	}
	before, _ := db.lookup("carol")
	withPasswordHashCost(t, bcrypt.MinCost+2)

	if _, ok, _ := db.verify("carol", "wrong"); ok {
		t.Fatal("wrong password accepted")
	}
	if a, _ := db.lookup("carol"); a.PasswordHash != old {
		t.Fatal("a failed sign-in must not touch the hash")
	}

	// Concurrent sign-ins of the same account (the race detector covers verify).
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if role, ok, mustChange := db.verify("Carol", "carol-strong-password"); !ok || role != RoleOperator || !mustChange {
				t.Errorf("verify: role %v ok %v mustChange %v", role, ok, mustChange)
			}
		}()
	}
	wg.Wait()

	after, _ := db.lookup("carol")
	if c, _ := bcrypt.Cost([]byte(after.PasswordHash)); c != passwordHashCost {
		t.Fatalf("hash cost %d, want %d", c, passwordHashCost)
	}
	if after.TokenGen != before.TokenGen || !after.MustChange {
		t.Fatalf("an upgrade is not a password change: %+v -> %+v", before, after)
	}

	// Persisted, and a second sign-in leaves it alone.
	reopened, err := openUserDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := reopened.verify("carol", "carol-strong-password"); !ok {
		t.Fatal("sign-in after the upgrade failed")
	}
	if a, _ := reopened.lookup("carol"); a.PasswordHash != after.PasswordHash {
		t.Fatal("an up-to-date hash was rewritten")
	}
}

func TestPasswordHashUpgradeYieldsToAPasswordChange(t *testing.T) {
	db, err := openUserDB(filepath.Join(t.TempDir(), "users.json"))
	if err != nil {
		t.Fatal(err)
	}
	old, _ := hashPassword("old-password-1234")
	if err := db.upsertUser("dave", old, RoleViewer, false); err != nil {
		t.Fatal(err)
	}
	withPasswordHashCost(t, bcrypt.MinCost+1)
	changed, _ := hashPassword("new-password-1234")
	if err := db.setPassword("dave", changed, false); err != nil {
		t.Fatal(err)
	}
	// The upgrade of a sign-in that verified the old hash arrives late.
	db.upgradePasswordHash("dave", old, "old-password-1234")
	if a, _ := db.lookup("dave"); a.PasswordHash != changed {
		t.Fatal("the old password was written back over a newer one")
	}
}

func TestCookieDiagnostic(t *testing.T) {
	if d := (&apiServer{}).cookieDiagnostic(); d.Status != diagWarn || d.Hint == "" {
		t.Fatalf("plain HTTP: %+v", d)
	}
	if d := (&apiServer{cookieSecureForce: true}).cookieDiagnostic(); d.Status != diagOK {
		t.Fatalf("--cookie-secure: %+v", d)
	}
}
