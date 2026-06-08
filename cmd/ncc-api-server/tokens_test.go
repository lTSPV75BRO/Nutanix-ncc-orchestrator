package main

import (
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

func newTokenTestServer(t *testing.T) *apiServer {
	t.Helper()
	dir := t.TempDir()
	db, err := openUserDB(filepath.Join(dir, "users.json"))
	if err != nil {
		t.Fatalf("openUserDB: %v", err)
	}
	return &apiServer{users: db, usersDBPath: db.path}
}

func addLocalAccount(t *testing.T, db *userDB, name string, role Role) {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte("pw-"+name), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	if err := db.upsertUser(name, string(hash), role, false); err != nil {
		t.Fatalf("upsertUser: %v", err)
	}
}

// mintToken stores a PAT for owner and returns the plaintext secret.
func mintToken(t *testing.T, s *apiServer, owner string, ownerLocal bool, role Role, expires string) (string, string) {
	t.Helper()
	secret := patPrefix + "secret-" + owner + "-" + role.String()
	id := "id-" + owner + "-" + role.String()
	pt := personalToken{
		ID:         id,
		Name:       "test",
		Owner:      owner,
		OwnerLocal: ownerLocal,
		Role:       role.String(),
		Hash:       patHash(secret),
		CreatedAt:  time.Now().UTC().Format(time.RFC3339),
		ExpiresAt:  expires,
	}
	if err := s.users.addToken(pt); err != nil {
		t.Fatalf("addToken: %v", err)
	}
	return secret, id
}

func reqWithToken(secret string) *http.Request {
	r, _ := http.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	r.Header.Set("X-API-Token", secret)
	return r
}

func TestPATResolvesLocalOwnerLiveRole(t *testing.T) {
	s := newTokenTestServer(t)
	addLocalAccount(t, s.users, "alice", RoleViewer)
	secret, _ := mintToken(t, s, "alice", true, RoleViewer, "")

	p, ok := s.principalFromPAT(reqWithToken(secret))
	if !ok || p.method != authPAT || p.subject != "alice" || p.role != RoleViewer {
		t.Fatalf("expected viewer alice via PAT, got ok=%v subject=%q role=%v", ok, p.subject, p.role)
	}

	// Promote alice; the live role must be re-resolved on the next request even
	// though the token snapshot still says viewer.
	addLocalAccount(t, s.users, "alice", RoleOperator)
	p, ok = s.principalFromPAT(reqWithToken(secret))
	if !ok || p.role != RoleOperator {
		t.Fatalf("expected re-resolved operator role, got ok=%v role=%v", ok, p.role)
	}
}

func TestPATRejectedWhenLocalOwnerDeleted(t *testing.T) {
	s := newTokenTestServer(t)
	addLocalAccount(t, s.users, "bob", RoleOperator)
	secret, _ := mintToken(t, s, "bob", true, RoleOperator, "")

	if _, ok := s.principalFromPAT(reqWithToken(secret)); !ok {
		t.Fatal("token should work before deletion")
	}
	if err := s.users.deleteUser("bob"); err != nil {
		t.Fatalf("deleteUser: %v", err)
	}
	if _, ok := s.principalFromPAT(reqWithToken(secret)); ok {
		t.Fatal("token must be rejected after owner account is deleted")
	}
}

func TestPATRejectedWhenExpired(t *testing.T) {
	s := newTokenTestServer(t)
	addLocalAccount(t, s.users, "carol", RoleAdmin)
	past := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	secret, _ := mintToken(t, s, "carol", true, RoleAdmin, past)

	if _, ok := s.principalFromPAT(reqWithToken(secret)); ok {
		t.Fatal("expired token must be rejected")
	}
}

func TestPATNonLocalOwnerUsesSnapshotRole(t *testing.T) {
	s := newTokenTestServer(t)
	// No local account: an AD/SAML-minted token relies on its snapshot role.
	secret, _ := mintToken(t, s, "ad\\dave", false, RoleOperator, "")
	p, ok := s.principalFromPAT(reqWithToken(secret))
	if !ok || p.role != RoleOperator || p.subject != "ad\\dave" {
		t.Fatalf("expected snapshot operator for AD owner, got ok=%v role=%v subject=%q", ok, p.role, p.subject)
	}
}

func TestPATBearerHeaderAccepted(t *testing.T) {
	s := newTokenTestServer(t)
	addLocalAccount(t, s.users, "erin", RoleViewer)
	secret, _ := mintToken(t, s, "erin", true, RoleViewer, "")
	r, _ := http.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	r.Header.Set("Authorization", "Bearer "+secret)
	if _, ok := s.principalFromPAT(r); !ok {
		t.Fatal("PAT presented via Authorization: Bearer must be accepted")
	}
}

func TestPATDeleteOwnershipRules(t *testing.T) {
	s := newTokenTestServer(t)
	addLocalAccount(t, s.users, "frank", RoleViewer)
	_, id := mintToken(t, s, "frank", true, RoleViewer, "")

	// A different non-admin user cannot revoke frank's token.
	if _, _, err := s.users.deleteToken(id, "mallory", false); err != errTokenForbidden {
		t.Fatalf("expected errTokenForbidden, got %v", err)
	}
	// An admin can revoke any token.
	if _, removed, err := s.users.deleteToken(id, "admin", true); err != nil || !removed {
		t.Fatalf("admin revoke failed: removed=%v err=%v", removed, err)
	}
	if _, ok := s.users.findTokenByHash(patHash(patPrefix + "secret-frank-viewer")); ok {
		t.Fatal("token should be gone after revoke")
	}
}

func TestPATPerOwnerCap(t *testing.T) {
	s := newTokenTestServer(t)
	addLocalAccount(t, s.users, "grace", RoleViewer)
	for i := 0; i < maxTokensPerOwner; i++ {
		pt := personalToken{ID: randID(t, i), Name: "t", Owner: "grace", OwnerLocal: true, Role: "viewer", Hash: patHash("h" + randID(t, i))}
		if err := s.users.addToken(pt); err != nil {
			t.Fatalf("addToken %d: %v", i, err)
		}
	}
	over := personalToken{ID: "over", Name: "t", Owner: "grace", OwnerLocal: true, Role: "viewer", Hash: patHash("over")}
	if err := s.users.addToken(over); err == nil {
		t.Fatal("expected per-owner cap to reject the extra token")
	}
}

func randID(t *testing.T, i int) string {
	t.Helper()
	return "id-" + time.Now().Format("150405.000000000") + "-" + string(rune('a'+i%26)) + string(rune('0'+i%10))
}
