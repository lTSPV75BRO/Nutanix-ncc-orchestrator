package main

import (
	"bufio"
	crand "crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
	"golang.org/x/term"

	yaml "go.yaml.in/yaml/v3"
)

// account is a single local user record persisted in the user database.
type account struct {
	Username     string `json:"username" yaml:"username"`
	PasswordHash string `json:"password_hash,omitempty" yaml:"password_hash"`
	Role         string `json:"role" yaml:"role"`
	// RunConfigPath is the user's preferred config file for run trigger/preflight.
	RunConfigPath string `json:"run_config_path,omitempty" yaml:"-"`
	// MustChange forces a password change on next login before any other
	// action is allowed (used for the bootstrap admin and admin resets).
	MustChange bool `json:"must_change_password,omitempty" yaml:"must_change_password,omitempty"`
	// TokenGen is the session-token generation. It is bumped on every password
	// change/reset so that previously issued session tokens (which embed the
	// generation they were minted under) are immediately invalidated.
	TokenGen  int    `json:"token_gen,omitempty" yaml:"token_gen,omitempty"`
	CreatedAt string `json:"created_at,omitempty" yaml:"-"`
	UpdatedAt string `json:"updated_at,omitempty" yaml:"-"`
}

// sessionPolicy holds runtime, admin-tunable session settings persisted in the
// user database. TTLSeconds controls how long an issued session stays valid; a
// zero value means "use the server's --session-ttl default".
type sessionPolicy struct {
	TTLSeconds int `json:"ttl_seconds,omitempty"`
}

// tlsPolicy records the admin-managed HTTPS/TLS configuration applied from
// Settings → Access. The certificate and private key live on disk (referenced
// by CertPath/KeyPath, 0600) — never in this JSON — so the document stays safe
// to back up and the key is not duplicated. The remaining fields are decoded
// certificate metadata kept only so the UI can show what is installed without
// re-parsing the PEM. When HTTPSEnabled is true the api-server marks session
// cookies Secure and the stack (re)starts with the UI server bound to TLS.
type tlsPolicy struct {
	HTTPSEnabled bool     `json:"https_enabled,omitempty"`
	CertPath     string   `json:"cert_path,omitempty"`
	KeyPath      string   `json:"key_path,omitempty"`
	Subject      string   `json:"subject,omitempty"`
	Issuer       string   `json:"issuer,omitempty"`
	NotBefore    string   `json:"not_before,omitempty"`
	NotAfter     string   `json:"not_after,omitempty"`
	DNSNames     []string `json:"dns_names,omitempty"`
	UpdatedAt    string   `json:"updated_at,omitempty"`
}

// clusterGroup segregates clusters for membership-based access control. A
// cluster may belong to multiple groups (many-to-many). Membership is the union
// of local accounts (matched by username) and AD groups (matched by CN or full
// DN). Members may see and act on only the clusters in groups they belong to;
// admins and static-token callers are unrestricted (see allowedClusters).
type clusterGroup struct {
	Name string `json:"name"`
	// Clusters are explicit cluster names/addresses in the group.
	Clusters []string `json:"clusters,omitempty"`
	// PrismCentrals lists Prism Central URLs/addresses whose registered clusters
	// are dynamically folded into this group: every cluster managed by the PC is
	// granted to the group's members (discovered via the orchestrator and cached).
	PrismCentrals []string `json:"prism_centrals,omitempty"`
	LocalUsers    []string `json:"local_users,omitempty"` // local account usernames
	ADGroups      []string `json:"ad_groups,omitempty"`   // AD group CN or full DN
	ADUsers       []string `json:"ad_users,omitempty"`    // individual AD users (sAMAccountName/UPN)
}

// reservedAdminUsername is the built-in administrator account. Its role is
// hardcoded to admin: it can never be demoted, deleted, or loaded as anything
// other than admin (a tampered store is coerced back on load).
const reservedAdminUsername = "admin"

// isReservedAdmin reports whether the username is the built-in admin account
// (case-insensitive, whitespace-trimmed).
func isReservedAdmin(username string) bool {
	return strings.EqualFold(strings.TrimSpace(username), reservedAdminUsername)
}

// personalToken is a user-minted personal access token (PAT): a long-lived
// bearer credential a logged-in user generates so they can call the API from
// curl/Postman/CI without a browser session. Only a SHA-256 hash of the secret
// is stored (the plaintext is shown once at creation); the token carries the
// owner's role so it cannot exceed the privileges the owner had when minting it.
type personalToken struct {
	ID    string `json:"id"`    // short opaque id used for listing/revocation
	Name  string `json:"name"`  // user-supplied label
	Owner string `json:"owner"` // subject (local username or AD/SAML subject)
	// OwnerLocal records whether the owner was a local account at creation. When
	// true the live role is re-resolved from the account on each request (and the
	// token dies if the account is deleted or flagged must-change); when false
	// (AD/SAML) the snapshot Role/Groups below are used.
	OwnerLocal bool     `json:"owner_local,omitempty"`
	Role       string   `json:"role"`             // role snapshot (authoritative for non-local owners)
	Groups     []string `json:"groups,omitempty"` // AD group snapshot (non-local owners, for cluster-group eval)
	Hash       string   `json:"hash"`             // SHA-256 hex of the secret (never the secret itself)
	CreatedAt  string   `json:"created_at,omitempty"`
	ExpiresAt  string   `json:"expires_at,omitempty"` // RFC3339; empty means no expiry
	LastUsedAt string   `json:"last_used_at,omitempty"`
	CreatedIP  string   `json:"created_ip,omitempty"`
}

// usersDBFile is the on-disk JSON schema for the writable user database. It
// also carries the persisted SAML configuration and session policy so a single
// 0600 file holds all auth state the admin can manage at runtime.
type usersDBFile struct {
	Users         []account              `json:"users"`
	SAML          *samlPersisted         `json:"saml,omitempty"`
	LDAP          *ldapPersisted         `json:"ldap,omitempty"`
	Session       *sessionPolicy         `json:"session,omitempty"`
	TLS           *tlsPolicy             `json:"tls,omitempty"`
	Resets        []passwordResetRequest `json:"password_resets,omitempty"`
	ClusterGroups []clusterGroup         `json:"cluster_groups,omitempty"`
	Tokens        []personalToken        `json:"personal_tokens,omitempty"`
}

// passwordResetRequest is a queued self-service "forgot password" request that
// an admin resolves out-of-band from Settings → Access. No secrets are stored;
// it only records who asked and from where so an admin can act on it.
type passwordResetRequest struct {
	Username    string `json:"username"`
	RequestedAt string `json:"requested_at"`
	ClientIP    string `json:"client_ip,omitempty"`
}

// userStoreBackend abstracts where the user database is persisted. The same
// JSON document is stored either as a 0600 file (file/dev/stack installs) or as
// a Kubernetes Secret (cluster installs, encrypted at rest by etcd). A nil
// backend means in-memory only (used by --users-file seeds and tests).
type userStoreBackend interface {
	// load returns the raw JSON document, or (nil, nil) when it does not exist.
	load() ([]byte, error)
	// save persists the raw JSON document.
	save(data []byte) error
	// setInitialPassword records the bootstrap admin password so an operator
	// can retrieve it, returning a human-readable hint of where it was stored.
	setInitialPassword(username, password string) (string, error)
	// clearInitialPassword removes the bootstrap password once it is changed.
	clearInitialPassword()
	// location is a human-readable description of the backend (for logs).
	location() string
}

// userDB is the runtime, writable store of local accounts (and SAML config).
type userDB struct {
	mu            sync.RWMutex
	path          string // file path when file-backed (messages/tests); "" otherwise
	backend       userStoreBackend
	accounts      map[string]*account
	saml          *samlPersisted
	ldapCfg       *ldapPersisted
	session       *sessionPolicy
	tls           *tlsPolicy
	resets        []passwordResetRequest
	clusterGroups []clusterGroup
	tokens        []personalToken
}

func newUserDB(path string) *userDB {
	return &userDB{path: strings.TrimSpace(path), accounts: map[string]*account{}}
}

// openUserDB loads (or initializes) the file-backed JSON user database at path.
func openUserDB(path string) (*userDB, error) {
	return openUserDBFromBackend(&fileStoreBackend{path: path})
}

// openUserDBFromBackend loads (or initializes) a user database from any backend.
func openUserDBFromBackend(be userStoreBackend) (*userDB, error) {
	db := &userDB{accounts: map[string]*account{}, backend: be}
	if fb, ok := be.(*fileStoreBackend); ok {
		db.path = fb.path
	}
	raw, err := be.load()
	if err != nil {
		return nil, fmt.Errorf("load users db: %w", err)
	}
	if len(raw) == 0 {
		return db, nil
	}
	var f usersDBFile
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("parse users db: %w", err)
	}
	for i := range f.Users {
		u := f.Users[i]
		if err := validateAccount(u); err != nil {
			return nil, fmt.Errorf("users db: %w", err)
		}
		key := strings.ToLower(strings.TrimSpace(u.Username))
		uu := u
		db.accounts[key] = &uu
	}
	coerceReservedAdminRole(db.accounts)
	db.saml = f.SAML
	db.ldapCfg = f.LDAP
	db.session = f.Session
	db.tls = f.TLS
	db.resets = f.Resets
	db.clusterGroups = f.ClusterGroups
	db.tokens = f.Tokens
	return db, nil
}

// coerceReservedAdminRole forces the built-in admin account to the admin role,
// defending against a hand-edited store/Secret that demoted it. The corrected
// role is persisted on the next write.
func coerceReservedAdminRole(accounts map[string]*account) {
	for key, a := range accounts {
		if a == nil || !isReservedAdmin(key) {
			continue
		}
		if r, _ := parseRole(a.Role); r != RoleAdmin {
			a.Role = RoleAdmin.String()
		}
	}
}

// writable reports whether the db has a persistence backend (vs. in-memory).
func (db *userDB) writable() bool { return db != nil && db.backend != nil }

// location returns a human-readable description of where the db is persisted.
func (db *userDB) location() string {
	if db == nil || db.backend == nil {
		return "(in-memory)"
	}
	return db.backend.location()
}

// setInitialPassword stores the bootstrap admin password via the backend and
// returns a retrieval hint (or "" when not applicable).
func (db *userDB) setInitialPassword(username, password string) string {
	if db == nil || db.backend == nil {
		return ""
	}
	hint, err := db.backend.setInitialPassword(username, password)
	if err != nil {
		return ""
	}
	return hint
}

// clearInitialPassword removes the stored bootstrap password (best effort).
func (db *userDB) clearInitialPassword() {
	if db != nil && db.backend != nil {
		db.backend.clearInitialPassword()
	}
}

func validateAccount(u account) error {
	if strings.TrimSpace(u.Username) == "" {
		return errors.New("username is required")
	}
	if _, ok := parseRole(u.Role); !ok {
		return fmt.Errorf("user %q: invalid role %q (want admin|operator|viewer)", u.Username, u.Role)
	}
	if strings.TrimSpace(u.PasswordHash) == "" {
		return fmt.Errorf("user %q: password_hash is required", u.Username)
	}
	if _, err := bcrypt.Cost([]byte(u.PasswordHash)); err != nil {
		return fmt.Errorf("user %q: password_hash is not a valid bcrypt hash: %w", u.Username, err)
	}
	return nil
}

// loadUserStore parses a read-only YAML seed file (--users-file) into an
// in-memory userDB. Used to import accounts into the writable db at first run.
func loadUserStore(path string) (*userDB, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read users file: %w", err)
	}
	var uf struct {
		Users []account `yaml:"users"`
	}
	if err := yaml.Unmarshal(b, &uf); err != nil {
		return nil, fmt.Errorf("parse users file: %w", err)
	}
	db := newUserDB("")
	for i := range uf.Users {
		u := uf.Users[i]
		if err := validateAccount(u); err != nil {
			return nil, fmt.Errorf("users[%d]: %w", i, err)
		}
		key := strings.ToLower(strings.TrimSpace(u.Username))
		if _, dup := db.accounts[key]; dup {
			return nil, fmt.Errorf("users[%d]: duplicate username %q", i, u.Username)
		}
		uu := u
		db.accounts[key] = &uu
	}
	coerceReservedAdminRole(db.accounts)
	return db, nil
}

// saveLocked serializes the db and persists it through the backend. Caller
// holds db.mu. In-memory stores (no backend) are a no-op.
func (db *userDB) saveLocked() error {
	if db.backend == nil {
		return nil // in-memory store
	}
	out := usersDBFile{SAML: db.saml, LDAP: db.ldapCfg, Session: db.session, TLS: db.tls, Resets: db.resets, ClusterGroups: db.clusterGroups, Tokens: db.tokens}
	keys := make([]string, 0, len(db.accounts))
	for k := range db.accounts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out.Users = append(out.Users, *db.accounts[k])
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	return db.backend.save(b)
}

func (db *userDB) count() int {
	if db == nil {
		return 0
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.accounts)
}

// maxResetRequests caps the queued self-service password-reset requests so a
// flood of forgot-password submissions cannot grow the store unbounded.
const maxResetRequests = 50

// addResetRequest records (or refreshes) a self-service password-reset request
// for username. It dedupes per user (newest timestamp/IP wins) and caps the
// queue length. Persisted so requests survive restarts.
func (db *userDB) addResetRequest(username, clientIP string) {
	name := strings.TrimSpace(username)
	if db == nil || name == "" {
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	now := time.Now().UTC().Format(time.RFC3339)
	for i := range db.resets {
		if strings.EqualFold(db.resets[i].Username, name) {
			db.resets[i].RequestedAt = now
			db.resets[i].ClientIP = clientIP
			_ = db.saveLocked()
			return
		}
	}
	db.resets = append(db.resets, passwordResetRequest{Username: name, RequestedAt: now, ClientIP: clientIP})
	if len(db.resets) > maxResetRequests {
		db.resets = db.resets[len(db.resets)-maxResetRequests:]
	}
	_ = db.saveLocked()
}

// listResetRequests returns a copy of the pending reset requests.
func (db *userDB) listResetRequests() []passwordResetRequest {
	if db == nil {
		return nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	out := make([]passwordResetRequest, len(db.resets))
	copy(out, db.resets)
	return out
}

// clearResetRequest removes any pending reset request for username (called when
// an admin resets the password or dismisses the request). No-op when absent.
func (db *userDB) clearResetRequest(username string) {
	name := strings.TrimSpace(username)
	if db == nil || name == "" {
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	kept := make([]passwordResetRequest, 0, len(db.resets))
	changed := false
	for _, r := range db.resets {
		if strings.EqualFold(r.Username, name) {
			changed = true
			continue
		}
		kept = append(kept, r)
	}
	if !changed {
		return
	}
	db.resets = kept
	_ = db.saveLocked()
}

func (db *userDB) adminCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	n := 0
	for _, a := range db.accounts {
		if r, _ := parseRole(a.Role); r == RoleAdmin {
			n++
		}
	}
	return n
}

// verify checks a username/password and returns the role and must-change flag.
// A dummy bcrypt comparison runs for unknown users to blunt timing-based
// username enumeration.
func (db *userDB) verify(username, password string) (Role, bool, bool) {
	if db == nil {
		return RoleNone, false, false
	}
	key := strings.ToLower(strings.TrimSpace(username))
	db.mu.RLock()
	a, found := db.accounts[key]
	db.mu.RUnlock()
	if !found {
		_ = bcrypt.CompareHashAndPassword([]byte(dummyBcryptHash), []byte(password))
		return RoleNone, false, false
	}
	if err := bcrypt.CompareHashAndPassword([]byte(a.PasswordHash), []byte(password)); err != nil {
		return RoleNone, false, false
	}
	role, _ := parseRole(a.Role)
	return role, true, a.MustChange
}

// lookup returns a copy of an account by username.
func (db *userDB) lookup(username string) (account, bool) {
	if db == nil {
		return account{}, false
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	a, ok := db.accounts[strings.ToLower(strings.TrimSpace(username))]
	if !ok {
		return account{}, false
	}
	return *a, true
}

// list returns all accounts (with password hashes blanked) sorted by username.
func (db *userDB) list() []account {
	db.mu.RLock()
	defer db.mu.RUnlock()
	out := make([]account, 0, len(db.accounts))
	for _, a := range db.accounts {
		c := *a
		c.PasswordHash = ""
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Username < out[j].Username })
	return out
}

// upsertUser creates or replaces an account and persists the db.
func (db *userDB) upsertUser(username, passwordHash string, role Role, mustChange bool) error {
	name := strings.TrimSpace(username)
	if name == "" {
		return errors.New("username is required")
	}
	// The built-in admin account is always admin, regardless of the caller.
	if isReservedAdmin(name) {
		role = RoleAdmin
	}
	key := strings.ToLower(name)
	now := time.Now().UTC().Format(time.RFC3339)
	db.mu.Lock()
	defer db.mu.Unlock()
	existing, ok := db.accounts[key]
	created := now
	if ok {
		created = existing.CreatedAt
		if passwordHash == "" {
			passwordHash = existing.PasswordHash
		}
	}
	db.accounts[key] = &account{
		Username:     name,
		PasswordHash: passwordHash,
		Role:         role.String(),
		MustChange:   mustChange,
		CreatedAt:    created,
		UpdatedAt:    now,
	}
	return db.saveLocked()
}

// setRole updates a user's role, refusing to demote the last admin.
func (db *userDB) setRole(username string, role Role) error {
	key := strings.ToLower(strings.TrimSpace(username))
	db.mu.Lock()
	defer db.mu.Unlock()
	a, ok := db.accounts[key]
	if !ok {
		return errUserNotFound
	}
	// The built-in admin account's role is hardcoded; it can never be demoted.
	if isReservedAdmin(key) && role != RoleAdmin {
		return errReservedAdminRole
	}
	if cur, _ := parseRole(a.Role); cur == RoleAdmin && role != RoleAdmin && db.adminCountLocked() <= 1 {
		return errLastAdmin
	}
	a.Role = role.String()
	a.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	return db.saveLocked()
}

// setPassword updates a user's password hash and must-change flag.
func (db *userDB) setPassword(username, passwordHash string, mustChange bool) error {
	key := strings.ToLower(strings.TrimSpace(username))
	db.mu.Lock()
	defer db.mu.Unlock()
	a, ok := db.accounts[key]
	if !ok {
		return errUserNotFound
	}
	a.PasswordHash = passwordHash
	a.MustChange = mustChange
	// Bump the token generation so every previously issued session token for
	// this account stops validating immediately (the caller re-issues a fresh
	// cookie for the acting user where appropriate).
	a.TokenGen++
	a.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	return db.saveLocked()
}

// revokeSessions bumps the account's token generation, invalidating every
// previously issued session for the user without touching the password. Used by
// self-service "sign out everywhere" and admin force-sign-out.
func (db *userDB) revokeSessions(username string) error {
	key := strings.ToLower(strings.TrimSpace(username))
	db.mu.Lock()
	defer db.mu.Unlock()
	a, ok := db.accounts[key]
	if !ok {
		return errUserNotFound
	}
	a.TokenGen++
	a.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	return db.saveLocked()
}

// getRunConfigPath returns a user's preferred run config path, if set.
func (db *userDB) getRunConfigPath(username string) string {
	key := strings.ToLower(strings.TrimSpace(username))
	db.mu.RLock()
	defer db.mu.RUnlock()
	a, ok := db.accounts[key]
	if !ok {
		return ""
	}
	return strings.TrimSpace(a.RunConfigPath)
}

// setRunConfigPath updates a user's preferred run config path.
func (db *userDB) setRunConfigPath(username, runConfigPath string) error {
	key := strings.ToLower(strings.TrimSpace(username))
	db.mu.Lock()
	defer db.mu.Unlock()
	a, ok := db.accounts[key]
	if !ok {
		return errUserNotFound
	}
	a.RunConfigPath = strings.TrimSpace(runConfigPath)
	a.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	return db.saveLocked()
}

// adminResetPassword sets the named account to a new random temporary password,
// forces a change at next login, and (via setPassword) bumps the token
// generation so every existing session for that account is invalidated. The
// built-in admin is recreated if it was wiped from the store. Returns the new
// plaintext temporary password so a recovery operator can communicate it
// out-of-band. Any pending self-service reset request for the user is cleared.
func (db *userDB) adminResetPassword(username string) (string, error) {
	name := strings.TrimSpace(username)
	if name == "" {
		return "", errors.New("username is required")
	}
	pw, err := randPassword()
	if err != nil {
		return "", err
	}
	hash, err := hashPassword(pw)
	if err != nil {
		return "", err
	}
	if _, ok := db.lookup(name); ok {
		if err := db.setPassword(name, hash, true); err != nil {
			return "", err
		}
		db.clearResetRequest(name)
		return pw, nil
	}
	// Only the reserved admin may be recreated when missing; any other unknown
	// account is a genuine "user not found".
	if !isReservedAdmin(name) {
		return "", errUserNotFound
	}
	if err := db.upsertUser(reservedAdminUsername, hash, RoleAdmin, true); err != nil {
		return "", err
	}
	db.clearResetRequest(reservedAdminUsername)
	return pw, nil
}

// deleteUser removes an account, refusing to remove the last admin.
func (db *userDB) deleteUser(username string) error {
	key := strings.ToLower(strings.TrimSpace(username))
	db.mu.Lock()
	defer db.mu.Unlock()
	a, ok := db.accounts[key]
	if !ok {
		return errUserNotFound
	}
	// The built-in admin account cannot be deleted.
	if isReservedAdmin(key) {
		return errReservedAdminDelete
	}
	if r, _ := parseRole(a.Role); r == RoleAdmin && db.adminCountLocked() <= 1 {
		return errLastAdmin
	}
	delete(db.accounts, key)
	return db.saveLocked()
}

func (db *userDB) adminCountLocked() int {
	n := 0
	for _, a := range db.accounts {
		if r, _ := parseRole(a.Role); r == RoleAdmin {
			n++
		}
	}
	return n
}

// importSeed copies accounts from an in-memory seed db into this db if this db
// is empty, then persists. Used to import a --users-file on first run.
func (db *userDB) importSeed(seed *userDB) error {
	if seed == nil || seed.count() == 0 {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.accounts) > 0 {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	seed.mu.RLock()
	for k, a := range seed.accounts {
		c := *a
		if c.CreatedAt == "" {
			c.CreatedAt = now
		}
		c.UpdatedAt = now
		db.accounts[k] = &c
	}
	seed.mu.RUnlock()
	return db.saveLocked()
}

// bootstrapAdminIfEmpty provisions an initial admin account with a random
// password when the db has no users. It returns the generated plaintext
// password (only on creation) so the caller can surface it to the operator.
func (db *userDB) bootstrapAdminIfEmpty(username string) (string, bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.accounts) > 0 {
		return "", false, nil
	}
	pw, err := randPassword()
	if err != nil {
		return "", false, err
	}
	hash, err := hashPassword(pw)
	if err != nil {
		return "", false, err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	name := strings.TrimSpace(username)
	if name == "" {
		name = "admin"
	}
	db.accounts[strings.ToLower(name)] = &account{
		Username:     name,
		PasswordHash: hash,
		Role:         RoleAdmin.String(),
		MustChange:   true,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := db.saveLocked(); err != nil {
		return "", false, err
	}
	return pw, true, nil
}

// bootstrapPending reports whether the built-in admin account still has its
// initial (random) bootstrap password — i.e. it exists and is still flagged
// must-change. Used to show the "where's the admin password?" hint on the login
// screen only until the first admin completes the forced password change.
func (db *userDB) bootstrapPending() bool {
	if db == nil {
		return false
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	a, ok := db.accounts[reservedAdminUsername]
	return ok && a.MustChange
}

// getSAML returns a copy of the persisted SAML config (or nil).
func (db *userDB) getSAML() *samlPersisted {
	if db == nil {
		return nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.saml == nil {
		return nil
	}
	c := *db.saml
	return &c
}

// setSAML persists the SAML config.
func (db *userDB) setSAML(cfg *samlPersisted) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.saml = cfg
	return db.saveLocked()
}

// getLDAP returns a copy of the persisted LDAP/AD config (or nil).
func (db *userDB) getLDAP() *ldapPersisted {
	if db == nil {
		return nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.ldapCfg == nil {
		return nil
	}
	c := *db.ldapCfg
	return &c
}

// setLDAP persists the LDAP/AD config.
func (db *userDB) setLDAP(cfg *ldapPersisted) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.ldapCfg = cfg
	return db.saveLocked()
}

// getClusterGroups returns a deep copy of the persisted cluster groups.
func (db *userDB) getClusterGroups() []clusterGroup {
	if db == nil {
		return nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	return cloneClusterGroups(db.clusterGroups)
}

// setClusterGroups persists the cluster groups (replacing the full set).
func (db *userDB) setClusterGroups(groups []clusterGroup) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.clusterGroups = cloneClusterGroups(groups)
	return db.saveLocked()
}

// cloneClusterGroups returns an independent copy so callers cannot mutate the
// store's slices in place.
func cloneClusterGroups(in []clusterGroup) []clusterGroup {
	if len(in) == 0 {
		return nil
	}
	out := make([]clusterGroup, len(in))
	for i, g := range in {
		out[i] = clusterGroup{
			Name:          g.Name,
			Clusters:      append([]string(nil), g.Clusters...),
			PrismCentrals: append([]string(nil), g.PrismCentrals...),
			LocalUsers:    append([]string(nil), g.LocalUsers...),
			ADGroups:      append([]string(nil), g.ADGroups...),
			ADUsers:       append([]string(nil), g.ADUsers...),
		}
	}
	return out
}

// maxTokensPerOwner caps how many personal access tokens a single user may hold
// so the store cannot grow unbounded from token churn.
const maxTokensPerOwner = 25

// errTokenForbidden is returned when a caller tries to revoke a token they do
// not own (and is not an admin).
var errTokenForbidden = errors.New("token belongs to another user")

// addToken stores a new personal access token, enforcing the per-owner cap.
func (db *userDB) addToken(pt personalToken) error {
	if db == nil {
		return errors.New("user database not available")
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	n := 0
	for _, t := range db.tokens {
		if strings.EqualFold(t.Owner, pt.Owner) {
			n++
		}
	}
	if n >= maxTokensPerOwner {
		return fmt.Errorf("token limit reached (%d per user); revoke an existing token first", maxTokensPerOwner)
	}
	db.tokens = append(db.tokens, pt)
	return db.saveLocked()
}

// findTokenByHash returns the token whose stored hash matches (constant-time),
// or false when none does.
func (db *userDB) findTokenByHash(hash string) (personalToken, bool) {
	if db == nil || strings.TrimSpace(hash) == "" {
		return personalToken{}, false
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	for _, t := range db.tokens {
		if secureCompare(t.Hash, hash) {
			return t, true
		}
	}
	return personalToken{}, false
}

// tokenView is a metadata copy with the secret hash blanked, safe to return
// over the API.
func tokenView(t personalToken) personalToken {
	c := t
	c.Hash = ""
	return c
}

// tokenExpired reports whether a PAT should be considered expired at `now`.
// Empty ExpiresAt means no expiry. Malformed timestamps are treated as expired
// (fail-closed) so bad data cannot remain "active" indefinitely.
func tokenExpired(t personalToken, now time.Time) bool {
	if strings.TrimSpace(t.ExpiresAt) == "" {
		return false
	}
	exp, err := time.Parse(time.RFC3339, t.ExpiresAt)
	if err != nil {
		return true
	}
	return !now.Before(exp)
}

// pruneExpiredTokensLocked removes expired PATs in-place and persists changes.
// Caller must hold db.mu.
func (db *userDB) pruneExpiredTokensLocked(now time.Time) {
	if db == nil || len(db.tokens) == 0 {
		return
	}
	kept := db.tokens[:0]
	changed := false
	for _, t := range db.tokens {
		if tokenExpired(t, now) {
			changed = true
			continue
		}
		kept = append(kept, t)
	}
	db.tokens = kept
	if changed {
		_ = db.saveLocked()
	}
}

// listTokensForOwner returns the caller's own tokens (hash blanked), newest first.
func (db *userDB) listTokensForOwner(owner string) []personalToken {
	out := []personalToken{}
	if db == nil {
		return out
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.pruneExpiredTokensLocked(time.Now().UTC())
	for _, t := range db.tokens {
		if strings.EqualFold(t.Owner, owner) {
			out = append(out, tokenView(t))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt > out[j].CreatedAt })
	return out
}

// listAllTokens returns every token (hash blanked), for admin management.
func (db *userDB) listAllTokens() []personalToken {
	out := []personalToken{}
	if db == nil {
		return out
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.pruneExpiredTokensLocked(time.Now().UTC())
	for _, t := range db.tokens {
		out = append(out, tokenView(t))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Owner != out[j].Owner {
			return out[i].Owner < out[j].Owner
		}
		return out[i].CreatedAt > out[j].CreatedAt
	})
	return out
}

// deleteToken revokes the token with the given id. Non-admin callers may only
// revoke tokens they own. Returns the removed token and whether it existed.
func (db *userDB) deleteToken(id, requester string, isAdmin bool) (personalToken, bool, error) {
	if db == nil {
		return personalToken{}, false, errors.New("user database not available")
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	for i, t := range db.tokens {
		if t.ID != id {
			continue
		}
		if !isAdmin && !strings.EqualFold(t.Owner, requester) {
			return personalToken{}, false, errTokenForbidden
		}
		removed := t
		db.tokens = append(db.tokens[:i], db.tokens[i+1:]...)
		return tokenView(removed), true, db.saveLocked()
	}
	return personalToken{}, false, nil
}

// touchTokenLastUsed records when a token was last used, throttled to at most
// once a minute per token to avoid a store write on every authenticated request.
func (db *userDB) touchTokenLastUsed(id, clientIP string) {
	if db == nil || strings.TrimSpace(id) == "" {
		return
	}
	now := time.Now().UTC()
	db.mu.Lock()
	defer db.mu.Unlock()
	for i := range db.tokens {
		if db.tokens[i].ID != id {
			continue
		}
		if db.tokens[i].LastUsedAt != "" {
			if last, err := time.Parse(time.RFC3339, db.tokens[i].LastUsedAt); err == nil && now.Sub(last) < time.Minute {
				return
			}
		}
		db.tokens[i].LastUsedAt = now.Format(time.RFC3339)
		_ = db.saveLocked()
		return
	}
}

// getSessionPolicy returns a copy of the persisted session policy (or nil when
// none is set, meaning the server default TTL applies).
func (db *userDB) getSessionPolicy() *sessionPolicy {
	if db == nil {
		return nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.session == nil {
		return nil
	}
	c := *db.session
	return &c
}

// getTLSPolicy returns a copy of the persisted HTTPS/TLS policy, or nil when
// none is configured (HTTP-only, the default).
func (db *userDB) getTLSPolicy() *tlsPolicy {
	if db == nil {
		return nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.tls == nil {
		return nil
	}
	c := *db.tls
	c.DNSNames = append([]string(nil), db.tls.DNSNames...)
	return &c
}

// setTLSPolicy persists the HTTPS/TLS policy (cert metadata + on-disk cert/key
// paths). A nil policy disables HTTPS and clears any stored metadata.
func (db *userDB) setTLSPolicy(p *tlsPolicy) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if p == nil {
		db.tls = nil
	} else {
		c := *p
		c.DNSNames = append([]string(nil), p.DNSNames...)
		db.tls = &c
	}
	return db.saveLocked()
}

// setSessionTTLSeconds persists the session TTL (in seconds). A value of 0
// clears the override so the server's --session-ttl default applies again.
func (db *userDB) setSessionTTLSeconds(seconds int) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if seconds <= 0 {
		db.session = nil
	} else {
		db.session = &sessionPolicy{TTLSeconds: seconds}
	}
	return db.saveLocked()
}

var (
	errUserNotFound        = errors.New("user not found")
	errLastAdmin           = errors.New("cannot remove or demote the last admin account")
	errReservedAdminRole   = errors.New("the built-in admin account's role is fixed and cannot be changed")
	errReservedAdminDelete = errors.New("the built-in admin account cannot be deleted")
)

// dummyBcryptHash is a valid bcrypt hash used only to equalize timing for
// unknown usernames. It never matches a real password.
const dummyBcryptHash = "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy"

// randPassword returns a URL-safe random password string (~24 chars).
func randPassword() (string, error) {
	b := make([]byte, 18)
	if _, err := crand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// hashPassword returns a bcrypt hash suitable for the user database.
func hashPassword(password string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// runHashPassword reads a password from $NCC_PASSWORD or stdin and prints its
// bcrypt hash, ready to paste into a seed users file. It powers --hash-password.
func runHashPassword() {
	password := os.Getenv("NCC_PASSWORD")
	if password == "" {
		if term.IsTerminal(int(os.Stdin.Fd())) {
			fmt.Fprint(os.Stderr, "Password: ")
			b, err := term.ReadPassword(int(os.Stdin.Fd()))
			fmt.Fprintln(os.Stderr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "read password: %v\n", err)
				os.Exit(1)
			}
			password = string(b)
		} else {
			sc := bufio.NewScanner(os.Stdin)
			if sc.Scan() {
				password = sc.Text()
			}
		}
	}
	password = strings.TrimRight(password, "\r\n")
	if password == "" {
		fmt.Fprintln(os.Stderr, "no password provided (set $NCC_PASSWORD or pipe/enter one)")
		os.Exit(1)
	}
	hash, err := hashPassword(password)
	if err != nil {
		fmt.Fprintf(os.Stderr, "hash password: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(hash)
}

// resolveUserStoreBackend selects the user-database backend exactly as the
// server does at startup: a Kubernetes Secret store (encrypted at rest by etcd)
// when --users-db-secret is set, otherwise a local 0600 JSON file when
// --users-db is set, otherwise nil. Shared by startup and the offline
// --reset-password recovery path so both operate on the same store.
func (s *apiServer) resolveUserStoreBackend() (userStoreBackend, error) {
	be, err := s.resolveBaseUserStoreBackend()
	if err != nil || be == nil {
		return be, err
	}
	// Optional envelope encryption at rest: when a master key is configured the
	// document is transparently AES-256-GCM encrypted on save and decrypted on
	// load (legacy plaintext stores are migrated on the next write). No key →
	// unchanged plaintext behavior.
	key, err := loadMasterKey(s.usersDBKeyFile)
	if err != nil {
		return nil, fmt.Errorf("user store encryption: %w", err)
	}
	if key != nil {
		s.userStoreEncrypted = true
		return &encryptingBackend{inner: be, key: key}, nil
	}
	// Fail-fast self-heal guard: if the on-disk store is already encrypted but
	// no master key is configured, loading it as plaintext would fail to parse
	// and the server would bootstrap a *fresh* admin — silently orphaning every
	// real account and locking operators out. Refuse to start with an
	// actionable message instead.
	if data, lerr := be.load(); lerr == nil && documentIsEncrypted(data) {
		return nil, fmt.Errorf("user store at %s is encrypted but no master key is configured: set %s (or --users-db-key-file / %s) to the 32-byte key it was encrypted with; refusing to start so a fresh admin is not bootstrapped over your existing accounts", be.location(), masterKeyEnv, masterKeyFileEnv)
	}
	return be, nil
}

// resolveBaseUserStoreBackend picks the unencrypted persistence backend
// (Kubernetes Secret in-cluster, else a local 0600 JSON file).
func (s *apiServer) resolveBaseUserStoreBackend() (userStoreBackend, error) {
	if strings.TrimSpace(s.usersDBSecret) != "" {
		cli, err := newInClusterSecretClient(s.usersDBSecretNS)
		if err != nil {
			return nil, fmt.Errorf("init Kubernetes Secret user store: %w", err)
		}
		key := strings.TrimSpace(s.usersDBSecretKey)
		if key == "" {
			key = "users.json"
		}
		return &k8sSecretBackend{client: cli, name: strings.TrimSpace(s.usersDBSecret), key: key}, nil
	}
	if strings.TrimSpace(s.usersDBPath) != "" {
		return &fileStoreBackend{path: s.usersDBPath}, nil
	}
	return nil, nil
}

// runResetPassword is the offline recovery entry point for --reset-password /
// --reset-admin. It resolves the configured store, resets the target account to
// a new random temporary password (forced change at next login, all sessions
// invalidated), prints it, and exits. Because a running server caches accounts
// in memory, the operator must restart the api-server for the change to load.
func runResetPassword(s *apiServer, username string) {
	username = strings.TrimSpace(username)
	if username == "" {
		fmt.Fprintln(os.Stderr, "--reset-password requires a username")
		os.Exit(2)
	}
	backend, err := s.resolveUserStoreBackend()
	if err != nil {
		fmt.Fprintf(os.Stderr, "reset-password: %v\n", err)
		os.Exit(1)
	}
	if backend == nil {
		fmt.Fprintln(os.Stderr, "reset-password: no user database configured; pass --users-db <path> or --users-db-secret <name> (the same backend the server uses)")
		os.Exit(1)
	}
	db, err := openUserDBFromBackend(backend)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reset-password: open user database (%s): %v\n", backend.location(), err)
		os.Exit(1)
	}
	pw, err := db.adminResetPassword(username)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reset-password: %v\n", err)
		os.Exit(1)
	}
	// Refresh the bootstrap hint file when resetting the built-in admin so the
	// login screen's "where is the admin password?" hint stays accurate.
	if isReservedAdmin(username) {
		_ = db.setInitialPassword(reservedAdminUsername, pw)
	}
	fmt.Println("==================================================================")
	fmt.Printf(" PASSWORD RESET (store: %s)\n", backend.location())
	fmt.Printf("   username: %s\n", username)
	fmt.Printf("   password: %s\n", pw)
	fmt.Println("   The user MUST change this password at next login.")
	fmt.Println("   All existing sessions for this account were invalidated.")
	fmt.Println("   Restart the api-server (or v2-stop && v2-start) for it to take effect.")
	fmt.Println("==================================================================")
}

// fileStoreBackend persists the user database to a local 0600 JSON file.
type fileStoreBackend struct{ path string }

func (b *fileStoreBackend) load() ([]byte, error) {
	data, err := os.ReadFile(b.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (b *fileStoreBackend) save(data []byte) error {
	tmp := b.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write users db: %w", err)
	}
	if err := os.Rename(tmp, b.path); err != nil {
		return fmt.Errorf("replace users db: %w", err)
	}
	return nil
}

func (b *fileStoreBackend) setInitialPassword(username, password string) (string, error) {
	return writeInitialPasswordFile(b.path, username, password)
}

func (b *fileStoreBackend) clearInitialPassword() { removeInitialPasswordFile(b.path) }

func (b *fileStoreBackend) location() string { return b.path }

// writeInitialPasswordFile writes the bootstrap admin password to a sibling
// 0600 file so the operator can retrieve it after startup. Returns the path.
func writeInitialPasswordFile(dbPath, username, password string) (string, error) {
	dir := filepath.Dir(dbPath)
	p := filepath.Join(dir, ".ncc-initial-admin-password")
	body := fmt.Sprintf("username: %s\npassword: %s\nnote: you must change this password on first login; this file is deleted afterward.\n", username, password)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		return "", err
	}
	return p, nil
}

// removeInitialPasswordFile deletes the bootstrap password file (best effort).
func removeInitialPasswordFile(dbPath string) {
	_ = os.Remove(filepath.Join(filepath.Dir(dbPath), ".ncc-initial-admin-password"))
}
