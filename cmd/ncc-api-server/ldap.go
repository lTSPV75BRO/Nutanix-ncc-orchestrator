package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/go-ldap/ldap/v3"
)

// defaultLDAPUserFilter matches an Active Directory user by sAMAccountName. The
// "%s" placeholder is replaced with the (filter-escaped) login name.
const defaultLDAPUserFilter = "(&(objectClass=user)(sAMAccountName=%s))"

const (
	defaultLDAPUsernameAttr = "sAMAccountName"
	defaultLDAPGroupAttr    = "memberOf"
	defaultLDAPTimeout      = 10 * time.Second
)

// ldapPersisted is the runtime, admin-editable LDAP/Active Directory
// configuration stored in the user database. BindPassword is a secret and is
// never returned over the API (GET reports has_bind_password instead).
type ldapPersisted struct {
	Enabled            bool   `json:"enabled"`
	URL                string `json:"url"`                 // ldap(s)://host:port, comma-separated for failover
	StartTLS           bool   `json:"start_tls,omitempty"` // upgrade a plain ldap:// connection with StartTLS
	InsecureSkipVerify bool   `json:"insecure_skip_verify,omitempty"`
	CACertPEM          string `json:"ca_cert_pem,omitempty"` // PEM bundle to verify the server cert
	BindDN             string `json:"bind_dn,omitempty"`     // read-only service account
	BindPassword       string `json:"bind_password,omitempty"`
	BaseDN             string `json:"base_dn"`               // search base for users
	UserFilter         string `json:"user_filter,omitempty"` // %s -> escaped login name
	UsernameAttr       string `json:"username_attribute,omitempty"`
	GroupAttr          string `json:"group_attribute,omitempty"`
	RoleMapRaw         string `json:"role_map,omitempty"` // group(CN or DN)=role, comma-separated
	DefaultRole        string `json:"default_role,omitempty"`
}

// configured reports whether the persisted config is complete enough to enable.
func (c *ldapPersisted) configured() bool {
	if c == nil || !c.Enabled {
		return false
	}
	return strings.TrimSpace(c.URL) != "" && strings.TrimSpace(c.BaseDN) != ""
}

// ldapAuthenticator verifies a username/password against the directory and
// resolves the caller's role. It is an interface so login dispatch can be
// unit-tested with a fake (no live server needed).
type ldapAuthenticator interface {
	// authenticate returns (role, canonicalUsername, ok, err). ok is false for a
	// bad password or unknown user (err stays nil for those expected cases); err
	// is non-nil only for operational failures (dial/bind/search errors).
	authenticate(username, password string) (Role, string, bool, error)
}

// ldapProvider is the live, resolved LDAP/AD client built from ldapPersisted.
type ldapProvider struct {
	urls         []string
	startTLS     bool
	insecure     bool
	caPEM        string
	bindDN       string
	bindPassword string
	baseDN       string
	userFilter   string
	usernameAttr string
	groupAttr    string
	roleMap      map[string]Role // keys lowercased for case-insensitive AD matching
	defaultRole  Role
	timeout      time.Duration
}

// buildLDAPProvider resolves a persisted config into a live provider. The bool
// reports whether LDAP is enabled+configured; (nil, false, nil) means disabled.
func buildLDAPProvider(c *ldapPersisted) (*ldapProvider, bool, error) {
	if !c.configured() {
		return nil, false, nil
	}
	urls := []string{}
	for _, u := range strings.Split(c.URL, ",") {
		if u = strings.TrimSpace(u); u != "" {
			urls = append(urls, u)
		}
	}
	if len(urls) == 0 {
		return nil, false, errors.New("ldap: no server url configured")
	}
	// AD group keys are DNs (e.g. CN=NCC-Admins,OU=Groups,DC=corp,DC=com) which
	// contain many "=" characters, so the role is parsed off the LAST "=" rather
	// than the first (unlike the SAML attribute map). Keys are lowercased because
	// AD DNs/CNs are case-insensitive.
	lowered, err := parseLDAPRoleMap(c.RoleMapRaw)
	if err != nil {
		return nil, false, err
	}
	defaultRole := RoleViewer
	if r, ok := parseRole(c.DefaultRole); ok {
		defaultRole = r
	}
	p := &ldapProvider{
		urls:         urls,
		startTLS:     c.StartTLS,
		insecure:     c.InsecureSkipVerify,
		caPEM:        c.CACertPEM,
		bindDN:       strings.TrimSpace(c.BindDN),
		bindPassword: c.BindPassword,
		baseDN:       strings.TrimSpace(c.BaseDN),
		userFilter:   defaultIfEmpty(c.UserFilter, defaultLDAPUserFilter),
		usernameAttr: defaultIfEmpty(c.UsernameAttr, defaultLDAPUsernameAttr),
		groupAttr:    defaultIfEmpty(c.GroupAttr, defaultLDAPGroupAttr),
		roleMap:      lowered,
		defaultRole:  defaultRole,
		timeout:      defaultLDAPTimeout,
	}
	return p, true, nil
}

// parseLDAPRoleMap parses "group=role" entries where group may be a full AD DN
// (which itself contains commas and "="). Entries are separated by newlines or
// semicolons (a comma cannot be used because DNs contain commas), and the role
// is taken from the final "=". Keys are lowercased for case-insensitive match.
func parseLDAPRoleMap(raw string) (map[string]Role, error) {
	m := map[string]Role{}
	entries := strings.FieldsFunc(raw, func(r rune) bool { return r == '\n' || r == ';' })
	for _, line := range entries {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		idx := strings.LastIndex(line, "=")
		if idx < 0 {
			return nil, fmt.Errorf("invalid ldap role mapping %q (want group=role)", line)
		}
		key := strings.TrimSpace(line[:idx])
		role, ok := parseRole(strings.TrimSpace(line[idx+1:]))
		if key == "" || !ok {
			return nil, fmt.Errorf("invalid ldap role mapping %q (want group=role)", line)
		}
		m[strings.ToLower(key)] = role
	}
	return m, nil
}

// roleFromGroups maps directory group values (full DNs) to the highest matching
// local role, falling back to the configured default. Each group is matched
// both by its full DN and by its extracted CN, case-insensitively, so admins
// can map either "CN=NCC-Admins,OU=...=admin" or just "NCC-Admins=admin".
func (p *ldapProvider) roleFromGroups(groups []string) Role {
	best := RoleNone
	for _, g := range groups {
		g = strings.TrimSpace(g)
		if g == "" {
			continue
		}
		for _, candidate := range []string{g, ldapFirstCN(g)} {
			if candidate == "" {
				continue
			}
			if r, ok := p.roleMap[strings.ToLower(candidate)]; ok && r > best {
				best = r
			}
		}
	}
	if best == RoleNone {
		return p.defaultRole
	}
	return best
}

// ldapFirstCN returns the value of the first CN= component of a DN, or "".
func ldapFirstCN(dn string) string {
	for _, rdn := range strings.Split(dn, ",") {
		rdn = strings.TrimSpace(rdn)
		if len(rdn) > 3 && strings.EqualFold(rdn[:3], "CN=") {
			return strings.TrimSpace(rdn[3:])
		}
	}
	return ""
}

func (p *ldapProvider) tlsConfig(serverURL string) (*tls.Config, error) {
	cfg := &tls.Config{InsecureSkipVerify: p.insecure} //nolint:gosec // opt-in via InsecureSkipVerify
	if host := ldapHost(serverURL); host != "" {
		cfg.ServerName = host
	}
	if strings.TrimSpace(p.caPEM) != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(p.caPEM)) {
			return nil, errors.New("ldap: invalid CA certificate PEM")
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

// ldapHost extracts the hostname from an ldap(s):// URL for TLS SNI/verification.
func ldapHost(serverURL string) string {
	u, err := url.Parse(strings.TrimSpace(serverURL))
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// dial connects to the first reachable configured URL, applying StartTLS when
// requested for plain ldap:// connections.
func (p *ldapProvider) dial() (*ldap.Conn, error) {
	var lastErr error
	for _, u := range p.urls {
		tlsCfg, err := p.tlsConfig(u)
		if err != nil {
			return nil, err
		}
		isLDAPS := strings.HasPrefix(strings.ToLower(u), "ldaps://")
		var conn *ldap.Conn
		if isLDAPS {
			conn, err = ldap.DialURL(u, ldap.DialWithTLSConfig(tlsCfg))
		} else {
			conn, err = ldap.DialURL(u)
		}
		if err != nil {
			lastErr = err
			continue
		}
		conn.SetTimeout(p.timeout)
		if p.startTLS && !isLDAPS {
			if err := conn.StartTLS(tlsCfg); err != nil {
				conn.Close()
				lastErr = fmt.Errorf("ldap StartTLS: %w", err)
				continue
			}
		}
		return conn, nil
	}
	if lastErr == nil {
		lastErr = errors.New("ldap: no server url configured")
	}
	return nil, lastErr
}

// currentLDAP returns the active LDAP authenticator (or nil) under a read lock.
func (s *apiServer) currentLDAP() ldapAuthenticator {
	s.ldapMu.RLock()
	defer s.ldapMu.RUnlock()
	if !s.ldapEnabled {
		return nil
	}
	return s.ldap
}

// reloadLDAPFromStore rebuilds the LDAP provider from the persisted config in
// the user database. A nil/disabled config disables LDAP. No-op when LDAP is
// managed by flags.
func (s *apiServer) reloadLDAPFromStore(ctx context.Context) error {
	if s.ldapFromFlags || s.users == nil {
		return nil
	}
	cfg := s.users.getLDAP()
	prov, enabled, err := buildLDAPProvider(cfg)
	if err != nil {
		return err
	}
	s.ldapMu.Lock()
	if enabled {
		s.ldap = prov
	} else {
		s.ldap = nil
	}
	s.ldapEnabled = enabled
	s.ldapMu.Unlock()
	return nil
}

// authenticate binds the service account, finds the user, then re-binds as that
// user to verify the password, finally mapping group membership to a role.
func (p *ldapProvider) authenticate(username, password string) (Role, string, bool, error) {
	username = strings.TrimSpace(username)
	// Reject empty credentials before binding: an empty password is an
	// anonymous bind in LDAP and would otherwise look like a successful auth.
	if username == "" || password == "" {
		return RoleNone, "", false, nil
	}
	conn, err := p.dial()
	if err != nil {
		return RoleNone, "", false, err
	}
	defer conn.Close()

	// Bind the read-only service account (or anonymously when no bind DN set).
	if p.bindDN != "" {
		if err := conn.Bind(p.bindDN, p.bindPassword); err != nil {
			return RoleNone, "", false, fmt.Errorf("ldap bind (service account): %w", err)
		}
	}

	filter := strings.ReplaceAll(p.userFilter, "%s", ldap.EscapeFilter(username))
	req := ldap.NewSearchRequest(
		p.baseDN,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 2, int(p.timeout.Seconds()), false,
		filter,
		[]string{"dn", p.usernameAttr, p.groupAttr},
		nil,
	)
	res, err := conn.Search(req)
	if err != nil {
		return RoleNone, "", false, fmt.Errorf("ldap search: %w", err)
	}
	if len(res.Entries) == 0 {
		return RoleNone, "", false, nil // unknown user
	}
	if len(res.Entries) > 1 {
		return RoleNone, "", false, fmt.Errorf("ldap search for %q matched %d entries (tighten user_filter)", username, len(res.Entries))
	}
	entry := res.Entries[0]

	// Verify the password by re-binding as the located user DN.
	if err := conn.Bind(entry.DN, password); err != nil {
		return RoleNone, "", false, nil // wrong password
	}

	canonical := strings.TrimSpace(entry.GetAttributeValue(p.usernameAttr))
	if canonical == "" {
		canonical = username
	}
	role := p.roleFromGroups(entry.GetAttributeValues(p.groupAttr))
	return role, canonical, true, nil
}
