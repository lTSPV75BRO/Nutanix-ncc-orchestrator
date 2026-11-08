package main

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
)

// Directory profiles: per-domain LDAP/AD routing.
//
// The top-level ldapPersisted fields are the *default* profile. Each entry in
// ldapPersisted.Profiles is a further, independent directory (its own server
// URLs, base DN, service account, CA and role map) that claims login names by
// UPN suffix ("jdoe@emea.corp.example.com") and/or NetBIOS domain
// ("EMEA\jdoe"). Routing:
//
//   - "DOMAIN\user": the profile claiming NetBIOS name DOMAIN gets "user".
//   - "user@suffix": the profile claiming suffix gets the name unchanged (the
//     default user filter matches userPrincipalName).
//   - anything else, or an unclaimed domain: the default profile gets the
//     name exactly as typed (the pre-profile behaviour).
//
// Only one directory is ever asked, so a password is never sent to a
// directory the login name doesn't route to.
//
// Canonical (stored) usernames stay unique across directories:
//
//   - a named profile's user is always named by its userPrincipalName,
//     lowercased, and that UPN's suffix must be one the profile claims;
//   - a default-profile user keeps the username attribute's value
//     (sAMAccountName by default, as before profiles existed), unless that
//     value is UPN-shaped with a suffix another profile claims.
//
// Claims are exclusive (a suffix or NetBIOS name belongs to one profile), so a
// name ending in "@suffix" can only come from the profile owning suffix, and
// a name without "@" only from the default profile. Violations refuse the
// sign-in with an *ldapRefusal (audited by code, generic error to the user).

const ldapDefaultProfileName = "default"

// ldapProfile is one named, non-default directory. Secrets (BindPassword,
// CACertPEM) are write-only over the API like the default profile's.
type ldapProfile struct {
	Name               string   `json:"name"`
	UPNSuffixes        []string `json:"upn_suffixes,omitempty"`
	NetBIOSNames       []string `json:"netbios_names,omitempty"`
	URL                string   `json:"url"`
	StartTLS           bool     `json:"start_tls,omitempty"`
	InsecureSkipVerify bool     `json:"insecure_skip_verify,omitempty"`
	CACertPEM          string   `json:"ca_cert_pem,omitempty"`
	BindDN             string   `json:"bind_dn,omitempty"`
	BindPassword       string   `json:"bind_password,omitempty"`
	BaseDN             string   `json:"base_dn"`
	UserFilter         string   `json:"user_filter,omitempty"`
	UsernameAttr       string   `json:"username_attribute,omitempty"`
	GroupAttr          string   `json:"group_attribute,omitempty"`
	RoleMapRaw         string   `json:"role_map,omitempty"`
	DefaultRole        string   `json:"default_role,omitempty"`
}

// asPersisted views a profile as a single enabled directory config so it is
// built by the same code as the default profile.
func (p ldapProfile) asPersisted() *ldapPersisted {
	return &ldapPersisted{
		Enabled:            true,
		URL:                p.URL,
		StartTLS:           p.StartTLS,
		InsecureSkipVerify: p.InsecureSkipVerify,
		CACertPEM:          p.CACertPEM,
		BindDN:             p.BindDN,
		BindPassword:       p.BindPassword,
		BaseDN:             p.BaseDN,
		UserFilter:         p.UserFilter,
		UsernameAttr:       p.UsernameAttr,
		GroupAttr:          p.GroupAttr,
		RoleMapRaw:         p.RoleMapRaw,
		DefaultRole:        p.DefaultRole,
		UPNSuffixes:        p.UPNSuffixes,
		NetBIOSNames:       p.NetBIOSNames,
	}
}

var ldapProfileNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$`)

// normalizeUPNSuffixes trims, strips a leading "@", lowercases and dedupes.
func normalizeUPNSuffixes(in []string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, s := range in {
		s = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(s), "@"))
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// normalizeNetBIOSNames trims, strips a trailing "\", uppercases and dedupes.
func normalizeNetBIOSNames(in []string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, s := range in {
		s = strings.ToUpper(strings.TrimSuffix(strings.TrimSpace(s), `\`))
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// validateLDAPProfiles normalizes the claims of the default profile and of
// every named profile in place, and rejects configs that would make routing
// or canonical names ambiguous. It does not touch the network.
func validateLDAPProfiles(c *ldapPersisted) error {
	if c == nil {
		return nil
	}
	claimedSuffix := map[string]string{}
	claimedNetBIOS := map[string]string{}
	claim := func(profile string, suffixes, netbios []string) error {
		for _, s := range suffixes {
			if strings.ContainsAny(s, "@\\ \t/") || strings.HasPrefix(s, ".") || strings.HasSuffix(s, ".") {
				return fmt.Errorf("directory %q: invalid UPN suffix %q (want e.g. emea.corp.example.com)", profile, s)
			}
			if other, dup := claimedSuffix[s]; dup {
				return fmt.Errorf("UPN suffix %q is claimed by both directory %q and %q", s, other, profile)
			}
			claimedSuffix[s] = profile
		}
		for _, n := range netbios {
			if len(n) > 15 || strings.ContainsAny(n, "@\\/. \t") {
				return fmt.Errorf("directory %q: invalid NetBIOS domain %q (want e.g. EMEA)", profile, n)
			}
			if other, dup := claimedNetBIOS[n]; dup {
				return fmt.Errorf("NetBIOS domain %q is claimed by both directory %q and %q", n, other, profile)
			}
			claimedNetBIOS[n] = profile
		}
		return nil
	}
	c.UPNSuffixes = normalizeUPNSuffixes(c.UPNSuffixes)
	c.NetBIOSNames = normalizeNetBIOSNames(c.NetBIOSNames)
	if err := claim(ldapDefaultProfileName, c.UPNSuffixes, c.NetBIOSNames); err != nil {
		return err
	}
	names := map[string]bool{}
	for i := range c.Profiles {
		p := &c.Profiles[i]
		p.Name = strings.TrimSpace(p.Name)
		if !ldapProfileNameRe.MatchString(p.Name) {
			return fmt.Errorf("directory name %q is invalid (letters, digits, '.', '_', '-'; up to 63 characters)", p.Name)
		}
		key := strings.ToLower(p.Name)
		if key == ldapDefaultProfileName {
			return fmt.Errorf("directory name %q is reserved for the default directory", p.Name)
		}
		if names[key] {
			return fmt.Errorf("directory name %q is used twice", p.Name)
		}
		names[key] = true
		if strings.TrimSpace(p.URL) == "" || strings.TrimSpace(p.BaseDN) == "" {
			return fmt.Errorf("directory %q: server URL and base DN are required", p.Name)
		}
		p.UPNSuffixes = normalizeUPNSuffixes(p.UPNSuffixes)
		p.NetBIOSNames = normalizeNetBIOSNames(p.NetBIOSNames)
		// A named profile's users are stored under their UPN, which must end
		// in a suffix the profile owns, so at least one suffix is required.
		if len(p.UPNSuffixes) == 0 {
			return fmt.Errorf("directory %q: at least one UPN suffix is required (its users are named by their userPrincipalName)", p.Name)
		}
		if _, err := parseLDAPRoleMap(p.RoleMapRaw); err != nil {
			return fmt.Errorf("directory %q: %w", p.Name, err)
		}
		if err := claim(p.Name, p.UPNSuffixes, p.NetBIOSNames); err != nil {
			return err
		}
	}
	return nil
}

// ldapRouter is the live ldapAuthenticator: the default directory plus any
// named profiles. With no profiles it behaves exactly like the default
// directory alone (plus optional "DOMAIN\user" stripping for its NetBIOS
// names).
type ldapRouter struct {
	def      *ldapProvider
	profiles []*ldapProvider
}

// buildLDAPRouter resolves a persisted config (default + profiles) into a
// router. (nil, false, nil) means LDAP is disabled.
func buildLDAPRouter(c *ldapPersisted) (*ldapRouter, bool, error) {
	if !c.configured() {
		return nil, false, nil
	}
	cp := *c
	cp.Profiles = append([]ldapProfile(nil), c.Profiles...)
	if err := validateLDAPProfiles(&cp); err != nil {
		return nil, false, err
	}
	def, _, err := buildLDAPProvider(&cp)
	if err != nil {
		return nil, false, err
	}
	r := &ldapRouter{def: def}
	for _, prof := range cp.Profiles {
		p, _, err := buildLDAPProvider(prof.asPersisted())
		if err != nil {
			return nil, false, fmt.Errorf("directory %q: %w", prof.Name, err)
		}
		p.name = prof.Name
		r.profiles = append(r.profiles, p)
	}
	return r, true, nil
}

// all returns the default directory first, then the named profiles.
func (r *ldapRouter) all() []*ldapProvider {
	return append([]*ldapProvider{r.def}, r.profiles...)
}

func (p *ldapProvider) claimsSuffix(suffix string) bool {
	for _, s := range p.upnSuffixes {
		if s == suffix {
			return true
		}
	}
	return false
}

func (p *ldapProvider) claimsNetBIOS(domain string) bool {
	for _, n := range p.netbiosNames {
		if n == domain {
			return true
		}
	}
	return false
}

// route picks the one directory a login name goes to and the name to search
// it with.
func (r *ldapRouter) route(username string) (*ldapProvider, string) {
	username = strings.TrimSpace(username)
	if i := strings.IndexByte(username, '\\'); i > 0 {
		domain := strings.ToUpper(strings.TrimSpace(username[:i]))
		for _, p := range r.all() {
			if p.claimsNetBIOS(domain) {
				return p, strings.TrimSpace(username[i+1:])
			}
		}
		return r.def, username
	}
	if i := strings.LastIndexByte(username, '@'); i > 0 && i < len(username)-1 {
		suffix := strings.ToLower(username[i+1:])
		for _, p := range r.all() {
			if p.claimsSuffix(suffix) {
				return p, username
			}
		}
	}
	return r.def, username
}

// owner returns the profile that claims the suffix of a UPN-shaped name, or
// nil when the name has no "@" or its suffix is unclaimed.
func (r *ldapRouter) owner(name string) *ldapProvider {
	i := strings.LastIndexByte(name, '@')
	if i < 0 || i == len(name)-1 {
		return nil
	}
	suffix := strings.ToLower(name[i+1:])
	for _, p := range r.all() {
		if p.claimsSuffix(suffix) {
			return p
		}
	}
	return nil
}

// ldapRefusal is a sign-in the directory accepted but NCC refuses because the
// resulting name would not be unique. code is the audit error.
type ldapRefusal struct {
	code   string
	detail string
}

func (e *ldapRefusal) Error() string { return "ldap sign-in refused (" + e.code + "): " + e.detail }

// authenticate implements ldapAuthenticator.
func (r *ldapRouter) authenticate(username, password string) (Role, string, []string, bool, error) {
	_, role, canonical, groups, ok, err := r.authenticateRouted(username, password)
	return role, canonical, groups, ok, err
}

// authenticateRouted is authenticate that also reports which directory was
// asked (for the settings test).
func (r *ldapRouter) authenticateRouted(username, password string) (string, Role, string, []string, bool, error) {
	p, login := r.route(username)
	res, ok, err := p.lookup(login, password)
	if err != nil {
		if len(r.profiles) > 0 {
			err = fmt.Errorf("directory %q: %w", p.name, err)
		}
		return p.name, RoleNone, "", nil, false, err
	}
	if !ok {
		return p.name, RoleNone, "", nil, false, nil
	}
	canonical, err := r.canonicalName(p, res)
	if err != nil {
		return p.name, RoleNone, "", nil, false, err
	}
	return p.name, res.role, canonical, res.groups, true, nil
}

// canonicalName applies the uniqueness rules described at the top of the file.
func (r *ldapRouter) canonicalName(p *ldapProvider, res ldapLookupResult) (string, error) {
	if p == r.def {
		name := strings.TrimSpace(res.name)
		if o := r.owner(name); o != nil && o != r.def {
			return "", &ldapRefusal{code: "directory_conflict", detail: fmt.Sprintf("default-directory user %q has a name claimed by directory %q", name, o.name)}
		}
		return name, nil
	}
	upn := strings.ToLower(strings.TrimSpace(res.upn))
	if upn == "" {
		return "", &ldapRefusal{code: "upn_missing", detail: fmt.Sprintf("user in directory %q has no userPrincipalName", p.name)}
	}
	if o := r.owner(upn); o != p {
		return "", &ldapRefusal{code: "upn_suffix_unclaimed", detail: fmt.Sprintf("UPN %q is not under a suffix claimed by directory %q (add the suffix to it)", upn, p.name)}
	}
	return upn, nil
}

// searchDirectory implements ldapAuthenticator: it queries every directory
// and merges the results. Users from a named profile are offered under their
// lowercased UPN (their canonical name). One unreachable directory doesn't
// hide the others' results; the error is returned only when all fail.
func (r *ldapRouter) searchDirectory(query, kind string, limit int) ([]directoryEntry, error) {
	if limit <= 0 || limit > 50 {
		limit = 25
	}
	var out []directoryEntry
	var errs []error
	for _, p := range r.all() {
		if len(out) >= limit {
			break
		}
		res, err := p.searchDirectory(query, kind, limit-len(out))
		if err != nil {
			if len(r.profiles) > 0 {
				err = fmt.Errorf("directory %q: %w", p.name, err)
			}
			errs = append(errs, err)
			continue
		}
		for _, e := range res {
			if p != r.def {
				e.Directory = p.name
				if e.Type == "user" {
					upn := strings.ToLower(strings.TrimSpace(e.UPN))
					if upn == "" || r.owner(upn) != p {
						continue // could never sign in under a unique name
					}
					e.Value = upn
				}
			}
			out = append(out, e)
		}
	}
	if len(errs) > 0 {
		if len(out) == 0 {
			return nil, errors.Join(errs...)
		}
		log.Printf("LDAP search: partial results: %v", errors.Join(errs...))
	}
	return out, nil
}

// validate runs the credential-free connectivity check on every directory.
func (r *ldapRouter) validate() error {
	var errs []error
	for _, p := range r.all() {
		if err := p.validate(); err != nil {
			if len(r.profiles) > 0 {
				err = fmt.Errorf("directory %q: %w", p.name, err)
			}
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// foreignUPNSuffixes returns the suffixes claimed by named (non-default)
// profiles. Cluster-group membership must not match "jdoe@<foreign suffix>"
// to a default-directory user "jdoe": they are different people.
func (r *ldapRouter) foreignUPNSuffixes() map[string]bool {
	out := map[string]bool{}
	for _, p := range r.profiles {
		for _, s := range p.upnSuffixes {
			out[s] = true
		}
	}
	return out
}
