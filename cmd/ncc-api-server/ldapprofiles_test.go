package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeDirUser is one account in a fake directory.
type fakeDirUser struct {
	password string
	name     string // username attribute (sAMAccountName)
	upn      string
	groups   []string
}

// fakeDirectory backs an ldapProvider's lookupOverride and records the login
// names it was asked about.
type fakeDirectory struct {
	users map[string]fakeDirUser // keyed by any login form it should find
	asked []string
}

func (d *fakeDirectory) lookup(p *ldapProvider) func(string, string) (ldapLookupResult, bool, error) {
	return func(username, password string) (ldapLookupResult, bool, error) {
		d.asked = append(d.asked, username)
		u, ok := d.users[strings.ToLower(username)]
		if !ok || u.password != password {
			return ldapLookupResult{}, false, nil
		}
		return ldapLookupResult{role: p.roleFromGroups(u.groups), name: u.name, upn: u.upn, groups: u.groups}, true, nil
	}
}

// twoForestConfig is a default CORP directory plus a named EMEA profile.
func twoForestConfig() *ldapPersisted {
	return &ldapPersisted{
		Enabled:      true,
		URL:          "ldaps://dc1.corp.example.com:636",
		BaseDN:       "DC=corp,DC=example,DC=com",
		RoleMapRaw:   "NCC-Admins=admin",
		DefaultRole:  "viewer",
		UPNSuffixes:  []string{"corp.example.com"},
		NetBIOSNames: []string{"corp"},
		Profiles: []ldapProfile{{
			Name:         "emea",
			UPNSuffixes:  []string{"@EMEA.corp.example.com", "emea.example.net"},
			NetBIOSNames: []string{"EMEA"},
			URL:          "ldaps://dc1.emea.example.net:636",
			BaseDN:       "DC=emea,DC=example,DC=net",
			RoleMapRaw:   "EMEA-NCC-Ops=operator",
			DefaultRole:  "viewer",
		}},
	}
}

// buildFakeRouter builds the router for cfg and wires each directory to a
// fake. The returned map is keyed by profile name.
func buildFakeRouter(t *testing.T, cfg *ldapPersisted, dirs map[string]*fakeDirectory) *ldapRouter {
	t.Helper()
	r, enabled, err := buildLDAPRouter(cfg)
	if err != nil || !enabled {
		t.Fatalf("buildLDAPRouter: enabled=%v err=%v", enabled, err)
	}
	for _, p := range r.all() {
		d := dirs[p.name]
		if d == nil {
			d = &fakeDirectory{}
			dirs[p.name] = d
		}
		p.lookupOverride = d.lookup(p)
	}
	return r
}

func corpAndEMEA() map[string]*fakeDirectory {
	return map[string]*fakeDirectory{
		"default": {users: map[string]fakeDirUser{
			"jdoe":                  {password: "corp-pw", name: "jdoe", upn: "jdoe@corp.example.com", groups: []string{"CN=NCC-Admins,OU=Groups,DC=corp,DC=example,DC=com"}},
			"jdoe@corp.example.com": {password: "corp-pw", name: "jdoe", upn: "jdoe@corp.example.com", groups: []string{"CN=NCC-Admins,OU=Groups,DC=corp,DC=example,DC=com"}},
		}},
		"emea": {users: map[string]fakeDirUser{
			"jdoe":                       {password: "emea-pw", name: "jdoe", upn: "JDoe@emea.corp.example.com", groups: []string{"CN=EMEA-NCC-Ops,OU=Groups,DC=emea,DC=example,DC=net"}},
			"jdoe@emea.corp.example.com": {password: "emea-pw", name: "jdoe", upn: "JDoe@emea.corp.example.com", groups: []string{"CN=EMEA-NCC-Ops,OU=Groups,DC=emea,DC=example,DC=net"}},
			"asmith":                     {password: "emea-pw", name: "asmith", upn: ""},
			"bwong":                      {password: "emea-pw", name: "bwong", upn: "bwong@other.example.org"},
		}},
	}
}

func TestValidateLDAPProfiles(t *testing.T) {
	cfg := twoForestConfig()
	if err := validateLDAPProfiles(cfg); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	// Claims are normalized in place.
	if got := cfg.Profiles[0].UPNSuffixes; len(got) != 2 || got[0] != "emea.corp.example.com" {
		t.Fatalf("suffixes not normalized: %v", got)
	}
	if got := cfg.NetBIOSNames; len(got) != 1 || got[0] != "CORP" {
		t.Fatalf("NetBIOS not normalized: %v", got)
	}

	bad := map[string]func(c *ldapPersisted){
		"reserved name":   func(c *ldapPersisted) { c.Profiles[0].Name = "Default" },
		"invalid name":    func(c *ldapPersisted) { c.Profiles[0].Name = "emea west" },
		"duplicate name":  func(c *ldapPersisted) { c.Profiles = append(c.Profiles, c.Profiles[0]) },
		"missing suffix":  func(c *ldapPersisted) { c.Profiles[0].UPNSuffixes = nil },
		"missing base dn": func(c *ldapPersisted) { c.Profiles[0].BaseDN = "" },
		"suffix claimed twice": func(c *ldapPersisted) {
			c.Profiles[0].UPNSuffixes = []string{"corp.example.com"}
		},
		"netbios claimed twice": func(c *ldapPersisted) { c.Profiles[0].NetBIOSNames = []string{"Corp"} },
		"bad role map":          func(c *ldapPersisted) { c.Profiles[0].RoleMapRaw = "x=superuser" },
		"bad suffix":            func(c *ldapPersisted) { c.Profiles[0].UPNSuffixes = []string{"emea\\corp"} },
		"bad netbios":           func(c *ldapPersisted) { c.Profiles[0].NetBIOSNames = []string{"emea.corp"} },
	}
	for name, mutate := range bad {
		t.Run(name, func(t *testing.T) {
			c := twoForestConfig()
			mutate(c)
			if err := validateLDAPProfiles(c); err == nil {
				t.Fatal("expected an error")
			}
			if _, _, err := buildLDAPRouter(c); err == nil {
				t.Fatal("buildLDAPRouter should reject it too")
			}
		})
	}
}

func TestLDAPRouterRoute(t *testing.T) {
	r, _, err := buildLDAPRouter(twoForestConfig())
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct{ in, dir, login string }{
		{"jdoe@emea.corp.example.com", "emea", "jdoe@emea.corp.example.com"},
		{"jdoe@EMEA.example.NET", "emea", "jdoe@EMEA.example.NET"},
		{`EMEA\jdoe`, "emea", "jdoe"},
		{`emea\jdoe`, "emea", "jdoe"},
		{`CORP\jdoe`, "default", "jdoe"},
		{"jdoe", "default", "jdoe"},
		{"jdoe@corp.example.com", "default", "jdoe@corp.example.com"},
		// Unclaimed domains go to the default directory unchanged, exactly as
		// before profiles existed.
		{"jdoe@unknown.example.org", "default", "jdoe@unknown.example.org"},
		{`APAC\jdoe`, "default", `APAC\jdoe`},
		{"jdoe@", "default", "jdoe@"},
	}
	for _, c := range cases {
		p, login := r.route(c.in)
		if p.name != c.dir || login != c.login {
			t.Errorf("route(%q) = %s/%q, want %s/%q", c.in, p.name, login, c.dir, c.login)
		}
	}
}

func TestLDAPRouterAuthenticate(t *testing.T) {
	t.Run("named profile user is stored under the lowercased UPN with its own role map", func(t *testing.T) {
		dirs := corpAndEMEA()
		r := buildFakeRouter(t, twoForestConfig(), dirs)
		for _, login := range []string{`EMEA\jdoe`, "jdoe@emea.corp.example.com"} {
			role, name, _, ok, err := r.authenticate(login, "emea-pw")
			if err != nil || !ok {
				t.Fatalf("%s: ok=%v err=%v", login, ok, err)
			}
			if name != "jdoe@emea.corp.example.com" || role != RoleOperator {
				t.Fatalf("%s: got %q/%v", login, name, role)
			}
		}
		if len(dirs["default"].asked) != 0 {
			t.Fatalf("default directory must not see EMEA logins: %v", dirs["default"].asked)
		}
	})

	t.Run("short name uses the default directory and keeps its sAMAccountName", func(t *testing.T) {
		dirs := corpAndEMEA()
		r := buildFakeRouter(t, twoForestConfig(), dirs)
		role, name, _, ok, err := r.authenticate("jdoe", "corp-pw")
		if err != nil || !ok || name != "jdoe" || role != RoleAdmin {
			t.Fatalf("got %q/%v ok=%v err=%v", name, role, ok, err)
		}
		// Same short name in two forests: two distinct canonical names.
		_, emeaName, _, _, _ := r.authenticate(`EMEA\jdoe`, "emea-pw")
		if emeaName == name {
			t.Fatalf("canonical names collide: %q", name)
		}
		if len(dirs["emea"].asked) != 1 {
			t.Fatalf("EMEA directory asked %v", dirs["emea"].asked)
		}
	})

	t.Run("the password only goes to the routed directory", func(t *testing.T) {
		dirs := corpAndEMEA()
		r := buildFakeRouter(t, twoForestConfig(), dirs)
		// The EMEA password with a CORP-routed name fails; EMEA is not tried.
		if _, _, _, ok, _ := r.authenticate("jdoe", "emea-pw"); ok {
			t.Fatal("wrong directory must not authenticate")
		}
		if len(dirs["emea"].asked) != 0 {
			t.Fatalf("EMEA directory was asked: %v", dirs["emea"].asked)
		}
	})

	t.Run("named profile user without a UPN is refused", func(t *testing.T) {
		r := buildFakeRouter(t, twoForestConfig(), corpAndEMEA())
		_, _, _, ok, err := r.authenticate(`EMEA\asmith`, "emea-pw")
		var ref *ldapRefusal
		if ok || !errors.As(err, &ref) || ref.code != "upn_missing" {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
	})

	t.Run("named profile user with an unclaimed UPN suffix is refused", func(t *testing.T) {
		r := buildFakeRouter(t, twoForestConfig(), corpAndEMEA())
		_, _, _, ok, err := r.authenticate(`EMEA\bwong`, "emea-pw")
		var ref *ldapRefusal
		if ok || !errors.As(err, &ref) || ref.code != "upn_suffix_unclaimed" {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
	})

	t.Run("default user named under another profile's suffix is refused", func(t *testing.T) {
		cfg := twoForestConfig()
		cfg.UsernameAttr = "userPrincipalName"
		dirs := map[string]*fakeDirectory{"default": {users: map[string]fakeDirUser{
			"mallory": {password: "pw", name: "jdoe@emea.corp.example.com"},
		}}}
		r := buildFakeRouter(t, cfg, dirs)
		_, _, _, ok, err := r.authenticate("mallory", "pw")
		var ref *ldapRefusal
		if ok || !errors.As(err, &ref) || ref.code != "directory_conflict" {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
	})

	t.Run("single config without profiles behaves as before", func(t *testing.T) {
		cfg := &ldapPersisted{Enabled: true, URL: "ldaps://dc1:636", BaseDN: "DC=corp,DC=com"}
		dirs := map[string]*fakeDirectory{"default": {users: map[string]fakeDirUser{
			"jdoe@corp.com": {password: "pw", name: "jdoe", upn: "jdoe@corp.com"},
			`corp\jdoe`:     {password: "pw", name: "never"},
		}}}
		r := buildFakeRouter(t, cfg, dirs)
		_, name, _, ok, err := r.authenticate("jdoe@corp.com", "pw")
		if err != nil || !ok || name != "jdoe" {
			t.Fatalf("got %q ok=%v err=%v", name, ok, err)
		}
		// No NetBIOS names claimed: the name is passed through unchanged.
		r.authenticate(`CORP\jdoe`, "pw")
		if got := dirs["default"].asked[1]; got != `CORP\jdoe` {
			t.Fatalf("login passed as %q", got)
		}
	})

	t.Run("operational errors name the directory", func(t *testing.T) {
		r := buildFakeRouter(t, twoForestConfig(), corpAndEMEA())
		r.profiles[0].lookupOverride = func(string, string) (ldapLookupResult, bool, error) {
			return ldapLookupResult{}, false, errLDAPTest
		}
		_, _, _, _, err := r.authenticate(`EMEA\jdoe`, "x")
		if err == nil || !strings.Contains(err.Error(), `directory "emea"`) {
			t.Fatalf("err=%v", err)
		}
	})
}

func TestHandleLoginDirectoryProfiles(t *testing.T) {
	newServer := func(t *testing.T) (*apiServer, map[string]*fakeDirectory) {
		dirs := corpAndEMEA()
		s, _ := newLDAPLoginTestServer(t, &fakeLDAP{})
		s.ldap = buildFakeRouter(t, twoForestConfig(), dirs)
		return s, dirs
	}

	t.Run("EMEA user signs in under the UPN", func(t *testing.T) {
		s, _ := newServer(t)
		code, data := loginRequest(t, s, `EMEA\\jdoe`, "emea-pw")
		if code != http.StatusOK || data["username"] != "jdoe@emea.corp.example.com" || data["role"] != "operator" {
			t.Fatalf("code=%d data=%+v", code, data)
		}
	})

	t.Run("short name goes to the default directory only", func(t *testing.T) {
		s, dirs := newServer(t)
		code, data := loginRequest(t, s, "jdoe", "corp-pw")
		if code != http.StatusOK || data["username"] != "jdoe" || len(dirs["default"].asked) != 1 || len(dirs["emea"].asked) != 0 {
			t.Fatalf("code=%d data=%v asked=%v/%v", code, data, dirs["default"].asked, dirs["emea"].asked)
		}
	})

	t.Run("refusal is a generic 401", func(t *testing.T) {
		s, _ := newServer(t)
		code, data := loginRequest(t, s, `EMEA\\asmith`, "emea-pw")
		if code != http.StatusUnauthorized {
			t.Fatalf("code=%d", code)
		}
		if msg, _ := data["__error"].(string); msg != "invalid username or password" {
			t.Fatalf("error should be generic, got %q", msg)
		}
	})

	t.Run("directory user named like a local account is still refused", func(t *testing.T) {
		s, dirs := newServer(t)
		dirs["default"].users["admin"] = fakeDirUser{password: "ad-pw", name: "Admin"}
		code, data := loginRequest(t, s, "admin", "ad-pw")
		if code != http.StatusUnauthorized || data["__error"] != "invalid username or password" {
			t.Fatalf("code=%d data=%+v", code, data)
		}
	})
}

func ldapSettingsRequest(t *testing.T, s *apiServer, method, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, "/api/v1/settings/ldap", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	s.handleLDAP(rr, req)
	var env struct {
		Data  map[string]any `json:"data"`
		Error string         `json:"error"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &env)
	if env.Data == nil {
		env.Data = map[string]any{}
	}
	env.Data["__error"] = env.Error
	return rr.Code, env.Data
}

func TestLDAPSettingsProfilesAPI(t *testing.T) {
	// enabled=false keeps the save off the network (no connectivity check).
	s, _ := newLDAPLoginTestServer(t, &fakeLDAP{})
	s.ldap, s.ldapEnabled = nil, false

	put := `{"enabled":false,"url":"ldaps://dc1.corp:636","base_dn":"DC=corp","bind_password":"corp-secret","ca_cert_pem":"CORP-CA",
		"netbios_names":["corp"],
		"profiles":[{"name":"emea","upn_suffixes":["emea.corp.example.com"],"netbios_names":["emea"],
		"url":"ldaps://dc1.emea:636","base_dn":"DC=emea","bind_dn":"CN=svc","bind_password":"emea-secret","ca_cert_pem":"EMEA-CA"}]}`
	if code, data := ldapSettingsRequest(t, s, http.MethodPut, put); code != http.StatusOK {
		t.Fatalf("PUT code=%d %v", code, data)
	}

	code, data := ldapSettingsRequest(t, s, http.MethodGet, "")
	if code != http.StatusOK {
		t.Fatalf("GET code=%d", code)
	}
	profiles, _ := data["profiles"].([]any)
	if len(profiles) != 1 {
		t.Fatalf("profiles=%v", data["profiles"])
	}
	p := profiles[0].(map[string]any)
	if p["name"] != "emea" || p["has_bind_password"] != true || p["has_ca_cert"] != true {
		t.Fatalf("profile=%v", p)
	}
	if _, leaked := p["bind_password"]; leaked {
		t.Fatal("bind password must never be returned")
	}
	if nb, _ := p["netbios_names"].([]any); len(nb) != 1 || nb[0] != "EMEA" {
		t.Fatalf("netbios not normalized: %v", p["netbios_names"])
	}

	// An older client that only knows the single-directory fields: profiles,
	// claims and the default's secrets are all kept.
	old := `{"enabled":false,"url":"ldaps://dc2.corp:636","base_dn":"DC=corp"}`
	if code, data := ldapSettingsRequest(t, s, http.MethodPut, old); code != http.StatusOK {
		t.Fatalf("legacy PUT code=%d %v", code, data)
	}
	cfg := s.users.getLDAP()
	if len(cfg.Profiles) != 1 || cfg.BindPassword != "corp-secret" || cfg.CACertPEM != "CORP-CA" || len(cfg.NetBIOSNames) != 1 {
		t.Fatalf("legacy PUT lost data: %+v", cfg)
	}

	// Renaming a profile with previous_name carries its write-only secrets;
	// omitting them keeps them.
	rename := `{"enabled":false,"url":"ldaps://dc2.corp:636","base_dn":"DC=corp",
		"profiles":[{"name":"europe","previous_name":"emea","upn_suffixes":["emea.corp.example.com"],"url":"ldaps://dc1.emea:636","base_dn":"DC=emea"}]}`
	if code, data := ldapSettingsRequest(t, s, http.MethodPut, rename); code != http.StatusOK {
		t.Fatalf("rename PUT code=%d %v", code, data)
	}
	cfg = s.users.getLDAP()
	if cfg.Profiles[0].Name != "europe" || cfg.Profiles[0].BindPassword != "emea-secret" || cfg.Profiles[0].CACertPEM != "EMEA-CA" {
		t.Fatalf("rename lost secrets: %+v", cfg.Profiles[0])
	}

	// Invalid profiles are rejected before saving.
	dup := `{"enabled":false,"url":"u","base_dn":"b","upn_suffixes":["emea.corp.example.com"],
		"profiles":[{"name":"europe","upn_suffixes":["emea.corp.example.com"],"url":"u","base_dn":"b"}]}`
	if code, data := ldapSettingsRequest(t, s, http.MethodPut, dup); code != http.StatusBadRequest || !strings.Contains(data["__error"].(string), "claimed by both") {
		t.Fatalf("dup claim: code=%d %v", code, data)
	}

	// An empty list clears the profiles.
	clear := `{"enabled":false,"url":"u","base_dn":"b","profiles":[]}`
	if code, _ := ldapSettingsRequest(t, s, http.MethodPut, clear); code != http.StatusOK {
		t.Fatalf("clear code=%d", code)
	}
	if got := s.users.getLDAP().Profiles; len(got) != 0 {
		t.Fatalf("profiles not cleared: %+v", got)
	}
}

func TestLDAPSingleConfigMigratesAsDefaultProfile(t *testing.T) {
	// A users.json written before directory profiles existed.
	dir := t.TempDir()
	path := filepath.Join(dir, "users.json")
	legacy := `{"users":[],"ldap":{"enabled":true,"url":"ldaps://dc1:636","bind_dn":"CN=svc","bind_password":"pw","base_dn":"DC=corp,DC=com","role_map":"NCC-Admins=admin"}}`
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := openUserDB(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg := db.getLDAP()
	if cfg == nil || len(cfg.Profiles) != 0 {
		t.Fatalf("cfg=%+v", cfg)
	}
	r, enabled, err := buildLDAPRouter(cfg)
	if err != nil || !enabled || r.def.name != "default" || r.def.bindPassword != "pw" || len(r.profiles) != 0 {
		t.Fatalf("router: enabled=%v err=%v %+v", enabled, err, r)
	}
}

func TestClusterGroupForeignUPNNotMatchedByLocalPart(t *testing.T) {
	grp := clusterGroup{Name: "g", ADUsers: []string{"jdoe@emea.corp.example.com", "asmith@corp.example.com"}}
	foreign := map[string]bool{"emea.corp.example.com": true}
	if principalInClusterGroup("jdoe", nil, grp, foreign) {
		t.Fatal("default-directory jdoe must not match the EMEA user jdoe@emea.corp.example.com")
	}
	if !principalInClusterGroup("jdoe@emea.corp.example.com", nil, grp, foreign) {
		t.Fatal("the EMEA user matches by UPN")
	}
	if !principalInClusterGroup("asmith", nil, grp, foreign) {
		t.Fatal("unclaimed suffix keeps the local-part match")
	}
	if !principalInClusterGroup("jdoe", nil, grp, nil) {
		t.Fatal("without profiles the local-part match is unchanged")
	}
}
