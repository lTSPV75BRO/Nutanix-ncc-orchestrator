package main

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jimlambrt/gldap"
)

// startFakeDirectory runs a small in-process LDAP server speaking the real
// wire protocol. The service account is cn=svc; users are found by
// sAMAccountName; binding as cn=good succeeds, cn=wrong is refused with
// invalidCredentials, and cn=busy answers "busy" (the directory could not
// judge the password).
func startFakeDirectory(t *testing.T) string {
	t.Helper()
	srv, err := gldap.NewServer()
	if err != nil {
		t.Fatal(err)
	}
	mux, err := gldap.NewMux()
	if err != nil {
		t.Fatal(err)
	}
	if err := mux.Bind(func(w *gldap.ResponseWriter, r *gldap.Request) {
		m, err := r.GetSimpleBindMessage()
		if err != nil {
			return
		}
		code := gldap.ResultInvalidCredentials
		switch {
		case m.UserName == "cn=svc,dc=example,dc=com" && string(m.Password) == "svc-pass":
			code = gldap.ResultSuccess
		case m.UserName == "cn=good,dc=example,dc=com" && string(m.Password) == "pw":
			code = gldap.ResultSuccess
		case strings.HasPrefix(m.UserName, "cn=busy,"):
			code = gldap.ResultBusy
		}
		_ = w.Write(r.NewBindResponse(gldap.WithResponseCode(code)))
	}); err != nil {
		t.Fatal(err)
	}
	if err := mux.Search(func(w *gldap.ResponseWriter, r *gldap.Request) {
		m, err := r.GetSearchMessage()
		if err != nil {
			return
		}
		for _, u := range []string{"good", "wrong", "busy"} {
			if strings.Contains(m.Filter, "(sAMAccountName="+u+")") {
				_ = w.Write(r.NewSearchResponseEntry("cn="+u+",dc=example,dc=com", gldap.WithAttributes(map[string][]string{
					"sAMAccountName": {u},
					"memberOf":       {"CN=Admins,DC=example,DC=com"},
				})))
			}
		}
		_ = w.Write(r.NewSearchDoneResponse(gldap.WithResponseCode(gldap.ResultSuccess)))
	}); err != nil {
		t.Fatal(err)
	}
	if err := srv.Router(mux); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	go func() { _ = srv.Run(addr) }()
	t.Cleanup(func() { _ = srv.Stop() })
	for i := 0; i < 100 && !srv.Ready(); i++ {
		time.Sleep(20 * time.Millisecond)
	}
	if !srv.Ready() {
		t.Fatal("fake directory did not start")
	}
	return "ldap://" + addr
}

func fakeDirectoryProvider(url string) *ldapProvider {
	return &ldapProvider{
		urls: []string{url}, bindDN: "cn=svc,dc=example,dc=com", bindPassword: "svc-pass",
		baseDN: "dc=example,dc=com", userFilter: "(sAMAccountName=%s)",
		usernameAttr: "sAMAccountName", groupAttr: "memberOf",
		roleMap: map[string]Role{"cn=admins,dc=example,dc=com": RoleAdmin}, defaultRole: RoleViewer,
		timeout: 5 * time.Second,
	}
}

// Behavioural check of COR-08 against a real LDAP conversation: only a
// directory that refuses the credentials counts as a wrong password.
func TestLDAPLookupAgainstFakeDirectory(t *testing.T) {
	p := fakeDirectoryProvider(startFakeDirectory(t))

	r, ok, err := p.lookup("good", "pw")
	if err != nil || !ok || r.name != "good" || !r.found {
		t.Fatalf("good user: ok=%v err=%v result=%+v", ok, err, r)
	}

	r, ok, err = p.lookup("wrong", "nope")
	if err != nil || ok || !r.found {
		t.Fatalf("wrong password must be (found, not ok, no error): ok=%v err=%v result=%+v", ok, err, r)
	}

	if _, ok, err = p.lookup("busy", "pw"); err == nil || ok {
		t.Fatalf("a busy directory must be an operational error, not a wrong password: ok=%v err=%v", ok, err)
	}

	if r, ok, err = p.lookup("nobody", "pw"); err != nil || ok || r.found {
		t.Fatalf("unknown user: ok=%v err=%v result=%+v", ok, err, r)
	}
}

func TestLDAPLookupServiceAccountRefused(t *testing.T) {
	p := fakeDirectoryProvider(startFakeDirectory(t))
	p.bindPassword = "wrong-service-password"
	if _, ok, err := p.lookup("good", "pw"); err == nil || ok {
		t.Fatalf("a refused service-account bind must be an operational error: ok=%v err=%v", ok, err)
	}
}
