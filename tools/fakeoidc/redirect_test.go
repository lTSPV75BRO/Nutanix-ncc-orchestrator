package main

import "testing"

func TestValidateRedirectURL(t *testing.T) {
	allowed := map[string]bool{"localhost": true, "ncc.example.com": true}
	cases := map[string]string{
		"https://NCC.example.com:8443/api/v1/auth/oidc/callback?x=1": "https://ncc.example.com:8443/api/v1/auth/oidc/callback?x=1",
		"http://localhost/cb": "http://localhost/cb",
	}
	for in, want := range cases {
		u, err := validateRedirectURL(in, allowed)
		if err != nil || u.String() != want {
			t.Fatalf("%s: got %v, %v; want %s", in, u, err, want)
		}
	}
	for _, bad := range []string{"https://evil.example/cb", "javascript:alert(1)", "https://u:p@localhost/cb", "https://localhost/cb#f", "https://localhost:99999/cb"} {
		if _, err := validateRedirectURL(bad, allowed); err == nil {
			t.Fatalf("%s was accepted", bad)
		}
	}
}
