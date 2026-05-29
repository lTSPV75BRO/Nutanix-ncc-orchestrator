package main

import (
	"reflect"
	"testing"
)

// TestUIExplicitFlagSet pins the contract used by uiApplyStackAwareDefaults:
// only flags actually typed on the command line should be treated as
// "user-set" and excluded from auto-redirect. Mirrors the api-server
// helper so behavior across the two sub-binaries stays in sync.
func TestUIExplicitFlagSet(t *testing.T) {
	cases := []struct {
		name string
		argv []string
		want map[string]bool
	}{
		{"empty", []string{}, map[string]bool{}},
		{"dir flag", []string{"--dir", "/srv/spa"}, map[string]bool{"dir": true}},
		{"equals form", []string{"--api-token-file=/etc/ncc/token"}, map[string]bool{"api-token-file": true}},
		{"single dash", []string{"-listen", ":8080"}, map[string]bool{"listen": true}},
		{"double dash separator", []string{"--", "trail"}, map[string]bool{}},
		{"mixed", []string{
			"--dir", "/srv/spa",
			"--backend-url=https://api.example.com",
		}, map[string]bool{
			"dir": true, "backend-url": true,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := uiExplicitFlagSet(tc.argv)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
