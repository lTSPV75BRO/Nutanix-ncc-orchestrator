package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestValidatePasswordFollowsSP80063B(t *testing.T) {
	accept := []string{
		"maple river lantern",          // passphrase with spaces
		"correct-horse-battery-42",     // words plus digits
		"Zürich Bahnhof Uhr 9",         // non-ASCII letters
		"839201746582910",              // random digits
		strings.Repeat("ab1", 5) + "Q", // not a pure repetition
		strings.Repeat("x", 3) + "yz-orbit-canyon",
	}
	for _, pw := range accept {
		if msg := validatePassword("alice", pw); msg != "" {
			t.Errorf("%q rejected: %s", pw, msg)
		}
	}

	reject := map[string]string{
		"short-pass":               "at least 15",      // too short
		"fourteen-chars":           "at least 15",      // 14 characters
		strings.Repeat("a", 73):    "at most 72 bytes", // past bcrypt's limit
		"alice-maple-river-1":      "username",         // contains username
		"ALICE maple river 1":      "username",         // case-insensitive
		"aaaaaaaaaaaaaaaa":         "predictable",      // one repeated character
		"abcabcabcabcabcabc":       "predictable",      // short unit repeated
		"123456789012345":          "predictable",      // sequential with wrap
		"abcdefghijklmnop":         "predictable",      // sequential letters
		"!!!!!!!!!!!!!!!!":         "predictable",      // symbols only, repeated
		"Password12345678":         "too common",       // common word + digits
		"P@ssword-P@ssword!":       "",                 // leet isn't in the list; accepted
		"adminadminadmin1":         "too common",       // repeated common word
		"nutanix-nutanix-2026":     "too common",       // product name
		"welcome2026password":      "too common",       // two common words
		"tab\there-is-long-enough": "control characters",
	}
	for pw, want := range reject {
		msg := validatePassword("alice", pw)
		if want == "" {
			if msg != "" {
				t.Errorf("%q rejected: %s", pw, msg)
			}
			continue
		}
		if !strings.Contains(msg, want) {
			t.Errorf("%q: got %q, want a message containing %q", pw, msg, want)
		}
	}
}

// Every place that sets a local password applies the policy.
func TestPasswordPolicyAppliedToEndpoints(t *testing.T) {
	s := newDBServer(t)
	if _, _, err := s.users.bootstrapAdminIfEmpty("admin"); err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	s.handleUsers(rr, httptest.NewRequest(http.MethodPost, "/api/v1/settings/users",
		strings.NewReader(`{"username":"erin","password":"Password12345678","role":"viewer"}`)))
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "too common") {
		t.Fatalf("create with common password: %d %s", rr.Code, rr.Body.String())
	}
	rr = httptest.NewRecorder()
	s.handleUsers(rr, httptest.NewRequest(http.MethodPost, "/api/v1/settings/users",
		strings.NewReader(`{"username":"erin","password":"maple-river-lantern-42","role":"viewer"}`)))
	if rr.Code != http.StatusOK {
		t.Fatalf("create erin: %d %s", rr.Code, rr.Body.String())
	}
	rr = httptest.NewRecorder()
	s.handleUserByName(rr, httptest.NewRequest(http.MethodPut, "/api/v1/settings/users/erin",
		strings.NewReader(`{"password":"erin-was-here-2026"}`)))
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "username") {
		t.Fatalf("reset with username in password: %d %s", rr.Code, rr.Body.String())
	}
}
