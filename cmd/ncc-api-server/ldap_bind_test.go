package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-ldap/ldap/v3"
)

// A failed re-bind as the user must only count as a wrong password when the
// directory actually refused the credentials; a dropped connection or a busy
// server must surface as an operational error so it never feeds the lockout.
func TestLDAPUserBindIsOperational(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"invalid credentials", &ldap.Error{ResultCode: ldap.LDAPResultInvalidCredentials}, false},
		{"constraint violation (locked/expired)", &ldap.Error{ResultCode: ldap.LDAPResultConstraintViolation}, false},
		{"wrapped invalid credentials", fmt.Errorf("bind: %w", &ldap.Error{ResultCode: ldap.LDAPResultInvalidCredentials}), false},
		{"network error", &ldap.Error{ResultCode: ldap.ErrorNetwork}, true},
		{"server busy", &ldap.Error{ResultCode: ldap.LDAPResultBusy}, true},
		{"server unavailable", &ldap.Error{ResultCode: ldap.LDAPResultUnavailable}, true},
		{"time limit exceeded", &ldap.Error{ResultCode: ldap.LDAPResultTimeLimitExceeded}, true},
		{"plain I/O error", errors.New("connection reset by peer"), true},
	}
	for _, c := range cases {
		if got := ldapUserBindIsOperational(c.err); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
