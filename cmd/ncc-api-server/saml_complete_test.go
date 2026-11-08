package main

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/crewjam/saml/samlsp"
)

// An assertion that names no user (no NameID, no username attribute) must not
// sign anyone in as a shared account.
func TestSAMLCompleteRefusesAssertionWithoutUser(t *testing.T) {
	buf := captureServerLog(t, slog.LevelDebug)
	s := &apiServer{}
	s.saml = &samlProvider{roleAtt: "groups", defaultRole: RoleViewer}

	sess := samlsp.JWTSessionClaims{Attributes: samlsp.Attributes{"groups": {"ncc-admins"}}}
	r := httptest.NewRequest(http.MethodGet, "/saml/complete", nil)
	r = r.WithContext(samlsp.ContextWithSession(r.Context(), sess))
	rr := httptest.NewRecorder()
	s.handleSAMLComplete(rr, r)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status %d, want 403: %s", rr.Code, rr.Body)
	}
	for _, c := range rr.Result().Cookies() {
		if c.Value != "" {
			t.Fatalf("set cookie %s on a refused login", c.Name)
		}
	}
	if !strings.Contains(buf.String(), "saml login refused") {
		t.Fatalf("refusal not logged: %s", buf)
	}
}
