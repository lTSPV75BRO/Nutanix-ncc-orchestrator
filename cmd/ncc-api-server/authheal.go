package main

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// This file implements live self-heal health probes for the external auth
// providers: an LDAP/AD service-account bind, the SAML SP certificate validity
// window, and a SAML-sensitive clock-skew check against the IdP host. These are
// surfaced (on demand) through the diagnostics endpoint and the System Health
// UI. They are deliberately *non-mutating*: a probe never rotates the SAML SP
// keypair automatically because that changes the SP metadata the IdP trusts and
// would break logins until an admin re-shares it — so an expiring SP cert warns
// rather than auto-renews (unlike the self-signed TLS cert, which is safe to
// auto-rotate).

type diagStatus string

const (
	diagOK   diagStatus = "ok"
	diagWarn diagStatus = "warn"
	diagFail diagStatus = "fail"
)

// diagResult mirrors the orchestrator's healResult so the diagnostics endpoint
// can merge api-server probes with the orchestrator's doctor checks.
type diagResult struct {
	ID       string     `json:"id"`
	Title    string     `json:"title"`
	Category string     `json:"category"`
	Status   diagStatus `json:"status"`
	Message  string     `json:"message"`
	Hint     string     `json:"hint,omitempty"`
}

// authDiagnostics runs the live external-auth probes and returns one result per
// configured provider. Providers that are not enabled report a benign ok.
func (s *apiServer) authDiagnostics() []diagResult {
	out := []diagResult{s.diagLDAP(), s.diagSAMLCert()}
	if r, ok := s.diagSAMLClockSkew(); ok {
		out = append(out, r)
	}
	return out
}

// diagLDAP performs a credential-free connectivity probe: dial the directory,
// bind the service account, and run a base-scoped search (reusing the same
// validate() used at config-save time).
func (s *apiServer) diagLDAP() diagResult {
	res := diagResult{ID: "ldap-bind", Title: "LDAP/AD directory reachable", Category: "directory"}
	var prov *ldapProvider
	if s.users != nil {
		if cfg := s.users.getLDAP(); cfg != nil && cfg.Enabled {
			p, enabled, err := buildLDAPProvider(cfg)
			if err != nil {
				res.Status = diagFail
				res.Message = "invalid LDAP config: " + err.Error()
				res.Hint = "Fix the directory settings under Settings → Access."
				return res
			}
			if enabled {
				prov = p
			}
		}
	}
	if prov == nil {
		if la := s.currentLDAP(); la != nil {
			if p, ok := la.(*ldapProvider); ok {
				prov = p
			}
		}
	}
	if prov == nil {
		res.Status = diagOK
		res.Message = "LDAP/AD not enabled"
		return res
	}
	if err := prov.validate(); err != nil {
		res.Status = diagFail
		res.Message = "directory probe failed: " + err.Error()
		res.Hint = "Check the service-account bind credentials, URL/port/TLS trust, and base DN. Logins via AD will fail until this recovers."
		return res
	}
	res.Status = diagOK
	res.Message = "service-account bind + base search OK"
	return res
}

// diagSAMLCert checks the SAML SP certificate's validity window.
func (s *apiServer) diagSAMLCert() diagResult {
	res := diagResult{ID: "saml-sp-cert", Title: "SAML SP certificate validity", Category: "sso"}
	var certPEM string
	if s.users != nil {
		if sc := s.users.getSAML(); sc != nil && sc.Enabled {
			certPEM = strings.TrimSpace(sc.SPCertPEM)
		}
	}
	if certPEM == "" {
		res.Status = diagOK
		res.Message = "SAML not enabled"
		return res
	}
	block, _ := pem.Decode([]byte(certPEM))
	if block == nil {
		res.Status = diagFail
		res.Message = "SP certificate PEM is unparseable"
		return res
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		res.Status = diagFail
		res.Message = "SP certificate parse error: " + err.Error()
		return res
	}
	left := time.Until(cert.NotAfter)
	res.Message = fmt.Sprintf("SP cert valid until %s (%s remaining)", cert.NotAfter.UTC().Format(time.RFC3339), humanShortDur(left))
	res.Status = certExpiryStatus(left)
	switch res.Status {
	case diagFail:
		res.Hint = "The SAML SP certificate has expired. Regenerate the SP keypair (Settings → Access → SSO) and re-share SP metadata with your IdP."
	case diagWarn:
		res.Hint = "SP certificate expires soon. Plan to regenerate the SP keypair and re-share SP metadata with your IdP (this is a coordinated change, not auto-renewed)."
	}
	return res
}

// certExpiryStatus maps remaining validity to a severity: expired -> fail,
// under 30 days -> warn, otherwise ok.
func certExpiryStatus(remaining time.Duration) diagStatus {
	switch {
	case remaining <= 0:
		return diagFail
	case remaining < 30*24*time.Hour:
		return diagWarn
	default:
		return diagOK
	}
}

// diagSAMLClockSkew compares the local clock to the IdP host's clock (via the
// Date response header of the metadata URL). SAML assertions are time-sensitive,
// so material skew silently breaks logins. Returns ok=false (skipped) when there
// is no remote IdP metadata URL to probe.
func (s *apiServer) diagSAMLClockSkew() (diagResult, bool) {
	res := diagResult{ID: "clock-skew", Title: "Clock skew (SAML-sensitive)", Category: "sso"}
	var metaURL string
	if s.users != nil {
		if sc := s.users.getSAML(); sc != nil && sc.Enabled {
			metaURL = strings.TrimSpace(sc.IDPMetadataURL)
		}
	}
	if metaURL == "" {
		return res, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, metaURL, nil)
	if err != nil {
		return res, false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		res.Status = diagWarn
		res.Message = "could not reach IdP metadata URL to check clock skew: " + err.Error()
		return res, true
	}
	defer resp.Body.Close()
	dateHdr := resp.Header.Get("Date")
	if dateHdr == "" {
		return res, false
	}
	idpTime, err := http.ParseTime(dateHdr)
	if err != nil {
		return res, false
	}
	skew := time.Since(idpTime)
	if skew < 0 {
		skew = -skew
	}
	res.Message = fmt.Sprintf("local clock differs from IdP host by %s", humanShortDur(skew))
	if skew > 2*time.Minute {
		res.Status = diagWarn
		res.Hint = "Sync the host clock with NTP — SAML assertions will be rejected for skew beyond the IdP's tolerance."
	} else {
		res.Status = diagOK
	}
	return res, true
}

func humanShortDur(d time.Duration) string {
	if d < 0 {
		d = -d
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%.1fh", d.Hours())
	default:
		return fmt.Sprintf("%.1fd", d.Hours()/24)
	}
}
