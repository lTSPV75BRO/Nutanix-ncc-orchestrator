// Package uiversion identifies NCC UI clients on API requests and matches
// blocked (known-vulnerable) UI / UI-server versions.
package uiversion

import (
	"strings"
	"unicode"
)

const (
	// VersionHeader is sent by the SPA (stamped at frontend build).
	VersionHeader = "X-NCC-UI-Version"
	// ServerVersionHeader is stamped by ncc-ui-server on proxied /api/ calls.
	// The proxy overwrites any inbound value so the browser cannot spoof it.
	ServerVersionHeader = "X-NCC-UI-Server-Version"
	// ClientHeader is how a custom dashboard or third-party integration names
	// itself. Such clients are never version-gated; they are only audited and
	// watched for suspicious access.
	ClientHeader = "X-NCC-Client"
	// RequestedWithValue is the X-Requested-With token the SPA sends.
	RequestedWithValue = "ncc-ui"
	// UnknownVersion is recorded when a request claims to be the UI but
	// sent no parseable version (builds before 2.4.0 cannot declare one).
	UnknownVersion = "unknown"
	// ErrorCode is returned when a blocked UI is refused.
	ErrorCode = "NCC_API_UI_VERSION_BLOCKED"
	// SupportedFrom is the first release that declares its version.
	SupportedFrom = "2.4.0"
)

// Entry is one blocked product version.
type Entry struct {
	Version    string   `json:"version"`
	Component  string   `json:"component,omitempty"` // ui, ui-server, or any
	Reason     string   `json:"reason"`
	Advisories []string `json:"advisories,omitempty"` // GO-..., GHSA-..., or an incident name
	Severity   string   `json:"severity,omitempty"`   // high, medium or low (highest known)
	FixedIn    string   `json:"fixed_in,omitempty"`
	Builtin    bool     `json:"builtin,omitempty"`
}

// release is the shipped list's compact form.
type release struct {
	version    string
	severity   string
	summary    string
	advisories []string
}

// shipped lists the releases whose dashboard or UI server carries a known
// vulnerability. It comes from scanning every tagged release (govulncheck on
// the Go call graph, osv-scanner on the frontend lockfile) and from the
// 2.2.1 incident; the scan and its limits are in docs/audit/release-scan.md.
// Releases before 2.0.0 have no dashboard, and 2.3.0 had no scanner finding,
// so neither is listed. Extend it by shipping a new release, or at run time
// from Settings (the admin-managed list).
var shipped = []release{
	{"2.0.0", "high", "react-router 7.15.1 and DOMPurify 3.4.7", []string{"GHSA-chx6-hx7r-mcp5", "GHSA-qwww-vcr4-c8h2", "GHSA-337j-9hxr-rhxg", "GHSA-h8fp-f39c-q6mh", "GHSA-wrjc-x8rr-h8h6", "DOMPurify 3.4.7 (9 advisories)"}},
	{"2.0.1", "high", "react-router 7.15.1 and DOMPurify 3.4.7", []string{"GHSA-chx6-hx7r-mcp5", "GHSA-qwww-vcr4-c8h2", "GHSA-337j-9hxr-rhxg", "GHSA-h8fp-f39c-q6mh", "GHSA-wrjc-x8rr-h8h6", "DOMPurify 3.4.7 (9 advisories)"}},
	{"2.0.2", "high", "react-router 7.15.1 and DOMPurify 3.4.7", []string{"GHSA-chx6-hx7r-mcp5", "GHSA-qwww-vcr4-c8h2", "GHSA-337j-9hxr-rhxg", "GHSA-h8fp-f39c-q6mh", "GHSA-wrjc-x8rr-h8h6", "DOMPurify 3.4.7 (9 advisories)"}},
	{"2.1.0", "medium", "HTTP/2 denial of service in golang.org/x/net, the SAML library and DOMPurify", []string{"GO-2026-6617", "GO-2026-6612", "GO-2026-6611", "GO-2026-6610", "GO-2026-6603", "GO-2026-6348", "GO-2026-6909", "GHSA-6688-9rhm-gjv2", "GHSA-c6gr-qvv4-5v3f", "GHSA-cg2g-xh64-v4q7", "GHSA-mrv2-3pqr-j5jr", "GHSA-p98j-92pf-mc4p"}},
	{"2.1.1", "medium", "HTTP/2 denial of service in golang.org/x/net, the SAML library and DOMPurify", []string{"GO-2026-6617", "GO-2026-6612", "GO-2026-6611", "GO-2026-6610", "GO-2026-6603", "GO-2026-6348", "GO-2026-6909", "GHSA-6688-9rhm-gjv2", "GHSA-c6gr-qvv4-5v3f", "GHSA-cg2g-xh64-v4q7", "GHSA-mrv2-3pqr-j5jr", "GHSA-p98j-92pf-mc4p"}},
	{"2.2.0", "medium", "HTTP/2 denial of service in golang.org/x/net, the SAML library and DOMPurify", []string{"GO-2026-6617", "GO-2026-6612", "GO-2026-6611", "GO-2026-6610", "GO-2026-6603", "GO-2026-6909", "GHSA-c6gr-qvv4-5v3f", "GHSA-cg2g-xh64-v4q7", "GHSA-mrv2-3pqr-j5jr"}},
	{"2.2.1", "high", "the node2shell issue and a nil-pointer panic in the SAML library", []string{"node2shell", "GO-2026-6909"}},
}

// Builtin is the shipped blocked-version list. Builds before 2.4.0 send no
// version, so a request from one cannot be matched against these entries; the
// "block unversioned" policy (on by default) is what refuses them.
func Builtin() []Entry {
	out := make([]Entry, 0, len(shipped))
	for _, r := range shipped {
		out = append(out, Entry{
			Version:    r.version,
			Component:  "any",
			Reason:     "NCC " + r.version + " ships a known vulnerability (" + r.summary + "); upgrade ncc-ui-server and frontend-dist to " + SupportedFrom + " or later",
			Advisories: append([]string(nil), r.advisories...),
			Severity:   r.severity,
			FixedIn:    SupportedFrom,
			Builtin:    true,
		})
	}
	return out
}

// UnversionedReason is the refusal text for a dashboard that declares no version.
func UnversionedReason() string {
	return "this dashboard build does not declare its version, which means it is older than " + SupportedFrom +
		" and may be a release with a known vulnerability (including 2.2.1, node2shell); upgrade ncc-ui-server and frontend-dist to " + SupportedFrom + " or later"
}

// Sanitize trims, strips a leading v, and keeps a short semver-ish token.
func Sanitize(raw string) string {
	s := strings.TrimSpace(raw)
	s = strings.TrimPrefix(s, "v")
	s = strings.TrimPrefix(s, "V")
	if s == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.' || r == '-' || r == '+' || r == '_' {
			b.WriteRune(r)
		}
		if b.Len() >= 32 {
			break
		}
	}
	return b.String()
}

// NormalizeComponent maps ui / ui-server / any. Empty becomes any.
func NormalizeComponent(c string) string {
	switch strings.ToLower(strings.TrimSpace(c)) {
	case "ui", "frontend", "spa":
		return "ui"
	case "ui-server", "uiserver", "ncc-ui-server":
		return "ui-server"
	default:
		return "any"
	}
}

// Match reports whether version (already sanitized) is blocked for component.
func Match(version, component string, list []Entry) (Entry, bool) {
	version = Sanitize(version)
	if version == "" {
		return Entry{}, false
	}
	want := NormalizeComponent(component)
	for _, e := range list {
		if Sanitize(e.Version) != version {
			continue
		}
		got := NormalizeComponent(e.Component)
		if got == "any" || got == want || want == "any" {
			return e, true
		}
	}
	return Entry{}, false
}

// Remediation is the operator-facing recovery text.
func Remediation() string {
	return "Upgrade ncc-ui-server and frontend-dist to " + SupportedFrom + " or later (reload the page afterwards). An administrator can review the list under System, Updates & backups, Blocked versions."
}
