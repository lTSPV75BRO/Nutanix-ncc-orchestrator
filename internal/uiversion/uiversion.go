// Package uiversion identifies NCC UI clients on API requests and matches
// compromised (blacklisted) UI / UI-server versions.
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
	// RequestedWithValue is the X-Requested-With token the SPA sends.
	RequestedWithValue = "ncc-ui"
	// UnknownVersion is recorded when a request claims to be the UI but
	// sent no parseable version (legacy 2.2.x / 2.3.x bundles).
	UnknownVersion = "unknown"
	// ErrorCode is returned when a blacklisted UI is refused.
	ErrorCode = "NCC_API_UI_VERSION_BLOCKED"
)

// Entry is one blacklisted product version.
type Entry struct {
	Version   string `json:"version"`
	Component string `json:"component,omitempty"` // ui, ui-server, or any
	Reason    string `json:"reason"`
	Builtin   bool   `json:"builtin,omitempty"`
}

// Builtin is the shipped compromised-version list. 2.2.1 UI is listed for
// the node2shell issue; that build also cannot send VersionHeader, so
// unversioned ncc-ui clients are treated as unknown and blocked by default.
func Builtin() []Entry {
	reason := "NCC UI 2.2.1 is affected by node2shell; upgrade ncc-ui-server and frontend-dist to 2.3.0 or later (2.4.0 recommended)"
	return []Entry{
		{Version: "2.2.1", Component: "ui", Reason: reason, Builtin: true},
		{Version: "2.2.1", Component: "ui-server", Reason: reason, Builtin: true},
	}
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
	return "Upgrade ncc-ui-server and frontend-dist to a supported release (2.4.0 or later). If you must keep a mixed stack, an admin can adjust Settings → Audit log → Compromised UI versions."
}
