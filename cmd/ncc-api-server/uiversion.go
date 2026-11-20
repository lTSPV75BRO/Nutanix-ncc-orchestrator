package main

import (
	"net/http"
	"strings"

	"goncc/internal/uiversion"
)

const uiVersionPolicyMaxExtra = 64

func isNCCUIRequest(r *http.Request) bool {
	if r == nil {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(r.Header.Get("X-Requested-With")), uiversion.RequestedWithValue) {
		return true
	}
	if uiversion.Sanitize(r.Header.Get(uiversion.VersionHeader)) != "" {
		return true
	}
	if uiversion.Sanitize(r.Header.Get(uiversion.ServerVersionHeader)) != "" {
		return true
	}
	return false
}

func presentedUIVersion(r *http.Request) string {
	if r == nil {
		return ""
	}
	if v := uiversion.Sanitize(r.Header.Get(uiversion.VersionHeader)); v != "" {
		return v
	}
	if isNCCUIRequest(r) {
		return uiversion.UnknownVersion
	}
	return ""
}

func presentedUIServerVersion(r *http.Request) string {
	if r == nil {
		return ""
	}
	return uiversion.Sanitize(r.Header.Get(uiversion.ServerVersionHeader))
}

func (s *apiServer) blockedUIList() []uiversion.Entry {
	out := uiversion.Builtin()
	if s != nil && s.users != nil {
		out = append(out, s.users.extraBlockedUIVersions()...)
	}
	return out
}

func (s *apiServer) blockUnversionedUI() bool {
	if s == nil || s.users == nil {
		return true
	}
	return s.users.blockUnversionedUI()
}

func (s *apiServer) matchBlockedUI(r *http.Request) (uiversion.Entry, bool) {
	list := s.blockedUIList()
	uiVer := presentedUIVersion(r)
	if uiVer != "" {
		if e, ok := uiversion.Match(uiVer, "ui", list); ok {
			return e, true
		}
		if uiVer == uiversion.UnknownVersion && isNCCUIRequest(r) && s.blockUnversionedUI() {
			return uiversion.Entry{
				Version:   uiversion.UnknownVersion,
				Component: "ui",
				Reason:    uiversion.UnversionedReason(),
				FixedIn:   uiversion.SupportedFrom,
				Builtin:   true,
			}, true
		}
	}
	if srv := presentedUIServerVersion(r); srv != "" {
		if e, ok := uiversion.Match(srv, "ui-server", list); ok {
			return e, true
		}
	}
	return uiversion.Entry{}, false
}

func skipUIVersionGate(r *http.Request) bool {
	if r == nil {
		return true
	}
	if r.Method == http.MethodOptions {
		return true
	}
	p := r.URL.Path
	return p == "/api/v1/health" || p == "/api/v1/health/ready" || p == "/metrics" ||
		p == "/" || p == "/docs" || p == "/docs/ui" || strings.HasPrefix(p, "/docs/assets/")
}

// rejectCompromisedUIClient writes 403 when the caller is a blacklisted NCC UI.
func (s *apiServer) rejectCompromisedUIClient(w http.ResponseWriter, r *http.Request) bool {
	if skipUIVersionGate(r) || !isNCCUIRequest(r) && presentedUIServerVersion(r) == "" {
		return false
	}
	e, blocked := s.matchBlockedUI(r)
	if !blocked {
		return false
	}
	writeJSON(w, http.StatusForbidden, envelope{
		Success:     false,
		Error:       e.Reason,
		ErrorCode:   uiversion.ErrorCode,
		Remediation: uiversion.Remediation(),
		Data: map[string]interface{}{
			"ui_version":        presentedUIVersion(r),
			"ui_server_version": presentedUIServerVersion(r),
			"blocked_version":   e.Version,
			"component":         e.Component,
			"advisory":          strings.Join(e.Advisories, ", "),
			"severity":          e.Severity,
			"fixed_in":          e.FixedIn,
		},
	})
	s.audit(r, "ui.blocked", false, map[string]interface{}{
		"error":             e.Reason,
		"error_code":        uiversion.ErrorCode,
		"remediation":       uiversion.Remediation(),
		"blocked_version":   e.Version,
		"blocked_component": e.Component,
		"builtin":           e.Builtin,
	})
	return true
}

func (s *apiServer) decorateAuditClient(r *http.Request, payload map[string]interface{}) {
	if r == nil || payload == nil {
		return
	}
	switch {
	case payload["channel"] == "mcp" || isMCPHTTPRequest(r):
		payload["client_kind"] = "mcp"
	case isNCCUIRequest(r) || presentedUIServerVersion(r) != "":
		payload["client_kind"] = "ui"
		if _, ok := payload["channel"]; !ok {
			payload["channel"] = "ui"
		}
	default:
		payload["client_kind"] = "api"
	}
	if v := presentedUIVersion(r); v != "" {
		payload["ui_version"] = v
	}
	if v := presentedUIServerVersion(r); v != "" {
		payload["ui_server_version"] = v
	}
	if _, blocked := s.matchBlockedUI(r); blocked {
		payload["ui_compromised"] = true
	}
}

func (s *apiServer) handleUIVersionPolicy(w http.ResponseWriter, r *http.Request) {
	if s.users == nil || !s.users.writable() {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "UI version policy requires a writable user database"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: s.uiVersionPolicyView()})
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		var body struct {
			BlockUnversioned *bool             `json:"block_unversioned"`
			Extra            []uiversion.Entry `json:"extra"`
		}
		if err := decodeJSON(r.Body, &body); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		extra := make([]uiversion.Entry, 0, len(body.Extra))
		for _, e := range body.Extra {
			v := uiversion.Sanitize(e.Version)
			if v == "" {
				writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "each extra entry needs a version"})
				return
			}
			reason := strings.TrimSpace(e.Reason)
			if reason == "" {
				reason = "operator-blocked UI version"
			}
			if len(reason) > 300 {
				reason = reason[:300]
			}
			extra = append(extra, uiversion.Entry{
				Version:   v,
				Component: uiversion.NormalizeComponent(e.Component),
				Reason:    reason,
			})
		}
		if len(extra) > uiVersionPolicyMaxExtra {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "too many extra blocked versions"})
			return
		}
		blockUnversioned := true
		if body.BlockUnversioned != nil {
			blockUnversioned = *body.BlockUnversioned
		}
		if err := s.users.setUIVersionPolicy(uiVersionPolicy{BlockUnversioned: blockUnversioned, Extra: extra}); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		s.audit(r, "settings.ui_versions.update", true, map[string]interface{}{
			"block_unversioned": blockUnversioned,
			"extra_count":       len(extra),
		})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "UI version policy updated", Data: s.uiVersionPolicyView()})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

func (s *apiServer) uiVersionPolicyView() map[string]interface{} {
	return map[string]interface{}{
		"block_unversioned": s.blockUnversionedUI(),
		"builtin":           uiversion.Builtin(),
		"extra":             s.users.extraBlockedUIVersions(),
		"blocked":           s.blockedUIList(),
	}
}
