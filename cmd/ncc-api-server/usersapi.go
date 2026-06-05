package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const minPasswordLen = 8

// handleUsers handles the local-accounts collection (admin-only via the
// /api/v1/settings/* prefix): GET lists accounts, POST creates one.
func (s *apiServer) handleUsers(w http.ResponseWriter, r *http.Request) {
	if s.users == nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "local accounts are not configured (start with --users-db)"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{"users": s.users.list()}})
	case http.MethodPost:
		var body struct {
			Username   string `json:"username"`
			Password   string `json:"password"`
			Role       string `json:"role"`
			MustChange *bool  `json:"must_change_password"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024))
		if err := dec.Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid JSON body"})
			return
		}
		username := strings.TrimSpace(body.Username)
		if username == "" {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "username is required"})
			return
		}
		if _, exists := s.users.lookup(username); exists {
			writeJSON(w, http.StatusConflict, envelope{Success: false, Error: "user already exists"})
			return
		}
		role, ok := parseRole(body.Role)
		if !ok {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid role (want admin|operator|viewer)"})
			return
		}
		if len(body.Password) < minPasswordLen {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: fmt.Sprintf("password must be at least %d characters", minPasswordLen)})
			return
		}
		hash, err := hashPassword(body.Password)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		// New accounts must change their (admin-set) password on first login
		// unless the admin explicitly opts out.
		mustChange := true
		if body.MustChange != nil {
			mustChange = *body.MustChange
		}
		if err := s.users.upsertUser(username, hash, role, mustChange); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		s.audit(r, "settings.users.create", true, map[string]interface{}{"username": username, "role": role.String()})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "user created"})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

// handleUserByName handles /api/v1/settings/users/<username>: PUT updates role
// and/or resets the password; DELETE removes the account.
func (s *apiServer) handleUserByName(w http.ResponseWriter, r *http.Request) {
	if s.users == nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "local accounts are not configured"})
		return
	}
	username := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/api/v1/settings/users/"))
	if username == "" || strings.Contains(username, "/") {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid username in path"})
		return
	}
	switch r.Method {
	case http.MethodPut:
		var body struct {
			Role             *string `json:"role"`
			Password         *string `json:"password"`
			MustChange       *bool   `json:"must_change_password"`
			GeneratePassword *bool   `json:"generate_password"`
			RevokeSessions   *bool   `json:"revoke_sessions"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024))
		if err := dec.Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid JSON body"})
			return
		}
		if _, ok := s.users.lookup(username); !ok {
			writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "user not found"})
			return
		}
		if body.Role != nil {
			role, ok := parseRole(*body.Role)
			if !ok {
				writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid role"})
				return
			}
			if err := s.users.setRole(username, role); err != nil {
				writeUserStoreError(w, err)
				return
			}
		}
		if body.RevokeSessions != nil && *body.RevokeSessions {
			// Force-sign-out: invalidate all of the target user's sessions
			// without changing their password or role.
			if err := s.users.revokeSessions(username); err != nil {
				writeUserStoreError(w, err)
				return
			}
			s.audit(r, "settings.users.update", true, map[string]interface{}{"username": username, "sessions_revoked": true})
			writeJSON(w, http.StatusOK, envelope{Success: true, Message: "all sessions for " + username + " have been signed out"})
			return
		}
		if body.GeneratePassword != nil && *body.GeneratePassword {
			// Auto-generate a random temporary password instead of accepting a
			// typed one. The built-in admin additionally follows the first-run
			// workflow (logs + .ncc-initial-admin-password file) so every admin
			// reset entry point behaves identically.
			pw, err := s.users.adminResetPassword(username)
			if err != nil {
				writeUserStoreError(w, err)
				return
			}
			s.users.clearResetRequest(username)
			s.loginGuard.reset(username) // a reset account should not stay locked
			respData := map[string]interface{}{"temporary_password": pw, "must_change_password": true}
			if isReservedAdmin(username) {
				hint := s.users.setInitialPassword(username, pw)
				logAdminPasswordReset("admin reset via Settings → Access", pw, hint)
				if hint != "" {
					respData["bootstrap_file"] = hint
				}
			}
			s.audit(r, "settings.users.update", true, map[string]interface{}{"username": username, "password_generated": true})
			writeJSON(w, http.StatusOK, envelope{Success: true, Message: "Temporary password generated; the user must change it on next login.", Data: respData})
			return
		}
		if body.Password != nil {
			if len(*body.Password) < minPasswordLen {
				writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: fmt.Sprintf("password must be at least %d characters", minPasswordLen)})
				return
			}
			hash, err := hashPassword(*body.Password)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
				return
			}
			mustChange := true // admin-reset passwords are temporary by default
			if body.MustChange != nil {
				mustChange = *body.MustChange
			}
			if err := s.users.setPassword(username, hash, mustChange); err != nil {
				writeUserStoreError(w, err)
				return
			}
			// An admin resetting the password resolves any pending
			// self-service "forgot password" request for this user and clears
			// any brute-force lockout so the user can sign in immediately.
			s.users.clearResetRequest(username)
			s.loginGuard.reset(username)
		} else if body.MustChange != nil {
			// Flip the flag without changing the password.
			if acct, ok := s.users.lookup(username); ok {
				if err := s.users.setPassword(username, acct.PasswordHash, *body.MustChange); err != nil {
					writeUserStoreError(w, err)
					return
				}
			}
		}
		s.audit(r, "settings.users.update", true, map[string]interface{}{"username": username})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "user updated"})
	case http.MethodDelete:
		if err := s.users.deleteUser(username); err != nil {
			writeUserStoreError(w, err)
			return
		}
		s.audit(r, "settings.users.delete", true, map[string]interface{}{"username": username})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "user deleted"})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

func writeUserStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errUserNotFound):
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "user not found"})
	case errors.Is(err, errLastAdmin):
		writeJSON(w, http.StatusConflict, envelope{Success: false, Error: err.Error(), ErrorCode: "NCC_API_LAST_ADMIN"})
	case errors.Is(err, errReservedAdminRole), errors.Is(err, errReservedAdminDelete):
		writeJSON(w, http.StatusConflict, envelope{Success: false, Error: err.Error(), ErrorCode: "NCC_API_RESERVED_ADMIN"})
	default:
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
	}
}

// handleSSO manages the runtime SAML configuration (admin-only). GET returns
// the current config (without the SP private key); PUT updates it and hot-
// reloads the SP. When SAML is configured via startup flags, it is read-only.
func (s *apiServer) handleSSO(w http.ResponseWriter, r *http.Request) {
	if s.users == nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "runtime SSO config requires --users-db"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		cfg := s.users.getSAML()
		data := map[string]interface{}{
			"enabled":         s.samlIsEnabled(),
			"managed_by":      ternary(s.samlFromFlags, "flags", "runtime"),
			"sp_metadata_url": "",
		}
		if cfg != nil {
			data["root_url"] = cfg.RootURL
			data["entity_id"] = cfg.EntityID
			data["idp_metadata_url"] = cfg.IDPMetadataURL
			data["has_idp_metadata_xml"] = strings.TrimSpace(cfg.IDPMetadataXML) != ""
			data["username_attribute"] = cfg.UsernameAttr
			data["role_attribute"] = cfg.RoleAttr
			data["role_map"] = cfg.RoleMapRaw
			data["default_role"] = cfg.DefaultRole
			data["allow_idp_initiated"] = cfg.AllowIDPInit
			data["sp_cert_pem"] = cfg.SPCertPEM // public; safe to expose
			if strings.TrimSpace(cfg.RootURL) != "" {
				data["sp_metadata_url"] = strings.TrimRight(cfg.RootURL, "/") + "/saml/metadata"
			}
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
	case http.MethodPut:
		if s.samlFromFlags {
			writeJSON(w, http.StatusConflict, envelope{Success: false, Error: "SAML is managed via startup flags and cannot be edited at runtime"})
			return
		}
		var body struct {
			Enabled        bool   `json:"enabled"`
			RootURL        string `json:"root_url"`
			EntityID       string `json:"entity_id"`
			IDPMetadataXML string `json:"idp_metadata_xml"`
			IDPMetadataURL string `json:"idp_metadata_url"`
			UsernameAttr   string `json:"username_attribute"`
			RoleAttr       string `json:"role_attribute"`
			RoleMapRaw     string `json:"role_map"`
			DefaultRole    string `json:"default_role"`
			AllowIDPInit   bool   `json:"allow_idp_initiated"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		if err := dec.Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid JSON body"})
			return
		}
		// Preserve the previously generated SP keypair across edits.
		prev := s.users.getSAML()
		cfg := &samlPersisted{
			Enabled:        body.Enabled,
			RootURL:        strings.TrimSpace(body.RootURL),
			EntityID:       strings.TrimSpace(body.EntityID),
			IDPMetadataXML: body.IDPMetadataXML,
			IDPMetadataURL: strings.TrimSpace(body.IDPMetadataURL),
			UsernameAttr:   strings.TrimSpace(body.UsernameAttr),
			RoleAttr:       strings.TrimSpace(body.RoleAttr),
			RoleMapRaw:     strings.TrimSpace(body.RoleMapRaw),
			DefaultRole:    strings.TrimSpace(body.DefaultRole),
			AllowIDPInit:   body.AllowIDPInit,
		}
		if prev != nil {
			cfg.SPCertPEM = prev.SPCertPEM
			cfg.SPKeyPEM = prev.SPKeyPEM
		}
		if body.Enabled && !cfg.configured() {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "root_url and IdP metadata (xml or url) are required to enable SAML"})
			return
		}
		if err := s.users.setSAML(cfg); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		if err := s.reloadSAMLFromStore(context.Background()); err != nil {
			// Persisted but failed to build: report so the admin can fix it.
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "saved but failed to activate SAML: " + err.Error()})
			return
		}
		s.audit(r, "settings.sso.update", true, map[string]interface{}{"enabled": body.Enabled})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "SSO configuration updated"})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

// handleLDAP manages the runtime LDAP/Active Directory configuration
// (admin-only). GET returns the current config without the bind password (a
// has_bind_password bool is reported instead); PUT updates it and hot-reloads
// the provider. When LDAP is configured via startup flags, it is read-only.
func (s *apiServer) handleLDAP(w http.ResponseWriter, r *http.Request) {
	if s.users == nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "runtime LDAP config requires --users-db"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		cfg := s.users.getLDAP()
		data := map[string]interface{}{
			"enabled":    s.ldapIsEnabled(),
			"managed_by": ternary(s.ldapFromFlags, "flags", "runtime"),
		}
		if cfg != nil {
			data["url"] = cfg.URL
			data["start_tls"] = cfg.StartTLS
			data["insecure_skip_verify"] = cfg.InsecureSkipVerify
			data["has_ca_cert"] = strings.TrimSpace(cfg.CACertPEM) != ""
			data["bind_dn"] = cfg.BindDN
			data["has_bind_password"] = strings.TrimSpace(cfg.BindPassword) != ""
			data["base_dn"] = cfg.BaseDN
			data["user_filter"] = cfg.UserFilter
			data["username_attribute"] = cfg.UsernameAttr
			data["group_attribute"] = cfg.GroupAttr
			data["role_map"] = cfg.RoleMapRaw
			data["default_role"] = cfg.DefaultRole
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
	case http.MethodPut:
		if s.ldapFromFlags {
			writeJSON(w, http.StatusConflict, envelope{Success: false, Error: "LDAP is managed via startup flags and cannot be edited at runtime"})
			return
		}
		var body struct {
			Enabled            bool    `json:"enabled"`
			URL                string  `json:"url"`
			StartTLS           bool    `json:"start_tls"`
			InsecureSkipVerify bool    `json:"insecure_skip_verify"`
			CACertPEM          string  `json:"ca_cert_pem"`
			BindDN             string  `json:"bind_dn"`
			BindPassword       *string `json:"bind_password"`
			BaseDN             string  `json:"base_dn"`
			UserFilter         string  `json:"user_filter"`
			UsernameAttr       string  `json:"username_attribute"`
			GroupAttr          string  `json:"group_attribute"`
			RoleMapRaw         string  `json:"role_map"`
			DefaultRole        string  `json:"default_role"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		if err := dec.Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid JSON body"})
			return
		}
		prev := s.users.getLDAP()
		cfg := &ldapPersisted{
			Enabled:            body.Enabled,
			URL:                strings.TrimSpace(body.URL),
			StartTLS:           body.StartTLS,
			InsecureSkipVerify: body.InsecureSkipVerify,
			CACertPEM:          strings.TrimSpace(body.CACertPEM),
			BindDN:             strings.TrimSpace(body.BindDN),
			BaseDN:             strings.TrimSpace(body.BaseDN),
			UserFilter:         strings.TrimSpace(body.UserFilter),
			UsernameAttr:       strings.TrimSpace(body.UsernameAttr),
			GroupAttr:          strings.TrimSpace(body.GroupAttr),
			RoleMapRaw:         strings.TrimSpace(body.RoleMapRaw),
			DefaultRole:        strings.TrimSpace(body.DefaultRole),
		}
		// Bind password is write-only: a nil field preserves the stored secret;
		// an empty string clears it (anonymous bind).
		if body.BindPassword != nil {
			cfg.BindPassword = *body.BindPassword
		} else if prev != nil {
			cfg.BindPassword = prev.BindPassword
		}
		if body.Enabled && !cfg.configured() {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "url and base_dn are required to enable LDAP"})
			return
		}
		// Validate the role map before persisting so a typo is reported clearly.
		if _, err := parseLDAPRoleMap(cfg.RoleMapRaw); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if err := s.users.setLDAP(cfg); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		if err := s.reloadLDAPFromStore(context.Background()); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "saved but failed to activate LDAP: " + err.Error()})
			return
		}
		s.audit(r, "settings.ldap.update", true, map[string]interface{}{"enabled": body.Enabled})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "LDAP configuration updated"})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

// handleLDAPTest validates an LDAP/AD configuration without persisting it
// (admin-only). It builds an ephemeral provider from the posted config and
// attempts to authenticate the supplied sample credentials, reporting the
// resolved role and groups so admins can verify connectivity and role mapping.
func (s *apiServer) handleLDAPTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	var body struct {
		ldapPersisted
		TestUsername string `json:"test_username"`
		TestPassword string `json:"test_password"`
		BindPassword string `json:"bind_password"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid JSON body"})
		return
	}
	cfg := body.ldapPersisted
	cfg.Enabled = true // force-evaluate even if the saved config is disabled
	cfg.BindPassword = body.BindPassword
	// A blank bind password reuses the stored secret so admins can test without
	// re-entering it.
	if strings.TrimSpace(cfg.BindPassword) == "" {
		if prev := s.users.getLDAP(); prev != nil {
			cfg.BindPassword = prev.BindPassword
		}
	}
	prov, enabled, err := buildLDAPProvider(&cfg)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	if !enabled {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "url and base_dn are required to test LDAP"})
		return
	}
	if strings.TrimSpace(body.TestUsername) == "" || body.TestPassword == "" {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "test_username and test_password are required"})
		return
	}
	role, canonical, ok, err := prov.authenticate(body.TestUsername, body.TestPassword)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, envelope{Success: false, Error: "LDAP error: " + err.Error()})
		return
	}
	if !ok {
		writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "LDAP rejected the sample credentials (bad username/password or no matching user)"})
		return
	}
	s.audit(r, "settings.ldap.test", true, map[string]interface{}{"username": canonical})
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "LDAP authentication succeeded", Data: map[string]interface{}{
		"username": canonical,
		"role":     role.String(),
	}})
}

// handleSessionPolicy manages the runtime session lifetime (admin-only). GET
// returns the current effective TTL and bounds; PUT sets how long an issued
// session stays active. A ttl of 0 clears the override and restores the
// server's --session-ttl default. Changes apply to sessions minted afterward.
func (s *apiServer) handleSessionPolicy(w http.ResponseWriter, r *http.Request) {
	if s.users == nil || !s.users.writable() {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "runtime session settings require a writable user database"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		effective := int(s.effectiveSessionTTL().Seconds())
		data := map[string]interface{}{
			"ttl_sec":         effective,
			"ttl_min":         effective / 60,
			"default_ttl_sec": int(s.sessionTTL.Seconds()),
			"min_sec":         int(sessionTTLMin.Seconds()),
			"max_sec":         int(sessionTTLMax.Seconds()),
			"source":          "default",
		}
		if pol := s.users.getSessionPolicy(); pol != nil && pol.TTLSeconds > 0 {
			data["source"] = "runtime"
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
	case http.MethodPut:
		var body struct {
			TTLSec *int `json:"ttl_sec"`
			TTLMin *int `json:"ttl_min"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024))
		if err := dec.Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid JSON body"})
			return
		}
		seconds := 0
		switch {
		case body.TTLSec != nil:
			seconds = *body.TTLSec
		case body.TTLMin != nil:
			seconds = *body.TTLMin * 60
		default:
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "provide ttl_sec or ttl_min"})
			return
		}
		// 0 means "clear override / use server default"; otherwise enforce bounds.
		if seconds != 0 {
			d := time.Duration(seconds) * time.Second
			if d < sessionTTLMin || d > sessionTTLMax {
				writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: fmt.Sprintf("session duration must be between %d and %d seconds", int(sessionTTLMin.Seconds()), int(sessionTTLMax.Seconds()))})
				return
			}
		}
		if err := s.users.setSessionTTLSeconds(seconds); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		s.audit(r, "settings.session.update", true, map[string]interface{}{"ttl_sec": seconds})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "session policy updated", Data: map[string]interface{}{
			"ttl_sec": int(s.effectiveSessionTTL().Seconds()),
		}})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

// handlePasswordResets lists the pending self-service password-reset requests
// (admin-only via the /api/v1/settings/* prefix). Admins action each request
// out-of-band by resetting the user's password from Settings → Access.
func (s *apiServer) handlePasswordResets(w http.ResponseWriter, r *http.Request) {
	if s.users == nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "local accounts are not configured"})
		return
	}
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	reqs := s.users.listResetRequests()
	items := make([]map[string]interface{}, 0, len(reqs))
	for _, req := range reqs {
		items = append(items, map[string]interface{}{
			"username":     req.Username,
			"requested_at": req.RequestedAt,
			"client_ip":    req.ClientIP,
		})
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{"requests": items}})
}

// handlePasswordResetByName dismisses a single pending request without
// resetting the password (DELETE /api/v1/settings/password-resets/<username>).
func (s *apiServer) handlePasswordResetByName(w http.ResponseWriter, r *http.Request) {
	if s.users == nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "local accounts are not configured"})
		return
	}
	if r.Method != http.MethodDelete {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	username := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/api/v1/settings/password-resets/"))
	if username == "" || strings.Contains(username, "/") {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid username in path"})
		return
	}
	s.users.clearResetRequest(username)
	s.audit(r, "settings.password_resets.dismiss", true, map[string]interface{}{"username": username})
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "request dismissed"})
}

func ternary(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}
