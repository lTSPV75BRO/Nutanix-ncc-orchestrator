package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

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
		if msg := validatePassword(username, body.Password); msg != "" {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: msg})
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
				logAdminPasswordReset("admin reset via Settings → Access", hint)
				if hint != "" {
					respData["bootstrap_file"] = hint
				}
			}
			s.audit(r, "settings.users.update", true, map[string]interface{}{"username": username, "password_generated": true})
			writeJSON(w, http.StatusOK, envelope{Success: true, Message: "Temporary password generated; the user must change it on next login.", Data: respData})
			return
		}
		if body.Password != nil {
			if msg := validatePassword(username, *body.Password); msg != "" {
				writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: msg})
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
	case errors.Is(err, errStoreConflict):
		writeJSON(w, http.StatusConflict, envelope{Success: false, Error: err.Error(), ErrorCode: "NCC_API_STORE_CONFLICT"})
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
			data["upn_suffixes"] = nonNilStrings(cfg.UPNSuffixes)
			data["netbios_names"] = nonNilStrings(cfg.NetBIOSNames)
			profiles := make([]map[string]interface{}, 0, len(cfg.Profiles))
			for _, p := range cfg.Profiles {
				profiles = append(profiles, map[string]interface{}{
					"name":                 p.Name,
					"upn_suffixes":         nonNilStrings(p.UPNSuffixes),
					"netbios_names":        nonNilStrings(p.NetBIOSNames),
					"url":                  p.URL,
					"start_tls":            p.StartTLS,
					"insecure_skip_verify": p.InsecureSkipVerify,
					"has_ca_cert":          strings.TrimSpace(p.CACertPEM) != "",
					"bind_dn":              p.BindDN,
					"has_bind_password":    strings.TrimSpace(p.BindPassword) != "",
					"base_dn":              p.BaseDN,
					"user_filter":          p.UserFilter,
					"username_attribute":   p.UsernameAttr,
					"group_attribute":      p.GroupAttr,
					"role_map":             p.RoleMapRaw,
					"default_role":         p.DefaultRole,
				})
			}
			data["profiles"] = profiles
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
	case http.MethodPut:
		if s.ldapFromFlags {
			writeJSON(w, http.StatusConflict, envelope{Success: false, Error: "LDAP is managed via startup flags and cannot be edited at runtime"})
			return
		}
		var body ldapConfigInput
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		if err := dec.Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid JSON body"})
			return
		}
		cfg, err := body.merge(s.users.getLDAP(), false)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
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
		// Directory profiles: names, claims and per-profile fields (also
		// normalizes suffixes / NetBIOS names before they are stored).
		if err := validateLDAPProfiles(cfg); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		// When enabling LDAP, verify connectivity (dial + service-account bind +
		// base-DN search) of every directory before persisting so the admin
		// gets a clear, actionable error instead of a config that only fails
		// later at login time.
		if cfg.configured() {
			prov, _, err := buildLDAPRouter(cfg)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
				return
			}
			if err := prov.validate(); err != nil {
				writeJSON(w, http.StatusBadGateway, envelope{Success: false, Error: "LDAP connection failed: " + err.Error()})
				return
			}
		}
		if err := s.users.setLDAP(cfg); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		if err := s.reloadLDAPFromStore(context.Background()); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "saved but failed to activate LDAP: " + err.Error()})
			return
		}
		s.audit(r, "settings.ldap.update", true, map[string]interface{}{"enabled": body.Enabled, "profiles": len(cfg.Profiles)})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "LDAP configuration updated"})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

func nonNilStrings(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}

// ldapProfileInput is a directory profile as sent by PUT /settings/ldap (and
// the test endpoint). Secrets are write-only: a nil bind_password /
// ca_cert_pem keeps the value stored for the profile of the same name (or
// previous_name, when the profile is being renamed); an empty string clears
// it.
type ldapProfileInput struct {
	Name               string   `json:"name"`
	PreviousName       string   `json:"previous_name"`
	UPNSuffixes        []string `json:"upn_suffixes"`
	NetBIOSNames       []string `json:"netbios_names"`
	URL                string   `json:"url"`
	StartTLS           bool     `json:"start_tls"`
	InsecureSkipVerify bool     `json:"insecure_skip_verify"`
	CACertPEM          *string  `json:"ca_cert_pem"`
	BindDN             string   `json:"bind_dn"`
	BindPassword       *string  `json:"bind_password"`
	BaseDN             string   `json:"base_dn"`
	UserFilter         string   `json:"user_filter"`
	UsernameAttr       string   `json:"username_attribute"`
	GroupAttr          string   `json:"group_attribute"`
	RoleMapRaw         string   `json:"role_map"`
	DefaultRole        string   `json:"default_role"`
}

// ldapConfigInput is the PUT /settings/ldap body. The flat fields are the
// default directory, exactly as before profiles existed. Omitting
// upn_suffixes / netbios_names / profiles keeps the stored values, so an
// older client that only knows the single-directory fields doesn't wipe the
// profiles; send [] to clear them.
type ldapConfigInput struct {
	Enabled            bool                `json:"enabled"`
	URL                string              `json:"url"`
	StartTLS           bool                `json:"start_tls"`
	InsecureSkipVerify bool                `json:"insecure_skip_verify"`
	CACertPEM          *string             `json:"ca_cert_pem"`
	BindDN             string              `json:"bind_dn"`
	BindPassword       *string             `json:"bind_password"`
	BaseDN             string              `json:"base_dn"`
	UserFilter         string              `json:"user_filter"`
	UsernameAttr       string              `json:"username_attribute"`
	GroupAttr          string              `json:"group_attribute"`
	RoleMapRaw         string              `json:"role_map"`
	DefaultRole        string              `json:"default_role"`
	UPNSuffixes        *[]string           `json:"upn_suffixes"`
	NetBIOSNames       *[]string           `json:"netbios_names"`
	Profiles           *[]ldapProfileInput `json:"profiles"`
}

// merge builds the config to store from the input and the stored config
// (prev, may be nil), carrying write-only secrets over. With blankKeeps (the
// test endpoint) an empty secret also means "use the stored one".
func (in *ldapConfigInput) merge(prev *ldapPersisted, blankKeeps bool) (*ldapPersisted, error) {
	keep := func(v *string, stored string) string {
		if v == nil || (blankKeeps && strings.TrimSpace(*v) == "") {
			return stored
		}
		return strings.TrimSpace(*v)
	}
	var prevDef ldapPersisted
	if prev != nil {
		prevDef = *prev
	}
	cfg := &ldapPersisted{
		Enabled:            in.Enabled,
		URL:                strings.TrimSpace(in.URL),
		StartTLS:           in.StartTLS,
		InsecureSkipVerify: in.InsecureSkipVerify,
		CACertPEM:          keep(in.CACertPEM, prevDef.CACertPEM),
		BindDN:             strings.TrimSpace(in.BindDN),
		BaseDN:             strings.TrimSpace(in.BaseDN),
		UserFilter:         strings.TrimSpace(in.UserFilter),
		UsernameAttr:       strings.TrimSpace(in.UsernameAttr),
		GroupAttr:          strings.TrimSpace(in.GroupAttr),
		RoleMapRaw:         strings.TrimSpace(in.RoleMapRaw),
		DefaultRole:        strings.TrimSpace(in.DefaultRole),
		UPNSuffixes:        prevDef.UPNSuffixes,
		NetBIOSNames:       prevDef.NetBIOSNames,
		Profiles:           prevDef.Profiles,
	}
	// Bind password is write-only: a nil field preserves the stored secret;
	// an empty string clears it (anonymous bind). The password itself is not
	// trimmed.
	cfg.BindPassword = prevDef.BindPassword
	if in.BindPassword != nil && !(blankKeeps && *in.BindPassword == "") {
		cfg.BindPassword = *in.BindPassword
	}
	if in.UPNSuffixes != nil {
		cfg.UPNSuffixes = *in.UPNSuffixes
	}
	if in.NetBIOSNames != nil {
		cfg.NetBIOSNames = *in.NetBIOSNames
	}
	if in.Profiles != nil {
		stored := map[string]ldapProfile{}
		for _, p := range prevDef.Profiles {
			stored[strings.ToLower(p.Name)] = p
		}
		cfg.Profiles = make([]ldapProfile, 0, len(*in.Profiles))
		for _, pi := range *in.Profiles {
			from := strings.TrimSpace(pi.PreviousName)
			if from == "" {
				from = strings.TrimSpace(pi.Name)
			}
			old := stored[strings.ToLower(from)]
			p := ldapProfile{
				Name:               strings.TrimSpace(pi.Name),
				UPNSuffixes:        pi.UPNSuffixes,
				NetBIOSNames:       pi.NetBIOSNames,
				URL:                strings.TrimSpace(pi.URL),
				StartTLS:           pi.StartTLS,
				InsecureSkipVerify: pi.InsecureSkipVerify,
				CACertPEM:          keep(pi.CACertPEM, old.CACertPEM),
				BindDN:             strings.TrimSpace(pi.BindDN),
				BindPassword:       old.BindPassword,
				BaseDN:             strings.TrimSpace(pi.BaseDN),
				UserFilter:         strings.TrimSpace(pi.UserFilter),
				UsernameAttr:       strings.TrimSpace(pi.UsernameAttr),
				GroupAttr:          strings.TrimSpace(pi.GroupAttr),
				RoleMapRaw:         strings.TrimSpace(pi.RoleMapRaw),
				DefaultRole:        strings.TrimSpace(pi.DefaultRole),
			}
			if pi.BindPassword != nil && !(blankKeeps && *pi.BindPassword == "") {
				p.BindPassword = *pi.BindPassword
			}
			cfg.Profiles = append(cfg.Profiles, p)
		}
	}
	if len(cfg.Profiles) == 0 {
		cfg.Profiles = nil
	}
	return cfg, nil
}

// handleLDAPTest validates an LDAP/AD configuration without persisting it
// (admin-only). It builds an ephemeral router from the posted config (the
// default directory plus any profiles), routes the sample login name exactly
// as sign-in would, and attempts to authenticate the supplied sample
// credentials, reporting the directory used, the resolved name, role and
// groups so admins can verify routing, connectivity and role mapping.
// Blank secrets reuse the stored ones so admins can test without
// re-entering them.
func (s *apiServer) handleLDAPTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	var body struct {
		ldapConfigInput
		TestUsername string `json:"test_username"`
		TestPassword string `json:"test_password"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid JSON body"})
		return
	}
	body.Enabled = true // force-evaluate even if the saved config is disabled
	cfg, err := body.merge(s.users.getLDAP(), true)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	prov, enabled, err := buildLDAPRouter(cfg)
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
	directory, role, canonical, groups, ok, err := prov.authenticateRouted(body.TestUsername, body.TestPassword)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, envelope{Success: false, Error: "LDAP error: " + err.Error()})
		return
	}
	if !ok {
		writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: fmt.Sprintf("LDAP rejected the sample credentials in directory %q (bad username/password or no matching user)", directory)})
		return
	}
	s.audit(r, "settings.ldap.test", true, map[string]interface{}{"username": canonical, "directory": directory})
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "LDAP authentication succeeded", Data: map[string]interface{}{
		"username":  canonical,
		"role":      role.String(),
		"groups":    groups,
		"directory": directory,
	}})
}

// handleLDAPSearch powers real-time AD/LDAP type-ahead when admins assign
// principals to cluster groups. GET ?q=<term>&type=group|user|all&limit=<n>.
// Returns an empty result set (not an error) when LDAP is not enabled so the UI
// can fall back to manual entry.
func (s *apiServer) handleLDAPSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	kind := strings.TrimSpace(r.URL.Query().Get("type"))
	if kind == "" {
		kind = "all"
	}
	limit := 25
	if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	prov := s.currentLDAP()
	if prov == nil {
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{"ldap_enabled": false, "results": []directoryEntry{}}})
		return
	}
	if len([]rune(q)) < 2 {
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{"ldap_enabled": true, "results": []directoryEntry{}}})
		return
	}
	results, err := prov.searchDirectory(q, kind, limit)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, envelope{Success: false, Error: "LDAP search failed: " + err.Error()})
		return
	}
	if results == nil {
		results = []directoryEntry{}
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{"ldap_enabled": true, "results": results}})
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
