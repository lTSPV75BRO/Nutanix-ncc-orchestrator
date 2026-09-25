package main

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"goncc/internal/selfsigned"
)

// v2StartStateFileName mirrors the orchestrator's persisted start-state file
// (goNCC.go: v2StartStateFile). The api-server patches it in place so a
// self-triggered v2-restart rebinds the UI server to (or off) TLS. Kept as a
// literal because the two binaries do not share a package.
const v2StartStateFileName = ".ncc-v2-start.json"

// tlsDirName is the subdirectory under the install dir where admin-uploaded UI
// TLS material is stored (0700; files 0600). Colocated with the rest of the
// install state so backups capture it.
const tlsDirName = "tls"

// maxTLSUploadBytes bounds the cert+key JSON body. Real PEM bundles are a few
// KB; 256 KiB is a generous ceiling that still rejects abuse.
const maxTLSUploadBytes = 256 * 1024

// handleTLSSettings is the admin-only HTTPS/TLS management endpoint:
//
//	GET    /api/v1/settings/tls -> current TLS policy (metadata only, no key)
//	PUT    /api/v1/settings/tls -> install cert+key, enable HTTPS, restart stack
//	DELETE /api/v1/settings/tls -> remove cert, disable HTTPS, restart stack
//
// The browser-facing UI server is the TLS terminator; the api-server stays on
// loopback HTTP behind it. Enabling HTTPS also flips session cookies to Secure
// (see cookieSecure) on the next start.
func (s *apiServer) handleTLSSettings(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: s.liveTLSPolicyView()})
		return
	}
	if s.users == nil || !s.users.writable() {
		writeJSON(w, http.StatusNotImplemented, envelope{Success: false, Error: "HTTPS/TLS management requires a writable user store (enable local accounts)"})
		return
	}
	switch r.Method {
	case http.MethodPut:
		s.handleTLSInstall(w, r)
	case http.MethodDelete:
		s.handleTLSDisable(w, r)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

// tlsPolicyView shapes the policy for the API response: HTTPS state plus
// decoded certificate metadata, never the private key or raw PEM.
func tlsPolicyView(p *tlsPolicy) map[string]interface{} {
	if p == nil {
		return map[string]interface{}{"https_enabled": false, "managed_by": "stack", "mutation_supported": true}
	}
	view := map[string]interface{}{
		"https_enabled":      p.HTTPSEnabled,
		"subject":            p.Subject,
		"issuer":             p.Issuer,
		"not_before":         p.NotBefore,
		"not_after":          p.NotAfter,
		"dns_names":          p.DNSNames,
		"updated_at":         p.UpdatedAt,
		"managed_by":         "stack",
		"mutation_supported": true,
	}
	if p.CertPath != "" {
		if fp, selfSigned, ok := fingerprintFromCertFile(p.CertPath); ok {
			view["fingerprint_sha256"] = fp
			view["self_signed"] = selfSigned
		}
	}
	return view
}

// handlePublicTLS is unauthenticated GET /api/v1/tls/public. It returns the
// UI leaf certificate (PEM) plus a Chrome-style SHA-256 fingerprint so the
// login page can offer Download / Copy without exposing the private key.
func (s *apiServer) handlePublicTLS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	httpsOn := s.capabilities.Kubernetes
	if s.users != nil {
		if p := s.users.getTLSPolicy(); p != nil && p.HTTPSEnabled {
			httpsOn = true
		}
	}
	certPath, _, ok := selfsigned.ActivePaths(s.tlsMaterialDir())
	if !ok && s.users != nil {
		if p := s.users.getTLSPolicy(); p != nil && strings.TrimSpace(p.CertPath) != "" {
			if _, err := os.Stat(p.CertPath); err == nil {
				certPath = p.CertPath
				ok = true
			}
		}
	}
	if !ok {
		msg := "No UI certificate file is available to download yet."
		if httpsOn {
			msg = "HTTPS is enabled but the certificate is not on disk yet. Reload after the UI finishes minting it."
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"https_enabled": httpsOn,
			"message":       msg,
		}})
		return
	}
	pemBytes, err := os.ReadFile(certPath)
	if err != nil {
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"https_enabled": httpsOn,
			"message":       "The UI certificate could not be read.",
		}})
		return
	}
	leaf, fp, selfSigned, ok := parseLeafCertificate(pemBytes)
	if !ok {
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"https_enabled": httpsOn,
			"message":       "The UI certificate file is not a valid PEM certificate.",
		}})
		return
	}
	sans := append([]string(nil), leaf.DNSNames...)
	for _, ip := range leaf.IPAddresses {
		sans = append(sans, ip.String())
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"https_enabled":      true,
		"fingerprint_sha256": fp,
		"subject":            leaf.Subject.String(),
		"issuer":             leaf.Issuer.String(),
		"not_before":         leaf.NotBefore.UTC().Format(time.RFC3339),
		"not_after":          leaf.NotAfter.UTC().Format(time.RFC3339),
		"dns_names":          sans,
		"self_signed":        selfSigned,
		"cert_pem":           string(pemBytes),
	}})
}

func fingerprintFromCertFile(certPath string) (fingerprint string, selfSigned bool, ok bool) {
	pemBytes, err := os.ReadFile(certPath)
	if err != nil {
		return "", false, false
	}
	_, fp, selfSigned, ok := parseLeafCertificate(pemBytes)
	return fp, selfSigned, ok
}

func parseLeafCertificate(pemBytes []byte) (leaf *x509.Certificate, fingerprint string, selfSigned bool, ok bool) {
	block, _ := pem.Decode(pemBytes)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, "", false, false
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, "", false, false
	}
	return cert, sha256FingerprintColon(block.Bytes), bytes.Equal(cert.RawSubject, cert.RawIssuer), true
}

// sha256FingerprintColon is the SHA-256 digest of the DER certificate with
// colon-separated uppercase hex, matching Chrome's certificate viewer.
func sha256FingerprintColon(der []byte) string {
	sum := sha256.Sum256(der)
	hexed := strings.ToUpper(hex.EncodeToString(sum[:]))
	if len(hexed) < 2 {
		return hexed
	}
	var b strings.Builder
	b.Grow(len(hexed) + len(hexed)/2 - 1)
	for i := 0; i < len(hexed); i += 2 {
		if i > 0 {
			b.WriteByte(':')
		}
		b.WriteString(hexed[i : i+2])
	}
	return b.String()
}

// liveTLSPolicyView is GET /settings/tls: persisted policy, then the cert
// files on disk (Kubernetes UI pods share /data/tls), then a Kubernetes
// default of "HTTPS is on, self-signed will be minted by the UI".
func (s *apiServer) liveTLSPolicyView() map[string]interface{} {
	if s.users != nil {
		if p := s.users.getTLSPolicy(); p != nil && p.HTTPSEnabled {
			view := tlsPolicyView(p)
			if s.capabilities.Kubernetes {
				view["message"] = "The UI serves HTTPS from the shared volume (/data/tls). Generate a self-signed certificate or paste a BYO PEM pair below; UI pods reload without a stack restart."
			}
			return view
		}
	}
	if certPath, _, ok := selfsigned.ActivePaths(s.tlsMaterialDir()); ok {
		if p := tlsPolicyFromCertFile(certPath); p != nil {
			view := tlsPolicyView(p)
			if s.capabilities.Kubernetes {
				view["message"] = "The UI serves HTTPS from the shared volume (/data/tls). Generate a self-signed certificate or paste a BYO PEM pair below; UI pods reload without a stack restart."
			}
			return view
		}
	}
	if s.capabilities.Kubernetes {
		return map[string]interface{}{
			"https_enabled":      true,
			"managed_by":         "stack",
			"mutation_supported": true,
			"message":            "The UI serves HTTPS with a stack-managed self-signed certificate on the shared volume (same as a Linux v2-start). Generate or upload a replacement here; UI pods reload it without a stack restart. Session cookies are marked Secure.",
		}
	}
	return tlsPolicyView(nil)
}

func (s *apiServer) tlsMaterialDir() string {
	return filepath.Join(s.maintenanceInstallDir(), tlsDirName)
}

func tlsPolicyFromCertFile(certPath string) *tlsPolicy {
	pemBytes, err := os.ReadFile(certPath)
	if err != nil {
		return nil
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil
	}
	sans := append([]string(nil), leaf.DNSNames...)
	for _, ip := range leaf.IPAddresses {
		sans = append(sans, ip.String())
	}
	return &tlsPolicy{
		HTTPSEnabled: true,
		CertPath:     certPath,
		Subject:      leaf.Subject.String(),
		Issuer:       leaf.Issuer.String(),
		NotBefore:    leaf.NotBefore.UTC().Format(time.RFC3339),
		NotAfter:     leaf.NotAfter.UTC().Format(time.RFC3339),
		DNSNames:     sans,
	}
}

// handleTLSGenerate mints a fresh self-signed certificate for the UI server and
// applies it exactly like an upload (enable HTTPS + restart). It powers the
// "Generate self-signed certificate" / "Renew" buttons in Settings → Access, so
// operators on an internal IP-addressed host get HTTPS without sourcing a CA.
//
// Body (all optional): {"hosts": ["10.0.0.5","ncc.local"], "valid_days": 825}.
// When hosts is empty we fall back to the request host. localhost/loopback are
// always covered. "Renew" is just this endpoint called again.
func (s *apiServer) handleTLSGenerate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if s.users == nil || !s.users.writable() {
		writeJSON(w, http.StatusNotImplemented, envelope{Success: false, Error: "HTTPS/TLS management requires a writable user store (enable local accounts)"})
		return
	}
	var body struct {
		Hosts     []string `json:"hosts"`
		ValidDays int      `json:"valid_days"`
	}
	// Body is optional; ignore decode errors on an empty/absent body.
	if r.Body != nil {
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024)).Decode(&body)
	}
	hosts := body.Hosts
	if len(hosts) == 0 {
		if h := strings.TrimSpace(r.Host); h != "" {
			if host, _, err := net.SplitHostPort(h); err == nil && host != "" {
				h = host
			}
			hosts = []string{h}
		}
	}
	var validity time.Duration
	if body.ValidDays > 0 {
		validity = time.Duration(body.ValidDays) * 24 * time.Hour
	}
	certPEM, keyPEM, gerr := selfsigned.Generate(hosts, validity)
	if gerr != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not generate self-signed certificate: " + gerr.Error()})
		return
	}
	s.applyTLSMaterial(w, r, string(certPEM), string(keyPEM), true)
}

func (s *apiServer) handleTLSInstall(w http.ResponseWriter, r *http.Request) {
	if err := requireJSONContentType(r); err != nil {
		writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
		return
	}
	var body struct {
		Cert string `json:"cert"`
		Key  string `json:"key"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxTLSUploadBytes))
	if err := dec.Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid JSON body (expected {cert, key} PEM strings)"})
		return
	}
	certPEM := strings.TrimSpace(body.Cert)
	keyPEM := strings.TrimSpace(body.Key)
	if certPEM == "" || keyPEM == "" {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "both cert and key PEM are required"})
		return
	}
	s.applyTLSMaterial(w, r, certPEM, keyPEM, false)
}

// applyTLSMaterial validates a cert/key pair, persists it (0600), records the
// policy, patches the orchestrator start-state, and restarts the stack so the
// UI server binds TLS. Shared by the upload (handleTLSInstall) and self-signed
// (handleTLSGenerate) paths. selfSigned only adjusts audit/message wording.
func (s *apiServer) applyTLSMaterial(w http.ResponseWriter, r *http.Request, certPEM, keyPEM string, selfSigned bool) {
	// Validate the pair: parses both PEMs and confirms the private key matches
	// the leaf certificate. This is the same check the UI server would do at
	// bind time, surfaced here as an actionable error instead of a crash loop.
	keypair, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid certificate/key pair: " + err.Error()})
		return
	}
	leaf, err := x509.ParseCertificate(keypair.Certificate[0])
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "could not parse leaf certificate: " + err.Error()})
		return
	}

	installDir := s.maintenanceInstallDir()
	tlsDir := filepath.Join(installDir, tlsDirName)
	if err := os.MkdirAll(tlsDir, 0o700); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not create TLS directory: " + err.Error()})
		return
	}
	certPath := filepath.Join(tlsDir, "ui.crt")
	keyPath := filepath.Join(tlsDir, "ui.key")
	if err := os.WriteFile(certPath, []byte(ensureTrailingNewline(certPEM)), 0o600); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not write certificate: " + err.Error()})
		return
	}
	if err := os.WriteFile(keyPath, []byte(ensureTrailingNewline(keyPEM)), 0o600); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not write private key: " + err.Error()})
		return
	}

	pol := &tlsPolicy{
		HTTPSEnabled: true,
		CertPath:     certPath,
		KeyPath:      keyPath,
		Subject:      leaf.Subject.String(),
		Issuer:       leaf.Issuer.String(),
		NotBefore:    leaf.NotBefore.UTC().Format(time.RFC3339),
		NotAfter:     leaf.NotAfter.UTC().Format(time.RFC3339),
		DNSNames:     append([]string(nil), leaf.DNSNames...),
		UpdatedAt:    time.Now().UTC().Format(time.RFC3339),
	}
	if err := s.users.setTLSPolicy(pol); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not persist TLS policy: " + err.Error()})
		return
	}

	auditAction := "settings.tls.install"
	noun := "Certificate installed"
	if selfSigned {
		auditAction = "settings.tls.generate"
		noun = "Self-signed certificate generated"
	}
	s.audit(r, auditAction, true, map[string]interface{}{
		"subject": pol.Subject, "not_after": pol.NotAfter, "dns_names": pol.DNSNames, "self_signed": selfSigned,
	})

	if s.capabilities.Kubernetes {
		msg := noun + ". UI pods reload the certificate from the shared volume within a few seconds — no stack restart. If the certificate is self-signed, accept the browser warning."
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: msg, Data: map[string]interface{}{
			"tls":              tlsPolicyView(pol),
			"restarting":       false,
			"restart_required": false,
		}})
		return
	}

	// Persist the cert/key into the start-state so the next (re)start binds the
	// UI server to TLS. If the state file is absent we cannot safely synthesize
	// one, so we skip the auto-restart and ask the operator to restart.
	patched, perr := patchV2StartStateUITLS(installDir, certPath, keyPath)
	restarting := false
	msg := "HTTPS enabled."
	switch {
	case perr != nil:
		msg = noun + ", but the start-state could not be updated (" + perr.Error() + "). Restart the stack manually with --ui-tls-cert-file/--ui-tls-key-file to serve HTTPS."
	case !patched:
		msg = noun + ". The start-state file was not found, so restart the stack manually with --ui-tls-cert-file/--ui-tls-key-file to serve HTTPS."
	default:
		restarting = s.spawnDetachedRestart(installDir)
		if restarting {
			msg = noun + ". The stack is restarting now to serve over TLS — reconnect over https:// in a few seconds. Session cookies will be marked Secure."
		} else {
			msg = noun + ", but an automatic restart is unavailable. Restart the stack (v2-restart) to begin serving over TLS."
		}
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: msg, Data: map[string]interface{}{
		"tls":              tlsPolicyView(pol),
		"restarting":       restarting,
		"restart_required": !restarting,
	}})
}

func (s *apiServer) handleTLSDisable(w http.ResponseWriter, r *http.Request) {
	installDir := s.maintenanceInstallDir()
	tlsDir := filepath.Join(installDir, tlsDirName)
	prev := s.users.getTLSPolicy()
	_ = os.Remove(filepath.Join(tlsDir, selfsigned.BYOCertFile))
	_ = os.Remove(filepath.Join(tlsDir, selfsigned.BYOKeyFile))
	if prev != nil {
		if prev.CertPath != "" {
			_ = os.Remove(prev.CertPath)
		}
		if prev.KeyPath != "" {
			_ = os.Remove(prev.KeyPath)
		}
	}

	if s.capabilities.Kubernetes {
		certPath, _, err := selfsigned.Ensure(tlsDir, nil)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not restore the self-signed certificate: " + err.Error()})
			return
		}
		pol := tlsPolicyFromCertFile(certPath)
		if pol == nil {
			pol = &tlsPolicy{HTTPSEnabled: true, CertPath: certPath, UpdatedAt: time.Now().UTC().Format(time.RFC3339)}
		}
		if err := s.users.setTLSPolicy(pol); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not persist TLS policy: " + err.Error()})
			return
		}
		s.audit(r, "settings.tls.disable", true, map[string]interface{}{"reverted_to": "self-signed"})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "Reverted to the stack-managed self-signed certificate. UI pods reload it within a few seconds. HTTPS stays enabled.", Data: map[string]interface{}{
			"tls":              tlsPolicyView(pol),
			"restarting":       false,
			"restart_required": false,
		}})
		return
	}

	if err := s.users.setTLSPolicy(nil); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not clear TLS policy: " + err.Error()})
		return
	}
	patched, perr := patchV2StartStateUITLS(installDir, "", "")
	s.audit(r, "settings.tls.disable", true, nil)

	restarting := false
	msg := "HTTPS disabled."
	switch {
	case perr != nil:
		msg = "HTTPS disabled, but the start-state could not be updated (" + perr.Error() + "). Restart the stack manually to fall back to HTTP."
	case !patched:
		msg = "HTTPS disabled. Restart the stack to fall back to HTTP."
	default:
		restarting = s.spawnDetachedRestart(installDir)
		if restarting {
			msg = "HTTPS disabled. The stack is restarting now to serve over plain HTTP — reconnect over http:// in a few seconds."
		} else {
			msg = "HTTPS disabled, but an automatic restart is unavailable. Restart the stack (v2-restart) to fall back to HTTP."
		}
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: msg, Data: map[string]interface{}{
		"tls":              tlsPolicyView(nil),
		"restarting":       restarting,
		"restart_required": !restarting,
	}})
}

// patchV2StartStateUITLS sets (or clears, when paths are empty) the UI TLS
// cert/key fields in the orchestrator's persisted start-state, preserving all
// other keys. ok is false when the state file does not exist (the caller then
// declines to auto-restart, since a bare restart would drop the operator's
// flags).
func patchV2StartStateUITLS(installDir, certPath, keyPath string) (ok bool, err error) {
	path := filepath.Join(installDir, v2StartStateFileName)
	data, rerr := os.ReadFile(path)
	if rerr != nil {
		if os.IsNotExist(rerr) {
			return false, nil
		}
		return false, rerr
	}
	state := map[string]json.RawMessage{}
	if uerr := json.Unmarshal(data, &state); uerr != nil {
		return false, uerr
	}
	setOrDelete := func(key, val string) error {
		if strings.TrimSpace(val) == "" {
			delete(state, key)
			return nil
		}
		raw, merr := json.Marshal(val)
		if merr != nil {
			return merr
		}
		state[key] = raw
		return nil
	}
	if e := setOrDelete("ui_tls_cert_file", certPath); e != nil {
		return false, e
	}
	if e := setOrDelete("ui_tls_key_file", keyPath); e != nil {
		return false, e
	}
	out, merr := json.MarshalIndent(state, "", "  ")
	if merr != nil {
		return false, merr
	}
	if werr := os.WriteFile(path, append(out, '\n'), 0o600); werr != nil {
		return false, werr
	}
	return true, nil
}

func ensureTrailingNewline(s string) string {
	if strings.HasSuffix(s, "\n") {
		return s
	}
	return s + "\n"
}
