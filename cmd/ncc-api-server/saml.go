package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/crewjam/saml"
	"github.com/crewjam/saml/samlsp"
)

// samlConfig captures SAML SP settings supplied via startup flags.
type samlConfig struct {
	RootURL       string
	IDPMetadata   string // IdP metadata URL (http/https) or local file path
	CertFile      string
	KeyFile       string
	EntityID      string
	UsernameAttr  string
	RoleAttr      string
	RoleMapRaw    string
	DefaultRole   string
	AllowIDPInit  bool
	clientTimeout time.Duration
}

func (c samlConfig) configured() bool {
	return strings.TrimSpace(c.RootURL) != "" &&
		strings.TrimSpace(c.IDPMetadata) != "" &&
		strings.TrimSpace(c.CertFile) != "" &&
		strings.TrimSpace(c.KeyFile) != ""
}

// samlPersisted is the runtime, admin-editable SAML configuration stored in the
// user database. The SP private key is generated server-side and never
// uploaded through the browser.
type samlPersisted struct {
	Enabled        bool   `json:"enabled"`
	RootURL        string `json:"root_url"`
	EntityID       string `json:"entity_id,omitempty"`
	IDPMetadataXML string `json:"idp_metadata_xml,omitempty"`
	IDPMetadataURL string `json:"idp_metadata_url,omitempty"`
	UsernameAttr   string `json:"username_attribute,omitempty"`
	RoleAttr       string `json:"role_attribute,omitempty"`
	RoleMapRaw     string `json:"role_map,omitempty"`
	DefaultRole    string `json:"default_role,omitempty"`
	AllowIDPInit   bool   `json:"allow_idp_initiated,omitempty"`
	// SP keypair (PEM). Generated on first enable when empty.
	SPCertPEM string `json:"sp_cert_pem,omitempty"`
	SPKeyPEM  string `json:"sp_key_pem,omitempty"`
}

func (c *samlPersisted) configured() bool {
	if c == nil || !c.Enabled {
		return false
	}
	return strings.TrimSpace(c.RootURL) != "" &&
		(strings.TrimSpace(c.IDPMetadataXML) != "" || strings.TrimSpace(c.IDPMetadataURL) != "")
}

// samlBuild is the resolved input used to construct a samlProvider.
type samlBuild struct {
	RootURL        string
	EntityID       string
	IDPMetadataXML []byte
	IDPMetadataURL string
	CertPEM        []byte
	KeyPEM         []byte
	UsernameAttr   string
	RoleAttr       string
	RoleMapRaw     string
	DefaultRole    string
	AllowIDPInit   bool
	clientTimeout  time.Duration
}

// samlProvider wraps the crewjam/saml SP middleware plus our attribute->role
// mapping. After the SP flow completes we mint our own role-bearing session
// cookie rather than relying on the SP's JWT session.
type samlProvider struct {
	mw          *samlsp.Middleware
	usernameAtt string
	roleAtt     string
	roleMap     map[string]Role
	defaultRole Role
}

func parseRoleMap(raw string) (map[string]Role, error) {
	m := map[string]Role{}
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid saml role mapping %q (want value=role)", pair)
		}
		role, ok := parseRole(kv[1])
		if !ok {
			return nil, fmt.Errorf("invalid role %q in saml role mapping", kv[1])
		}
		m[strings.TrimSpace(kv[0])] = role
	}
	return m, nil
}

// newSAMLProvider builds a provider from startup-flag config (reads files).
func newSAMLProvider(ctx context.Context, c samlConfig) (*samlProvider, error) {
	cert, err := os.ReadFile(c.CertFile)
	if err != nil {
		return nil, fmt.Errorf("read saml sp cert: %w", err)
	}
	key, err := os.ReadFile(c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("read saml sp key: %w", err)
	}
	b := samlBuild{
		RootURL:        c.RootURL,
		EntityID:       c.EntityID,
		IDPMetadataURL: "",
		CertPEM:        cert,
		KeyPEM:         key,
		UsernameAttr:   c.UsernameAttr,
		RoleAttr:       c.RoleAttr,
		RoleMapRaw:     c.RoleMapRaw,
		DefaultRole:    c.DefaultRole,
		AllowIDPInit:   c.AllowIDPInit,
		clientTimeout:  c.clientTimeout,
	}
	// IDPMetadata may be a URL or a file path.
	src := strings.TrimSpace(c.IDPMetadata)
	if strings.HasPrefix(src, "http://") || strings.HasPrefix(src, "https://") {
		b.IDPMetadataURL = src
	} else {
		xml, err := os.ReadFile(src)
		if err != nil {
			return nil, fmt.Errorf("read saml idp metadata file: %w", err)
		}
		b.IDPMetadataXML = xml
	}
	return buildSAMLProvider(ctx, b)
}

// buildSAMLProvider constructs a samlProvider from resolved PEM + metadata.
func buildSAMLProvider(ctx context.Context, b samlBuild) (*samlProvider, error) {
	rootURL, err := url.Parse(strings.TrimSpace(b.RootURL))
	if err != nil {
		return nil, fmt.Errorf("parse saml root url: %w", err)
	}
	keyPair, err := tls.X509KeyPair(b.CertPEM, b.KeyPEM)
	if err != nil {
		return nil, fmt.Errorf("load saml sp keypair: %w", err)
	}
	keyPair.Leaf, err = x509.ParseCertificate(keyPair.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("parse saml sp certificate: %w", err)
	}
	rsaKey, ok := keyPair.PrivateKey.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("saml sp key must be an RSA private key")
	}

	timeout := b.clientTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	var idpMeta *saml.EntityDescriptor
	if len(b.IDPMetadataXML) > 0 {
		idpMeta, err = samlsp.ParseMetadata(b.IDPMetadataXML)
		if err != nil {
			return nil, fmt.Errorf("parse saml idp metadata: %w", err)
		}
	} else if strings.TrimSpace(b.IDPMetadataURL) != "" {
		u, perr := url.Parse(strings.TrimSpace(b.IDPMetadataURL))
		if perr != nil {
			return nil, fmt.Errorf("parse saml idp metadata url: %w", perr)
		}
		client := &http.Client{Timeout: timeout}
		idpMeta, err = samlsp.FetchMetadata(ctx, client, *u)
		if err != nil {
			return nil, fmt.Errorf("fetch saml idp metadata: %w", err)
		}
	} else {
		return nil, fmt.Errorf("saml idp metadata (xml or url) is required")
	}

	opts := samlsp.Options{
		URL:               *rootURL,
		Key:               rsaKey,
		Certificate:       keyPair.Leaf,
		IDPMetadata:       idpMeta,
		AllowIDPInitiated: b.AllowIDPInit,
		// The IdP returns its assertion as a top-level cross-site form POST to
		// our ACS (idp-host → ui-host), so the request-tracking cookie minted
		// during the AuthnRequest must survive that navigation. A default/Lax
		// cookie is dropped on cross-site POST, which would make every login
		// fail with "request not found". SameSite=None requires Secure, which
		// crewjam already sets because the ACS URL is https (UI is HTTPS).
		CookieSameSite: http.SameSiteNoneMode,
	}
	if eid := strings.TrimSpace(b.EntityID); eid != "" {
		opts.EntityID = eid
	}
	mw, err := samlsp.New(opts)
	if err != nil {
		return nil, fmt.Errorf("init saml middleware: %w", err)
	}
	roleMap, err := parseRoleMap(b.RoleMapRaw)
	if err != nil {
		return nil, err
	}
	defRole := RoleViewer
	if r, ok := parseRole(b.DefaultRole); ok {
		defRole = r
	}
	roleAtt := strings.TrimSpace(b.RoleAttr)
	if roleAtt == "" {
		roleAtt = "Role"
	}
	return &samlProvider{
		mw:          mw,
		usernameAtt: strings.TrimSpace(b.UsernameAttr),
		roleAtt:     roleAtt,
		roleMap:     roleMap,
		defaultRole: defRole,
	}, nil
}

// buildSAMLFromPersisted builds a provider from the admin-managed config,
// generating an SP keypair the first time if none is stored. When a keypair is
// generated, the (mutated) config should be persisted by the caller.
func buildSAMLFromPersisted(ctx context.Context, c *samlPersisted) (*samlProvider, bool, error) {
	generated := false
	if strings.TrimSpace(c.SPCertPEM) == "" || strings.TrimSpace(c.SPKeyPEM) == "" {
		certPEM, keyPEM, err := generateSPKeypair(c.RootURL)
		if err != nil {
			return nil, false, err
		}
		c.SPCertPEM = certPEM
		c.SPKeyPEM = keyPEM
		generated = true
	}
	b := samlBuild{
		RootURL:        c.RootURL,
		EntityID:       c.EntityID,
		IDPMetadataXML: []byte(c.IDPMetadataXML),
		IDPMetadataURL: c.IDPMetadataURL,
		CertPEM:        []byte(c.SPCertPEM),
		KeyPEM:         []byte(c.SPKeyPEM),
		UsernameAttr:   c.UsernameAttr,
		RoleAttr:       c.RoleAttr,
		RoleMapRaw:     c.RoleMapRaw,
		DefaultRole:    c.DefaultRole,
		AllowIDPInit:   c.AllowIDPInit,
	}
	p, err := buildSAMLProvider(ctx, b)
	if err != nil {
		return nil, generated, err
	}
	return p, generated, nil
}

// generateSPKeypair creates a self-signed RSA keypair for the SP, returning PEM
// strings. The certificate is public (published in SP metadata); the private
// key stays on the server.
func generateSPKeypair(rootURL string) (string, string, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", err
	}
	cn := "ncc-api-server"
	if u, perr := url.Parse(strings.TrimSpace(rootURL)); perr == nil && u.Host != "" {
		cn = u.Host
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn, Organization: []string{"ncc-orchestrator"}},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().AddDate(10, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return "", "", err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return string(certPEM), string(keyPEM), nil
}

// roleFromValues maps IdP-provided role/group values to the highest matching
// local role, falling back to the configured default.
func (p *samlProvider) roleFromValues(values []string) Role {
	best := RoleNone
	for _, v := range values {
		if r, ok := p.roleMap[strings.TrimSpace(v)]; ok && r > best {
			best = r
		}
	}
	if best == RoleNone {
		return p.defaultRole
	}
	return best
}

// currentSAML returns the active SAML provider (or nil) under a read lock.
func (s *apiServer) currentSAML() *samlProvider {
	s.samlMu.RLock()
	defer s.samlMu.RUnlock()
	return s.saml
}

// reloadSAMLFromStore rebuilds the SAML provider from the persisted config in
// the user database. Generated SP keypairs are written back to the store. A
// nil/disabled config disables SAML. No-op when SAML is managed by flags.
func (s *apiServer) reloadSAMLFromStore(ctx context.Context) error {
	if s.samlFromFlags || s.users == nil {
		return nil
	}
	cfg := s.users.getSAML()
	if !cfg.configured() {
		s.samlMu.Lock()
		s.saml = nil
		s.samlEnabled = false
		s.samlMu.Unlock()
		return nil
	}
	prov, generated, err := buildSAMLFromPersisted(ctx, cfg)
	if err != nil {
		return err
	}
	if generated {
		if err := s.users.setSAML(cfg); err != nil {
			return err
		}
	}
	s.samlMu.Lock()
	s.saml = prov
	s.samlEnabled = true
	s.samlMu.Unlock()
	return nil
}

// registerSAML wires the SAML endpoints. Handlers resolve the current provider
// at request time so runtime reconfiguration takes effect without restart.
func (s *apiServer) registerSAML(mux *http.ServeMux) {
	mux.HandleFunc("/saml/", func(w http.ResponseWriter, r *http.Request) {
		p := s.currentSAML()
		if p == nil {
			http.Error(w, "saml not configured", http.StatusServiceUnavailable)
			return
		}
		// /saml/complete is our post-auth landing page; everything else
		// (metadata, acs) is served by the SP middleware.
		if r.URL.Path == "/saml/complete" {
			p.mw.RequireAccount(http.HandlerFunc(s.handleSAMLComplete)).ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/saml/login" {
			http.Redirect(w, r, "/saml/complete", http.StatusFound)
			return
		}
		p.mw.ServeHTTP(w, r)
	})
}

// handleSAMLComplete runs after the SP middleware authenticates the user,
// translating the assertion attributes into our role-bearing session cookie.
func (s *apiServer) handleSAMLComplete(w http.ResponseWriter, r *http.Request) {
	p := s.currentSAML()
	if p == nil {
		http.Error(w, "saml not configured", http.StatusServiceUnavailable)
		return
	}
	attrs := samlsp.Attributes{}
	if sess := samlsp.SessionFromContext(r.Context()); sess != nil {
		if swa, ok := sess.(samlsp.SessionWithAttributes); ok {
			attrs = swa.GetAttributes()
		}
	}
	username := ""
	if p.usernameAtt != "" {
		username = attrs.Get(p.usernameAtt)
	}
	if username == "" {
		for _, k := range []string{"uid", "mail", "email", "urn:oasis:names:tc:SAML:attribute:subject-id"} {
			if v := attrs.Get(k); v != "" {
				username = v
				break
			}
		}
	}
	if username == "" {
		if jc, ok := samlsp.SessionFromContext(r.Context()).(samlsp.JWTSessionClaims); ok {
			username = jc.Subject
		}
	}
	if username == "" {
		username = "saml-user"
	}
	role := p.roleFromValues(attrs[p.roleAtt])
	token, exp, err := s.issueRoleSessionToken(cleanClientIP(r), username, role)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	if err := s.setSessionCookies(w, token, exp); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	s.audit(r, "auth.saml.login", true, map[string]interface{}{"username": username, "role": role.String()})
	http.Redirect(w, r, "/", http.StatusFound)
}
