// Command fakeoidc is a throwaway OpenID Connect provider for trying the
// dashboard's OIDC sign-in locally without a real identity provider. Its
// "login page" lists a few test users; picking one completes the
// authorization-code flow (PKCE enforced) with an RS256-signed ID token whose
// groups claim drives role mapping.
//
//	go run ./tools/fakeoidc                      # issuer http://127.0.0.1:9800
//
// Then in Settings → Access → External authentication → OpenID Connect:
// issuer http://127.0.0.1:9800, client ID ncc-dev, any secret, role mapping
// ncc-admins=admin,ncc-ops=operator.
//
// Test tool only: keys are generated at start-up and every user is accepted.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

type testUser struct {
	Name   string
	Email  string
	Groups []string
}

var users = []testUser{
	{"alice", "alice@example.com", []string{"ncc-admins"}},
	{"oscar", "oscar@example.com", []string{"ncc-ops"}},
	{"vera", "vera@example.com", []string{"staff"}},
}

type grant struct {
	user      testUser
	challenge string
	nonce     string
	clientID  string
	expires   time.Time
}

const certificateSerialBits = 128

var page = template.Must(template.New("login").Parse(`<!doctype html>
<html><head><meta charset="utf-8"><title>fakeoidc sign-in</title>
<style>body{font-family:system-ui,sans-serif;max-width:420px;margin:64px auto;padding:0 16px}
a{display:block;padding:12px 16px;margin:8px 0;border:1px solid #ccc;border-radius:8px;text-decoration:none;color:inherit}
a:hover{background:#f3f3f3}small{color:#666}</style></head>
<body><h1>fakeoidc</h1><p>Test identity provider. Choose who to sign in as:</p>
{{range .Users}}<a href="{{$.Approve}}&user={{.Name}}"><b>{{.Name}}</b><br><small>{{.Email}} · groups: {{range .Groups}}{{.}} {{end}}</small></a>{{end}}
<a href="{{.Deny}}"><b>Cancel</b><br><small>returns error=access_denied</small></a>
</body></html>`))

func main() {
	listen := flag.String("listen", "127.0.0.1:9800", "address to listen on")
	https := flag.Bool("https", false, "serve HTTPS with a generated self-signed certificate")
	certFile := flag.String("tls-cert", "", "PEM certificate file (HTTPS only; generated when omitted)")
	keyFile := flag.String("tls-key", "", "PEM private key file (HTTPS only; generated when omitted)")
	issuer := flag.String("issuer", "", "issuer URL (default scheme://advertised listen host)")
	redirectHosts := flag.String("redirect-hosts", "", "comma-separated callback hostnames/IPs (default loopback plus issuer host)")
	flag.Parse()
	iss := *issuer
	if iss == "" {
		scheme := "http"
		if *https {
			scheme = "https"
		}
		iss = scheme + "://" + advertisedHost(*listen)
	}
	issuerURL, err := url.Parse(iss)
	if err != nil || issuerURL.Scheme != "http" && issuerURL.Scheme != "https" || issuerURL.Hostname() == "" {
		log.Fatalf("invalid issuer URL %q", iss)
	}
	allowedRedirectHosts := buildRedirectHostAllowlist(*redirectHosts, issuerURL.Hostname())
	if len(allowedRedirectHosts) == 0 {
		log.Fatal("redirect host allowlist cannot be empty")
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		log.Fatal(err)
	}
	var mu sync.Mutex
	codes := map[string]grant{}

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"issuer":                                iss,
			"authorization_endpoint":                iss + "/authorize",
			"token_endpoint":                        iss + "/token",
			"jwks_uri":                              iss + "/jwks",
			"response_types_supported":              []string{"code"},
			"subject_types_supported":               []string{"public"},
			"id_token_signing_alg_values_supported": []string{"RS256"},
			"code_challenge_methods_supported":      []string{"S256"},
			"scopes_supported":                      []string{"openid", "profile", "email", "groups"},
			"claims_supported":                      []string{"sub", "email", "preferred_username", "groups"},
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "dev", Algorithm: "RS256", Use: "sig"}}})
	})
	mux.HandleFunc("/authorize", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		redirect, err := validateRedirectURL(q.Get("redirect_uri"), allowedRedirectHosts)
		if err != nil || q.Get("response_type") != "code" || q.Get("code_challenge_method") != "S256" {
			http.Error(w, "need response_type=code, a redirect_uri and PKCE (S256)", http.StatusBadRequest)
			return
		}
		if user := q.Get("user"); user != "" {
			for _, u := range users {
				if u.Name != user {
					continue
				}
				b := make([]byte, 24)
				_, _ = rand.Read(b)
				code := base64.RawURLEncoding.EncodeToString(b)
				mu.Lock()
				codes[code] = grant{user: u, challenge: q.Get("code_challenge"), nonce: q.Get("nonce"), clientID: q.Get("client_id"), expires: time.Now().Add(time.Minute)}
				mu.Unlock()
				rq := redirect.Query()
				rq.Set("code", code)
				rq.Set("state", q.Get("state"))
				redirect.RawQuery = rq.Encode()
				http.Redirect(w, r, redirect.String(), http.StatusFound)
				return
			}
		}
		deny := *redirect
		dq := deny.Query()
		dq.Set("error", "access_denied")
		dq.Set("state", q.Get("state"))
		deny.RawQuery = dq.Encode()
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = page.Execute(w, map[string]interface{}{"Users": users, "Approve": "/authorize?" + r.URL.RawQuery, "Deny": deny.String()})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		mu.Lock()
		g, ok := codes[r.Form.Get("code")]
		delete(codes, r.Form.Get("code"))
		mu.Unlock()
		sum := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
		w.Header().Set("Content-Type", "application/json")
		if !ok || time.Now().After(g.expires) || base64.RawURLEncoding.EncodeToString(sum[:]) != g.challenge {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_grant"})
			return
		}
		signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key}, (&jose.SignerOptions{}).WithHeader("kid", "dev"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		now := time.Now()
		idt, err := jwt.Signed(signer).Claims(map[string]interface{}{
			"iss": iss, "aud": g.clientID, "sub": "fake-" + g.user.Name,
			"iat": now.Unix(), "exp": now.Add(5 * time.Minute).Unix(), "nonce": g.nonce,
			"preferred_username": g.user.Name, "email": g.user.Email, "groups": g.user.Groups,
		}).Serialize()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"access_token": "dev-access-token", "token_type": "Bearer", "expires_in": 300, "id_token": idt})
	})
	log.Printf("fakeoidc issuer %s (users: alice=ncc-admins, oscar=ncc-ops, vera=staff)", iss)
	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	if !*https {
		log.Fatal(srv.ListenAndServe())
	}

	certificate, err := loadOrGenerateCertificate(*certFile, *keyFile, certificateNames(issuerURL, *listen))
	if err != nil {
		log.Fatal(err)
	}
	tlsListener, err := tls.Listen("tcp", *listen, &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("fakeoidc HTTPS enabled; certificate is self-signed (use -tls-cert/-tls-key for a trusted certificate)")
	log.Fatal(srv.Serve(tlsListener))
}

func advertisedHost(listen string) string {
	host, _, err := net.SplitHostPort(listen)
	if err != nil || host == "" || host == "0.0.0.0" || host == "::" {
		return "127.0.0.1:" + strings.TrimSpace(listenPort(listen))
	}
	return listen
}

func listenPort(listen string) string {
	_, port, err := net.SplitHostPort(listen)
	if err != nil || port == "" {
		return "9800"
	}
	return port
}

func buildRedirectHostAllowlist(raw, issuerHost string) map[string]bool {
	allowed := map[string]bool{"localhost": true, "127.0.0.1": true, "::1": true}
	if strings.TrimSpace(raw) == "" {
		allowed[strings.ToLower(strings.TrimSpace(issuerHost))] = true
		return allowed
	}
	for _, host := range strings.Split(raw, ",") {
		host = strings.ToLower(strings.TrimSpace(host))
		if host != "" {
			allowed[host] = true
		}
	}
	return allowed
}

func validateRedirectURL(raw string, allowedHosts map[string]bool) (*url.URL, error) {
	redirect, err := url.Parse(raw)
	if err != nil || redirect.Scheme != "http" && redirect.Scheme != "https" ||
		redirect.Hostname() == "" || redirect.User != nil || redirect.Fragment != "" {
		return nil, errors.New("redirect_uri must be an HTTP(S) URL without credentials or fragments")
	}
	if !allowedHosts[strings.ToLower(redirect.Hostname())] {
		return nil, errors.New("redirect_uri host is not in the fakeoidc allowlist")
	}
	return redirect, nil
}

func certificateNames(issuer *url.URL, listen string) []string {
	names := []string{"localhost", "127.0.0.1", "::1"}
	if host := issuer.Hostname(); host != "" {
		names = append(names, host)
	}
	if host, _, err := net.SplitHostPort(listen); err == nil && host != "" && host != "0.0.0.0" && host != "::" {
		names = append(names, host)
	}
	return uniqueStrings(names)
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]bool, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" && !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	return out
}

func loadOrGenerateCertificate(certFile, keyFile string, names []string) (tls.Certificate, error) {
	if (certFile == "") != (keyFile == "") {
		return tls.Certificate{}, errors.New("-tls-cert and -tls-key must be provided together")
	}
	if certFile != "" {
		certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return tls.Certificate{}, fmt.Errorf("load TLS certificate: %w", err)
		}
		return certificate, nil
	}

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate TLS key: %w", err)
	}
	serialLimit := new(big.Int).Lsh(big.NewInt(1), certificateSerialBits)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate certificate serial: %w", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: names[0]},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     nil,
		IPAddresses:  nil,
	}
	for _, name := range names {
		if ip := net.ParseIP(name); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, name)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create self-signed certificate: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("marshal TLS key: %w", err)
	}
	return tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	)
}
