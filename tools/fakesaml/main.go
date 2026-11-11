// Command fakesaml is a test-only SAML identity provider for NCC.
//
// It registers NCC's SAML metadata, seeds three test users, and serves a
// landing page with a "Launch NCC login" button. It is not suitable for
// production or exposure to untrusted networks.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/crewjam/saml"
	"github.com/crewjam/saml/samlidp"
	"github.com/crewjam/saml/samlsp"
	"golang.org/x/crypto/bcrypt"
)

const certificateSerialBits = 128

var landingPage = template.Must(template.New("landing").Parse(`<!doctype html>
<html>
<head><meta charset="utf-8"><title>NCC test SAML</title>
<style>
body{font-family:system-ui,sans-serif;max-width:600px;margin:64px auto;padding:0 20px}
a,button{display:inline-block;padding:12px 18px;border:1px solid #999;border-radius:8px;text-decoration:none}
small{color:#666}
</style></head>
<body>
<h1>NCC test SAML provider</h1>
<p>This is a development-only identity provider.</p>
<p><a href="{{.LaunchURL}}">Launch NCC login</a></p>
<p>Test users: <code>alice</code>, <code>oscar</code>, and <code>vera</code>.
The password for each user is <code>password</code>.</p>
<p><small><a href="/metadata">View IdP metadata</a></small></p>
</body>
</html>`))

func main() {
	listen := flag.String("listen", "127.0.0.1:9900", "address to listen on")
	httpsEnabled := flag.Bool("https", false, "serve HTTPS with a generated self-signed certificate")
	certFile := flag.String("tls-cert", "", "PEM certificate file (HTTPS only; generated when omitted)")
	keyFile := flag.String("tls-key", "", "PEM private key file (HTTPS only; generated when omitted)")
	signingCertFile := flag.String("signing-cert", "", "SAML signing certificate PEM file (generated when omitted)")
	signingKeyFile := flag.String("signing-key", "", "SAML signing private key PEM file (generated when omitted)")
	issuer := flag.String("issuer", "", "IdP issuer URL (default scheme://advertised listen host)")
	spMetadataURL := flag.String("sp-metadata-url", "", "NCC SAML SP metadata URL to register")
	launchURL := flag.String("launch-url", "", "NCC SAML login URL (default derived from SP metadata)")
	flag.Parse()

	issuerURL, err := resolveIssuer(*issuer, *listen, *httpsEnabled)
	if err != nil {
		log.Fatal(err)
	}
	if *spMetadataURL == "" {
		log.Fatal("-sp-metadata-url is required, for example https://ncc.example.com/saml/metadata")
	}

	idpKey, idpCertificate, err := loadOrGenerateSigningPair(*signingCertFile, *signingKeyFile, issuerURL, *listen)
	if err != nil {
		log.Fatalf("load SAML signing pair: %v", err)
	}

	store := &samlidp.MemoryStore{}
	metadata, rawMetadata, err := fetchSPMetadata(*spMetadataURL)
	if err != nil {
		log.Fatalf("fetch SAML SP metadata: %v", err)
	}
	if err := store.Put("/services/ncc", &samlidp.Service{
		Name:        "ncc",
		Metadata:    *metadata,
		RawMetadata: rawMetadata,
	}); err != nil {
		log.Fatalf("register NCC SAML service: %v", err)
	}
	if err := store.Put("/shortcuts/ncc", &samlidp.Shortcut{
		Name:              "ncc",
		ServiceProviderID: metadata.EntityID,
	}); err != nil {
		log.Fatalf("register NCC SAML shortcut: %v", err)
	}
	if err := seedUsers(store); err != nil {
		log.Fatalf("seed SAML users: %v", err)
	}
	if *launchURL == "" {
		*launchURL = deriveLaunchURL(metadata)
	}
	if *launchURL == "" {
		log.Fatal("-launch-url is required when SP metadata has no ACS endpoint")
	}

	idp, err := samlidp.New(samlidp.Options{
		URL:         *issuerURL,
		Key:         idpKey,
		Certificate: idpCertificate,
		Store:       store,
	})
	if err != nil {
		log.Fatalf("initialize SAML IdP: %v", err)
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			_ = landingPage.Execute(w, map[string]string{"LaunchURL": *launchURL})
			return
		}
		idp.ServeHTTP(w, r)
	})
	server := &http.Server{
		Addr:              *listen,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("fakesaml issuer %s; NCC launch page %s", issuerURL, *launchURL)
	if !*httpsEnabled {
		log.Fatal(server.ListenAndServe())
	}

	tlsCertificate, err := loadOrGenerateCertificate(*certFile, *keyFile, certificateNames(issuerURL, *listen))
	if err != nil {
		log.Fatal(err)
	}
	listener, err := tls.Listen("tcp", *listen, &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{tlsCertificate},
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("fakesaml HTTPS enabled; certificate is self-signed (use -tls-cert/-tls-key for a persistent certificate)")
	log.Fatal(server.Serve(listener))
}

func resolveIssuer(raw, listen string, httpsEnabled bool) (*url.URL, error) {
	if raw == "" {
		scheme := "http"
		if httpsEnabled {
			scheme = "https"
		}
		raw = scheme + "://" + advertisedHost(listen)
	}
	issuer, err := url.Parse(raw)
	if err != nil || (issuer.Scheme != "http" && issuer.Scheme != "https") || issuer.Hostname() == "" {
		return nil, fmt.Errorf("invalid issuer URL %q", raw)
	}
	return issuer, nil
}

func fetchSPMetadata(rawURL string) (*saml.EntityDescriptor, []byte, error) {
	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // test provider only
		},
	}
	resp, err := client.Get(rawURL)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("SP metadata returned HTTP %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return nil, nil, fmt.Errorf("read SP metadata: %w", err)
	}
	metadata, err := samlsp.ParseMetadata(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("parse SP metadata: %w", err)
	}
	return metadata, raw, nil
}

func deriveLaunchURL(metadata *saml.EntityDescriptor) string {
	if metadata == nil || len(metadata.SPSSODescriptors) == 0 {
		return ""
	}
	for _, service := range metadata.SPSSODescriptors[0].AssertionConsumerServices {
		if service.Location == "" {
			continue
		}
		u, err := url.Parse(service.Location)
		if err != nil || u.Hostname() == "" {
			continue
		}
		u.Path = strings.TrimRight(strings.TrimSuffix(u.Path, "/acs"), "/") + "/login"
		u.RawQuery = ""
		u.Fragment = ""
		return u.String()
	}
	return ""
}

func seedUsers(store *samlidp.MemoryStore) error {
	password, err := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.MinCost)
	if err != nil {
		return err
	}
	users := []samlidp.User{
		{Name: "alice", Email: "alice@example.com", GivenName: "Alice", Groups: []string{"ncc-admins"}},
		{Name: "oscar", Email: "oscar@example.com", GivenName: "Oscar", Groups: []string{"ncc-ops"}},
		{Name: "vera", Email: "vera@example.com", GivenName: "Vera", Groups: []string{"staff"}},
	}
	for i := range users {
		users[i].HashedPassword = password
		if err := store.Put("/users/"+users[i].Name, &users[i]); err != nil {
			return err
		}
	}
	return nil
}

func advertisedHost(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil || host == "" || host == "0.0.0.0" || host == "::" {
		if port == "" {
			port = "9900"
		}
		return "127.0.0.1:" + port
	}
	return listen
}

func certificateNames(issuer *url.URL, listen string) []string {
	names := []string{"localhost", "127.0.0.1", "::1", issuer.Hostname()}
	if host, _, err := net.SplitHostPort(listen); err == nil && host != "" && host != "0.0.0.0" && host != "::" {
		names = append(names, host)
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(names))
	for _, name := range names {
		if name != "" && !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	return out
}

func generateCertificate(issuer *url.URL, listen string, key *rsa.PrivateKey) (*x509.Certificate, error) {
	serialLimit := new(big.Int).Lsh(big.NewInt(1), certificateSerialBits)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: issuer.Hostname()},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	for _, name := range certificateNames(issuer, listen) {
		if ip := net.ParseIP(name); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, name)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	return x509.ParseCertificate(der)
}

func loadOrGenerateSigningPair(certFile, keyFile string, issuer *url.URL, listen string) (*rsa.PrivateKey, *x509.Certificate, error) {
	if (certFile == "") != (keyFile == "") {
		return nil, nil, errors.New("-signing-cert and -signing-key must be provided together")
	}
	if certFile == "" {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return nil, nil, err
		}
		cert, err := generateCertificate(issuer, listen, key)
		if err != nil {
			return nil, nil, err
		}
		return key, cert, nil
	}

	certPEM, err := os.ReadFile(certFile)
	if err != nil {
		return nil, nil, fmt.Errorf("read signing certificate: %w", err)
	}
	keyPEM, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("read signing key: %w", err)
	}
	certBlock, _ := pem.Decode(certPEM)
	if certBlock == nil || certBlock.Type != "CERTIFICATE" {
		return nil, nil, errors.New("signing certificate does not contain a CERTIFICATE PEM block")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parse signing certificate: %w", err)
	}
	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return nil, nil, errors.New("signing key does not contain a PEM block")
	}
	keyAny, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		if parsed, pkcs1Err := x509.ParsePKCS1PrivateKey(keyBlock.Bytes); pkcs1Err == nil {
			keyAny = parsed
		} else {
			return nil, nil, fmt.Errorf("parse signing key: %w", err)
		}
	}
	key, ok := keyAny.(*rsa.PrivateKey)
	if !ok {
		return nil, nil, errors.New("SAML signing key must be RSA")
	}
	if !cert.PublicKey.(*rsa.PublicKey).Equal(&key.PublicKey) {
		return nil, nil, errors.New("SAML signing certificate does not match signing key")
	}
	return key, cert, nil
}

func loadOrGenerateCertificate(certFile, keyFile string, names []string) (tls.Certificate, error) {
	if (certFile == "") != (keyFile == "") {
		return tls.Certificate{}, errors.New("-tls-cert and -tls-key must be provided together")
	}
	if certFile != "" {
		return tls.LoadX509KeyPair(certFile, keyFile)
	}
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	serialLimit := new(big.Int).Lsh(big.NewInt(1), certificateSerialBits)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return tls.Certificate{}, err
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: names[0]},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
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
		return tls.Certificate{}, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	)
}
