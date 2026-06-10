// Package selfsigned generates self-signed TLS certificate/key pairs for the
// NCC Orchestrator UI server. It is shared by the orchestrator (which can mint
// a default certificate at v2-start so HTTPS is on out of the box) and the
// api-server (which exposes a "generate / renew" action in Settings → Access).
//
// The certificates are intended for an internal, IP-addressed ops tool where a
// public CA cannot issue (private RFC1918 hosts). Browsers will show a one-time
// trust warning; that is expected for self-signed material.
package selfsigned

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"sort"
	"strings"
	"time"
)

// DefaultValidity is used when Generate is called with validity <= 0. ~2 years
// keeps re-issuance infrequent while staying within common client limits.
const DefaultValidity = 825 * 24 * time.Hour

// Generate creates a PEM-encoded self-signed certificate and matching private
// key valid for the supplied hosts. Each host may be a DNS name, an IP literal,
// or a host:port (the port is stripped). localhost, 127.0.0.1 and ::1 are
// always added so the stack can always reach itself. validity bounds NotAfter;
// pass <= 0 for DefaultValidity.
func Generate(hosts []string, validity time.Duration) (certPEM, keyPEM []byte, err error) {
	if validity <= 0 {
		validity = DefaultValidity
	}

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, err
	}

	dnsSet := map[string]struct{}{"localhost": {}}
	ipSet := map[string]net.IP{}
	for _, ipStr := range []string{"127.0.0.1", "::1"} {
		if ip := net.ParseIP(ipStr); ip != nil {
			ipSet[ip.String()] = ip
		}
	}
	primary := ""
	for _, h := range hosts {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		if host, _, splitErr := net.SplitHostPort(h); splitErr == nil && host != "" {
			h = host
		}
		if primary == "" {
			primary = h
		}
		if ip := net.ParseIP(h); ip != nil {
			ipSet[ip.String()] = ip
		} else {
			dnsSet[strings.ToLower(h)] = struct{}{}
		}
	}
	if primary == "" {
		primary = "localhost"
	}

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: primary, Organization: []string{"NCC Orchestrator (self-signed)"}},
		NotBefore:             time.Now().Add(-5 * time.Minute),
		NotAfter:              time.Now().Add(validity),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	dnsNames := make([]string, 0, len(dnsSet))
	for d := range dnsSet {
		dnsNames = append(dnsNames, d)
	}
	sort.Strings(dnsNames)
	tmpl.DNSNames = dnsNames
	ipKeys := make([]string, 0, len(ipSet))
	for k := range ipSet {
		ipKeys = append(ipKeys, k)
	}
	sort.Strings(ipKeys)
	for _, k := range ipKeys {
		tmpl.IPAddresses = append(tmpl.IPAddresses, ipSet[k])
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, err
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, nil, err
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM, nil
}
