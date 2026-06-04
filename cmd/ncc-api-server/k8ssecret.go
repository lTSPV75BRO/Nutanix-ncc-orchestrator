package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// saTokenDir is where Kubernetes projects the pod's service-account credentials.
const saTokenDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// k8sSecretClient is a minimal Kubernetes API client scoped to reading and
// writing a single Secret in one namespace. It deliberately avoids client-go
// (which would balloon the static binary and image) and instead speaks the
// REST API directly using the pod's projected service-account token + CA.
type k8sSecretClient struct {
	base       string // https://host:port
	namespace  string
	token      string
	httpClient *http.Client
}

// newInClusterSecretClient builds a client from the in-cluster service-account
// credentials. nsOverride, when set, wins over the projected namespace file.
func newInClusterSecretClient(nsOverride string) (*k8sSecretClient, error) {
	host := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST"))
	port := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT"))
	if host == "" || port == "" {
		return nil, fmt.Errorf("not running inside a Kubernetes cluster (KUBERNETES_SERVICE_HOST/PORT unset); the Kubernetes Secret store requires in-cluster execution")
	}
	token, err := os.ReadFile(filepath.Join(saTokenDir, "token"))
	if err != nil {
		return nil, fmt.Errorf("read service-account token: %w", err)
	}
	caPEM, err := os.ReadFile(filepath.Join(saTokenDir, "ca.crt"))
	if err != nil {
		return nil, fmt.Errorf("read service-account CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("parse service-account CA bundle")
	}
	ns := strings.TrimSpace(nsOverride)
	if ns == "" {
		b, err := os.ReadFile(filepath.Join(saTokenDir, "namespace"))
		if err != nil {
			return nil, fmt.Errorf("determine namespace: %w (set --users-db-secret-namespace)", err)
		}
		ns = strings.TrimSpace(string(b))
	}
	return &k8sSecretClient{
		base:      "https://" + net.JoinHostPort(host, port),
		namespace: ns,
		token:     strings.TrimSpace(string(token)),
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
			},
		},
	}, nil
}

func (c *k8sSecretClient) secretPath(name string) string {
	return fmt.Sprintf("/api/v1/namespaces/%s/secrets/%s", c.namespace, name)
}

func (c *k8sSecretClient) do(method, path, contentType string, body []byte) (int, []byte, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, c.base+path, rdr)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, out, nil
}

// k8sSecret is the subset of the Secret object we care about.
type k8sSecret struct {
	Metadata struct {
		Name            string `json:"name"`
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Data map[string]string `json:"data"` // values are base64-encoded
	Type string            `json:"type"`
}

func apiError(code int, body []byte) error {
	msg := strings.TrimSpace(string(body))
	if len(msg) > 300 {
		msg = msg[:300]
	}
	return fmt.Errorf("kubernetes API %d: %s", code, msg)
}

// getSecret returns the Secret, or found=false when it does not exist.
func (c *k8sSecretClient) getSecret(name string) (*k8sSecret, bool, error) {
	code, body, err := c.do(http.MethodGet, c.secretPath(name), "", nil)
	if err != nil {
		return nil, false, err
	}
	switch code {
	case http.StatusOK:
		var s k8sSecret
		if err := json.Unmarshal(body, &s); err != nil {
			return nil, false, fmt.Errorf("decode secret: %w", err)
		}
		return &s, true, nil
	case http.StatusNotFound:
		return nil, false, nil
	default:
		return nil, false, apiError(code, body)
	}
}

// ensureSecret creates an empty Opaque Secret if it does not already exist.
func (c *k8sSecretClient) ensureSecret(name string) error {
	_, found, err := c.getSecret(name)
	if err != nil {
		return err
	}
	if found {
		return nil
	}
	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata":   map[string]any{"name": name},
		"type":       "Opaque",
	}
	b, _ := json.Marshal(obj)
	code, body, err := c.do(http.MethodPost, fmt.Sprintf("/api/v1/namespaces/%s/secrets", c.namespace), "application/json", b)
	if err != nil {
		return err
	}
	if code == http.StatusConflict { // created concurrently — fine
		return nil
	}
	if code/100 != 2 {
		return apiError(code, body)
	}
	return nil
}

// patchData applies a strategic merge-patch to the Secret's data map. A nil
// value deletes that key; a non-nil value sets it (already base64-encoded).
func (c *k8sSecretClient) patchData(name string, data map[string]*string) error {
	if err := c.ensureSecret(name); err != nil {
		return err
	}
	patch := map[string]any{"data": data}
	b, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	code, body, err := c.do(http.MethodPatch, c.secretPath(name), "application/merge-patch+json", b)
	if err != nil {
		return err
	}
	if code/100 != 2 {
		return apiError(code, body)
	}
	return nil
}

// k8sSecretBackend persists the user database as a key inside a Kubernetes
// Secret. Confidentiality at rest is provided by the cluster's etcd
// encryption-at-rest (KMS / aescbc / secretbox) — see docs/SECURITY_AND_TRUST.md.
type k8sSecretBackend struct {
	client *k8sSecretClient
	name   string // Secret name
	key    string // data key holding the user-db JSON
}

const initialPasswordSecretKey = "initial-admin-password"

func (b *k8sSecretBackend) load() ([]byte, error) {
	sec, found, err := b.client.getSecret(b.name)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	enc, ok := sec.Data[b.key]
	if !ok || strings.TrimSpace(enc) == "" {
		return nil, nil
	}
	raw, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return nil, fmt.Errorf("decode secret key %q: %w", b.key, err)
	}
	return raw, nil
}

func (b *k8sSecretBackend) save(data []byte) error {
	enc := base64.StdEncoding.EncodeToString(data)
	return b.client.patchData(b.name, map[string]*string{b.key: &enc})
}

func (b *k8sSecretBackend) setInitialPassword(username, password string) (string, error) {
	enc := base64.StdEncoding.EncodeToString([]byte(password))
	if err := b.client.patchData(b.name, map[string]*string{initialPasswordSecretKey: &enc}); err != nil {
		return "", err
	}
	return fmt.Sprintf("kubectl -n %s get secret %s -o jsonpath='{.data.%s}' | base64 -d",
		b.client.namespace, b.name, initialPasswordSecretKey), nil
}

func (b *k8sSecretBackend) clearInitialPassword() {
	_ = b.client.patchData(b.name, map[string]*string{initialPasswordSecretKey: nil})
}

func (b *k8sSecretBackend) location() string {
	return fmt.Sprintf("k8s secret %s/%s", b.client.namespace, b.name)
}
