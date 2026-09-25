package selfsigned

import (
	"os"
	"path/filepath"
)

const (
	// BYOCertFile is the admin-uploaded certificate (Settings → Access).
	BYOCertFile = "ui.crt"
	// BYOKeyFile is the matching private key for BYOCertFile.
	BYOKeyFile = "ui.key"
	// SelfCertFile is the stack-managed self-signed certificate.
	SelfCertFile = "ui-selfsigned.crt"
	// SelfKeyFile is the matching private key for SelfCertFile.
	SelfKeyFile = "ui-selfsigned.key"
	lockName    = ".ncc-tls.lock"
)

// ActivePaths returns the certificate/key pair the UI should serve from dir:
// an admin-uploaded pair wins over the stack-managed self-signed pair.
func ActivePaths(dir string) (certPath, keyPath string, ok bool) {
	dir = filepath.Clean(dir)
	byoCert := filepath.Join(dir, BYOCertFile)
	byoKey := filepath.Join(dir, BYOKeyFile)
	if fileExists(byoCert) && fileExists(byoKey) {
		return byoCert, byoKey, true
	}
	selfCert := filepath.Join(dir, SelfCertFile)
	selfKey := filepath.Join(dir, SelfKeyFile)
	if fileExists(selfCert) && fileExists(selfKey) {
		return selfCert, selfKey, true
	}
	return "", "", false
}

// Ensure returns a usable UI cert/key under dir, generating a self-signed
// pair when neither a BYO nor a self-signed pair exists. Concurrent callers
// (two UI replicas on a shared volume) serialize on an advisory lock.
func Ensure(dir string, hosts []string) (certPath, keyPath string, err error) {
	dir = filepath.Clean(dir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", "", err
	}
	err = withDirLock(dir, func() error {
		if cert, key, ok := ActivePaths(dir); ok {
			certPath, keyPath = cert, key
			return nil
		}
		certPEM, keyPEM, gerr := Generate(hosts, 0)
		if gerr != nil {
			return gerr
		}
		certPath = filepath.Join(dir, SelfCertFile)
		keyPath = filepath.Join(dir, SelfKeyFile)
		if wErr := os.WriteFile(certPath, certPEM, 0o600); wErr != nil {
			return wErr
		}
		if wErr := os.WriteFile(keyPath, keyPEM, 0o600); wErr != nil {
			return wErr
		}
		return nil
	})
	if err != nil {
		return "", "", err
	}
	return certPath, keyPath, nil
}

func fileExists(p string) bool {
	st, err := os.Stat(p)
	return err == nil && !st.IsDir()
}
