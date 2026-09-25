package selfsigned

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestActivePathsPrefersBYO(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, SelfCertFile), []byte("self-cert"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, SelfKeyFile), []byte("self-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	cert, key, ok := ActivePaths(dir)
	if !ok || filepath.Base(cert) != SelfCertFile {
		t.Fatalf("want self-signed pair, got %q %q ok=%v", cert, key, ok)
	}
	if err := os.WriteFile(filepath.Join(dir, BYOCertFile), []byte("byo-cert"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, BYOKeyFile), []byte("byo-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	cert, key, ok = ActivePaths(dir)
	if !ok || filepath.Base(cert) != BYOCertFile || filepath.Base(key) != BYOKeyFile {
		t.Fatalf("want BYO pair, got %q %q ok=%v", cert, key, ok)
	}
}

func TestEnsureGeneratesOnce(t *testing.T) {
	dir := t.TempDir()
	aCert, aKey, err := Ensure(dir, []string{"ncc.example.test"})
	if err != nil {
		t.Fatal(err)
	}
	bCert, bKey, err := Ensure(dir, []string{"other.example.test"})
	if err != nil {
		t.Fatal(err)
	}
	if aCert != bCert || aKey != bKey {
		t.Fatalf("Ensure reused different paths: %s/%s vs %s/%s", aCert, aKey, bCert, bKey)
	}
	first, err := os.ReadFile(aCert)
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(bCert)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("second Ensure regenerated the certificate")
	}
}

func TestEnsureSerializesReplicas(t *testing.T) {
	dir := t.TempDir()
	var wg sync.WaitGroup
	paths := make([]string, 8)
	errs := make([]error, 8)
	wg.Add(len(paths))
	for i := range paths {
		go func(i int) {
			defer wg.Done()
			cert, _, err := Ensure(dir, []string{"replica.test"})
			paths[i] = cert
			errs[i] = err
		}(i)
	}
	wg.Wait()
	var first []byte
	for i, err := range errs {
		if err != nil {
			t.Fatalf("replica %d: %v", i, err)
		}
		b, rerr := os.ReadFile(paths[i])
		if rerr != nil {
			t.Fatal(rerr)
		}
		if first == nil {
			first = b
			continue
		}
		if !bytes.Equal(first, b) {
			t.Fatal("replicas generated different certificates")
		}
	}
}
