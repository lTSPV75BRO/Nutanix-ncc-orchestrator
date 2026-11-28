package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"runtime"
	"strings"
	"testing"
)

func TestReleaseVersion(t *testing.T) {
	sha := strings.Repeat("0123456789", 4)
	for in, want := range map[string]string{
		"2.4.0":                   "2.4.0",
		"2.4.0-" + sha:            "2.4.0",
		"2.4.0-dirty":             "2.4.0",
		"2.4.0-dirty-" + sha:      "2.4.0",
		"2.4.0-rc.1":              "2.4.0-rc.1",
		"2.4.0-rc.1-" + sha:       "2.4.0-rc.1",
		"2.4.0-rc.1-dirty-" + sha: "2.4.0-rc.1",
		"2.4.0+build":             "2.4.0",
	} {
		if got := releaseVersion(in); got != want {
			t.Errorf("releaseVersion(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestVerifyOnline(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	withEmbeddedKey(t, base64.StdEncoding.EncodeToString(pub))
	asset := fmt.Sprintf("ncc-orchestrator-%s-%s", runtime.GOOS, runtime.GOARCH)
	if runtime.GOOS == "windows" {
		asset += ".exe"
	}
	hash := strings.Repeat("ab", 32)
	sums := []byte(hash + "  " + asset + "\n" + strings.Repeat("cd", 32) + "  " + asset + ".asc\n")
	sign := func(b []byte) []byte {
		return []byte(base64.StdEncoding.EncodeToString(ed25519.Sign(priv, b)))
	}
	// The detached signature has the program's name in its own and must not be
	// taken for it, in whatever order the files are listed.
	files := func(sig []byte) map[string][]byte {
		m := map[string][]byte{asset: []byte("x"), asset + ".asc": []byte("x"), "checksums.txt": sums}
		if sig != nil {
			m["checksums.txt.sig"] = sig
		}
		return m
	}
	for _, c := range []struct {
		name    string
		hash    string
		sig     []byte
		dirty   bool
		require bool
		want    string
		fails   bool
	}{
		{"valid", hash, sign(sums), false, true, "MATCH", false},
		{"changed program", strings.Repeat("ef", 32), sign(sums), false, true, "MISMATCH", true},
		{"changed checksums", hash, sign([]byte("other")), false, true, "FAILED", true},
		{"no signature", hash, nil, false, true, "FAILED", true},
		{"no signature, not required", hash, nil, false, false, "MATCH", false},
		{"modified build", hash, sign(sums), true, true, "SKIPPED", true},
		{"modified build, not required", hash, sign(sums), true, false, "SKIPPED", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			releaseServer(t, "2.4.0-rc.1", files(c.sig), nil)
			var out bytes.Buffer
			err := runVerifyOnline(&out, "ncc-orchestrator", c.hash, nil, c.dirty, "2.4.0-rc.1", "", c.require)
			if (err != nil) != c.fails {
				t.Errorf("error = %v", err)
			}
			if !strings.Contains(out.String(), "verify_result:     "+c.want) {
				t.Errorf("want %s:\n%s", c.want, out.String())
			}
		})
	}
}
