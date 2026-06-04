package httpclient

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"testing"
	"time"

	"goncc/internal/model"
)

func TestNormalizePin(t *testing.T) {
	cases := map[string]string{
		"AA:BB:cc":  "aabbcc",
		"  Ab Cd ":  "abcd",
		"DEAD:beef": "deadbeef",
	}
	for in, want := range cases {
		if got := normalizePin(in); got != want {
			t.Fatalf("normalizePin(%q)=%q want %q", in, got, want)
		}
	}
}

func TestPinVerifier(t *testing.T) {
	raw := []byte("some-cert-bytes")
	sum := sha256.Sum256(raw)
	good := hex.EncodeToString(sum[:])

	verify := pinVerifier([]string{"AA:BB", good})
	if err := verify([][]byte{raw}, nil); err != nil {
		t.Fatalf("expected matching pin to verify, got %v", err)
	}
	verifyBad := pinVerifier([]string{"deadbeef"})
	if err := verifyBad([][]byte{raw}, nil); err == nil {
		t.Fatalf("expected non-matching pin to be rejected")
	}
}

// FuzzRedactJSONPasswordValue ensures the redactor never panics on arbitrary
// bytes and that, when it produces output for a well-formed password field,
// the literal secret value no longer appears verbatim.
func FuzzRedactJSONPasswordValue(f *testing.F) {
	f.Add([]byte(`{"password":"hunter2"}`))
	f.Add([]byte(`{"Password": "p@ss \"quoted\""}`))
	f.Add([]byte(`{"nested":{"password":"x"}}`))
	f.Add([]byte(`not json at all`))
	f.Add([]byte(``))
	f.Fuzz(func(t *testing.T, in []byte) {
		out := redactJSONPasswordValue(in)
		if out == nil {
			t.Fatalf("redactJSONPasswordValue returned nil")
		}
		// A simple, unambiguous secret must be scrubbed from the output.
		if bytes.Equal(in, []byte(`{"password":"hunter2"}`)) {
			if strings.Contains(string(out), "hunter2") {
				t.Fatalf("secret leaked through redaction: %s", out)
			}
		}
	})
}

func TestRedactHTTPDump(t *testing.T) {
	dump := []byte("POST /api HTTP/1.1\r\n" +
		"Authorization: Bearer secret-token\r\n" +
		"Cookie: session=abc123\r\n" +
		"Content-Type: application/json\r\n\r\n" +
		`{"username":"admin","password":"hunter2"}`)

	out := redactHTTPDump(dump, 0)
	s := string(out)

	if bytes.Contains(out, []byte("secret-token")) {
		t.Errorf("Authorization value leaked: %s", s)
	}
	if bytes.Contains(out, []byte("session=abc123")) {
		t.Errorf("Cookie value leaked: %s", s)
	}
	if bytes.Contains(out, []byte("hunter2")) {
		t.Errorf("password value leaked: %s", s)
	}
	if !bytes.Contains(out, []byte("Authorization: [REDACTED]")) {
		t.Errorf("missing redacted Authorization marker: %s", s)
	}
	if !bytes.Contains(out, []byte(`"[REDACTED]"`)) {
		t.Errorf("missing redacted password marker: %s", s)
	}
	// Non-sensitive content is preserved.
	if !bytes.Contains(out, []byte("Content-Type: application/json")) {
		t.Errorf("non-sensitive header should be preserved: %s", s)
	}
	if !bytes.Contains(out, []byte(`"username":"admin"`)) {
		t.Errorf("non-sensitive body field should be preserved: %s", s)
	}
}

func TestRedactHTTPDump_MaxBodyTruncates(t *testing.T) {
	dump := bytes.Repeat([]byte("x"), 100)
	out := redactHTTPDump(dump, 10)
	if !bytes.HasSuffix(out, []byte("...[truncated]")) {
		t.Errorf("expected truncation marker, got %q", out)
	}
}

func TestRedactJSONPasswordValue_NumberAndUppercase(t *testing.T) {
	in := []byte(`{"Password": 12345, "other":"keep"}`)
	out := redactJSONPasswordValue(in)
	if bytes.Contains(out, []byte("12345")) {
		t.Errorf("numeric password value leaked: %s", out)
	}
	if !bytes.Contains(out, []byte(`"other":"keep"`)) {
		t.Errorf("non-password field should be preserved: %s", out)
	}
}

func TestNew_AppliesDefaultsAndTimeout(t *testing.T) {
	cfg := model.Config{RequestTimeout: 7 * time.Second}
	c := New(cfg)
	if c.Timeout != 7*time.Second {
		t.Errorf("client timeout: got %v, want 7s", c.Timeout)
	}
	tr, ok := c.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport when logging disabled, got %T", c.Transport)
	}
	if tr.MaxIdleConns != DefaultMaxIdleConns {
		t.Errorf("MaxIdleConns: got %d, want default %d", tr.MaxIdleConns, DefaultMaxIdleConns)
	}
	if tr.IdleConnTimeout != DefaultIdleConnTimeout {
		t.Errorf("IdleConnTimeout: got %v, want default %v", tr.IdleConnTimeout, DefaultIdleConnTimeout)
	}
}

func TestNew_LoggingTransportWhenEnabled(t *testing.T) {
	c := New(model.Config{LogHTTP: true})
	if _, ok := c.Transport.(*LoggingTransport); !ok {
		t.Errorf("expected *LoggingTransport when LogHTTP=true, got %T", c.Transport)
	}
}
