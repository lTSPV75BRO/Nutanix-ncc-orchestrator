package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testSecret() []byte {
	return []byte("0123456789abcdef0123456789abcdef")
}

type memStore struct {
	byHash map[string]PATOwner
}

func (m *memStore) LookupPAT(hash string) (PATOwner, bool) {
	if m == nil || m.byHash == nil {
		return PATOwner{}, false
	}
	o, ok := m.byHash[hash]
	return o, ok
}

func okHandler(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-User-ID", UserIDFromContext(r.Context()))
		w.Header().Set("X-Role", RoleFromContext(r.Context()))
		w.Header().Set("X-Auth-Type", AuthTypeFromContext(r.Context()))
		w.WriteHeader(http.StatusOK)
	})
}

func doAuth(t *testing.T, mw func(http.Handler) http.Handler, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	mw(okHandler(t)).ServeHTTP(rr, req)
	return rr
}

func TestJWTValidationViaHeader(t *testing.T) {
	tok, err := GenerateSessionJWT("alice", "operator", time.Hour, testSecret())
	if err != nil {
		t.Fatalf("GenerateSessionJWT: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rr := doAuth(t, AuthMiddleware(testSecret(), nil, ""), req)
	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("X-User-ID") != "alice" || rr.Header().Get("X-Role") != "operator" || rr.Header().Get("X-Auth-Type") != AuthTypeJWT {
		t.Fatalf("context = user=%q role=%q type=%q", rr.Header().Get("X-User-ID"), rr.Header().Get("X-Role"), rr.Header().Get("X-Auth-Type"))
	}
}

func TestJWTValidationViaCookie(t *testing.T) {
	tok, err := GenerateSessionJWT("bob", "admin", time.Hour, testSecret())
	if err != nil {
		t.Fatalf("GenerateSessionJWT: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req.AddCookie(&http.Cookie{Name: CookieName, Value: tok})
	rr := doAuth(t, AuthMiddleware(testSecret(), nil, ""), req)
	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("X-User-ID") != "bob" || rr.Header().Get("X-Auth-Type") != AuthTypeJWT {
		t.Fatalf("cookie JWT context user=%q type=%q", rr.Header().Get("X-User-ID"), rr.Header().Get("X-Auth-Type"))
	}
}

func TestJWTExpiredRejected(t *testing.T) {
	tok, err := GenerateSessionJWT("alice", "viewer", time.Millisecond, testSecret())
	if err != nil {
		t.Fatalf("GenerateSessionJWT: %v", err)
	}
	time.Sleep(15 * time.Millisecond)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	before := Snapshot()
	rr := doAuth(t, AuthMiddleware(testSecret(), nil, ""), req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", rr.Code)
	}
	assertUnauthorizedJSON(t, rr)
	after := Snapshot()
	if after.FailureExpired <= before.FailureExpired {
		t.Fatalf("expected expired failure counter to increase: before=%d after=%d", before.FailureExpired, after.FailureExpired)
	}
}

func TestJWTTamperedRejected(t *testing.T) {
	tok, err := GenerateSessionJWT("alice", "admin", time.Hour, testSecret())
	if err != nil {
		t.Fatalf("GenerateSessionJWT: %v", err)
	}
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3-part JWT, got %d", len(parts))
	}
	sig := []byte(parts[2])
	sig[0] ^= 0x7f
	tampered := parts[0] + "." + parts[1] + "." + string(sig)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req.Header.Set("Authorization", "Bearer "+tampered)
	rr := doAuth(t, AuthMiddleware(testSecret(), nil, ""), req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("want 401 for tampered JWT, got %d", rr.Code)
	}
	assertUnauthorizedJSON(t, rr)
}

func TestJWTWrongSecretRejected(t *testing.T) {
	tok, err := GenerateSessionJWT("alice", "admin", time.Hour, testSecret())
	if err != nil {
		t.Fatalf("GenerateSessionJWT: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rr := doAuth(t, AuthMiddleware([]byte("different-secret-value-0123456789"), nil, ""), req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("want 401 for wrong secret, got %d", rr.Code)
	}
}

func TestPATAuthentication(t *testing.T) {
	plain, hash, err := GeneratePAT()
	if err != nil {
		t.Fatalf("GeneratePAT: %v", err)
	}
	if !strings.HasPrefix(plain, PATPrefix) {
		t.Fatalf("PAT missing prefix: %s", plain)
	}
	store := &memStore{byHash: map[string]PATOwner{hash: {UserID: "carol", Role: "viewer"}}}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req.Header.Set("Authorization", "Bearer "+plain)
	rr := doAuth(t, AuthMiddleware(testSecret(), store, ""), req)
	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("X-User-ID") != "carol" || rr.Header().Get("X-Role") != "viewer" || rr.Header().Get("X-Auth-Type") != AuthTypePAT {
		t.Fatalf("PAT context user=%q role=%q type=%q", rr.Header().Get("X-User-ID"), rr.Header().Get("X-Role"), rr.Header().Get("X-Auth-Type"))
	}
}

func TestPATInvalidHashRejected(t *testing.T) {
	plain, _, err := GeneratePAT()
	if err != nil {
		t.Fatalf("GeneratePAT: %v", err)
	}
	store := &memStore{byHash: map[string]PATOwner{
		HashPAT("ncc_pat_" + strings.Repeat("00", 32)): {UserID: "carol", Role: "viewer"},
	}}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req.Header.Set("Authorization", "Bearer "+plain)
	rr := doAuth(t, AuthMiddleware(testSecret(), store, ""), req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("want 401 for unknown PAT, got %d", rr.Code)
	}
	assertUnauthorizedJSON(t, rr)
}

func TestPATXAPITokenHeader(t *testing.T) {
	plain, hash, err := GeneratePAT()
	if err != nil {
		t.Fatalf("GeneratePAT: %v", err)
	}
	store := &memStore{byHash: map[string]PATOwner{hash: {UserID: "dave", Role: "operator"}}}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req.Header.Set("X-API-Token", plain)
	rr := doAuth(t, AuthMiddleware(testSecret(), store, ""), req)
	if rr.Code != http.StatusOK {
		t.Fatalf("legacy X-API-Token PAT: want 200, got %d", rr.Code)
	}
	if rr.Header().Get("X-Auth-Type") != AuthTypePAT {
		t.Fatalf("want auth_type=pat, got %q", rr.Header().Get("X-Auth-Type"))
	}
}

func TestStaticTokenFallback(t *testing.T) {
	const static = "legacy-static-token-value"
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req.Header.Set("Authorization", "Bearer "+static)
	rr := doAuth(t, AuthMiddleware(testSecret(), nil, static), req)
	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
	if rr.Header().Get("X-User-ID") != "static-admin-token" || rr.Header().Get("X-Role") != "admin" || rr.Header().Get("X-Auth-Type") != AuthTypeStatic {
		t.Fatalf("static context user=%q role=%q type=%q", rr.Header().Get("X-User-ID"), rr.Header().Get("X-Role"), rr.Header().Get("X-Auth-Type"))
	}
}

func TestStaticTokenWrongValueRejected(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req.Header.Set("Authorization", "Bearer not-the-static-token")
	rr := doAuth(t, AuthMiddleware(testSecret(), nil, "legacy-static-token-value"), req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", rr.Code)
	}
}

func TestMissingCredentialsUnauthorized(t *testing.T) {
	before := Snapshot()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	rr := doAuth(t, AuthMiddleware(testSecret(), nil, "static"), req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", rr.Code)
	}
	assertUnauthorizedJSON(t, rr)
	after := Snapshot()
	if after.FailureMissing <= before.FailureMissing {
		t.Fatalf("expected missing failure counter to increase")
	}
}

func TestMalformedBearerUnauthorized(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req.Header.Set("Authorization", "Bearer")
	rr := doAuth(t, AuthMiddleware(testSecret(), nil, ""), req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", rr.Code)
	}
}

func TestHeaderTakesPrecedenceOverCookie(t *testing.T) {
	headerTok, err := GenerateSessionJWT("from-header", "admin", time.Hour, testSecret())
	if err != nil {
		t.Fatal(err)
	}
	cookieTok, err := GenerateSessionJWT("from-cookie", "viewer", time.Hour, testSecret())
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req.Header.Set("Authorization", "Bearer "+headerTok)
	req.AddCookie(&http.Cookie{Name: CookieName, Value: cookieTok})
	rr := doAuth(t, AuthMiddleware(testSecret(), nil, ""), req)
	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
	if rr.Header().Get("X-User-ID") != "from-header" {
		t.Fatalf("Bearer must win over cookie, got user=%q", rr.Header().Get("X-User-ID"))
	}
}

func TestValidatePATHashConstantTime(t *testing.T) {
	plain, hash, err := GeneratePAT()
	if err != nil {
		t.Fatal(err)
	}
	if !ValidatePATHash(plain, hash) {
		t.Fatal("expected matching PAT hash")
	}
	if ValidatePATHash(plain+"x", hash) {
		t.Fatal("expected mismatch to fail")
	}
	if ValidatePATHash("", hash) || ValidatePATHash(plain, "") {
		t.Fatal("empty inputs must not match")
	}
}

func TestLooksLikeJWT(t *testing.T) {
	tok, err := GenerateSessionJWT("u", "admin", time.Hour, testSecret())
	if err != nil {
		t.Fatal(err)
	}
	if !LooksLikeJWT(tok) {
		t.Fatal("expected JWT to look like JWT")
	}
	if LooksLikeJWT("payload.sig") || LooksLikeJWT("") || LooksLikeJWT(PATPrefix+"abcd") {
		t.Fatal("non-JWT values must not look like JWT")
	}
}

func TestAlgNoneRejected(t *testing.T) {
	// Unsigned "none" token with session claims must never authenticate.
	payload, _ := json.Marshal(map[string]interface{}{
		"sub": "attacker", "role": "admin", "token_type": "session",
		"iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix(),
	})
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	body := base64.RawURLEncoding.EncodeToString(payload)
	noneTok := header + "." + body + "."
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req.Header.Set("Authorization", "Bearer "+noneTok)
	rr := doAuth(t, AuthMiddleware(testSecret(), nil, ""), req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("alg=none must be rejected, got %d", rr.Code)
	}
}

func TestHMACSessionShapeNotAcceptedAsJWT(t *testing.T) {
	// Legacy api-server sessions are payload.hexsig (two segments). They must
	// not be treated as JWTs by this middleware (the api-server HMAC verifier
	// is the fallback for those).
	mac := hmac.New(sha256.New, testSecret())
	_, _ = mac.Write([]byte("payload"))
	legacy := "payload." + strings.Repeat("ab", 32)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req.Header.Set("Authorization", "Bearer "+legacy)
	rr := doAuth(t, AuthMiddleware(testSecret(), nil, ""), req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("legacy HMAC session is not a JWT: want 401, got %d", rr.Code)
	}
}

func TestGenerateSessionJWTRejectsBadInput(t *testing.T) {
	if _, err := GenerateSessionJWT("", "admin", time.Hour, testSecret()); err == nil {
		t.Fatal("empty user id must fail")
	}
	if _, err := GenerateSessionJWT("u", "admin", 0, testSecret()); err == nil {
		t.Fatal("zero duration must fail")
	}
	if _, err := GenerateSessionJWT("u", "admin", time.Hour, nil); err == nil {
		t.Fatal("empty secret must fail")
	}
}

func assertUnauthorizedJSON(t *testing.T, rr *httptest.ResponseRecorder) {
	t.Helper()
	var body map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("401 body is not JSON: %v (%s)", err, rr.Body.String())
	}
	if body["error"] != "Unauthorized" {
		t.Fatalf("want {\"error\":\"Unauthorized\"}, got %#v", body)
	}
}
