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
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"flag"
	"html/template"
	"log"
	"net/http"
	"net/url"
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
	issuer := flag.String("issuer", "", "issuer URL (default http://<listen>)")
	flag.Parse()
	iss := *issuer
	if iss == "" {
		iss = "http://" + *listen
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
		redirect, err := url.Parse(q.Get("redirect_uri"))
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
	log.Fatal(srv.ListenAndServe())
}
