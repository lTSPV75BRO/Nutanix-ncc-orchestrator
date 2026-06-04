package main

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeK8sAPI is an in-memory stand-in for the Kubernetes Secrets REST API,
// supporting the GET / POST(create) / PATCH(merge) calls the client uses.
type fakeK8sAPI struct {
	mu      sync.Mutex
	secrets map[string]map[string]string // name -> data(base64)
	rv      int
}

func newFakeK8sAPI() *fakeK8sAPI { return &fakeK8sAPI{secrets: map[string]map[string]string{}} }

func (f *fakeK8sAPI) handler(t *testing.T, namespace string) http.Handler {
	prefix := "/api/v1/namespaces/" + namespace + "/secrets"
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			t.Errorf("missing bearer token on %s %s", r.Method, r.URL.Path)
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		f.rv++
		writeObj := func(name string) {
			obj := map[string]any{
				"apiVersion": "v1",
				"kind":       "Secret",
				"metadata":   map[string]any{"name": name, "resourceVersion": "1"},
				"type":       "Opaque",
				"data":       f.secrets[name],
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(obj)
		}
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, prefix+"/"):
			name := strings.TrimPrefix(r.URL.Path, prefix+"/")
			if _, ok := f.secrets[name]; !ok {
				http.Error(w, `{"kind":"Status","code":404}`, http.StatusNotFound)
				return
			}
			writeObj(name)
		case r.Method == http.MethodPost && r.URL.Path == prefix:
			var obj struct {
				Metadata struct{ Name string } `json:"metadata"`
				Data     map[string]string     `json:"data"`
			}
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &obj)
			if _, exists := f.secrets[obj.Metadata.Name]; exists {
				http.Error(w, `{"code":409}`, http.StatusConflict)
				return
			}
			if obj.Data == nil {
				obj.Data = map[string]string{}
			}
			f.secrets[obj.Metadata.Name] = obj.Data
			w.WriteHeader(http.StatusCreated)
			writeObj(obj.Metadata.Name)
		case r.Method == http.MethodPatch && strings.HasPrefix(r.URL.Path, prefix+"/"):
			name := strings.TrimPrefix(r.URL.Path, prefix+"/")
			cur, ok := f.secrets[name]
			if !ok {
				http.Error(w, `{"code":404}`, http.StatusNotFound)
				return
			}
			var patch struct {
				Data map[string]*string `json:"data"`
			}
			body, _ := io.ReadAll(r.Body)
			if err := json.Unmarshal(body, &patch); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			for k, v := range patch.Data {
				if v == nil {
					delete(cur, k) // merge-patch null deletes the key
				} else {
					cur[k] = *v
				}
			}
			writeObj(name)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected", http.StatusInternalServerError)
		}
	})
}

func newTestSecretBackend(t *testing.T) (*k8sSecretBackend, *fakeK8sAPI) {
	t.Helper()
	api := newFakeK8sAPI()
	ts := httptest.NewServer(api.handler(t, "ncc"))
	t.Cleanup(ts.Close)
	cli := &k8sSecretClient{
		base:       ts.URL,
		namespace:  "ncc",
		token:      "test-token",
		httpClient: ts.Client(),
	}
	return &k8sSecretBackend{client: cli, name: "ncc-v2-users", key: "users.json"}, api
}

func TestK8sSecretBackendRoundTrip(t *testing.T) {
	be, api := newTestSecretBackend(t)

	// Empty store: load returns nothing, secret does not exist yet.
	if raw, err := be.load(); err != nil || raw != nil {
		t.Fatalf("initial load: raw=%v err=%v", raw, err)
	}

	// Open a db on the secret backend and bootstrap an admin -> persists.
	db, err := openUserDBFromBackend(be)
	if err != nil {
		t.Fatal(err)
	}
	if !db.writable() {
		t.Fatal("secret-backed db should be writable")
	}
	pw, created, err := db.bootstrapAdminIfEmpty("admin")
	if err != nil || !created {
		t.Fatalf("bootstrap: created=%v err=%v", created, err)
	}

	// The Secret now carries the encoded user-db JSON.
	api.mu.Lock()
	enc, ok := api.secrets["ncc-v2-users"]["users.json"]
	api.mu.Unlock()
	if !ok {
		t.Fatal("users.json not written to secret")
	}
	if raw, _ := base64.StdEncoding.DecodeString(enc); !strings.Contains(string(raw), `"admin"`) {
		t.Fatalf("secret payload missing admin: %s", raw)
	}

	// Reopen from the same backend: the admin round-trips.
	db2, err := openUserDBFromBackend(be)
	if err != nil {
		t.Fatal(err)
	}
	if role, okv, mustChange := db2.verify("admin", pw); !okv || role != RoleAdmin || !mustChange {
		t.Fatalf("reloaded verify: ok=%v role=%v mustChange=%v", okv, role, mustChange)
	}

	// Initial password is stored under its own key and removed on clear.
	hint := db.setInitialPassword("admin", pw)
	if !strings.Contains(hint, initialPasswordSecretKey) {
		t.Fatalf("unexpected hint: %q", hint)
	}
	api.mu.Lock()
	storedPW := api.secrets["ncc-v2-users"][initialPasswordSecretKey]
	api.mu.Unlock()
	if dec, _ := base64.StdEncoding.DecodeString(storedPW); string(dec) != pw {
		t.Fatalf("initial password not stored correctly: %q", dec)
	}
	db.clearInitialPassword()
	api.mu.Lock()
	_, stillThere := api.secrets["ncc-v2-users"][initialPasswordSecretKey]
	api.mu.Unlock()
	if stillThere {
		t.Fatal("initial password key should be deleted after clear")
	}

	// A runtime user-management write also persists through the backend.
	hash, _ := hashPassword("operator-strong-pass")
	if err := db.upsertUser("bob", hash, RoleOperator, true); err != nil {
		t.Fatal(err)
	}
	db3, _ := openUserDBFromBackend(be)
	if _, ok := db3.lookup("bob"); !ok {
		t.Fatal("bob did not persist through the secret backend")
	}
}
