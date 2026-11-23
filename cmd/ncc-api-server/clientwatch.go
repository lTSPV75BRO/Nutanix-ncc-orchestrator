package main

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Third-party integrations and custom dashboards are not version-gated, so
// they are watched instead: repeated refusals from one address, or one token
// used from several addresses, raise an `access.suspicious` audit record and a
// warning in the server log. Our own dashboard is covered by sign-in lockout
// and is not counted here. Detection only: nothing is blocked.
const (
	watchWindow       = 5 * time.Minute
	watchDenialLimit  = 10
	watchTokenWindow  = 10 * time.Minute
	watchTokenSources = 4
	watchCooldown     = 10 * time.Minute
	watchMaxKeys      = 10000
)

type clientWatch struct {
	mu      sync.Mutex
	denials map[string][]time.Time
	tokens  map[string]map[string]time.Time
	flagged map[string]time.Time
}

// observe records one finished request and returns the reasons it became
// suspicious (empty most of the time). now is a parameter for tests.
func (w *clientWatch) observe(kind, ip, tokenID string, status int, now time.Time) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.denials == nil || len(w.denials) > watchMaxKeys || len(w.tokens) > watchMaxKeys || len(w.flagged) > watchMaxKeys {
		w.denials = map[string][]time.Time{}
		w.tokens = map[string]map[string]time.Time{}
		w.flagged = map[string]time.Time{}
	}
	var reasons []string
	if status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusTooManyRequests {
		key := kind + "|" + ip
		kept := w.denials[key][:0]
		for _, t := range w.denials[key] {
			if now.Sub(t) < watchWindow {
				kept = append(kept, t)
			}
		}
		kept = append(kept, now)
		w.denials[key] = kept
		if len(kept) >= watchDenialLimit && w.cool("denials|"+key, now) {
			reasons = append(reasons, "repeated_denials")
		}
	}
	if tokenID != "" && ip != "" {
		m := w.tokens[tokenID]
		if m == nil {
			m = map[string]time.Time{}
			w.tokens[tokenID] = m
		}
		m[ip] = now
		for a, t := range m {
			if now.Sub(t) >= watchTokenWindow {
				delete(m, a)
			}
		}
		if len(m) >= watchTokenSources && w.cool("token|"+tokenID, now) {
			reasons = append(reasons, "token_many_sources")
		}
	}
	sort.Strings(reasons)
	return reasons
}

// cool reports true (and starts the cooldown) when key has not been flagged
// recently, so one burst raises one record instead of one per request.
func (w *clientWatch) cool(key string, now time.Time) bool {
	if t, ok := w.flagged[key]; ok && now.Sub(t) < watchCooldown {
		return false
	}
	w.flagged[key] = now
	return true
}

func tokenFingerprint(token string) string {
	if token == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:6])
}

func requestClientKind(r *http.Request) string {
	switch {
	case isMCPHTTPRequest(r):
		return "mcp"
	case isNCCUIRequest(r):
		return "ui"
	case customClientName(r) != "":
		return "custom"
	default:
		return "api"
	}
}

// watchClient feeds a finished request to the watcher and records the result.
func (s *apiServer) watchClient(r *http.Request, status int) {
	kind := requestClientKind(r)
	if kind == "ui" || skipUIVersionGate(r) {
		return
	}
	reasons := s.watch.observe(kind, cleanClientIP(r), tokenFingerprint(extractAPIToken(r)), status, time.Now())
	if len(reasons) == 0 {
		return
	}
	fields := map[string]interface{}{
		"reasons":     reasons,
		"http_status": status,
		"route":       routeLabel(r.URL.Path),
	}
	if n := customClientName(r); n != "" {
		fields["client_name"] = n
	}
	s.audit(r, "access.suspicious", false, fields)
	serverLog.Warn("suspicious client access", "client_kind", kind, "client_ip", cleanClientIP(r), "reasons", reasons, "status", status)
}
