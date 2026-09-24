package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

// pcDiscoveryTTL is how long a discovered Prism Central cluster list is trusted
// before it is refreshed in the background. Cluster-group access checks read the
// cache and never block on discovery; a stale entry is served while a refresh
// runs, so newly-registered clusters appear automatically within a TTL window.
const pcDiscoveryTTL = 10 * time.Minute

// pcDiscoveryTimeout bounds a single discover-clusters invocation.
const pcDiscoveryTimeout = 45 * time.Second

// pcCluster is one cluster registered under a Prism Central.
type pcCluster struct {
	Name    string `json:"name"`
	Address string `json:"address"`
	ExtID   string `json:"ext_id,omitempty"`
}

// pcCacheEntry is the cached discovery result for a single PC.
type pcCacheEntry struct {
	clusters  []pcCluster
	fetchedAt time.Time
	lastErr   string
}

// expandPrismCentral returns the clusters registered under pc from the cache,
// kicking off a background refresh when the entry is missing or stale. It never
// blocks the caller: a cold entry returns nil (no access yet) until the first
// discovery completes, and a stale entry returns the last known list.
func (s *apiServer) expandPrismCentral(pc string) []pcCluster {
	pc = strings.TrimSpace(pc)
	if pc == "" {
		return nil
	}
	key := normClusterName(pc)

	s.pcCacheMu.Lock()
	if s.pcCache == nil {
		s.pcCache = map[string]*pcCacheEntry{}
	}
	entry := s.pcCache[key]
	fresh := entry != nil && time.Since(entry.fetchedAt) < pcDiscoveryTTL
	var snapshot []pcCluster
	if entry != nil {
		snapshot = entry.clusters
	}
	if !fresh {
		s.refreshPrismCentralLocked(pc, key)
	}
	s.pcCacheMu.Unlock()
	return snapshot
}

// refreshPrismCentralLocked launches a single background discovery for pc when
// one is not already in flight. The caller must hold pcCacheMu.
func (s *apiServer) refreshPrismCentralLocked(pc, key string) {
	if s.pcInflight == nil {
		s.pcInflight = map[string]bool{}
	}
	if s.pcInflight[key] {
		return
	}
	s.pcInflight[key] = true
	go func() {
		clusters, err := s.discoverPrismCentralClusters(pc)
		s.pcCacheMu.Lock()
		defer s.pcCacheMu.Unlock()
		if s.pcCache == nil {
			s.pcCache = map[string]*pcCacheEntry{}
		}
		prev := s.pcCache[key]
		ce := &pcCacheEntry{fetchedAt: time.Now()}
		if err != nil {
			ce.lastErr = err.Error()
			// Preserve the last good list so a transient discovery failure does
			// not silently revoke access mid-session.
			if prev != nil {
				ce.clusters = prev.clusters
			}
		} else {
			ce.clusters = clusters
		}
		s.pcCache[key] = ce
		delete(s.pcInflight, key)
	}()
}

// discoverPrismCentralClusters invokes the orchestrator's discover-clusters
// against pc, reusing the active run config for credentials, and returns the
// registered clusters. It writes JSON to a temp file (rather than parsing
// CombinedOutput) so informational stderr lines never corrupt the payload.
func (s *apiServer) discoverPrismCentralClusters(pc string) ([]pcCluster, error) {
	tmp, err := os.CreateTemp("", "ncc-pc-discover-*.json")
	if err != nil {
		return nil, err
	}
	tmpPath := tmp.Name()
	_ = tmp.Close()
	defer os.Remove(tmpPath)

	args := []string{"discover-clusters", "--prism-central-url", pc, "--format", "json", "--output", tmpPath}
	if cfg := s.absPath(s.configPath); cfg != "" {
		if _, statErr := os.Stat(cfg); statErr == nil {
			args = append(args, "--config", cfg)
		}
	}
	out, runErr := s.runOrchestrator(args, pcDiscoveryTimeout)
	b, readErr := os.ReadFile(tmpPath)
	if readErr != nil || len(strings.TrimSpace(string(b))) == 0 {
		if runErr != nil {
			return nil, fmt.Errorf("discover-clusters failed: %s", strings.TrimSpace(tailString(out, 500)))
		}
		return nil, fmt.Errorf("discover-clusters produced no output")
	}
	var rows []struct {
		Name    string `json:"name"`
		Address string `json:"address"`
		ExtID   string `json:"ext_id"`
	}
	if err := json.Unmarshal(b, &rows); err != nil {
		return nil, fmt.Errorf("parse discover-clusters output: %w", err)
	}
	clusters := make([]pcCluster, 0, len(rows))
	for _, r := range rows {
		name := strings.TrimSpace(r.Name)
		addr := strings.TrimSpace(r.Address)
		extID := strings.TrimSpace(r.ExtID)
		if name == "" && addr == "" && extID == "" {
			continue
		}
		clusters = append(clusters, pcCluster{Name: name, Address: addr, ExtID: extID})
	}
	return clusters, nil
}

// handlePCDiscover previews/refreshes the clusters managed by a Prism Central so
// admins can confirm a PC resolves before assigning it to a group. Admin-only
// (registered under /api/v1/settings/). It performs a synchronous discovery
// (bounded by pcDiscoveryTimeout) and updates the cache.
func (s *apiServer) handlePCDiscover(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	pc := strings.TrimSpace(r.URL.Query().Get("pc"))
	if pc == "" {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "pc query parameter is required"})
		return
	}
	clusters, err := s.discoverPrismCentralClusters(pc)
	// Update the cache regardless so a successful preview primes access checks.
	key := normClusterName(pc)
	s.pcCacheMu.Lock()
	if s.pcCache == nil {
		s.pcCache = map[string]*pcCacheEntry{}
	}
	ce := &pcCacheEntry{fetchedAt: time.Now()}
	if err != nil {
		ce.lastErr = err.Error()
		if prev := s.pcCache[key]; prev != nil {
			ce.clusters = prev.clusters
		}
	} else {
		ce.clusters = clusters
	}
	s.pcCache[key] = ce
	s.pcCacheMu.Unlock()

	if err != nil {
		writeJSON(w, http.StatusBadGateway, envelope{Success: false, Error: "discovery failed: " + err.Error()})
		return
	}
	s.audit(r, "settings.pc_discover", true, map[string]interface{}{"pc": pc, "count": len(clusters)})
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"prism_central": pc,
		"count":         len(clusters),
		"clusters":      clusters,
	}})
}

// primePrismCentralCache asynchronously discovers every PC referenced by the
// given groups so the first access check after a save already has data. Called
// (best-effort) after the cluster groups are updated.
func (s *apiServer) primePrismCentralCache(groups []clusterGroup) {
	seen := map[string]bool{}
	for _, g := range groups {
		for _, pc := range g.PrismCentrals {
			pc = strings.TrimSpace(pc)
			if pc == "" {
				continue
			}
			key := normClusterName(pc)
			if seen[key] {
				continue
			}
			seen[key] = true
			s.pcCacheMu.Lock()
			s.refreshPrismCentralLocked(pc, key)
			s.pcCacheMu.Unlock()
		}
	}
}

func (c pcCluster) identityKeys() []string {
	out := make([]string, 0, 3)
	for _, k := range []string{c.ExtID, c.Name, c.Address} {
		k = strings.TrimSpace(k)
		if k != "" {
			out = append(out, k)
		}
	}
	return out
}

// kickPCDiscovery starts background discovery for every Prism Central we know
// about (alert targets + cluster-group assignments) without blocking.
func (s *apiServer) kickPCDiscovery() {
	if s == nil {
		return
	}
	seen := map[string]bool{}
	add := func(pc string) {
		pc = strings.TrimSpace(pc)
		if pc == "" {
			return
		}
		key := normClusterName(pc)
		if seen[key] {
			return
		}
		seen[key] = true
		s.expandPrismCentral(pc)
	}
	if cfg, err := s.loadRawConfigMap(); err == nil {
		for _, t := range pcAlertTargets(cfg) {
			add(t)
		}
	}
	if s.users != nil {
		for _, g := range s.users.getClusterGroups() {
			for _, pc := range g.PrismCentrals {
				add(pc)
			}
		}
	}
}

// pcIdentityIndex is a UUID/name/IP dictionary of clusters discovered from
// Prism Central. Keys are normalized (lowercased) so lookups match whatever
// identity an alert or access check uses.
func (s *apiServer) pcIdentityIndex() map[string]pcCluster {
	idx := map[string]pcCluster{}
	if s == nil {
		return idx
	}
	s.kickPCDiscovery()
	s.pcCacheMu.Lock()
	defer s.pcCacheMu.Unlock()
	for _, e := range s.pcCache {
		if e == nil {
			continue
		}
		for _, c := range e.clusters {
			for _, k := range c.identityKeys() {
				idx[normClusterName(k)] = c
			}
		}
	}
	return idx
}

// pcClusterMapView shapes the discovery dictionary for the alerts API: each
// UUID, name, and IP maps to name/address/ext_id so the UI can resolve PC
// alerts that only carry a cluster UUID.
func pcClusterMapView(idx map[string]pcCluster) map[string]map[string]string {
	out := map[string]map[string]string{}
	if len(idx) == 0 {
		return out
	}
	seen := map[string]bool{}
	for _, c := range idx {
		sig := strings.ToLower(strings.TrimSpace(c.ExtID) + "|" + strings.TrimSpace(c.Name) + "|" + strings.TrimSpace(c.Address))
		if seen[sig] {
			continue
		}
		seen[sig] = true
		entry := map[string]string{}
		if n := strings.TrimSpace(c.Name); n != "" {
			entry["name"] = n
		}
		if a := strings.TrimSpace(c.Address); a != "" {
			entry["address"] = a
		}
		if id := strings.TrimSpace(c.ExtID); id != "" {
			entry["ext_id"] = id
		}
		if len(entry) == 0 {
			continue
		}
		for _, k := range c.identityKeys() {
			out[k] = entry
		}
	}
	return out
}
