package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// normClusterName canonicalizes a cluster name/address for comparison (trim +
// lowercase). Cluster identity is the same string used in the clusters file,
// config, and report artifacts.
func normClusterName(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// clusterAccess is a principal's resolved cluster visibility. When restricted,
// allowed maps a normalized cluster name to the original (display) name so
// callers can both test membership and emit the exact names the orchestrator
// and report artifacts use.
type clusterAccess struct {
	unrestricted bool              // admin / static token: sees every cluster
	allowed      map[string]string // normalized cluster name -> display name (when restricted)
}

// permits reports whether the principal may see/act on the named cluster.
func (a clusterAccess) permits(cluster string) bool {
	if a.unrestricted {
		return true
	}
	_, ok := a.allowed[normClusterName(cluster)]
	return ok
}

// display returns the canonical display name for an allowed cluster.
func (a clusterAccess) display(cluster string) (string, bool) {
	d, ok := a.allowed[normClusterName(cluster)]
	return d, ok
}

// empty reports whether a restricted principal has no clusters at all.
func (a clusterAccess) empty() bool {
	return !a.unrestricted && len(a.allowed) == 0
}

// displayList returns all allowed cluster display names. Meaningless when
// unrestricted; callers should check that first.
func (a clusterAccess) displayList() []string {
	out := make([]string, 0, len(a.allowed))
	for _, d := range a.allowed {
		out = append(out, d)
	}
	sort.Strings(out)
	return out
}

// allowedClusters resolves which clusters a principal may see/act on. Admins and
// static-token callers (infra credentials) are unrestricted. Every other caller
// gets the union of clusters from each group they belong to, where membership is
// a local-account username match OR an AD group match (by CN or full DN). Because
// only grouped clusters are ever granted, ungrouped clusters are admin-only.
func (s *apiServer) allowedClusters(p principal) clusterAccess {
	if p.role == RoleAdmin || p.method == authStaticAdminToken || p.method == authStaticViewerToken {
		return clusterAccess{unrestricted: true}
	}
	allowed := map[string]string{}
	if s.users == nil {
		return clusterAccess{allowed: allowed}
	}
	// Lower-case the principal's AD group values and their extracted CNs once so
	// matching is case-insensitive and works whether a group is stored as a CN or
	// a full DN.
	adVals := map[string]bool{}
	for _, g := range p.groups {
		g = strings.TrimSpace(g)
		if g == "" {
			continue
		}
		adVals[strings.ToLower(g)] = true
		if cn := ldapFirstCN(g); cn != "" {
			adVals[strings.ToLower(cn)] = true
		}
	}
	subj := strings.ToLower(strings.TrimSpace(p.subject))
	add := func(name string) {
		if n := normClusterName(name); n != "" {
			if _, exists := allowed[n]; !exists {
				allowed[n] = strings.TrimSpace(name)
			}
		}
	}
	for _, grp := range s.users.getClusterGroups() {
		if !principalInClusterGroup(subj, adVals, grp) {
			continue
		}
		for _, c := range grp.Clusters {
			add(c)
		}
		// Prism Central expansion: every cluster registered under a PC in this
		// group is granted to the member. Both the cluster name and address are
		// added so filtering matches whichever identity reports/runs use. The
		// list comes from a cached discovery (see expandPrismCentral), which
		// refreshes in the background so newly-registered clusters appear
		// automatically without an admin edit.
		for _, pc := range grp.PrismCentrals {
			for _, c := range s.expandPrismCentral(pc) {
				add(c.Name)
				add(c.Address)
			}
		}
	}
	return clusterAccess{allowed: allowed}
}

// artifactsRestricted reports whether the request's caller is confined to
// cluster groups (i.e. not an admin/static token). Such callers may not pull
// raw multi-cluster report artifacts (index.html, CSV/JSON exports, NCC logs),
// which cannot be filtered per cluster after the orchestrator renders them.
func (s *apiServer) artifactsRestricted(r *http.Request) bool {
	p, ok := principalFromContext(r.Context())
	if !ok {
		return false
	}
	return !s.allowedClusters(p).unrestricted
}

// handleClusterGroups manages the cluster-group definitions (admin-only via
// routeMinRole). GET returns the current groups; PUT replaces the full set.
func (s *apiServer) handleClusterGroups(w http.ResponseWriter, r *http.Request) {
	if s.users == nil || !s.users.writable() {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "cluster groups require a writable user database"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		groups := s.users.getClusterGroups()
		if groups == nil {
			groups = []clusterGroup{}
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{"groups": groups}})
	case http.MethodPut:
		var body struct {
			Groups []clusterGroup `json:"groups"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		if err := dec.Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid JSON body"})
			return
		}
		cleaned, err := validateClusterGroups(body.Groups)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if err := s.users.setClusterGroups(cleaned); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		// Warm the PC discovery cache so the first access check after this save
		// already resolves any Prism Central clusters (best-effort, async).
		s.primePrismCentralCache(cleaned)
		s.audit(r, "settings.cluster_groups.update", true, map[string]interface{}{"count": len(cleaned)})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "cluster groups updated"})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

// validateClusterGroups trims, validates, and dedupes a posted set of groups.
// Group names must be present and unique (case-insensitive); the member and
// cluster lists are trimmed and de-duplicated.
func validateClusterGroups(in []clusterGroup) ([]clusterGroup, error) {
	out := make([]clusterGroup, 0, len(in))
	seenNames := map[string]bool{}
	for _, g := range in {
		name := strings.TrimSpace(g.Name)
		if name == "" {
			return nil, errors.New("every cluster group must have a name")
		}
		key := strings.ToLower(name)
		if seenNames[key] {
			return nil, fmt.Errorf("duplicate cluster group name %q", name)
		}
		seenNames[key] = true
		out = append(out, clusterGroup{
			Name:          name,
			Clusters:      dedupeTrim(g.Clusters),
			PrismCentrals: dedupeTrim(g.PrismCentrals),
			LocalUsers:    dedupeTrim(g.LocalUsers),
			ADGroups:      dedupeTrim(g.ADGroups),
			ADUsers:       dedupeTrim(g.ADUsers),
		})
	}
	return out, nil
}

// dedupeTrim trims whitespace and removes empty/duplicate (case-insensitive)
// entries while preserving first-seen order.
func dedupeTrim(in []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		key := strings.ToLower(v)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// handleClusterInventory lists the clusters known to the active config
// (admin-only via routeMinRole) so the UI can offer them when assigning
// clusters to groups.
func (s *apiServer) handleClusterInventory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{"clusters": s.knownClusters()}})
}

// knownClusters enumerates cluster names from the active config: the inline
// `clusters` CSV and the `clusters-file` (CSV `cluster[,user[,pass]]`). Names
// are de-duplicated (case-insensitive) and sorted.
func (s *apiServer) knownClusters() []string {
	names := []string{}
	seen := map[string]bool{}
	add := func(n string) {
		n = strings.TrimSpace(n)
		if n == "" || strings.HasPrefix(n, "#") {
			return
		}
		key := normClusterName(n)
		if seen[key] {
			return
		}
		seen[key] = true
		names = append(names, n)
	}
	for _, part := range strings.FieldsFunc(s.configScalarValue("clusters", ""), func(r rune) bool { return r == ',' || r == ' ' || r == '\t' }) {
		add(part)
	}
	if cfPath := strings.TrimSpace(s.configScalarValue("clusters-file", "")); cfPath != "" {
		resolved := cfPath
		if !filepath.IsAbs(resolved) {
			resolved = filepath.Join(filepath.Dir(s.absPath(s.configPath)), cfPath)
		}
		if b, err := os.ReadFile(resolved); err == nil {
			for _, line := range strings.Split(string(b), "\n") {
				line = strings.TrimSpace(line)
				if line == "" || strings.HasPrefix(line, "#") {
					continue
				}
				fields := strings.Split(line, ",")
				add(fields[0])
			}
		}
	}
	sort.Strings(names)
	return names
}

// findClusterGroup returns the named cluster group (case-insensitive) and true
// when it exists in the store.
func (s *apiServer) findClusterGroup(name string) (clusterGroup, bool) {
	if s.users == nil {
		return clusterGroup{}, false
	}
	want := strings.ToLower(strings.TrimSpace(name))
	for _, g := range s.users.getClusterGroups() {
		if strings.ToLower(strings.TrimSpace(g.Name)) == want {
			return g, true
		}
	}
	return clusterGroup{}, false
}

// resolveRunClusterScope computes the cluster subset (if any) a run should be
// pinned to via --clusters, enforcing the caller's group membership. It returns
// an error (surfaced as 403) when a restricted caller has no access or requests
// clusters outside their groups. An empty result with nil error means "do not
// constrain" (admin running everything).
func (s *apiServer) resolveRunClusterScope(req runTriggerRequest, access clusterAccess, extra []string) ([]string, error) {
	requested := []string{}
	seen := map[string]bool{}
	add := func(name string) {
		n := normClusterName(name)
		if n == "" || seen[n] {
			return
		}
		seen[n] = true
		requested = append(requested, strings.TrimSpace(name))
	}
	if g := strings.TrimSpace(req.Group); g != "" {
		grp, ok := s.findClusterGroup(g)
		if !ok {
			return nil, fmt.Errorf("unknown cluster group %q", g)
		}
		for _, c := range grp.Clusters {
			add(c)
		}
		// Fold in the clusters managed by each Prism Central in the group. Only
		// the discovered cluster address is pinned for the run (the orchestrator
		// runs against addresses); names still grant report visibility.
		for _, pc := range grp.PrismCentrals {
			for _, c := range s.expandPrismCentral(pc) {
				if strings.TrimSpace(c.Address) != "" {
					add(c.Address)
				} else {
					add(c.Name)
				}
			}
		}
	}
	for _, c := range req.Clusters {
		add(c)
	}

	callerPinnedClusters := extraArgsHaveFlag(extra, "clusters") || extraArgsHaveFlag(extra, "cluster-file")

	if access.unrestricted {
		// Admin: honor an explicit subset when given; otherwise run everything.
		// Defer entirely to extra args if the caller already pinned clusters.
		if callerPinnedClusters {
			return nil, nil
		}
		return requested, nil // may be empty -> run all
	}
	// Restricted callers must not smuggle their own cluster selection to escape
	// their groups.
	if callerPinnedClusters {
		return nil, errors.New("you are not permitted to override cluster selection")
	}
	if access.empty() {
		return nil, errors.New("you are not a member of any cluster group; ask an administrator for access")
	}
	effective := []string{}
	if len(requested) == 0 {
		effective = access.displayList()
	} else {
		for _, c := range requested {
			if disp, ok := access.display(c); ok {
				effective = append(effective, disp)
			}
		}
	}
	if len(effective) == 0 {
		return nil, errors.New("none of the requested clusters are in your allowed groups")
	}
	return effective, nil
}

// userIdentifierMatches reports whether a stored member identifier matches the
// caller's (lower-cased) subject. It compares case-insensitively and also
// accepts a UPN whose local part equals the subject (e.g. "jdoe@corp.example.com"
// matches subject "jdoe").
func userIdentifierMatches(subjLower, candidate string) bool {
	c := strings.ToLower(strings.TrimSpace(candidate))
	if c == "" {
		return false
	}
	if c == subjLower {
		return true
	}
	if at := strings.IndexByte(c, '@'); at > 0 && c[:at] == subjLower {
		return true
	}
	return false
}

// clusterKeyCandidates are object field names that hold a cluster identity in
// the various report artifacts (checks snapshot, agg rows, cluster links, the
// NCC per-cluster summary, and nested per-cluster arrays).
var clusterKeyCandidates = []string{"cluster", "Cluster", "address", "Address"}

// objClusterName returns the cluster identity carried by an object, if any.
func objClusterName(m map[string]interface{}) (string, bool) {
	for _, k := range clusterKeyCandidates {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				return s, true
			}
		}
	}
	return "", false
}

// deepFilterClusters recursively removes, from any array at any depth, objects
// that carry a cluster identity the access does not permit. Objects without a
// cluster field and all non-cluster data are left intact. Returns the value
// unchanged when access is unrestricted.
func deepFilterClusters(v interface{}, access clusterAccess) interface{} {
	if access.unrestricted {
		return v
	}
	switch t := v.(type) {
	case map[string]interface{}:
		for k, val := range t {
			t[k] = deepFilterClusters(val, access)
		}
		return t
	case []interface{}:
		out := make([]interface{}, 0, len(t))
		for _, item := range t {
			if m, ok := item.(map[string]interface{}); ok {
				if name, has := objClusterName(m); has && !access.permits(name) {
					continue
				}
			}
			out = append(out, deepFilterClusters(item, access))
		}
		return out
	case []map[string]interface{}:
		out := make([]map[string]interface{}, 0, len(t))
		for _, m := range t {
			if name, has := objClusterName(m); has && !access.permits(name) {
				continue
			}
			out = append(out, m)
		}
		return out
	default:
		return v
	}
}

// principalInClusterGroup reports whether the (lower-cased) subject or any of the
// principal's (lower-cased) AD group values/CNs are a member of grp.
func principalInClusterGroup(subjLower string, adVals map[string]bool, grp clusterGroup) bool {
	if subjLower != "" {
		// Local accounts and individual AD users are both matched by the
		// caller's canonical subject (an AD login resolves to its
		// sAMAccountName). UPN-style entries are matched on their local part
		// too, so "jdoe@corp" also matches the subject "jdoe".
		for _, u := range append(append([]string{}, grp.LocalUsers...), grp.ADUsers...) {
			if userIdentifierMatches(subjLower, u) {
				return true
			}
		}
	}
	for _, ag := range grp.ADGroups {
		ag = strings.TrimSpace(ag)
		if ag == "" {
			continue
		}
		if adVals[strings.ToLower(ag)] {
			return true
		}
		if cn := ldapFirstCN(ag); cn != "" && adVals[strings.ToLower(cn)] {
			return true
		}
	}
	return false
}
