package main

import (
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

// newClusterGroupTestServer builds a file-backed apiServer with a fixed set of
// cluster groups for authorization tests.
func newClusterGroupTestServer(t *testing.T, groups []clusterGroup) *apiServer {
	t.Helper()
	dir := t.TempDir()
	db, err := openUserDB(filepath.Join(dir, "users.json"))
	if err != nil {
		t.Fatalf("openUserDB: %v", err)
	}
	if err := db.setClusterGroups(groups); err != nil {
		t.Fatalf("setClusterGroups: %v", err)
	}
	return &apiServer{users: db, usersDBPath: db.path}
}

func sortedCopy(in []string) []string {
	out := append([]string{}, in...)
	sort.Strings(out)
	return out
}

func TestAllowedClustersMembership(t *testing.T) {
	groups := []clusterGroup{
		{
			Name:       "Platform",
			Clusters:   []string{"pc-east", "10.0.0.1"},
			LocalUsers: []string{"alice"},
			ADGroups:   []string{"CN=NCC-Platform,OU=Groups,DC=corp,DC=example,DC=com"},
		},
		{
			Name:     "Storage",
			Clusters: []string{"pc-west"},
			ADGroups: []string{"NCC-Storage"}, // stored as CN only
			ADUsers:  []string{"erin@corp.example.com"},
		},
	}
	s := newClusterGroupTestServer(t, groups)

	cases := []struct {
		name string
		p    principal
		want []string // empty slice => empty allowed set; nil unrestricted handled separately
	}{
		{
			name: "local user match",
			p:    principal{role: RoleOperator, subject: "alice", method: authSessionCookie},
			want: []string{"10.0.0.1", "pc-east"},
		},
		{
			name: "local user no match",
			p:    principal{role: RoleViewer, subject: "carol", method: authSessionCookie},
			want: []string{},
		},
		{
			name: "AD full DN match",
			p:    principal{role: RoleViewer, subject: "bob", method: authSessionCookie, groups: []string{"CN=NCC-Platform,OU=Groups,DC=corp,DC=example,DC=com"}},
			want: []string{"10.0.0.1", "pc-east"},
		},
		{
			name: "AD CN match against DN-configured group",
			p:    principal{role: RoleViewer, subject: "bob", method: authSessionCookie, groups: []string{"CN=NCC-Platform,OU=Other,DC=corp,DC=example,DC=com"}},
			want: []string{"10.0.0.1", "pc-east"},
		},
		{
			name: "AD DN match against CN-configured group",
			p:    principal{role: RoleViewer, subject: "dan", method: authSessionCookie, groups: []string{"CN=NCC-Storage,OU=Groups,DC=corp,DC=example,DC=com"}},
			want: []string{"pc-west"},
		},
		{
			name: "member of two groups gets the union",
			p:    principal{role: RoleViewer, subject: "alice", method: authSessionCookie, groups: []string{"NCC-Storage"}},
			want: []string{"10.0.0.1", "pc-east", "pc-west"},
		},
		{
			name: "individual AD user matched by UPN local part",
			p:    principal{role: RoleViewer, subject: "erin", method: authSessionCookie},
			want: []string{"pc-west"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			access := s.allowedClusters(tc.p)
			if access.unrestricted {
				t.Fatalf("expected restricted access")
			}
			got := sortedCopy(access.displayList())
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("allowed = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestAllowedClustersPrismCentralExpansion(t *testing.T) {
	s := newClusterGroupTestServer(t, []clusterGroup{{
		Name:          "PCGroup",
		PrismCentrals: []string{"https://pc.corp.example.com:9440"},
		LocalUsers:    []string{"alice"},
	}})
	// Prime the discovery cache so allowedClusters expands the PC without a live
	// discover-clusters call (fetchedAt=now keeps it fresh, no background refresh).
	s.pcCache = map[string]*pcCacheEntry{
		normClusterName("https://pc.corp.example.com:9440"): {
			clusters: []pcCluster{
				{Name: "PC-EAST", Address: "10.0.0.1"},
				{Name: "PC-WEST", Address: "10.0.0.2"},
			},
			fetchedAt: time.Now(),
		},
	}
	member := principal{role: RoleOperator, subject: "alice", method: authSessionCookie}
	access := s.allowedClusters(member)
	if access.unrestricted {
		t.Fatal("expected restricted access")
	}
	for _, want := range []string{"PC-EAST", "PC-WEST", "10.0.0.1", "10.0.0.2"} {
		if !access.permits(want) {
			t.Fatalf("expected PC-expanded cluster %q to be permitted; allowed=%v", want, access.displayList())
		}
	}
	// A non-member of the group still sees nothing.
	if !s.allowedClusters(principal{role: RoleViewer, subject: "bob", method: authSessionCookie}).empty() {
		t.Fatal("non-member should have no clusters")
	}
}

func TestAllowedClustersUnrestricted(t *testing.T) {
	s := newClusterGroupTestServer(t, []clusterGroup{{Name: "G", Clusters: []string{"c1"}}})
	for _, p := range []principal{
		{role: RoleAdmin, subject: "admin", method: authSessionCookie},
		{role: RoleAdmin, subject: "static-admin-token", method: authStaticAdminToken},
		{role: RoleViewer, subject: "static-viewer-token", method: authStaticViewerToken},
	} {
		if !s.allowedClusters(p).unrestricted {
			t.Fatalf("expected unrestricted access for %+v", p)
		}
	}
}

func TestAllowedClustersUngroupedIsAdminOnly(t *testing.T) {
	// "pc-lonely" is in no group: a member sees nothing, an admin sees all.
	s := newClusterGroupTestServer(t, []clusterGroup{{Name: "G", Clusters: []string{"pc-east"}, LocalUsers: []string{"alice"}}})
	member := principal{role: RoleViewer, subject: "alice", method: authSessionCookie}
	access := s.allowedClusters(member)
	if access.permits("pc-lonely") {
		t.Fatal("ungrouped cluster must not be permitted for members")
	}
	if !access.permits("pc-east") {
		t.Fatal("grouped cluster should be permitted")
	}
	admin := principal{role: RoleAdmin, subject: "admin", method: authSessionCookie}
	if !s.allowedClusters(admin).permits("pc-lonely") {
		t.Fatal("admin must see ungrouped clusters")
	}
}

func TestResolveRunClusterScope(t *testing.T) {
	s := newClusterGroupTestServer(t, []clusterGroup{
		{Name: "Platform", Clusters: []string{"pc-east", "pc-west"}, LocalUsers: []string{"alice"}},
	})
	member := principal{role: RoleOperator, subject: "alice", method: authSessionCookie}
	admin := principal{role: RoleAdmin, subject: "admin", method: authSessionCookie}
	stranger := principal{role: RoleOperator, subject: "nobody", method: authSessionCookie}

	t.Run("member with no request gets full allowed set", func(t *testing.T) {
		got, err := s.resolveRunClusterScope(runTriggerRequest{}, s.allowedClusters(member), nil)
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{"pc-east", "pc-west"}; !reflect.DeepEqual(sortedCopy(got), want) {
			t.Fatalf("got %v want %v", got, want)
		}
	})
	t.Run("member subset intersects allowed", func(t *testing.T) {
		got, err := s.resolveRunClusterScope(runTriggerRequest{Clusters: []string{"pc-east", "pc-foreign"}}, s.allowedClusters(member), nil)
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{"pc-east"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v want %v", got, want)
		}
	})
	t.Run("member requesting only foreign clusters is rejected", func(t *testing.T) {
		if _, err := s.resolveRunClusterScope(runTriggerRequest{Clusters: []string{"pc-foreign"}}, s.allowedClusters(member), nil); err == nil {
			t.Fatal("expected error for entirely-foreign request")
		}
	})
	t.Run("member smuggling --clusters is rejected", func(t *testing.T) {
		if _, err := s.resolveRunClusterScope(runTriggerRequest{}, s.allowedClusters(member), []string{"--clusters", "pc-east,pc-secret"}); err == nil {
			t.Fatal("expected error when member overrides cluster selection")
		}
	})
	t.Run("non-member with empty allowed set is rejected", func(t *testing.T) {
		if _, err := s.resolveRunClusterScope(runTriggerRequest{}, s.allowedClusters(stranger), nil); err == nil {
			t.Fatal("expected 403-style error for a user in no group")
		}
	})
	t.Run("admin unrestricted runs everything", func(t *testing.T) {
		got, err := s.resolveRunClusterScope(runTriggerRequest{}, s.allowedClusters(admin), nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Fatalf("admin should not be pinned to a subset, got %v", got)
		}
	})
	t.Run("admin can scope to a named group", func(t *testing.T) {
		got, err := s.resolveRunClusterScope(runTriggerRequest{Group: "Platform"}, s.allowedClusters(admin), nil)
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{"pc-east", "pc-west"}; !reflect.DeepEqual(sortedCopy(got), want) {
			t.Fatalf("got %v want %v", got, want)
		}
	})
}

func TestDeepFilterClusters(t *testing.T) {
	s := newClusterGroupTestServer(t, []clusterGroup{
		{Name: "G", Clusters: []string{"pc-east"}, LocalUsers: []string{"alice"}},
	})
	member := s.allowedClusters(principal{role: RoleViewer, subject: "alice", method: authSessionCookie})
	admin := s.allowedClusters(principal{role: RoleAdmin, subject: "admin", method: authSessionCookie})

	snapshot := []interface{}{
		map[string]interface{}{"cluster": "pc-east", "check": "a"},
		map[string]interface{}{"cluster": "pc-west", "check": "b"},
		map[string]interface{}{"summary": "no-cluster-row"},
	}
	filtered, ok := deepFilterClusters(snapshot, member).([]interface{})
	if !ok {
		t.Fatal("expected slice")
	}
	if len(filtered) != 2 {
		t.Fatalf("expected pc-east row + no-cluster row, got %d", len(filtered))
	}
	for _, item := range filtered {
		if m, ok := item.(map[string]interface{}); ok {
			if m["cluster"] == "pc-west" {
				t.Fatal("pc-west should have been filtered out")
			}
		}
	}

	// Admin (unrestricted) sees everything untouched.
	if got := deepFilterClusters(snapshot, admin).([]interface{}); len(got) != 3 {
		t.Fatalf("admin should see all rows, got %d", len(got))
	}

	// Nested per-cluster array inside a map is filtered too.
	nested := map[string]interface{}{
		"per_cluster": []interface{}{
			map[string]interface{}{"address": "pc-east"},
			map[string]interface{}{"address": "pc-west"},
		},
		"total": 2,
	}
	out := deepFilterClusters(nested, member).(map[string]interface{})
	if rows := out["per_cluster"].([]interface{}); len(rows) != 1 {
		t.Fatalf("nested filter expected 1 row, got %d", len(rows))
	}
}

func TestClusterGroupsPersistAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "users.json")
	db, err := openUserDB(path)
	if err != nil {
		t.Fatal(err)
	}
	groups := []clusterGroup{{Name: "Platform", Clusters: []string{"pc-east"}, LocalUsers: []string{"alice"}, ADGroups: []string{"CN=NCC-Platform,DC=corp"}}}
	if err := db.setClusterGroups(groups); err != nil {
		t.Fatal(err)
	}
	// Reopen the same file (mirrors a v2-stop / v2-start cycle and backup/restore,
	// since cluster groups live inside the user database file).
	db2, err := openUserDB(path)
	if err != nil {
		t.Fatal(err)
	}
	got := db2.getClusterGroups()
	if !reflect.DeepEqual(got, groups) {
		t.Fatalf("persisted groups = %+v, want %+v", got, groups)
	}
}
