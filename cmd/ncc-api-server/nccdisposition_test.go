package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOperatorCanAcknowledgeNCCDisposition(t *testing.T) {
	dir := t.TempDir()
	s := &apiServer{repoRoot: dir}
	body := []byte(`{"cluster":"prod-a","check":"CVM memory","action":"acknowledge"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/alerts/ncc-dispositions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = withPrincipal(req, principal{subject: "ops1", role: RoleOperator})
	rec := httptest.NewRecorder()
	if need := routeMinRole(req); need != RoleOperator {
		t.Fatalf("routeMinRole = %v, want operator", need)
	}
	s.handleNCCDispositions(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	items, err := s.loadNCCDispositions()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].By != "ops1" || items[0].Status != "acknowledged" {
		t.Fatalf("items = %#v", items)
	}
}

func TestNCCDispositionRecordsActor(t *testing.T) {
	dir := t.TempDir()
	s := &apiServer{repoRoot: dir}
	body := []byte(`{"cluster":"prod-a","check":"CVM memory","action":"acknowledge"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/alerts/ncc-dispositions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = withPrincipal(req, principal{subject: "alice", role: RoleAdmin})
	rec := httptest.NewRecorder()
	s.handleNCCDispositions(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	items, err := s.loadNCCDispositions()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].By != "alice" || items[0].Status != "acknowledged" {
		t.Fatalf("items = %#v", items)
	}
	if _, err := os.Stat(filepath.Join(dir, "outputfiles", "ncc-alert-dispositions.json")); err != nil {
		t.Fatal(err)
	}

	reopen := httptest.NewRequest(http.MethodPost, "/api/v1/alerts/ncc-dispositions", strings.NewReader(`{"cluster":"prod-a","check":"CVM memory","action":"reopen"}`))
	reopen.Header.Set("Content-Type", "application/json")
	reopen = withPrincipal(reopen, principal{subject: "alice", role: RoleAdmin})
	rec = httptest.NewRecorder()
	s.handleNCCDispositions(rec, reopen)
	items, err = s.loadNCCDispositions()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("reopen left %#v", items)
	}
}

func TestResolvedMarkReopensWhenLaterRunRepeatsTheCheck(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "outputfiles")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	summary := []byte(`{"timestamp":"2026-10-05T12:00:00Z"}` + "\n")
	if err := os.WriteFile(filepath.Join(out, "run-summary.json"), summary, 0o644); err != nil {
		t.Fatal(err)
	}
	html := []byte(`const AGG = [{"cluster":"prod-a","check":"CVM memory","severity":"FAIL"}];` + "\n")
	if err := os.WriteFile(filepath.Join(out, "index.html"), html, 0o644); err != nil {
		t.Fatal(err)
	}
	s := &apiServer{repoRoot: dir, outputDir: out}
	seed := nccDispositionFile{Items: []nccDisposition{{
		Cluster: "prod-a",
		Check:   "CVM memory",
		Status:  "resolved",
		By:      "alice",
		At:      "2026-10-05T11:00:00Z",
		RunAt:   "2026-10-05T10:00:00Z",
		Note:    "patched",
	}}}
	raw, _ := json.Marshal(seed)
	if err := os.WriteFile(s.nccDispositionPath(), raw, 0o644); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/alerts/ncc-dispositions", nil)
	req = withPrincipal(req, principal{subject: "alice", role: RoleAdmin})
	rec := httptest.NewRecorder()
	s.handleNCCDispositions(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	items, err := s.loadNCCDispositions()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Status != "reopened" || items[0].By != nccAutoActor {
		t.Fatalf("items = %#v", items)
	}
	if !strings.Contains(items[0].Reason, "generated again") || items[0].RunAt != "2026-10-05T12:00:00Z" {
		t.Fatalf("reason/run = %#v", items[0])
	}
	if len(items[0].History) < 2 || items[0].History[0].By != "alice" || items[0].History[0].Note != "patched" {
		t.Fatalf("history = %#v", items[0].History)
	}

	// The same run must not clear a resolve made against it.
	stay := []nccDisposition{{
		Cluster: "prod-a", Check: "other check", Status: "resolved", By: "alice",
		At: "2026-10-05T12:05:00Z", RunAt: "2026-10-05T12:00:00Z",
	}}
	got, n := applyResolvedRecurrence(stay, "2026-10-05T12:00:00Z", map[string]struct{}{
		nccDispositionKey("prod-a", "other check"): {},
	}, time.Now())
	if n != 0 || got[0].Status != "resolved" {
		t.Fatalf("same run reopened %#v (%d)", got, n)
	}

	// A later run that no longer contains the check keeps the resolve.
	gone, n := applyResolvedRecurrence(stay, "2026-10-06T12:00:00Z", map[string]struct{}{
		nccDispositionKey("prod-a", "something else"): {},
	}, time.Now())
	if n != 0 || gone[0].Status != "resolved" {
		t.Fatalf("absent check reopened %#v", gone)
	}

	acked := []nccDisposition{{
		Cluster: "prod-a", Check: "CVM memory", Status: "acknowledged", By: "alice",
		At: "2026-10-05T10:00:00Z", RunAt: "2026-10-05T09:00:00Z",
	}}
	kept, n := applyResolvedRecurrence(acked, "2026-10-05T12:00:00Z", map[string]struct{}{
		nccDispositionKey("prod-a", "CVM memory"): {},
	}, time.Now())
	if n != 0 || kept[0].Status != "acknowledged" {
		t.Fatalf("acknowledge was cleared %#v", kept)
	}
}

func TestResolvePCAlertClusterCanonicalUUID(t *testing.T) {
	idx := map[string]pcCluster{
		canonicalClusterKey("AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE"): {
			Name: "prod-east", Address: "10.9.8.7", ExtID: "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE",
		},
	}
	alert := map[string]interface{}{"cluster_uuid": "{aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee}", "cluster": "{aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee}"}
	got := resolvePCAlertCluster(alert, idx)
	if got["cluster"] != "prod-east" || got["cluster_ip"] != "10.9.8.7" {
		b, _ := json.Marshal(got)
		t.Fatalf("resolved %s", b)
	}
}
