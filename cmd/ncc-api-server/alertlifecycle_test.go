package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func testStore(t *testing.T) (*alertLifecycleStore, string) {
	t.Helper()
	dir := t.TempDir()
	st := newAlertLifecycleStore(filepath.Join(dir, alertLifecycleFileName), filepath.Join(dir, "outputfiles", "ncc-alert-dispositions.json"))
	return st, dir
}

func runSnap(at string, covered []string, findings ...nccFinding) nccRunSnapshot {
	snap := nccRunSnapshot{RunAt: at, Findings: findings, Covered: map[string]struct{}{}}
	for _, c := range covered {
		snap.Covered[clusterCoverageKey(c)] = struct{}{}
	}
	return snap
}

func activeFor(t *testing.T, st *alertLifecycleStore, cluster, check string) nccAlert {
	t.Helper()
	doc, err := st.read()
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range doc.Alerts {
		if !a.Closed && a.CorrelationKey == alertCorrelationKey(cluster, check) {
			return a
		}
	}
	t.Fatalf("no active alert for %s/%s in %s", cluster, check, mustJSON(doc.Alerts))
	return nccAlert{}
}

func eventTypes(t *testing.T, st *alertLifecycleStore, alertID string) []string {
	t.Helper()
	doc, err := st.read()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range doc.Events {
		if e.AlertID == alertID {
			out = append(out, e.Type)
		}
	}
	return out
}

const (
	run1 = "2026-10-20T10:00:00Z"
	run2 = "2026-10-21T10:00:00Z"
	run3 = "2026-10-22T10:00:00Z"
	run4 = "2026-10-23T10:00:00Z"
)

var memFail = nccFinding{Cluster: "10.0.0.1", ClusterName: "prod-a", Check: "Detailed information for CVM memory:", Severity: "FAIL"}

func TestAcknowledgePersistsAcrossNewReports(t *testing.T) {
	st, _ := testStore(t)
	if _, err := st.ingestRun(runSnap(run1, []string{"10.0.0.1"}, memFail)); err != nil {
		t.Fatal(err)
	}
	first := activeFor(t, st, "10.0.0.1", "CVM memory")
	if _, err := st.applyAction([]alertActionTarget{{Cluster: "10.0.0.1", Check: "CVM memory"}}, "acknowledge", "alice", "looking", run1, nil); err != nil {
		t.Fatal(err)
	}
	for _, r := range []string{run2, run3} {
		if _, err := st.ingestRun(runSnap(r, []string{"10.0.0.1"}, memFail)); err != nil {
			t.Fatal(err)
		}
	}
	got := activeFor(t, st, "10.0.0.1", "CVM memory")
	if got.ID != first.ID {
		t.Fatalf("new report replaced the alert instance: %s -> %s", first.ID, got.ID)
	}
	if got.State != alertStateAcknowledged || got.AcknowledgedBy != "alice" || got.Note != "looking" {
		t.Fatalf("acknowledgement lost: %+v", got)
	}
	if got.LastSeenRun != run3 || got.FirstSeenRun != run1 || got.Detection != alertDetected {
		t.Fatalf("seen bookkeeping wrong: %+v", got)
	}
}

func TestResolveThenNewReportStillDetectedOpensNewInstance(t *testing.T) {
	st, _ := testStore(t)
	mustIngest(t, st, runSnap(run1, []string{"10.0.0.1"}, memFail))
	if _, err := st.applyAction([]alertActionTarget{{Cluster: "10.0.0.1", Check: "CVM memory"}}, "resolve", "bob", "patched", run1, nil); err != nil {
		t.Fatal(err)
	}
	resolved := activeFor(t, st, "10.0.0.1", "CVM memory")
	// Re-ingesting the run the resolve was made against changes nothing.
	mustIngest(t, st, runSnap(run1, []string{"10.0.0.1"}, memFail))
	if again := activeFor(t, st, "10.0.0.1", "CVM memory"); again.ID != resolved.ID || again.State != alertStateResolved {
		t.Fatalf("same run must not reopen a resolve: %+v", again)
	}
	mustIngest(t, st, runSnap(run2, []string{"10.0.0.1"}, memFail))
	next := activeFor(t, st, "10.0.0.1", "CVM memory")
	if next.ID == resolved.ID || next.PreviousID != resolved.ID || !next.Reopened || next.State != alertStateOpen {
		t.Fatalf("recurrence should open a linked new instance: %+v", next)
	}
	doc, _ := st.read()
	old := doc.Alerts[findAlertIndex(doc, resolved.ID)]
	if !old.Closed || old.CloseReason != "recurred" || old.ResolvedBy != "bob" || old.NextID != next.ID {
		t.Fatalf("resolved instance must be preserved as history: %+v", old)
	}
	if got := eventTypes(t, st, resolved.ID); strings.Join(got, ",") != "detected,resolved,closed" {
		t.Fatalf("old instance events = %v", got)
	}
	// Legacy view shows the "Returned" status with the resolve in history.
	items := legacyDispositionsFromDoc(doc)
	if len(items) != 1 || items[0].Status != "reopened" || len(items[0].History) < 2 || items[0].History[0].By != "bob" {
		t.Fatalf("legacy view = %s", mustJSON(items))
	}
}

func TestResolveThenNotDetectedClosesInstance(t *testing.T) {
	st, _ := testStore(t)
	mustIngest(t, st, runSnap(run1, []string{"10.0.0.1"}, memFail))
	if _, err := st.applyAction([]alertActionTarget{{Cluster: "10.0.0.1", Check: "CVM memory"}}, "resolve", "bob", "", run1, nil); err != nil {
		t.Fatal(err)
	}
	id := activeFor(t, st, "10.0.0.1", "CVM memory").ID
	mustIngest(t, st, runSnap(run2, []string{"10.0.0.1"}))
	doc, _ := st.read()
	a := doc.Alerts[findAlertIndex(doc, id)]
	if !a.Closed || a.CloseReason != "resolved_not_detected" || a.State != alertStateResolved || a.Detection != alertNotDetected {
		t.Fatalf("confirmed resolution should close the instance: %+v", a)
	}
	// It is still queryable as history.
	items, _ := filterAlerts(doc.Alerts, alertQuery{IncludeClosed: true}, nil)
	if len(items) != 1 || items[0].ID != id {
		t.Fatalf("closed instance not retrievable: %s", mustJSON(items))
	}
	if open, _ := filterAlerts(doc.Alerts, alertQuery{}, nil); len(open) != 0 {
		t.Fatalf("closed instance must not be listed by default: %s", mustJSON(open))
	}
}

func TestDisappearAndReappearKeepsStateAndDistinguishesNotDetected(t *testing.T) {
	st, _ := testStore(t)
	mustIngest(t, st, runSnap(run1, []string{"10.0.0.1"}, memFail))
	if _, err := st.applyAction([]alertActionTarget{{Cluster: "10.0.0.1", Check: "CVM memory"}}, "acknowledge", "alice", "", run1, nil); err != nil {
		t.Fatal(err)
	}
	id := activeFor(t, st, "10.0.0.1", "CVM memory").ID
	mustIngest(t, st, runSnap(run2, []string{"10.0.0.1"}))
	gone := activeFor(t, st, "10.0.0.1", "CVM memory")
	if gone.ID != id || gone.Detection != alertNotDetected || gone.State != alertStateAcknowledged || gone.NotDetectedRun != run2 {
		t.Fatalf("not-detected must not be treated as resolved: %+v", gone)
	}
	mustIngest(t, st, runSnap(run3, []string{"10.0.0.1"}, memFail))
	back := activeFor(t, st, "10.0.0.1", "CVM memory")
	if back.ID != id || back.Detection != alertDetected || back.State != alertStateAcknowledged || back.NotDetectedRun != "" {
		t.Fatalf("reappearance should keep the instance and acknowledgement: %+v", back)
	}
	if got := strings.Join(eventTypes(t, st, id), ","); got != "detected,acknowledged,not_detected,redetected" {
		t.Fatalf("events = %s", got)
	}
}

func TestPartialRunDoesNotMarkUncoveredClustersNotDetected(t *testing.T) {
	st, _ := testStore(t)
	other := nccFinding{Cluster: "10.0.0.2", Check: "Disk usage", Severity: "WARN"}
	mustIngest(t, st, runSnap(run1, []string{"10.0.0.1", "10.0.0.2"}, memFail, other))
	// Run 2 only covered cluster 1 (cluster 2 was unreachable or not selected).
	mustIngest(t, st, runSnap(run2, []string{"10.0.0.1"}, memFail))
	if a := activeFor(t, st, "10.0.0.2", "Disk usage"); a.Detection != alertDetected {
		t.Fatalf("uncovered cluster finding must stay detected: %+v", a)
	}
}

func TestSeverityChangeRecordedAndEscalationReopensAcknowledgement(t *testing.T) {
	st, _ := testStore(t)
	warn := memFail
	warn.Severity = "WARN"
	mustIngest(t, st, runSnap(run1, []string{"10.0.0.1"}, warn))
	if _, err := st.applyAction([]alertActionTarget{{Cluster: "10.0.0.1", Check: "CVM memory"}}, "acknowledge", "alice", "", run1, nil); err != nil {
		t.Fatal(err)
	}
	mustIngest(t, st, runSnap(run2, []string{"10.0.0.1"}, memFail))
	esc := activeFor(t, st, "10.0.0.1", "CVM memory")
	if esc.Severity != "FAIL" || esc.State != alertStateOpen || !esc.Reopened || !strings.Contains(esc.ReopenReason, "escalated") {
		t.Fatalf("escalation should reopen the acknowledgement: %+v", esc)
	}
	if _, err := st.applyAction([]alertActionTarget{{ID: esc.ID}}, "acknowledge", "alice", "", run2, nil); err != nil {
		t.Fatal(err)
	}
	mustIngest(t, st, runSnap(run3, []string{"10.0.0.1"}, warn))
	down := activeFor(t, st, "10.0.0.1", "CVM memory")
	if down.Severity != "WARN" || down.State != alertStateAcknowledged {
		t.Fatalf("de-escalation should keep the acknowledgement: %+v", down)
	}
	n := 0
	for _, e := range eventTypes(t, st, esc.ID) {
		if e == alertEvtSeverityChanged {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("want 2 severity_changed events, got %d", n)
	}
}

func TestDuplicateAndOutOfOrderIngestionIsIdempotent(t *testing.T) {
	st, _ := testStore(t)
	mustIngest(t, st, runSnap(run2, []string{"10.0.0.1"}, memFail))
	before, _ := st.read()
	for i := 0; i < 3; i++ {
		n, err := st.ingestRun(runSnap(run2, []string{"10.0.0.1"}, memFail))
		if err != nil || n != 0 {
			t.Fatalf("duplicate ingestion changed state (n=%d err=%v)", n, err)
		}
	}
	// An older run arriving late must not rewind state.
	if n, err := st.ingestRun(runSnap(run1, []string{"10.0.0.1"})); err != nil || n != 0 {
		t.Fatalf("older run applied (n=%d err=%v)", n, err)
	}
	after, _ := st.read()
	if len(after.Events) != len(before.Events) || len(after.Alerts) != len(before.Alerts) {
		t.Fatalf("idempotency broken: before %d/%d after %d/%d", len(before.Alerts), len(before.Events), len(after.Alerts), len(after.Events))
	}
	// Repeating an action with the same note is a no-op as well.
	for i := 0; i < 2; i++ {
		if _, err := st.applyAction([]alertActionTarget{{Cluster: "10.0.0.1", Check: "CVM memory"}}, "acknowledge", "alice", "same", run2, nil); err != nil {
			t.Fatal(err)
		}
	}
	if got := eventTypes(t, st, activeFor(t, st, "10.0.0.1", "CVM memory").ID); strings.Count(strings.Join(got, ","), "acknowledged") != 1 {
		t.Fatalf("repeated acknowledge recorded twice: %v", got)
	}
}

// Two "replicas" (independent store instances on the same file, as with two
// API pods on a shared RWX volume) racing a run ingestion against operator
// marks must not lose any update.
func TestConcurrentReplicasDoNotLoseUpdates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, alertLifecycleFileName)
	legacy := filepath.Join(dir, "outputfiles", "ncc-alert-dispositions.json")
	replicaA := newAlertLifecycleStore(path, legacy)
	replicaB := newAlertLifecycleStore(path, legacy)
	var findings []nccFinding
	for i := 0; i < 40; i++ {
		findings = append(findings, nccFinding{Cluster: "10.0.0.1", Check: fmt.Sprintf("check-%02d", i), Severity: "WARN"})
	}
	mustIngest(t, replicaA, runSnap(run1, []string{"10.0.0.1"}, findings...))

	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st := replicaA
			if i%2 == 1 {
				st = replicaB
			}
			_, err := st.applyAction([]alertActionTarget{{Cluster: "10.0.0.1", Check: fmt.Sprintf("check-%02d", i)}}, "acknowledge", fmt.Sprintf("user%d", i), "", run1, nil)
			errs <- err
		}(i)
	}
	// A new report lands while the marks are being written.
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := replicaB.ingestRun(runSnap(run2, []string{"10.0.0.1"}, findings...))
		errs <- err
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	doc, err := newAlertLifecycleStore(path, legacy).read()
	if err != nil {
		t.Fatal(err)
	}
	acked := 0
	for _, a := range doc.Alerts {
		if !a.Closed && a.State == alertStateAcknowledged {
			acked++
		}
	}
	if acked != 40 {
		t.Fatalf("lost updates: %d/40 acknowledgements survived", acked)
	}
	if doc.LastIngestRun != run2 {
		t.Fatalf("concurrent ingestion lost: last run %q", doc.LastIngestRun)
	}
	// Temp files from atomic writes must not be left behind.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("leftover temp file %s", e.Name())
		}
	}
}

func TestAlertIDsAreUniqueUUIDv4AndCollisionsAreRejected(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 20000; i++ {
		id, err := newUUIDv4()
		if err != nil {
			t.Fatal(err)
		}
		if !alertIDPattern.MatchString(id) {
			t.Fatalf("not a v4 UUID: %s", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id %s", id)
		}
		seen[id] = true
	}
	// A colliding generator must not alias two instances.
	st, _ := testStore(t)
	calls := 0
	st.newID = func() (string, error) {
		calls++
		if calls <= 3 {
			return "11111111-1111-4111-8111-111111111111", nil
		}
		return newUUIDv4()
	}
	mustIngest(t, st, runSnap(run1, []string{"c"}, nccFinding{Cluster: "c", Check: "a"}, nccFinding{Cluster: "c", Check: "b"}))
	doc, _ := st.read()
	if doc.Alerts[0].ID == doc.Alerts[1].ID {
		t.Fatalf("duplicate alert ids persisted: %s", mustJSON(doc.Alerts))
	}
	// Separate occurrences of the same finding never overwrite each other.
	if doc.Alerts[0].FindingID == doc.Alerts[1].FindingID {
		t.Fatal("distinct findings must have distinct finding ids")
	}
	if alertFindingID(alertCorrelationKey("C", "Detailed information for a:")) != doc.Alerts[0].FindingID {
		t.Fatal("finding id must be stable across title/case variants")
	}
}

func TestHistorySurvivesRestartAndLegacyFileDeletion(t *testing.T) {
	st, dir := testStore(t)
	mustIngest(t, st, runSnap(run1, []string{"10.0.0.1"}, memFail))
	if _, err := st.applyAction([]alertActionTarget{{Cluster: "10.0.0.1", Check: "CVM memory"}}, "acknowledge", "alice", "", run1, nil); err != nil {
		t.Fatal(err)
	}
	// Simulate the runner's artifact retention deleting the legacy file.
	if err := os.Remove(filepath.Join(dir, "outputfiles", "ncc-alert-dispositions.json")); err != nil {
		t.Fatal(err)
	}
	restarted := newAlertLifecycleStore(st.path, st.legacyPath)
	mustIngest(t, restarted, runSnap(run2, []string{"10.0.0.1"}, memFail))
	if a := activeFor(t, restarted, "10.0.0.1", "CVM memory"); a.State != alertStateAcknowledged {
		t.Fatalf("acknowledgement lost after restart + legacy deletion: %+v", a)
	}
	if _, err := os.Stat(st.legacyPath); err != nil {
		t.Fatalf("legacy mirror should be rewritten: %v", err)
	}
}

func TestLegacyDispositionsAreImportedOnce(t *testing.T) {
	st, _ := testStore(t)
	if err := os.MkdirAll(filepath.Dir(st.legacyPath), 0o755); err != nil {
		t.Fatal(err)
	}
	legacy := nccDispositionFile{Items: []nccDisposition{
		{Cluster: "prod-a", Check: "CVM memory", Status: "acknowledged", By: "carol", At: "2026-10-01T00:00:00Z", Note: "ticket 7",
			History: []nccDispositionEvent{{Status: "resolved", By: "dave", At: "2026-09-30T00:00:00Z"}}},
	}}
	b, _ := json.Marshal(legacy)
	if err := os.WriteFile(st.legacyPath, b, 0o644); err != nil {
		t.Fatal(err)
	}
	a := activeFor(t, st, "prod-a", "CVM memory")
	if a.State != alertStateAcknowledged || a.AcknowledgedBy != "carol" || !a.LegacyMigrated || a.Note != "ticket 7" {
		t.Fatalf("legacy import = %+v", a)
	}
	if got := strings.Join(eventTypes(t, st, a.ID), ","); got != "resolved,acknowledged,migrated" {
		t.Fatalf("legacy history events = %s", got)
	}
	// A second load does not import again.
	again := newAlertLifecycleStore(st.path, st.legacyPath)
	doc, _ := again.read()
	if len(doc.Alerts) != 1 {
		t.Fatalf("legacy imported twice: %d alerts", len(doc.Alerts))
	}
}

func TestCorruptStoreIsNotOverwritten(t *testing.T) {
	st, _ := testStore(t)
	if err := os.WriteFile(st.path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ingestRun(runSnap(run1, []string{"10.0.0.1"}, memFail)); err == nil {
		t.Fatal("expected an error for a corrupt store")
	}
	if b, _ := os.ReadFile(st.path); string(b) != "{not json" {
		t.Fatal("corrupt store must be left untouched for recovery")
	}
}

func TestRetentionPrunesOnlyOldClosedInstances(t *testing.T) {
	st, _ := testStore(t)
	st.retainDays = 30
	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	st.now = func() time.Time { return clock }
	mustIngest(t, st, runSnap("2026-01-01T00:00:00Z", []string{"c"}, nccFinding{Cluster: "c", Check: "old"}, nccFinding{Cluster: "c", Check: "active"}))
	if _, err := st.applyAction([]alertActionTarget{{Cluster: "c", Check: "old"}}, "resolve", "x", "", "2026-01-01T00:00:00Z", nil); err != nil {
		t.Fatal(err)
	}
	mustIngest(t, st, runSnap("2026-01-02T00:00:00Z", []string{"c"}, nccFinding{Cluster: "c", Check: "active"}))
	clock = clock.AddDate(0, 3, 0)
	mustIngest(t, st, runSnap("2026-04-02T00:00:00Z", []string{"c"}, nccFinding{Cluster: "c", Check: "active"}))
	doc, _ := st.read()
	if len(doc.Alerts) != 1 || doc.Alerts[0].Check != "active" {
		t.Fatalf("retention kept/dropped the wrong alerts: %s", mustJSON(doc.Alerts))
	}
	for _, e := range doc.Events {
		if e.AlertID != doc.Alerts[0].ID {
			t.Fatalf("events of pruned alerts must go too: %+v", e)
		}
	}
}

func TestFilterAlertsCursorPagingAndScoping(t *testing.T) {
	st, _ := testStore(t)
	var fs []nccFinding
	for i := 0; i < 25; i++ {
		cluster := "allowed"
		if i%5 == 0 {
			cluster = "secret"
		}
		fs = append(fs, nccFinding{Cluster: cluster, Check: fmt.Sprintf("c%02d", i)})
	}
	mustIngest(t, st, runSnap(run1, []string{"allowed", "secret"}, fs...))
	doc, _ := st.read()
	permits := func(cluster, _ string) bool { return cluster == "allowed" }
	seen := map[string]bool{}
	cursor := alertCursor{}
	pages := 0
	for {
		items, next := filterAlerts(doc.Alerts, alertQuery{Limit: 7, Cursor: cursor}, permits)
		for _, a := range items {
			if a.Cluster != "allowed" {
				t.Fatalf("scoped caller saw %s", a.Cluster)
			}
			if seen[a.ID] {
				t.Fatalf("duplicate across pages: %s", a.ID)
			}
			seen[a.ID] = true
		}
		pages++
		if next == "" {
			break
		}
		c, err := decodeAlertCursor(next)
		if err != nil {
			t.Fatal(err)
		}
		cursor = c
	}
	if len(seen) != 20 || pages != 3 {
		t.Fatalf("paged %d items in %d pages", len(seen), pages)
	}
	if _, err := decodeAlertCursor("not-a-cursor"); err == nil {
		t.Fatal("invalid cursor accepted")
	}
}

// End-to-end through the HTTP handlers, including the reported Kubernetes
// scenario: the runner's retention deletes outputfiles/* between runs.
func TestNCCAlertsAPIEndToEnd(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "outputfiles")
	writeRun := func(ts string, rows string) {
		t.Helper()
		if err := os.MkdirAll(out, 0o755); err != nil {
			t.Fatal(err)
		}
		sum := fmt.Sprintf(`{"timestamp":%q,"clusters":[{"address":"10.0.0.1","ok":true}]}`, ts)
		if err := os.WriteFile(filepath.Join(out, "run-summary.json"), []byte(sum), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(out, "index.html"), []byte("const AGG = "+rows+";\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeRun(run1, `[{"cluster":"10.0.0.1","clusterName":"prod-a","check":"CVM memory","severity":"FAIL"}]`)
	s := &apiServer{repoRoot: dir, outputDir: out}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/alerts/ncc-dispositions") {
			s.handleNCCDispositions(w, r)
			return
		}
		s.handleNCCAlerts(w, r)
	})
	do := func(method, path, body string, role Role) *httptest.ResponseRecorder {
		var req *http.Request
		if body != "" {
			req = httptest.NewRequest(method, path, bytes.NewBufferString(body))
			req.Header.Set("Content-Type", "application/json")
		} else {
			req = httptest.NewRequest(method, path, nil)
		}
		if need := routeMinRole(req); role < need {
			rec := httptest.NewRecorder()
			rec.Code = http.StatusForbidden
			return rec
		}
		req = withPrincipal(req, principal{subject: "op-" + role.String(), role: role})
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	type listResp struct {
		Data struct {
			Items      []nccAlert `json:"items"`
			NextCursor string     `json:"next_cursor"`
		} `json:"data"`
	}
	rec := do(http.MethodGet, "/api/v1/alerts/ncc", "", RoleViewer)
	var lr listResp
	if err := json.Unmarshal(rec.Body.Bytes(), &lr); err != nil || rec.Code != 200 || len(lr.Data.Items) != 1 {
		t.Fatalf("list: %d %s", rec.Code, rec.Body.String())
	}
	id := lr.Data.Items[0].ID
	if rec := do(http.MethodPost, "/api/v1/alerts/ncc/"+id+"/acknowledge", `{"note":"vendor case"}`, RoleViewer); rec.Code != http.StatusForbidden {
		t.Fatalf("viewer must not acknowledge: %d", rec.Code)
	}
	if rec := do(http.MethodPost, "/api/v1/alerts/ncc/"+id+"/acknowledge", `{"note":"vendor case"}`, RoleOperator); rec.Code != 200 {
		t.Fatalf("operator acknowledge: %d %s", rec.Code, rec.Body.String())
	}
	// Kubernetes regression: retention wipes outputfiles/ and a new run lands.
	if err := os.RemoveAll(out); err != nil {
		t.Fatal(err)
	}
	writeRun(run2, `[{"cluster":"10.0.0.1","clusterName":"prod-a","check":"CVM memory","severity":"FAIL"}]`)
	rec = do(http.MethodGet, "/api/v1/alerts/ncc-dispositions", "", RoleViewer)
	if !strings.Contains(rec.Body.String(), `"status":"acknowledged"`) || !strings.Contains(rec.Body.String(), id) {
		t.Fatalf("acknowledgement lost after new run: %s", rec.Body.String())
	}
	rec = do(http.MethodGet, "/api/v1/alerts/ncc/"+id, "", RoleViewer)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"type":"acknowledged"`) || !strings.Contains(rec.Body.String(), `"actor":"op-operator"`) {
		t.Fatalf("history: %d %s", rec.Code, rec.Body.String())
	}
	rec = do(http.MethodGet, "/api/v1/alerts/ncc/events?limit=1", "", RoleViewer)
	var ev struct {
		Data struct {
			Items      []nccAlertEvent `json:"items"`
			NextCursor string          `json:"next_cursor"`
			HasMore    bool            `json:"has_more"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &ev); err != nil || len(ev.Data.Items) != 1 || !ev.Data.HasMore {
		t.Fatalf("events page: %s", rec.Body.String())
	}
	rec = do(http.MethodGet, "/api/v1/alerts/ncc/events?cursor="+ev.Data.NextCursor, "", RoleViewer)
	if !strings.Contains(rec.Body.String(), `"type":"acknowledged"`) || strings.Contains(rec.Body.String(), `"seq":1,`) {
		t.Fatalf("events after cursor: %s", rec.Body.String())
	}
	if rec := do(http.MethodGet, "/api/v1/alerts/ncc/not-a-uuid", "", RoleViewer); rec.Code != http.StatusNotFound {
		t.Fatalf("bad id: %d", rec.Code)
	}
	if rec := do(http.MethodGet, "/api/v1/alerts/ncc?state=bogus", "", RoleViewer); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad state filter: %d", rec.Code)
	}
}

func TestRedactPathsHidesAbsolutePaths(t *testing.T) {
	cases := map[string]string{
		"open /data/auth/.ncc-api-token: permission denied":      "/data/auth",
		`open C:\ncc\config\config.yaml: The system cannot find`: `C:\ncc`,
		"read /data/config/config.yaml failed":                   "/data/config",
	}
	for in, leak := range cases {
		got := redactPaths(in)
		if strings.Contains(got, leak) {
			t.Fatalf("redactPaths(%q) = %q still leaks %q", in, got, leak)
		}
	}
}

func mustIngest(t *testing.T, st *alertLifecycleStore, snap nccRunSnapshot) {
	t.Helper()
	if _, err := st.ingestRun(snap); err != nil {
		t.Fatal(err)
	}
}

func TestAlertLifecycleMetrics(t *testing.T) {
	dir := t.TempDir()
	s := &apiServer{repoRoot: dir}
	mustIngest(t, s.alerts(), runSnap(run1, []string{"10.0.0.1"}, memFail, nccFinding{Cluster: "10.0.0.1", Check: "Disk", Severity: "WARN"}))
	if _, err := s.alerts().applyAction([]alertActionTarget{{Cluster: "10.0.0.1", Check: "Disk"}}, "acknowledge", "a", "", run1, nil); err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	s.writeAlertLifecycleMetrics(&b)
	out := b.String()
	for _, want := range []string{
		`ncc_alerts_active{state="open",detection="detected",severity="FAIL"} 1`,
		`ncc_alerts_active{state="acknowledged",detection="detected",severity="WARN"} 1`,
		"ncc_alerts_store_up 1",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in\n%s", want, out)
		}
	}
}
