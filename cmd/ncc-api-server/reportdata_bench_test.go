package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// benchReportServer writes a run the size of a 500-cluster fleet: 25 findings and
// 450 checks per cluster, the shape /report/data serves to the dashboard.
func benchReportServer(tb testing.TB, clusters int) *apiServer {
	tb.Helper()
	dir := tb.TempDir()
	out := filepath.Join(dir, "outputfiles")
	var rows, snap strings.Builder
	rows.WriteString("[")
	snap.WriteString(`{"timestamp":"2026-11-23T00:00:00Z","clusters":[`)
	for c := 0; c < clusters; c++ {
		addr := fmt.Sprintf("10.%d.%d.%d", c/65025, (c/255)%255, c%255+1)
		for f := 0; f < 25; f++ {
			if c > 0 || f > 0 {
				rows.WriteString(",")
			}
			fmt.Fprintf(&rows, `{"cluster":%q,"clusterName":"cl-%d","check":"check_%d","severity":"FAIL","detail":"Volume Group vg-data-%d space usage (91 %%) above 90 %% Refer to KB 4600 (http://portal.nutanix.com/kb/4600) for details","clusterVersion":"7.3.1","nccVersion":"5.2.0","kb":"4600"}`, addr, c, f, f)
		}
		if c > 0 {
			snap.WriteString(",")
		}
		fmt.Fprintf(&snap, `{"address":%q,"name":"cl-%d","checks":[`, addr, c)
		for k := 0; k < 450; k++ {
			if k > 0 {
				snap.WriteString(",")
			}
			fmt.Fprintf(&snap, `{"check":"check_%d","status":"PASS","severity":"INFO"}`, k)
		}
		snap.WriteString("]}")
	}
	rows.WriteString("]")
	snap.WriteString("]}")
	writeTestFileTB(tb, filepath.Join(out, "index.html"), "const AGG = []\nconst AGG_DATA_URL = \"aggregated-data.json\"\n")
	writeTestFileTB(tb, filepath.Join(out, "aggregated-data.json"), rows.String())
	writeTestFileTB(tb, filepath.Join(out, "run-summary.json"), `{"timestamp":"2026-11-23T00:00:00Z","clusters":[]}`)
	writeTestFileTB(tb, filepath.Join(out, "checks-snapshot.json"), snap.String())
	return &apiServer{repoRoot: dir, outputDir: out}
}

func writeTestFileTB(tb testing.TB, path, body string) {
	tb.Helper()
	if t, ok := tb.(*testing.T); ok {
		writeTestFile(t, path, body)
		return
	}
	b := tb.(*testing.B)
	// reuse the helper's behaviour without a *testing.T
	if err := mkdirAndWrite(path, body); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkReportData500Clusters(b *testing.B) {
	s := benchReportServer(b, 500)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := withPrincipal(httptest.NewRequest(http.MethodGet, "/api/v1/report/data?limit=100", nil), principal{subject: "a", role: RoleAdmin})
		rec := httptest.NewRecorder()
		s.handleReportData(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status %d", rec.Code)
		}
	}
}

func mkdirAndWrite(path, body string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(body), 0o644)
}
