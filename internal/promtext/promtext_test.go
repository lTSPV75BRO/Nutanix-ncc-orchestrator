package promtext

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"goncc/internal/model"
)

func TestRenderRunSummaryMetrics(t *testing.T) {
	out := RenderRunSummaryMetrics(RunSummaryView{
		Timestamp:      "2026-06-04T10:00:00Z",
		DurationS:      12.5,
		ClustersOK:     1,
		ClustersFailed: 1,
		ExitCode:       2,
		Clusters: []ClusterMetricRow{
			{Address: "10.0.0.1", OK: true, FailCount: 0, WarnCount: 1, ChecksTotal: 10, HealthScore: 95},
			{Address: "10.0.0.2", OK: false, FailCount: 3, ChecksTotal: 8, HealthScore: 40},
		},
	})
	for _, want := range []string{
		`ncc_cluster_up{cluster="10.0.0.1"} 1`,
		`ncc_cluster_up{cluster="10.0.0.2"} 0`,
		`ncc_cluster_checks_total{cluster="10.0.0.2",severity="FAIL"} 3`,
		`ncc_cluster_health_score{cluster="10.0.0.1"} 95`,
		"ncc_last_run_clusters_ok 1",
		"ncc_last_run_clusters_failed 1",
		"ncc_last_run_exit_code 2",
		"ncc_last_run_duration_seconds 12.500",
		"ncc_last_run_timestamp_seconds ",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("run-summary metrics missing %q in:\n%s", want, out)
		}
	}
}

// osFS is a minimal model.FS backed by the real filesystem for tests.
type osFS struct{}

func (osFS) MkdirAll(path string, perm os.FileMode) error { return os.MkdirAll(path, perm) }
func (osFS) WriteFile(path string, data []byte, perm os.FileMode) error {
	return os.WriteFile(path, data, perm)
}
func (osFS) ReadFile(path string) ([]byte, error)       { return os.ReadFile(path) }
func (osFS) ReadDir(path string) ([]os.DirEntry, error) { return os.ReadDir(path) }
func (osFS) Create(path string) (*os.File, error)       { return os.Create(path) }

func TestSanitizeLabel(t *testing.T) {
	got := SanitizeLabel("  a\"b\\c\nd  ")
	if got != `a\"b\\c d` {
		t.Fatalf("SanitizeLabel = %q", got)
	}
}

func TestWritePrometheusFile(t *testing.T) {
	tmp := t.TempDir()
	blocks := []model.ParsedBlock{
		{Severity: "FAIL", CheckName: "disk_check", DetailRaw: "bad"},
		{Severity: "WARN", CheckName: "mem_check", DetailRaw: "meh"},
		{Severity: "", CheckName: "info_check", DetailRaw: "x"}, // empty -> INFO
	}
	if err := WritePrometheusFile(osFS{}, tmp, "10.0.0.1", blocks); err != nil {
		t.Fatalf("WritePrometheusFile: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(tmp, "10.0.0.1.prom"))
	if err != nil {
		t.Fatalf("read prom: %v", err)
	}
	out := string(data)
	for _, want := range []string{
		`nutanix_ncc_check_total{cluster="10.0.0.1"} 3`,
		`nutanix_ncc_check_summary_total{cluster="10.0.0.1",severity="FAIL"} 1`,
		`nutanix_ncc_check_summary_total{cluster="10.0.0.1",severity="INFO"} 1`,
		`nutanix_ncc_run_has_failures{cluster="10.0.0.1"} 1`,
		`nutanix_ncc_check_result{cluster="10.0.0.1",check="info_check",severity="INFO"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("prom output missing %q", want)
		}
	}
}

func TestWriteNotificationMetricsFile(t *testing.T) {
	tmp := t.TempDir()
	channels := []string{"email", "webhook", "slack"}
	attempts := map[string]int{"email": 2, "webhook": 1}
	failures := map[string]int{"email": 1}
	if err := WriteNotificationMetricsFile(osFS{}, tmp, channels, attempts, failures); err != nil {
		t.Fatalf("WriteNotificationMetricsFile: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(tmp, "notifications.prom"))
	if err != nil {
		t.Fatalf("read notifications.prom: %v", err)
	}
	out := string(data)
	for _, want := range []string{
		`nutanix_ncc_notification_attempts_total{channel="email"} 2`,
		`nutanix_ncc_notification_failures_total{channel="email"} 1`,
		`nutanix_ncc_notification_attempts_total{channel="slack"} 0`,
		`nutanix_ncc_notification_failures_total{channel="slack"} 0`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("notifications output missing %q", want)
		}
	}
}
