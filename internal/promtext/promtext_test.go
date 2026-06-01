package promtext

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"goncc/internal/model"
)

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
