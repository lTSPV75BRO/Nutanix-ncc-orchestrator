package nccparse

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testdata/*.log copy the layout of real NCC summaries from a Prism Element
// cluster and a Prism Central VM: several nodes per check, wrapped lists of
// VMs, blank lines before the KB line, "ERR :" with a space, Python byte
// strings such as b'/dev/sdf1' and separator lines between blocks. Check names and KB numbers are
// NCC's own; every address, VM, container, plan and licence is made up.
func TestParseSummaryRealShapedLogs(t *testing.T) {
	cases := []struct {
		file string
		want map[string]string // check name -> severity
	}{
		{
			file: "pe-cluster.log",
			want: map[string]string{
				"check_cvm_ssh_security":         "INFO",
				"ec_enablement_check":            "INFO",
				"gflags_diff_check":              "INFO",
				"backup_schedule_check":          "WARN",
				"host_boot_disk_uvm_check":       "WARN",
				"uvm_ide_disk_check":             "ERR",
				"file_server_container_check":    "WARN",
				"ngt_installer_version_check":    "INFO",
				"remote_site_config_check":       "FAIL",
				"vg_space_usage_check":           "FAIL",
				"storage_pool_space_usage_check": "FAIL",
				"ngt_client_cert_expiry_check":   "FAIL",
				"sufficient_disk_space_check":    "INFO",
				"weekly_pre_expiry":              "INFO",
			},
		},
		{
			file: "prism-central.log",
			want: map[string]string{
				"check_pcvm_ssh_security":                 "INFO",
				"pcvm_disk_usage_check":                   "WARN",
				"recovery_plan_entities_protection_check": "WARN",
				"recovery_plan_modified_network_check":    "WARN",
				"recovery_plan_last_test_status_check":    "INFO",
				"weekly_pre_expiry":                       "INFO",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.file, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join("testdata", tc.file))
			if err != nil {
				t.Fatal(err)
			}
			// Checked-out files may have CRLF line endings on Windows.
			blocks, err := ParseSummary(string(raw))
			if err != nil {
				t.Fatalf("ParseSummary: %v", err)
			}
			if len(blocks) != len(tc.want) {
				t.Fatalf("got %d blocks, want %d", len(blocks), len(tc.want))
			}
			for _, b := range blocks {
				name := strings.TrimSuffix(strings.TrimPrefix(b.CheckName, "Detailed information for "), ":")
				want, ok := tc.want[name]
				if !ok {
					t.Errorf("unexpected check %q", b.CheckName)
					continue
				}
				if b.Severity != want {
					t.Errorf("%s: severity %s, want %s", name, b.Severity, want)
				}
				// Each block runs to its KB line, so the detail keeps every node
				// and the link the UI turns into a KB button.
				if !strings.Contains(b.DetailRaw, "Refer to KB ") || !strings.Contains(b.DetailRaw, "portal.nutanix.com/kb/") {
					t.Errorf("%s: detail does not end with its KB line: %q", name, b.DetailRaw)
				}
				if strings.Contains(b.DetailRaw, "Detailed information for") {
					t.Errorf("%s: detail ran into the next block", name)
				}
			}
			if err := ValidateParsedAlertsAgainstPluginResults(string(raw), blocks); err != nil {
				t.Errorf("validate: %v", err)
			}
		})
	}
}

// A multi-node block keeps every node's lines, not only the first.
func TestParseSummaryKeepsEveryNode(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "pe-cluster.log"))
	if err != nil {
		t.Fatal(err)
	}
	blocks, _ := ParseSummary(string(raw))
	for _, b := range blocks {
		if !strings.HasSuffix(b.CheckName, "host_boot_disk_uvm_check:") {
			continue
		}
		for _, want := range []string{"vm-migrate-01", "vm-migrate-02", "vm-tools-01", "Node 10.10.20.33"} {
			if !strings.Contains(b.DetailRaw, want) {
				t.Errorf("detail is missing %q", want)
			}
		}
		return
	}
	t.Fatal("host_boot_disk_uvm_check not parsed")
}
