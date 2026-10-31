package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Runs above 20,000 findings keep their rows in aggregated-data.json; the
// run-history snapshot must include it or the past run has no findings.
func TestRunHistorySnapshotKeepsAggregatedSidecar(t *testing.T) {
	out := t.TempDir()
	for name, body := range map[string]string{
		"index.html":           "const AGG = []\nconst AGG_DATA_URL = \"aggregated-data.json\"\n",
		"aggregated-data.json": `[{"cluster":"10.0.0.1","check":"c","severity":"FAIL"}]`,
		"run-summary.json":     `{"timestamp":"2026-10-31T00:00:00Z"}`,
	} {
		if err := os.WriteFile(filepath.Join(out, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cfg := Config{OutputDirFiltered: out, RunHistoryDir: filepath.Join(out, "runs")}
	runDir, err := writeRunHistorySnapshot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(runDir, "aggregated-data.json"))
	if err != nil || !strings.Contains(string(b), "10.0.0.1") {
		t.Fatalf("sidecar not in snapshot: %v %q", err, b)
	}
}

// The live path parses the summary from memory; it must match parsing the
// saved file.
func TestFilterBlocksFromTextMatchesFile(t *testing.T) {
	dir := t.TempDir()
	text := "Cluster Name: prod-a\nCluster Version: 6.8\nNCC Version: 5.0.2\n" +
		"Detailed information for disk_usage_check:\nNode 1:\nFAIL: disk 95%\nRefer to KB 1 for details.\n" +
		"Detailed information for ntp_check:\nNode 2:\nWARN: drift\nRefer to KB 2 for details.\n"
	raw := filepath.Join(dir, "raw.log")
	if err := os.WriteFile(raw, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
	fromFile, err := filterBlocksToFile(OSFS{}, raw, filepath.Join(dir, "a.log"))
	if err != nil {
		t.Fatal(err)
	}
	fromText, err := filterBlocksFromText(OSFS{}, text, raw, filepath.Join(dir, "b.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(fromFile) != 2 || len(fromText) != 2 {
		t.Fatalf("blocks: file=%d text=%d", len(fromFile), len(fromText))
	}
	for i := range fromFile {
		if fromFile[i] != fromText[i] {
			t.Fatalf("block %d differs: %+v vs %+v", i, fromFile[i], fromText[i])
		}
	}
	a, _ := os.ReadFile(filepath.Join(dir, "a.log"))
	b, _ := os.ReadFile(filepath.Join(dir, "b.log"))
	if string(a) != string(b) {
		t.Fatal("filtered files differ")
	}
	meta := parseNCCHeaderText(text)
	fileMeta, _ := parseNCCHeader(raw)
	if meta != fileMeta || meta.ClusterName != "prod-a" || meta.NCCVersion != "5.0.2" {
		t.Fatalf("meta = %+v, file = %+v", meta, fileMeta)
	}
}

func TestMaxParallelAllowsLargeFleets(t *testing.T) {
	if maxParallelLimit < 500 {
		t.Fatalf("maxParallelLimit = %d; fleets of thousands need hundreds in flight", maxParallelLimit)
	}
}
