package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGenerateCSVMatchesGolden(t *testing.T) {
	blocks := []ParsedBlock{
		{Severity: "FAIL", CheckName: "sample_check", DetailRaw: "Sample detail line"},
	}
	dir := t.TempDir()
	out := filepath.Join(dir, "out.csv")
	if err := generateCSV(OSFS{}, blocks, out); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join("testdata", "golden", "simple.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("CSV mismatch\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestGenerateHTMLMatchesGolden(t *testing.T) {
	prev := htmlNowForReport
	htmlNowForReport = func() time.Time {
		return time.Date(2020, 1, 2, 15, 4, 5, 0, time.UTC)
	}
	t.Cleanup(func() { htmlNowForReport = prev })

	rows := []Row{
		{Severity: "FAIL", CheckName: "sample_check", Detail: "Sample detail line"},
	}
	meta := HTMLMeta{
		ClusterName:     "golden-cluster",
		ClusterVersion:  "6.8",
		NCCVersion:      "ncc-4.0.0",
	}
	dir := t.TempDir()
	out := filepath.Join(dir, "out.html")
	if err := generateHTML(OSFS{}, rows, out, meta); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join("testdata", "golden", "simple.html"))
	if err != nil {
		t.Fatal(err)
	}
	// Normalize EOF newline: template output may omit final \n; golden files often end with one.
	got = bytes.TrimSuffix(got, []byte("\n"))
	want = bytes.TrimSuffix(want, []byte("\n"))
	if !bytes.Equal(got, want) {
		t.Fatalf("HTML mismatch\ngot:\n%s\nwant:\n%s", got, want)
	}
}
