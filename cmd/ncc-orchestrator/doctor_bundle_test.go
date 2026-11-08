package main

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A support bundle carries every process log (not only the stack's console
// logs) and the environment files, redacted; rotated .gz logs stay out.
func TestDoctorBundleCarriesAllLogsAndEnvironments(t *testing.T) {
	dir := t.TempDir()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.MkdirAll(filepath.Join(dir, "logs"), 0o755))
	must(os.MkdirAll(filepath.Join(dir, "configs", "environments"), 0o755))
	write := func(rel, content string) {
		must(os.WriteFile(filepath.Join(dir, rel), []byte(content), 0o644))
	}
	write("logs/v2-api.log", "console line\n")
	write("logs/ncc-api.log", `{"level":"INFO","msg":"run accepted"}`+"\n")
	write("logs/ncc-runner.log", `{"level":"info","message":"cluster run started"}`+"\n")
	write("logs/ncc-api.log.1.gz", "rotated")
	write("config.yaml", "runner:\n  password: hunter2\n")
	write("configs/environments/prod.yaml", "runner:\n  targets:\n    clusters: [a]\n  api-token: s3cret\n")

	out := filepath.Join(dir, "bundle.tar.gz")
	must(writeDoctorBundle(out, "report", dir))

	f, err := os.Open(out)
	must(err)
	defer f.Close()
	gz, err := gzip.NewReader(f)
	must(err)
	tr := tar.NewReader(gz)
	files := map[string]string{}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		must(err)
		b, _ := io.ReadAll(tr)
		files[h.Name] = string(b)
	}
	for _, want := range []string{"report.txt", "logs/v2-api.log", "logs/ncc-api.log", "logs/ncc-runner.log", "config.redacted.yaml", "environments/prod.redacted.yaml"} {
		if _, ok := files[want]; !ok {
			t.Errorf("bundle is missing %s (has %v)", want, keys(files))
		}
	}
	if _, ok := files["logs/ncc-api.log.1.gz"]; ok {
		t.Error("rotated .gz log was bundled")
	}
	if !strings.Contains(files["logs/ncc-runner.log"], "cluster run started") {
		t.Errorf("runner log content: %q", files["logs/ncc-runner.log"])
	}
	if strings.Contains(files["environments/prod.redacted.yaml"], "s3cret") || strings.Contains(files["config.redacted.yaml"], "hunter2") {
		t.Error("secret left in a bundled config")
	}
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
