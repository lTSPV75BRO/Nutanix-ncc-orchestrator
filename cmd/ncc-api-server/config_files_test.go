package main

import (
	"os"
	"strings"
	"testing"
)

func TestValidateClustersFileContent(t *testing.T) {
	ok := "10.1.1.1\ncluster.local,admin\ncluster2.local,admin,password\n"
	if err := validateClustersFileContent(ok); err != nil {
		t.Fatalf("expected valid clusters-file content, got: %v", err)
	}
	bad := "cluster.local,\n"
	if err := validateClustersFileContent(bad); err == nil {
		t.Fatal("expected username validation error")
	}
}

func TestValidateExcludeAlertTitlesFileContentRegex(t *testing.T) {
	ok := "AOS.*health\n"
	if err := validateExcludeAlertTitlesFileContent(ok, "regex"); err != nil {
		t.Fatalf("expected valid regex exclusions, got: %v", err)
	}
	bad := "([a-z\n"
	if err := validateExcludeAlertTitlesFileContent(bad, "regex"); err == nil {
		t.Fatal("expected invalid regex error")
	}
}

func TestValidateSecretsFileContent(t *testing.T) {
	if err := validateSecretsFileContent("password: secret\napi_key: xyz\n"); err != nil {
		t.Fatalf("expected valid YAML secrets map, got: %v", err)
	}
	if err := validateSecretsFileContent("{\"password\":\"secret\"}"); err != nil {
		t.Fatalf("expected valid JSON secrets map, got: %v", err)
	}
	if err := validateSecretsFileContent("password: 123\n"); err == nil {
		t.Fatal("expected non-string secrets value validation error")
	}
}

func TestDiscoverConfigRelatedFilesExcludesLogFile(t *testing.T) {
	tmp := t.TempDir()
	cfg := strings.Join([]string{
		"clusters-file: clusters.txt",
		"exclude-alert-titles-file: exclude.txt",
		"secrets-file: secrets.yaml",
		"log-file: logs/ncc-runner.log",
		"",
	}, "\n")
	if err := os.WriteFile(tmp+"/config.yaml", []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	s := &apiServer{
		repoRoot:   tmp,
		configPath: "config.yaml",
	}
	items, err := s.discoverConfigRelatedFiles()
	if err != nil {
		t.Fatalf("discoverConfigRelatedFiles failed: %v", err)
	}
	for _, item := range items {
		if item.Key == "log-file" {
			t.Fatalf("log-file should not be editable/discoverable, got %+v", item)
		}
	}
}
