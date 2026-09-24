package main

import "testing"

func TestComponentVersionCoreStripsPrefixAndDirty(t *testing.T) {
	if got := componentVersionCore("v2.2.0-dirty-abc"); got != "2.2.0" {
		t.Fatalf("got %q", got)
	}
	if got := componentVersionCore("2.2.0"); got != "2.2.0" {
		t.Fatalf("got %q", got)
	}
}

func TestK8sComponentStatusUsesImageTags(t *testing.T) {
	t.Setenv("NCC_IMAGE_TAG", "2.2.0")
	t.Setenv("NCC_ORCHESTRATOR_IMAGE_TAG", "2.2.0")
	t.Setenv("NCC_UI_IMAGE_TAG", "2.2.0")
	s := &apiServer{}
	s.capabilities.Kubernetes = true
	comps := s.k8sComponentStatus()
	if !componentVersionsMatch(comps) {
		t.Fatalf("expected consistent image tags, got %#v", comps)
	}
}

func TestK8sDoctorCheckIDsOmitHostProcess(t *testing.T) {
	s := &apiServer{}
	s.capabilities.Kubernetes = true
	ids := s.doctorCheckIDs(nil)
	for _, id := range ids {
		if id == "stale-pids" || id == "runtime-mode-drift" || id == "tls-cert-expiry" {
			t.Fatalf("k8s doctor set must omit host check %s: %v", id, ids)
		}
	}
	filtered := s.doctorCheckIDs([]string{"stale-pids", "config-schema"})
	if len(filtered) != 1 || filtered[0] != "config-schema" {
		t.Fatalf("expected only config-schema, got %v", filtered)
	}
}
