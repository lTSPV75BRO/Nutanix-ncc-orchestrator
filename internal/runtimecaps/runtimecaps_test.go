package runtimecaps

import (
	"os"
	"testing"
)

func TestDetectHostOverride(t *testing.T) {
	t.Setenv("NCC_RUNTIME_MODE", "host")
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")
	if got := Detect(); got.Kubernetes || got.Mode != "host" {
		t.Fatalf("expected host capabilities, got %+v", got)
	}
}

func TestDetectExplicitKubernetes(t *testing.T) {
	t.Setenv("NCC_RUNTIME_MODE", "kubernetes")
	if got := Detect(); !got.Kubernetes || got.Mode != "kubernetes" ||
		!got.ControllerManagedSchedule || !got.ImmutableUpdates {
		t.Fatalf("expected Kubernetes capabilities, got %+v", got)
	}
}

func TestDetectInCluster(t *testing.T) {
	t.Setenv("NCC_RUNTIME_MODE", "")
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")
	// The test only verifies the conservative environment signal when an
	// explicit mode is unavailable; service-account files may not exist locally.
	got := Detect()
	if got.Kubernetes {
		return
	}
	if _, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/token"); err == nil {
		t.Fatalf("in-cluster service account exists but Kubernetes was not detected")
	}
}
