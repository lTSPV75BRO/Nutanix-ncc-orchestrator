package runtimecaps

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Capabilities describes lifecycle responsibilities owned by the current
// platform. Host deployments retain the existing native service behavior;
// Kubernetes deployments defer lifecycle management to controllers.
type Capabilities struct {
	Mode                      string `json:"mode"`
	Kubernetes                bool   `json:"kubernetes"`
	ControllerManagedRestart  bool   `json:"controller_managed_restart"`
	ControllerManagedSchedule bool   `json:"controller_managed_schedule"`
	ImmutableUpdates          bool   `json:"immutable_updates"`
	PVCStorage                bool   `json:"pvc_storage"`
	Namespace                 string `json:"namespace,omitempty"`
	ServiceAccount            bool   `json:"service_account"`
}

func Detect() Capabilities {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("NCC_RUNTIME_MODE")))
	inCluster := os.Getenv("KUBERNETES_SERVICE_HOST") != "" &&
		os.Getenv("KUBERNETES_SERVICE_PORT") != ""
	tokenExists := fileExists("/var/run/secrets/kubernetes.io/serviceaccount/token")
	caExists := fileExists("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	kubernetes := mode == "kubernetes" || (mode != "host" && inCluster && (tokenExists || caExists))
	if kubernetes {
		return Capabilities{
			Mode:                      "kubernetes",
			Kubernetes:                true,
			ControllerManagedRestart:  true,
			ControllerManagedSchedule: true,
			ImmutableUpdates:          true,
			PVCStorage:                true,
			Namespace:                 readNamespace(),
			ServiceAccount:            tokenExists,
		}
	}
	return Capabilities{Mode: "host"}
}

func (c Capabilities) RejectHostOperation(operation string) error {
	if !c.Kubernetes {
		return nil
	}
	return fmt.Errorf("%s is managed by Kubernetes; use the Deployment or CronJob controller", operation)
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func readNamespace() string {
	b, err := os.ReadFile(filepath.Join("/var/run/secrets/kubernetes.io/serviceaccount", "namespace"))
	if err != nil {
		return strings.TrimSpace(os.Getenv("NCC_KUBERNETES_NAMESPACE"))
	}
	return strings.TrimSpace(string(b))
}
