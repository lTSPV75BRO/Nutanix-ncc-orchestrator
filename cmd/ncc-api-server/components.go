package main

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func (s *apiServer) handleComponents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	var components map[string]interface{}
	if s.capabilities.Kubernetes {
		components = s.k8sComponentStatus()
	} else {
		orchestrator := s.orchestratorBin
		ui := filepath.Join(filepath.Dir(orchestrator), "ncc-ui-server")
		components = map[string]interface{}{
			"orchestrator": componentVersion(orchestrator, "verify"),
			"api-server":   component{Version: Version, Status: "ok"},
			"ui-server":    componentVersion(ui, "version"),
		}
	}
	components["consistent"] = componentVersionsMatch(components)
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{"components": components}})
}

func (s *apiServer) k8sComponentStatus() map[string]interface{} {
	imageTag := strings.TrimSpace(os.Getenv("NCC_IMAGE_TAG"))
	apiVer := strings.TrimSpace(Version)
	if apiVer == "" {
		apiVer = imageTag
	}
	orch := strings.TrimSpace(os.Getenv("NCC_ORCHESTRATOR_IMAGE_TAG"))
	if orch == "" {
		orch = imageTag
	}
	if orch == "" {
		orch = apiVer
	}
	ui := strings.TrimSpace(os.Getenv("NCC_UI_IMAGE_TAG"))
	if ui == "" {
		ui = imageTag
	}
	if ui == "" {
		ui = apiVer
	}
	if apiVer == "" {
		apiVer = orch
	}
	return map[string]interface{}{
		"orchestrator": component{Version: orch, Status: "ok"},
		"api-server":   component{Version: apiVer, Status: "ok"},
		"ui-server":    component{Version: ui, Status: "ok"},
	}
}

type component struct {
	Version string `json:"version"`
	Status  string `json:"status"`
}

func componentVersion(binary, command string) component {
	if binary == "" {
		return component{Status: "Component not found"}
	}
	if _, err := os.Stat(binary); err != nil {
		return component{Status: "Component not found"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, binary, command).CombinedOutput()
	if err != nil {
		return component{Status: "Component not found"}
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "version:") {
			return component{Version: strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "version:")), Status: "ok"}
		}
	}
	return component{Status: "Component not found"}
}

func componentVersionsMatch(raw map[string]interface{}) bool {
	var versions []string
	for _, name := range []string{"orchestrator", "api-server", "ui-server"} {
		c, ok := raw[name].(component)
		if !ok || c.Status != "ok" || c.Version == "" {
			return false
		}
		versions = append(versions, componentVersionCore(c.Version))
	}
	return versions[0] == versions[1] && versions[1] == versions[2]
}

func componentVersionCore(v string) string {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(v, "v")
	v = strings.TrimPrefix(v, "V")
	return strings.SplitN(v, "-", 2)[0]
}
