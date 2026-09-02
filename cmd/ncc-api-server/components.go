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
	orchestrator := s.orchestratorBin
	ui := filepath.Join(filepath.Dir(orchestrator), "ncc-ui-server")
	data := map[string]interface{}{
		"components": map[string]interface{}{
			"orchestrator": componentVersion(orchestrator, "verify"),
			"api-server":   component{Version: Version, Status: "ok"},
			"ui-server":    componentVersion(ui, "version"),
		},
	}
	components := data["components"].(map[string]interface{})
	components["consistent"] = componentVersionsMatch(components)
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
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
		versions = append(versions, strings.SplitN(c.Version, "-", 2)[0])
	}
	return versions[0] == versions[1] && versions[1] == versions[2]
}
