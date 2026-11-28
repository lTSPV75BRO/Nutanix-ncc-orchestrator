package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Version-skew protection on Kubernetes. The API server, the runner CronJob
// and the UI ship as separate images, so a partial rollout can leave them on
// different releases. The Updates page reports any difference; Run now is
// refused only when the runner's major.minor differs from the API server's,
// because runs write report data that the API server then reads.

// runnerImageCacheTTL bounds how often the CronJob is read for its image.
const runnerImageCacheTTL = time.Minute

// runnerImageCache keeps the runner CronJob's image tag briefly so the
// Updates page and Run now do not call the Kubernetes API on every request.
type runnerImageCache struct {
	mu  sync.Mutex
	at  time.Time
	tag string
}

// runnerContainerName is the runner container in both the Helm chart and
// k8s/runner-cronjob.yaml; other containers (init or sidecars) are skipped.
const runnerContainerName = "ncc-orchestrator"

type cronJobImages struct {
	Spec struct {
		JobTemplate struct {
			Spec struct {
				Template struct {
					Spec struct {
						Containers []struct {
							Name  string `json:"name"`
							Image string `json:"image"`
						} `json:"containers"`
					} `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		} `json:"jobTemplate"`
	} `json:"spec"`
}

// imageTag returns the tag of an image reference ("repo/name:tag@sha256:..."
// gives "tag"), or "" when the reference has no tag.
func imageTag(image string) string {
	image = strings.TrimSpace(image)
	if i := strings.Index(image, "@"); i >= 0 {
		image = image[:i]
	}
	colon := strings.LastIndex(image, ":")
	if colon < 0 || colon < strings.LastIndex(image, "/") {
		return ""
	}
	return image[colon+1:]
}

// runnerImageFromCronJob picks the runner container's image tag from a
// CronJob object, or "" when there is none.
func runnerImageFromCronJob(body []byte) string {
	var job cronJobImages
	if err := json.Unmarshal(body, &job); err != nil {
		return ""
	}
	containers := job.Spec.JobTemplate.Spec.Template.Spec.Containers
	for _, c := range containers {
		if c.Name == runnerContainerName {
			return imageTag(c.Image)
		}
	}
	if len(containers) == 1 {
		return imageTag(containers[0].Image)
	}
	return ""
}

// cronJobRunnerImageTag reads the runner image tag from the runner CronJob
// (the same GET Settings > Schedule makes, so it needs no extra RBAC). It
// returns "" when the CronJob cannot be read; failures are cached too, so an
// unreachable API is not retried on every request.
func (s *apiServer) cronJobRunnerImageTag() string {
	c := &s.runnerImage
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.at.IsZero() && time.Since(c.at) < runnerImageCacheTTL {
		return c.tag
	}
	c.tag = ""
	c.at = time.Now()
	client, err := s.kubernetesScheduleClient()
	if err != nil {
		return ""
	}
	code, body, err := client.do(http.MethodGet, client.cronJobPath(s.runnerCronJobName()), "", nil)
	if err != nil || code != http.StatusOK {
		return ""
	}
	c.tag = runnerImageFromCronJob(body)
	return c.tag
}

// k8sRunnerVersion is the runner's version and where it came from: the live
// CronJob's image tag when readable, else NCC_ORCHESTRATOR_IMAGE_TAG. Both
// empty means the version is unknown.
func (s *apiServer) k8sRunnerVersion() (version, source string) {
	if tag := s.cronJobRunnerImageTag(); tag != "" {
		return tag, "the " + s.runnerCronJobName() + " CronJob"
	}
	if tag := strings.TrimSpace(os.Getenv("NCC_ORCHESTRATOR_IMAGE_TAG")); tag != "" {
		return tag, "NCC_ORCHESTRATOR_IMAGE_TAG"
	}
	return "", ""
}

// versionMajorMinor returns "major.minor" for versions like v2.4.1 or
// 2.4.1-rc1, and false for tags that are not versions (latest, a digest).
func versionMajorMinor(v string) (string, bool) {
	parts := strings.Split(componentVersionCore(v), ".")
	if len(parts) < 2 {
		return "", false
	}
	for _, p := range parts[:2] {
		if _, err := strconv.Atoi(p); err != nil || p == "" {
			return "", false
		}
	}
	return parts[0] + "." + parts[1], true
}

// versionsCompatible reports whether two versions share major.minor. known
// is false when either is not a version, in which case nothing is blocked.
func versionsCompatible(a, b string) (compatible, known bool) {
	am, okA := versionMajorMinor(a)
	bm, okB := versionMajorMinor(b)
	if !okA || !okB {
		return true, false
	}
	return am == bm, true
}

// errorCodeVersionSkew marks a run refused because of version skew.
const errorCodeVersionSkew = "NCC_API_VERSION_SKEW"

// runnerVersionSkewError explains why a run cannot start on Kubernetes, or
// returns nil when the runner's version is compatible or unknown.
func (s *apiServer) runnerVersionSkewError() error {
	if !s.capabilities.Kubernetes {
		return nil
	}
	runner, source := s.k8sRunnerVersion()
	if runner == "" {
		return nil
	}
	if ok, known := versionsCompatible(Version, runner); !known || ok {
		return nil
	}
	serverLog.Warn("run refused: runner and API server versions differ", "subsystem", "runs", "runner_version", runner, "runner_version_source", source, "api_version", Version)
	return fmt.Errorf("the runner image is version %s (from %s) but the API server is version %s; runs need the same major.minor version. Roll out matching image tags with Helm or Kustomize, then start the run again", runner, source, Version)
}

// componentSkewMessage lists the components whose version differs from the
// API server's, for the Updates page. It returns "" when they all match.
func componentSkewMessage(raw map[string]interface{}, kubernetes bool) string {
	labels := map[string]string{"orchestrator": "orchestrator", "ui-server": "UI server"}
	if kubernetes {
		labels = map[string]string{"orchestrator": "runner image", "ui-server": "UI image"}
	}
	api, _ := raw["api-server"].(component)
	apiCore := componentVersionCore(api.Version)
	var diffs []string
	for _, name := range []string{"orchestrator", "ui-server"} {
		c, _ := raw[name].(component)
		switch {
		case c.Status != "ok" || c.Version == "":
			diffs = append(diffs, labels[name]+" version unknown")
		case componentVersionCore(c.Version) != apiCore:
			diffs = append(diffs, fmt.Sprintf("%s is %s", labels[name], c.Version))
		}
	}
	if len(diffs) == 0 {
		return ""
	}
	return fmt.Sprintf("API server is %s; %s.", defaultIfEmpty(api.Version, "unknown"), strings.Join(diffs, ", "))
}
