package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const kubernetesRunnerCronJob = "ncc-v2-runner"

const (
	kubernetesScheduleAnnotation = "ncc.nutanix.com/schedule-every"
	kubernetesScheduleUpdated    = "ncc.nutanix.com/schedule-updated-at"
)

type kubernetesCronJob struct {
	Metadata struct {
		Name        string            `json:"name"`
		Annotations map[string]string `json:"annotations"`
	} `json:"metadata"`
	Spec struct {
		Schedule string `json:"schedule"`
		Suspend  bool   `json:"suspend"`
	} `json:"spec"`
}

func (s *apiServer) kubernetesScheduleClient() (*k8sSecretClient, error) {
	return newInClusterSecretClient("")
}

func (s *apiServer) loadKubernetesSchedule() (scheduleState, error) {
	client, err := s.kubernetesScheduleClient()
	if err != nil {
		return scheduleState{}, err
	}
	code, body, err := client.do(http.MethodGet, client.cronJobPath(kubernetesRunnerCronJob), "", nil)
	if err != nil {
		return scheduleState{}, fmt.Errorf("read Kubernetes CronJob: %w", err)
	}
	if code != http.StatusOK {
		return scheduleState{}, apiError(code, body)
	}
	var job kubernetesCronJob
	if err := json.Unmarshal(body, &job); err != nil {
		return scheduleState{}, fmt.Errorf("decode Kubernetes CronJob: %w", err)
	}
	updated := job.Metadata.Annotations[kubernetesScheduleUpdated]
	every := job.Metadata.Annotations[kubernetesScheduleAnnotation]
	action := "create"
	if job.Spec.Suspend {
		action = "remove"
	}
	return scheduleState{
		Type:      "auto",
		Action:    action,
		Cron:      strings.TrimSpace(job.Spec.Schedule),
		Every:     strings.TrimSpace(every),
		Config:    s.configPath,
		WithLock:  true,
		PrintOnly: false,
		UpdatedAt: updated,
	}, nil
}

var scheduleEveryPattern = regexp.MustCompile(`^([1-9][0-9]*)([mhd])$`)

// kubernetesCron converts the UI's interval form to a Kubernetes CronJob
// expression. Kubernetes schedules are wall-clock based, so intervals are
// rounded to minute/hour/day boundaries.
func kubernetesCron(every string) (string, error) {
	m := scheduleEveryPattern.FindStringSubmatch(strings.TrimSpace(every))
	if len(m) != 3 {
		return "", fmt.Errorf("interval must look like 15m, 4h, or 1d")
	}
	n, _ := strconv.Atoi(m[1])
	switch m[2] {
	case "m":
		if n > 59 {
			return "", fmt.Errorf("minute interval must be between 1m and 59m")
		}
		return fmt.Sprintf("*/%d * * * *", n), nil
	case "h":
		if n == 24 {
			return "0 0 * * *", nil
		}
		if n > 23 {
			return "", fmt.Errorf("hour interval must be between 1h and 24h")
		}
		return fmt.Sprintf("0 */%d * * *", n), nil
	case "d":
		if n > 31 {
			return "", fmt.Errorf("day interval must be between 1d and 31d")
		}
		if n == 1 {
			return "0 0 * * *", nil
		}
		return fmt.Sprintf("0 0 */%d * *", n), nil
	default:
		return "", fmt.Errorf("unsupported interval unit %q", m[2])
	}
}

func (s *apiServer) updateKubernetesSchedule(st scheduleState) (map[string]interface{}, error) {
	client, err := s.kubernetesScheduleClient()
	if err != nil {
		return nil, err
	}
	cron := strings.TrimSpace(st.Cron)
	if cron == "" && st.Action != "remove" {
		cron, err = kubernetesCron(st.Every)
		if err != nil {
			return nil, err
		}
	}
	if st.Action != "remove" && cron == "" {
		return nil, fmt.Errorf("schedule requires a cron expression or interval")
	}
	suspended := st.Action == "remove"
	annotations := map[string]string{
		kubernetesScheduleUpdated: time.Now().UTC().Format(time.RFC3339),
	}
	if strings.TrimSpace(st.Every) != "" {
		annotations[kubernetesScheduleAnnotation] = strings.TrimSpace(st.Every)
	}
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{"annotations": annotations},
		"spec": map[string]interface{}{
			"schedule": cron,
			"suspend":  suspended,
		},
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return nil, err
	}
	code, response, err := client.do(http.MethodPatch, client.cronJobPath(kubernetesRunnerCronJob), "application/merge-patch+json", body)
	if err != nil {
		return nil, fmt.Errorf("update Kubernetes CronJob: %w", err)
	}
	if code != http.StatusOK {
		return nil, apiError(code, response)
	}
	return map[string]interface{}{
		"schedule":   cron,
		"suspended":  suspended,
		"controller": "CronJob",
	}, nil
}

func (c *k8sSecretClient) cronJobPath(name string) string {
	return fmt.Sprintf("/apis/batch/v1/namespaces/%s/cronjobs/%s", c.namespace, name)
}
