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

// kubernetesRunnerCronJob is the Kustomize CronJob name; the Helm chart names
// its CronJob ncc-orchestrator and passes it with NCC_RUNNER_CRONJOB.
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
		TimeZone string `json:"timeZone"`
	} `json:"spec"`
	Status struct {
		LastScheduleTime   string `json:"lastScheduleTime"`
		LastSuccessfulTime string `json:"lastSuccessfulTime"`
		Active             []struct {
			Name string `json:"name"`
		} `json:"active"`
	} `json:"status"`
}

// kubernetesSchedule is the CronJob as Settings > Schedule sees it.
type kubernetesSchedule struct {
	State              scheduleState
	Name               string
	Suspended          bool
	TimeZone           string
	LastScheduleTime   string
	LastSuccessfulTime string
	ActiveJobs         int
}

func (s *apiServer) runnerCronJobName() string {
	return defaultIfEmpty(s.runnerCronJob, kubernetesRunnerCronJob)
}

func (s *apiServer) kubernetesScheduleClient() (*k8sSecretClient, error) {
	if s.scheduleK8sClient != nil {
		return s.scheduleK8sClient()
	}
	return newInClusterSecretClient("")
}

// cronJobError turns a Kubernetes API failure into a message that says what
// to fix: the usual causes are a missing RBAC rule or a CronJob name that does
// not match the deployment (Helm and Kustomize name it differently).
func (s *apiServer) cronJobError(code int, body []byte) error {
	name := s.runnerCronJobName()
	switch code {
	case http.StatusNotFound:
		return fmt.Errorf("CronJob %q not found; set --runner-cronjob or NCC_RUNNER_CRONJOB to the runner CronJob's name", name)
	case http.StatusForbidden:
		return fmt.Errorf("the API service account may not read or patch CronJob %q; grant get and patch on cronjobs.batch for that name (see k8s/rbac.yaml)", name)
	}
	return apiError(code, body)
}

func (s *apiServer) readKubernetesSchedule() (kubernetesSchedule, error) {
	client, err := s.kubernetesScheduleClient()
	if err != nil {
		return kubernetesSchedule{}, err
	}
	code, body, err := client.do(http.MethodGet, client.cronJobPath(s.runnerCronJobName()), "", nil)
	if err != nil {
		return kubernetesSchedule{}, fmt.Errorf("read Kubernetes CronJob: %w", err)
	}
	if code != http.StatusOK {
		return kubernetesSchedule{}, s.cronJobError(code, body)
	}
	var job kubernetesCronJob
	if err := json.Unmarshal(body, &job); err != nil {
		return kubernetesSchedule{}, fmt.Errorf("decode Kubernetes CronJob: %w", err)
	}
	cron := strings.TrimSpace(job.Spec.Schedule)
	// The interval annotation is only trusted while it still produces the
	// live schedule; kubectl, Helm or Kustomize may have changed the cron.
	every := strings.TrimSpace(job.Metadata.Annotations[kubernetesScheduleAnnotation])
	if derived, err := kubernetesCron(every); err != nil || derived != cron {
		every = kubernetesEvery(cron)
	}
	action := "create"
	if job.Spec.Suspend {
		action = "remove"
	}
	return kubernetesSchedule{
		State: scheduleState{
			Type:      "auto",
			Action:    action,
			Cron:      cron,
			Every:     every,
			Config:    s.configPath,
			WithLock:  true,
			TaskName:  job.Metadata.Name,
			UpdatedAt: job.Metadata.Annotations[kubernetesScheduleUpdated],
		},
		Name:               job.Metadata.Name,
		Suspended:          job.Spec.Suspend,
		TimeZone:           job.Spec.TimeZone,
		LastScheduleTime:   job.Status.LastScheduleTime,
		LastSuccessfulTime: job.Status.LastSuccessfulTime,
		ActiveJobs:         len(job.Status.Active),
	}, nil
}

func (s *apiServer) loadKubernetesSchedule() (scheduleState, error) {
	ks, err := s.readKubernetesSchedule()
	return ks.State, err
}

var scheduleEveryPattern = regexp.MustCompile(`^([1-9][0-9]*)([mhd])$`)

// kubernetesCron converts the UI's interval form to a CronJob schedule.
// CronJob schedules are wall-clock based, so an interval must divide its
// period evenly: "*/45 * * * *" fires at :00 and :45, i.e. 45 then 15 minutes
// apart, which is not "every 45 minutes".
func kubernetesCron(every string) (string, error) {
	m := scheduleEveryPattern.FindStringSubmatch(strings.TrimSpace(every))
	if len(m) != 3 {
		return "", fmt.Errorf("interval must look like 15m, 4h, or 1d")
	}
	n, _ := strconv.Atoi(m[1])
	switch m[2] {
	case "m":
		if n == 60 {
			return "0 * * * *", nil
		}
		if n > 59 || 60%n != 0 {
			return "", fmt.Errorf("a minute interval must divide an hour evenly (1, 2, 3, 4, 5, 6, 10, 12, 15, 20 or 30m); use a custom cron expression for other times")
		}
		return fmt.Sprintf("*/%d * * * *", n), nil
	case "h":
		if n == 24 {
			return "0 0 * * *", nil
		}
		if n > 23 || 24%n != 0 {
			return "", fmt.Errorf("an hour interval must divide a day evenly (1, 2, 3, 4, 6, 8, 12 or 24h); use a custom cron expression for other times")
		}
		if n == 1 {
			return "0 * * * *", nil
		}
		return fmt.Sprintf("0 */%d * * *", n), nil
	case "d":
		switch n {
		case 1:
			return "0 0 * * *", nil
		case 7:
			return "0 0 * * 0", nil
		}
		return "", fmt.Errorf("a day interval must be 1d or 7d (day-of-month steps restart each month); use a custom cron expression for other times")
	}
	return "", fmt.Errorf("unsupported interval unit %q", m[2])
}

// kubernetesEvery is the inverse of kubernetesCron for the schedules it
// produces, so the UI can show a CronJob set by kubectl or Helm as an
// interval. Other expressions return "" and stay as custom cron.
func kubernetesEvery(cron string) string {
	f := strings.Fields(cron)
	if len(f) != 5 {
		return ""
	}
	switch strings.Join(f, " ") {
	case "0 * * * *":
		return "1h"
	case "0 0 * * *":
		return "1d"
	case "0 0 * * 0":
		return "7d"
	}
	step := func(field string) int {
		if !strings.HasPrefix(field, "*/") {
			return 0
		}
		n, err := strconv.Atoi(field[2:])
		if err != nil {
			return 0
		}
		return n
	}
	if f[2] != "*" || f[3] != "*" || f[4] != "*" {
		return ""
	}
	if n := step(f[0]); n > 0 && f[1] == "*" {
		return strconv.Itoa(n) + "m"
	}
	if n := step(f[1]); n > 0 && f[0] == "0" {
		return strconv.Itoa(n) + "h"
	}
	return ""
}

// validKubernetesCron accepts the five-field form and the @-macros the
// CronJob controller understands. The API server checks the fields properly
// on PATCH; this rejects shapes it would refuse.
func validKubernetesCron(cron string) error {
	switch cron {
	case "@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly":
		return nil
	}
	if !reCronSafe.MatchString(cron) || len(strings.Fields(cron)) != 5 {
		return fmt.Errorf("cron must have five fields (minute hour day-of-month month day-of-week), e.g. 15 */4 * * *")
	}
	return nil
}

// updateKubernetesSchedule patches the runner CronJob: action "create" sets
// the schedule and resumes it, "remove" pauses it (spec.suspend) and keeps
// the schedule so resuming restores it.
func (s *apiServer) updateKubernetesSchedule(st scheduleState) error {
	var spec map[string]interface{}
	annotations := map[string]interface{}{
		kubernetesScheduleUpdated: time.Now().UTC().Format(time.RFC3339),
	}
	switch st.Action {
	case "create":
		cron := strings.TrimSpace(st.Cron)
		every := strings.TrimSpace(st.Every)
		if cron == "" {
			var err error
			if cron, err = kubernetesCron(every); err != nil {
				return badScheduleInput{err}
			}
		} else if err := validKubernetesCron(cron); err != nil {
			return badScheduleInput{err}
		}
		// Record the interval only when it is what the cron says; a custom
		// cron clears it (null deletes the key in a merge patch).
		if derived, err := kubernetesCron(every); err == nil && derived == cron {
			annotations[kubernetesScheduleAnnotation] = every
		} else {
			annotations[kubernetesScheduleAnnotation] = nil
		}
		spec = map[string]interface{}{"schedule": cron, "suspend": false}
	case "remove":
		spec = map[string]interface{}{"suspend": true}
	default:
		return badScheduleInput{fmt.Errorf("action %q is not supported for the Kubernetes CronJob; use create or remove (pause)", st.Action)}
	}
	body, err := json.Marshal(map[string]interface{}{
		"metadata": map[string]interface{}{"annotations": annotations},
		"spec":     spec,
	})
	if err != nil {
		return err
	}
	client, err := s.kubernetesScheduleClient()
	if err != nil {
		return err
	}
	code, response, err := client.do(http.MethodPatch, client.cronJobPath(s.runnerCronJobName()), "application/merge-patch+json", body)
	if err != nil {
		return fmt.Errorf("update Kubernetes CronJob: %w", err)
	}
	if code == http.StatusUnprocessableEntity {
		return badScheduleInput{apiError(code, response)}
	}
	if code != http.StatusOK {
		return s.cronJobError(code, response)
	}
	return nil
}

// badScheduleInput marks errors caused by the request rather than the
// cluster, so the handler answers 400 instead of 502.
type badScheduleInput struct{ error }

func (e badScheduleInput) Unwrap() error { return e.error }

func (s *apiServer) handleKubernetesScheduleUpdate(w http.ResponseWriter, r *http.Request, st scheduleState) {
	err := s.updateKubernetesSchedule(st)
	s.audit(r, "schedule.update", err == nil, map[string]interface{}{
		"applied": err == nil, "controller": "CronJob", "cronjob": s.runnerCronJobName(),
		"action": st.Action, "cron": st.Cron, "every": st.Every,
	})
	if err != nil {
		status := http.StatusBadGateway
		if _, ok := err.(badScheduleInput); ok {
			status = http.StatusBadRequest
		}
		writeJSON(w, status, envelope{Success: false, Error: err.Error()})
		return
	}
	// Answer with what the CronJob now holds, not with what was asked for.
	saved, err := s.loadKubernetesSchedule()
	if err != nil {
		writeJSON(w, http.StatusBadGateway, envelope{Success: false, Error: "schedule updated but could not be read back: " + err.Error()})
		return
	}
	msg := "Kubernetes schedule updated"
	if saved.Action == "remove" {
		msg = "Kubernetes schedule paused"
	}
	writeJSON(w, http.StatusOK, envelope{
		Success: true, Message: msg,
		Data: map[string]interface{}{"schedule": saved, "applied": true, "controller": "CronJob"},
	})
}

func (s *apiServer) handleKubernetesScheduleHealth(w http.ResponseWriter) {
	ks, err := s.readKubernetesSchedule()
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, envelope{Success: false, Error: err.Error()})
		return
	}
	st := ks.State
	configured := !ks.Suspended && st.Cron != ""
	data := map[string]interface{}{
		"configured": configured, "saved": true, "installed": true,
		"controller_managed": true, "controller": "CronJob",
		"detector": "Kubernetes CronJob",
		"type":     "kubernetes", "action": st.Action, "cron": st.Cron, "every": st.Every,
		"config": st.Config, "task_name": ks.Name, "with_lock": true,
		"suspended":      ks.Suspended,
		"managed_by_ncc": st.UpdatedAt != "",
		"active_jobs":    ks.ActiveJobs,
		"message":        fmt.Sprintf("Scheduling is managed by the %s CronJob.", ks.Name),
	}
	if st.UpdatedAt != "" {
		data["last_updated_at"] = st.UpdatedAt
	}
	if ks.TimeZone != "" {
		data["time_zone"] = ks.TimeZone
	}
	if ks.LastScheduleTime != "" {
		data["last_schedule_time"] = ks.LastScheduleTime
	}
	if ks.LastSuccessfulTime != "" {
		data["last_successful_time"] = ks.LastSuccessfulTime
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
}

func (c *k8sSecretClient) cronJobPath(name string) string {
	return fmt.Sprintf("/apis/batch/v1/namespaces/%s/cronjobs/%s", c.namespace, name)
}
