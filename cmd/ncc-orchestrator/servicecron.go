package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// A schedule that a root service put in root's crontab (the "auto" type before
// U5) keeps running as root after the service became unprivileged. The
// migration moves it: a systemd timer for the service user, created by the
// service itself through the service helper, and the crontab line removed. A
// rollback puts root's crontab back and removes the timer.

const cronDefPrefix = "cron:"

// cronSaved is what a rollback needs of root's crontab.
type cronSaved struct {
	Task    string `json:"task"`
	Crontab string `json:"crontab"`
}

// crontabCommand reads or writes root's crontab; a variable for tests.
var crontabCommand = func(stdin *string, args ...string) (string, error) {
	c := exec.Command("crontab", args...)
	if stdin != nil {
		c.Stdin = strings.NewReader(*stdin)
	}
	out, err := c.CombinedOutput()
	return string(out), err
}

// rootCrontab returns root's crontab ("" when it has none).
func rootCrontab() (string, bool) {
	if _, err := exec.LookPath("crontab"); err != nil {
		return "", false
	}
	out, err := crontabCommand(nil, "-l")
	if err != nil {
		return "", strings.Contains(strings.ToLower(out), "no crontab")
	}
	return out, true
}

// cronScheduleToMove returns the change for the saved schedule when root's
// crontab runs it and no systemd timer does.
func (s *systemdService) cronScheduleToMove() (defChange, bool) {
	if !s.unprivileged() || os.Geteuid() != 0 {
		return defChange{}, false
	}
	task := savedScheduleTask(s.o.InstallDir)
	if task == "" || fileExists(s.unit(systemdUnitBase(task)+".timer")) {
		return defChange{}, false
	}
	tab, ok := rootCrontab()
	if !ok {
		return defChange{}, false
	}
	if _, found := removeScheduleLine(tab, scheduleMarker(task)); !found {
		return defChange{}, false
	}
	return defChange{ID: cronDefPrefix + task, Title: "Schedule " + task, Action: defReplace,
		Detail: "moved from root's crontab to a systemd timer of " + s.o.ServiceUser}, true
}

// saveCron records root's crontab for a rollback.
func saveCron(task string) (savedDefinition, error) {
	tab, ok := rootCrontab()
	if !ok {
		return savedDefinition{}, errors.New("root's crontab cannot be read")
	}
	b, _ := json.Marshal(cronSaved{Task: task, Crontab: tab})
	return savedDefinition{ID: cronDefPrefix + task, Body: string(b)}, nil
}

// moveCronSchedule creates the timer as the service user, then removes the
// crontab line. Run after the service helper is enabled.
func (s *systemdService) moveCronSchedule(task string) error {
	b, err := os.ReadFile(filepath.Join(s.o.InstallDir, ".ncc-api-schedule.json"))
	if err != nil {
		return err
	}
	var st map[string]interface{}
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	cfg := scheduleStateString(st, "config")
	if cfg == "" {
		cfg = filepath.Join(s.o.InstallDir, "config.yaml")
	}
	// create-schedule only previews unless told otherwise.
	args := []string{"create-schedule", "--type", "systemd", "--action", "create", "--print-only=false", "--config", cfg, "--task-name", task}
	if v := scheduleStateString(st, "cron"); v != "" {
		args = append(args, "--cron", v)
	} else if v := scheduleStateString(st, "every"); v != "" {
		args = append(args, "--every", v)
	}
	if v := scheduleStateString(st, "log_path"); v != "" {
		args = append(args, "--log-path", v)
	} else {
		args = append(args, "--log-path", filepath.Join(s.o.InstallDir, "logs", "ncc-scheduler.log"))
	}
	if v := scheduleStateString(st, "with_lock"); v == "false" {
		args = append(args, "--with-lock=false")
	}
	out, err := serviceUserRunner(s.o.InstallDir)(2*time.Minute, s.o.OrchestratorBin, args...)
	if err != nil {
		return fmt.Errorf("create the systemd timer as %s: %s", s.o.ServiceUser, upgradeErrorLine(out, err))
	}
	// Never drop the crontab line before the timer is there.
	if !fileExists(s.unit(systemdUnitBase(task) + ".timer")) {
		return fmt.Errorf("create the systemd timer as %s: no %s.timer was written (%s)", s.o.ServiceUser, systemdUnitBase(task), strings.TrimSpace(out))
	}
	tab, ok := rootCrontab()
	if !ok {
		return nil
	}
	updated, found := removeScheduleLine(tab, scheduleMarker(task))
	if !found {
		return nil
	}
	if out, err := crontabCommand(&updated, "-"); err != nil {
		return fmt.Errorf("remove the line from root's crontab: %v (%s)", err, strings.TrimSpace(out))
	}
	return nil
}

// restoreCron puts root's crontab back and removes the timer made from it.
func (s *systemdService) restoreCron(d savedDefinition) error {
	var c cronSaved
	dec := json.NewDecoder(strings.NewReader(d.Body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil || validateScheduleTaskName(c.Task) != nil || cronDefPrefix+c.Task != d.ID {
		return fmt.Errorf("the saved crontab cannot be read")
	}
	if err := removeScheduleUnits(s, c.Task); err != nil {
		return err
	}
	if _, err := crontabCommand(&c.Crontab, "-"); err != nil {
		return fmt.Errorf("put root's crontab back: %w", err)
	}
	return nil
}
