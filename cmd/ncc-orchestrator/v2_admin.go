package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type v2ResetPasswordOptions struct {
	InstallDir   string
	User         string
	UsersDB      string
	UsersDBSec   string
	UsersDBSecNS string
}

// runV2ResetPassword recovers a lost account password offline by invoking the
// api-server's --reset-password against the same user store the stack uses. It
// locates the api binary in the install dir, defaults the user database to
// <install-dir>/.ncc-api-users.json (unless a Secret or explicit path is
// given), streams the child's output (which prints the new temporary
// password), and reminds the operator to restart the stack so it takes effect.
func runV2ResetPassword(opts v2ResetPasswordOptions) error {
	installDir := strings.TrimSpace(opts.InstallDir)
	if installDir == "" {
		installDir = defaultV2InstallDir()
	}
	if abs, err := filepath.Abs(installDir); err == nil {
		installDir = abs
	}
	if st, err := os.Stat(installDir); err != nil || !st.IsDir() {
		return fmt.Errorf("install dir not found: %s", installDir)
	}
	user := strings.TrimSpace(opts.User)
	if user == "" {
		user = "admin"
	}
	apiBin, _ := resolveV2APIBinary(installDir)
	if apiBin == "" {
		return fmt.Errorf("ncc-api-server binary not found near %s; place it in the install dir or on PATH", installDir)
	}

	args := []string{"--reset-password", user}
	usingSecret := strings.TrimSpace(opts.UsersDBSec) != ""
	if usingSecret {
		args = append(args, "--users-db-secret", strings.TrimSpace(opts.UsersDBSec))
		if ns := strings.TrimSpace(opts.UsersDBSecNS); ns != "" {
			args = append(args, "--users-db-secret-namespace", ns)
		}
	} else {
		usersDB := strings.TrimSpace(opts.UsersDB)
		if usersDB == "" {
			usersDB = filepath.Join(installDir, ".ncc-api-users.json")
		}
		if abs, err := filepath.Abs(usersDB); err == nil {
			usersDB = abs
		}
		if !isRegularFile(usersDB) {
			return fmt.Errorf("user database not found: %s (start the stack once to bootstrap it, or pass --users-db / --users-db-secret)", usersDB)
		}
		args = append(args, "--users-db", usersDB)
	}

	c := exec.Command(apiBin, args...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if err := c.Run(); err != nil {
		return fmt.Errorf("reset-password (%s): %w", apiBin, err)
	}
	if running, _ := v2StackRunning(installDir); running {
		fmt.Println("\nNOTE: the stack is currently running and caches accounts in memory.")
		fmt.Println("      Run 'ncc-orchestrator v2-stop' then 'ncc-orchestrator v2-start' for the new password to take effect.")
	} else {
		fmt.Println("\nStart the stack with 'ncc-orchestrator v2-start'; log in with the temporary password and change it at first login.")
	}
	return nil
}

func runUninstallCommand(cmd *cobra.Command, args []string) error {
	configPath, _ := cmd.Flags().GetString("config")
	installDir, _ := cmd.Flags().GetString("install-dir")
	taskName, _ := cmd.Flags().GetString("task-name")
	force, _ := cmd.Flags().GetBool("force")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	removeLocal, _ := cmd.Flags().GetBool("remove-local")
	removeSchedule, _ := cmd.Flags().GetBool("remove-schedule")
	removeV2Runtime, _ := cmd.Flags().GetBool("remove-v2-runtime")

	opts := uninstallOptions{
		ConfigPath:      strings.TrimSpace(configPath),
		InstallDir:      strings.TrimSpace(installDir),
		TaskName:        strings.TrimSpace(taskName),
		Force:           force,
		DryRun:          dryRun,
		RemoveLocal:     removeLocal,
		RemoveSchedule:  removeSchedule,
		RemoveV2Runtime: removeV2Runtime,
	}
	if opts.InstallDir == "" {
		opts.InstallDir = defaultV2InstallDir()
	}
	if opts.TaskName == "" {
		opts.TaskName = "ncc-orchestrator"
	}
	fmt.Fprintf(os.Stderr, "Uninstall plan:\n")
	fmt.Fprintf(os.Stderr, "  remove-local    : %t\n", opts.RemoveLocal)
	fmt.Fprintf(os.Stderr, "  remove-schedule : %t\n", opts.RemoveSchedule)
	fmt.Fprintf(os.Stderr, "  remove-v2-runtime: %t\n", opts.RemoveV2Runtime)
	fmt.Fprintf(os.Stderr, "  dry-run         : %t\n", opts.DryRun)

	if opts.RemoveSchedule {
		if opts.DryRun {
			fmt.Fprintf(os.Stderr, "[dry-run] would remove scheduler task marker %q\n", scheduleMarker(opts.TaskName))
		} else {
			if runtime.GOOS == "windows" {
				if err := removeWindowsSchedule(opts.TaskName); err != nil {
					fmt.Fprintf(os.Stderr, "schedule remove warning: %v\n", err)
				}
			} else {
				if err := removeCronSchedule(opts.TaskName); err != nil {
					fmt.Fprintf(os.Stderr, "schedule remove warning: %v\n", err)
				}
			}
		}
	}

	if opts.RemoveV2Runtime {
		if opts.DryRun {
			fmt.Fprintf(os.Stderr, "[dry-run] would stop detached v2 services from install-dir %s\n", opts.InstallDir)
		} else {
			stopErr := runV2Stop(v2StopOptions{
				InstallDir:  opts.InstallDir,
				Force:       opts.Force,
				StopTimeout: 5 * time.Second,
			})
			if stopErr != nil {
				fmt.Fprintf(os.Stderr, "v2-stop warning: %v\n", stopErr)
			}
		}
	}

	if opts.RemoveLocal {
		dirsToRemove := map[string]bool{}
		filesToRemove := map[string]bool{
			".ncc-api-token":              true,
			".ncc-api-schedule.json":      true,
			".ncc-api-notifications.json": true,
			"ncc-orchestrator.new.exe":    true,
			"apply-ncc-update.cmd":        true,
			".ncc-preflight-check":        true,
			".ncc-prefight-check":         true,
		}
		if opts.RemoveV2Runtime {
			dirsToRemove[opts.InstallDir] = true
		}

		// Defaults used by runner. Cover both layouts:
		//   * legacy v2.0.0/v2.0.1: paths relative to CWD
		//   * v2.0.2+: paths relative to install-dir (auto-detected when
		//     running from inside a bootstrapped stack)
		// In both modes RemoveV2Runtime=true nukes the install-dir wholesale
		// so the install-dir-relative entries below are belt-and-braces for
		// the rare --remove-v2-runtime=false invocation.
		dirsToRemove["nccfiles"] = true
		dirsToRemove["outputfiles"] = true
		dirsToRemove["promfiles"] = true
		dirsToRemove["logs"] = true
		if installDirAbs := strings.TrimSpace(opts.InstallDir); installDirAbs != "" {
			dirsToRemove[filepath.Join(installDirAbs, "nccfiles")] = true
			dirsToRemove[filepath.Join(installDirAbs, "outputfiles")] = true
			dirsToRemove[filepath.Join(installDirAbs, "promfiles")] = true
			dirsToRemove[filepath.Join(installDirAbs, "logs")] = true
			filesToRemove[filepath.Join(installDirAbs, ".ncc-api-token")] = true
			filesToRemove[filepath.Join(installDirAbs, ".ncc-api-schedule.json")] = true
			filesToRemove[filepath.Join(installDirAbs, ".ncc-api-notifications.json")] = true
		}

		if opts.ConfigPath != "" {
			cfg, err := loadConfigForValidation(opts.ConfigPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "config parse warning (%s): %v\n", opts.ConfigPath, err)
			} else {
				if strings.TrimSpace(cfg.OutputDirLogs) != "" {
					dirsToRemove[cfg.OutputDirLogs] = true
				}
				if strings.TrimSpace(cfg.OutputDirFiltered) != "" {
					dirsToRemove[cfg.OutputDirFiltered] = true
				}
				if strings.TrimSpace(cfg.PromDir) != "" {
					dirsToRemove[cfg.PromDir] = true
				}
				if strings.TrimSpace(cfg.RunHistoryDir) != "" {
					dirsToRemove[cfg.RunHistoryDir] = true
				}
				if strings.TrimSpace(cfg.LogFile) != "" {
					dirsToRemove[filepath.Dir(cfg.LogFile)] = true
				}
			}
		}

		for f := range filesToRemove {
			if opts.DryRun {
				fmt.Fprintf(os.Stderr, "[dry-run] would remove file %s\n", f)
				continue
			}
			if err := os.Remove(f); err != nil && !errors.Is(err, os.ErrNotExist) {
				fmt.Fprintf(os.Stderr, "remove file warning (%s): %v\n", f, err)
			}
		}
		for d := range dirsToRemove {
			clean := strings.TrimSpace(d)
			if clean == "" || clean == "." || clean == "/" {
				continue
			}
			if opts.DryRun {
				fmt.Fprintf(os.Stderr, "[dry-run] would remove directory %s\n", clean)
				continue
			}
			if err := os.RemoveAll(clean); err != nil {
				fmt.Fprintf(os.Stderr, "remove directory warning (%s): %v\n", clean, err)
			}
		}
	}

	fmt.Fprintln(os.Stderr, "Uninstall completed.")
	return nil
}
