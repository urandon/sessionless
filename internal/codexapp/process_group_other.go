//go:build !darwin && !linux

package codexapp

import "os/exec"

func configureProcessGroup(_ *exec.Cmd) {}

func killProcessGroup(command *exec.Cmd) {
	if command != nil && command.Process != nil {
		_ = command.Process.Kill()
	}
}
