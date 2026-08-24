//go:build !darwin && !linux

package codexsurface

import "os/exec"

func configureProbeProcessGroup(_ *exec.Cmd) {}

func killProbeProcessGroup(command *exec.Cmd) {
	if command != nil && command.Process != nil {
		_ = command.Process.Kill()
	}
}
