//go:build darwin || linux

package codexsurface

import (
	"os/exec"
	"syscall"
)

func configureProbeProcessGroup(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killProbeProcessGroup(command *exec.Cmd) {
	if command == nil || command.Process == nil {
		return
	}
	if err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL); err != nil {
		_ = command.Process.Kill()
	}
}
