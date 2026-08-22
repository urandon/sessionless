//go:build darwin || linux

package codexapp

import (
	"os/exec"
	"syscall"
)

func configureProcessGroup(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killProcessGroup(command *exec.Cmd) {
	if command == nil || command.Process == nil {
		return
	}
	// The child is its process-group leader. A negative pid addresses the
	// entire group, preventing tools or other descendants from surviving a
	// deadline or shutdown escalation.
	if err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL); err != nil {
		_ = command.Process.Kill()
	}
}
