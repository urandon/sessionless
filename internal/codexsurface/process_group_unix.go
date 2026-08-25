//go:build darwin || linux

package codexsurface

import (
	"errors"
	"os/exec"
	"syscall"
)

type probeProcessGroupSignal uint8

const (
	probeProcessGroupTerminate probeProcessGroupSignal = iota + 1
	probeProcessGroupKill
)

func configureProbeProcessGroup(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killProbeProcessGroup(command *exec.Cmd) {
	if command == nil || command.Process == nil {
		return
	}
	if err := signalProbeProcessGroup(command.Process.Pid, probeProcessGroupKill); err != nil {
		_ = command.Process.Kill()
	}
}

func signalProbeProcessGroup(processGroupID int, signal probeProcessGroupSignal) error {
	if processGroupID <= 0 {
		return errors.New("invalid probe process group")
	}
	var platformSignal syscall.Signal
	switch signal {
	case probeProcessGroupTerminate:
		platformSignal = syscall.SIGTERM
	case probeProcessGroupKill:
		platformSignal = syscall.SIGKILL
	default:
		return errors.New("invalid probe process group signal")
	}
	return syscall.Kill(-processGroupID, platformSignal)
}

func probeProcessGroupAlive(processGroupID int) (bool, error) {
	if processGroupID <= 0 {
		return false, errors.New("invalid probe process group")
	}
	err := syscall.Kill(-processGroupID, 0)
	switch {
	case err == nil, errors.Is(err, syscall.EPERM):
		return true, nil
	case errors.Is(err, syscall.ESRCH):
		return false, nil
	default:
		return false, err
	}
}
