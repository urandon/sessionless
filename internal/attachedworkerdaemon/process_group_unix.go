//go:build darwin || linux

package attachedworkerdaemon

import (
	"errors"
	"os/exec"
	"syscall"
)

type processSignal uint8

const (
	processTerminate processSignal = iota + 1
	processKill
)

func configureProcessGroup(command *exec.Cmd) {
	if command.SysProcAttr == nil {
		command.SysProcAttr = &syscall.SysProcAttr{}
	}
	command.SysProcAttr.Setpgid = true
}

func signalProcessGroup(processGroupID int, signal processSignal) error {
	if processGroupID <= 0 {
		return errors.New("invalid attached worker process group")
	}
	platformSignal := syscall.SIGTERM
	if signal == processKill {
		platformSignal = syscall.SIGKILL
	} else if signal != processTerminate {
		return errors.New("invalid attached worker process signal")
	}
	return syscall.Kill(-processGroupID, platformSignal)
}

func processGroupAlive(processGroupID int) (bool, error) {
	if processGroupID <= 0 {
		return false, errors.New("invalid attached worker process group")
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
