//go:build !darwin && !linux

package attachedworkerdaemon

import (
	"errors"
	"os/exec"
)

type processSignal uint8

const (
	processTerminate processSignal = iota + 1
	processKill
)

func configureProcessGroup(_ *exec.Cmd) {}

func signalProcessGroup(_ int, _ processSignal) error {
	return errors.New("attached worker process groups are unsupported")
}

func processGroupAlive(_ int) (bool, error) {
	return false, errors.New("attached worker process groups are unsupported")
}
