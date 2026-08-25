//go:build !darwin && !linux

package codexsurface

import (
	"errors"
	"os/exec"
)

type probeProcessGroupSignal uint8

const (
	probeProcessGroupTerminate probeProcessGroupSignal = iota + 1
	probeProcessGroupKill
)

func configureProbeProcessGroup(_ *exec.Cmd) {}

func killProbeProcessGroup(command *exec.Cmd) {
	if command != nil && command.Process != nil {
		_ = command.Process.Kill()
	}
}

func signalProbeProcessGroup(_ int, _ probeProcessGroupSignal) error {
	return errors.New("probe process groups are unsupported")
}

func probeProcessGroupAlive(_ int) (bool, error) {
	return false, errors.New("probe process groups are unsupported")
}
