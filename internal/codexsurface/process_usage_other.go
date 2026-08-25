//go:build !darwin && !linux

package codexsurface

import (
	"errors"
	"os"
)

type processGroupUsage struct {
	PeakRSSBytes   int64
	MaxDescendants int
}

type processGroupUsageSampler struct{}

func startProcessGroupUsageSampler(_ int) *processGroupUsageSampler {
	return &processGroupUsageSampler{}
}

func (*processGroupUsageSampler) finish() (processGroupUsage, error) {
	return processGroupUsage{}, errors.New("process usage sampler is unsupported")
}

func processStatePeakRSS(_ *os.ProcessState) int64 { return 0 }
