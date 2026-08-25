//go:build !darwin && !linux

package codexsurface

import "os"

func samplePlatformProcessGroupUsage(_ int) (processGroupUsage, error) {
	return processGroupUsage{}, errProcessGroupUsageUnsupported
}

func processStatePeakRSS(_ *os.ProcessState) int64 { return 0 }
