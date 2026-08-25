//go:build darwin

package codexsurface

import (
	"os"
	"syscall"
)

func samplePlatformProcessGroupUsage(_ int) (processGroupUsage, error) {
	// Darwin's kinfo_proc.e_xrssize is the text resident-set size, not total
	// process RSS. Until a trustworthy bounded libproc-backed implementation is
	// available, aggregate PGID RSS and descendant counts are unavailable.
	return processGroupUsage{}, errProcessGroupUsageUnsupported
}

func processStatePeakRSS(state *os.ProcessState) int64 {
	if state == nil {
		return 0
	}
	usage, ok := state.SysUsage().(*syscall.Rusage)
	if !ok || usage.Maxrss < 0 {
		return 0
	}
	return usage.Maxrss
}
