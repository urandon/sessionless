//go:build darwin

package codexsurface

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func samplePlatformProcessGroupUsage(processGroupID int) (processGroupUsage, error) {
	processes, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return processGroupUsage{}, errors.New("enumerate process group usage")
	}
	members := 0
	leaderPresent := false
	var aggregateRSS int64
	pageSize := int64(os.Getpagesize())
	for _, process := range processes {
		if int(process.Eproc.Pgid) == processGroupID {
			members++
			if int(process.Proc.P_pid) == processGroupID {
				leaderPresent = true
			}
			// kern.proc.all exposes the current resident-set size in pages.
			// Ignore unavailable/overflowed legacy values, while retaining
			// the direct child's peak rusage as a lower bound at finish.
			residentPages := int64(process.Eproc.Xrssize)
			if residentPages > 0 {
				residentBytes := residentPages * pageSize
				if residentBytes > 1<<63-1-aggregateRSS {
					return processGroupUsage{}, errors.New("process group usage overflow")
				}
				aggregateRSS += residentBytes
			}
		}
	}
	descendants := members
	if leaderPresent {
		descendants--
	}
	return processGroupUsage{PeakRSSBytes: aggregateRSS, MaxDescendants: descendants}, nil
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
