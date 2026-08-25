//go:build linux

package codexsurface

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

const maxProcEntriesPerSample = 131072

func samplePlatformProcessGroupUsage(processGroupID int) (processGroupUsage, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil || len(entries) > maxProcEntriesPerSample {
		return processGroupUsage{}, errors.New("enumerate process group usage")
	}
	members := 0
	leaderPresent := false
	var aggregateRSS int64
	pageSize := int64(os.Getpagesize())
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		data, readErr := os.ReadFile(filepath.Join("/proc", entry.Name(), "stat"))
		if readErr != nil {
			continue
		}
		closing := strings.LastIndexByte(string(data), ')')
		if closing < 0 {
			continue
		}
		fields := strings.Fields(string(data[closing+1:]))
		if len(fields) <= 21 {
			continue
		}
		group, groupErr := strconv.Atoi(fields[2])
		if groupErr != nil || group != processGroupID {
			continue
		}
		rssPages, rssErr := strconv.ParseInt(fields[21], 10, 64)
		if rssErr != nil || rssPages < 0 || rssPages > (1<<63-1)/pageSize {
			return processGroupUsage{}, errors.New("invalid process group usage")
		}
		rssBytes := rssPages * pageSize
		if rssBytes > 1<<63-1-aggregateRSS {
			return processGroupUsage{}, errors.New("process group usage overflow")
		}
		aggregateRSS += rssBytes
		members++
		if entry.Name() == strconv.Itoa(processGroupID) {
			leaderPresent = true
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
	if !ok || usage.Maxrss < 0 || usage.Maxrss > (1<<63-1)/1024 {
		return 0
	}
	return usage.Maxrss * 1024
}
