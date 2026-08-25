//go:build darwin || linux

package codexsurface

import (
	"sync"
	"time"
)

const processUsageSampleInterval = 10 * time.Millisecond

type processGroupUsage struct {
	PeakRSSBytes   int64
	MaxDescendants int
}

type processGroupUsageSampler struct {
	processGroupID int
	stopOnce       sync.Once
	stop           chan struct{}
	done           chan processGroupUsageSampleResult
}

type processGroupUsageSampleResult struct {
	usage processGroupUsage
	err   error
}

func startProcessGroupUsageSampler(processGroupID int) *processGroupUsageSampler {
	sampler := &processGroupUsageSampler{
		processGroupID: processGroupID,
		stop:           make(chan struct{}),
		done:           make(chan processGroupUsageSampleResult, 1),
	}
	go sampler.run()
	return sampler
}

func (sampler *processGroupUsageSampler) finish() (processGroupUsage, error) {
	sampler.stopOnce.Do(func() { close(sampler.stop) })
	result := <-sampler.done
	return result.usage, result.err
}

func (sampler *processGroupUsageSampler) run() {
	ticker := time.NewTicker(processUsageSampleInterval)
	defer ticker.Stop()
	var result processGroupUsageSampleResult
	for {
		usage, err := samplePlatformProcessGroupUsage(sampler.processGroupID)
		if err != nil {
			result.err = err
			sampler.done <- result
			return
		}
		if usage.PeakRSSBytes > result.usage.PeakRSSBytes {
			result.usage.PeakRSSBytes = usage.PeakRSSBytes
		}
		if usage.MaxDescendants > result.usage.MaxDescendants {
			result.usage.MaxDescendants = usage.MaxDescendants
		}
		select {
		case <-sampler.stop:
			sampler.done <- result
			return
		case <-ticker.C:
		}
	}
}
