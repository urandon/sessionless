package codexsurface

import (
	"errors"
	"sync"
	"time"
)

const (
	processUsageSampleInterval = 10 * time.Millisecond
	processUsageFinishBound    = 100 * time.Millisecond
)

var (
	errProcessGroupUsageUnsupported = errors.New("process group usage unsupported")
	errProcessGroupUsagePermission  = errors.New("process group usage permission denied")
	errProcessGroupUsageInvalid     = errors.New("process group usage invalid")
	errProcessGroupUsageUnavailable = errors.New("process group usage unavailable")
	errProcessGroupUsageTimeout     = errors.New("process group usage sampler timeout")
)

type processGroupUsage struct {
	PeakRSSBytes   int64
	MaxDescendants int
}

type processGroupUsageSampler struct {
	processGroupID int
	sample         processGroupUsageSampleFunc
	stopOnce       sync.Once
	stop           chan struct{}
	done           chan processGroupUsageSampleResult
}

type processGroupUsageSampleResult struct {
	usage processGroupUsage
	err   error
}

type processGroupUsageSampleFunc func(int) (processGroupUsage, error)

func startProcessGroupUsageSampler(
	processGroupID int,
	sample processGroupUsageSampleFunc,
) *processGroupUsageSampler {
	if sample == nil {
		sample = samplePlatformProcessGroupUsage
	}
	sampler := &processGroupUsageSampler{
		processGroupID: processGroupID,
		sample:         sample,
		stop:           make(chan struct{}),
		done:           make(chan processGroupUsageSampleResult, 1),
	}
	go sampler.run()
	return sampler
}

func (sampler *processGroupUsageSampler) finish(bound time.Duration) (processGroupUsage, error) {
	sampler.stopOnce.Do(func() { close(sampler.stop) })
	timer := time.NewTimer(bound)
	defer timer.Stop()
	select {
	case result := <-sampler.done:
		return result.usage, result.err
	case <-timer.C:
		return processGroupUsage{}, errProcessGroupUsageTimeout
	}
}

func (sampler *processGroupUsageSampler) run() {
	ticker := time.NewTicker(processUsageSampleInterval)
	defer ticker.Stop()
	var result processGroupUsageSampleResult
	for {
		usage, err := sampler.sample(sampler.processGroupID)
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

func processGroupUsageFailureCode(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, errProcessGroupUsageUnsupported):
		return "process_group_usage_unsupported"
	case errors.Is(err, errProcessGroupUsagePermission):
		return "process_group_usage_permission_denied"
	case errors.Is(err, errProcessGroupUsageInvalid):
		return "process_group_usage_invalid"
	case errors.Is(err, errProcessGroupUsageTimeout):
		return "process_group_usage_timeout"
	default:
		return "process_group_usage_unavailable"
	}
}
