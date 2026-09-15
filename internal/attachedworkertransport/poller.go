// Package attachedworkertransport contains transport scheduling independent of
// protocol state, persistence, and a concrete HTTP adapter.
package attachedworkertransport

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrPollingDisabled = errors.New("attached worker timer polling is disabled")
	ErrAlreadyRunning  = errors.New("attached worker poller is already running")
	ErrInvalidConfig   = errors.New("attached worker poller configuration is invalid")
)

type Cycle interface {
	Exchange(context.Context) error
}

type Config struct {
	Enabled        bool
	PollInterval   time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Random         io.Reader
}

// MinimumHeartbeatInterval is the canonical AW-03 lower bound for both the
// durable heartbeat checkpoint and the worker poll interval. Every accepted
// heartbeat advances durable envelope state, so shorter intervals would break
// the bounded write-cost contract.
const MinimumHeartbeatInterval = 15 * time.Minute

type Poller struct {
	enabled        bool
	pollInterval   time.Duration
	initialBackoff time.Duration
	maxBackoff     time.Duration
	cycle          Cycle
	jitter         *jitterSource
	wait           func(context.Context, time.Duration) error
	waitForWake    func(context.Context, time.Duration, <-chan struct{}) error
	wake           chan struct{}
	running        atomic.Bool
	// lastSuccess is retained across Run calls on the same poller. A new
	// process still needs an authoritative durable cadence checkpoint.
	lastSuccess time.Time
	now         func() time.Time
}

type jitterSource struct {
	mu    sync.Mutex
	state uint64
}

func NewPoller(config Config, cycle Cycle) (*Poller, error) {
	if cycle == nil || config.PollInterval < MinimumHeartbeatInterval || config.InitialBackoff <= 0 ||
		config.MaxBackoff < config.InitialBackoff || config.MaxBackoff > 24*time.Hour {
		return nil, ErrInvalidConfig
	}
	if config.Random == nil {
		config.Random = rand.Reader
	}
	var seedBytes [8]byte
	if _, err := io.ReadFull(config.Random, seedBytes[:]); err != nil {
		return nil, ErrInvalidConfig
	}
	seed := binary.BigEndian.Uint64(seedBytes[:])
	if seed == 0 {
		seed = 0x9e3779b97f4a7c15
	}
	return &Poller{
		enabled: config.Enabled, pollInterval: config.PollInterval,
		initialBackoff: config.InitialBackoff, maxBackoff: config.MaxBackoff,
		cycle: cycle, jitter: &jitterSource{state: seed}, wait: waitContext,
		waitForWake: waitContextOrWake, wake: make(chan struct{}, 1), now: time.Now,
	}, nil
}

// Wake coalesces local work into at most one earlier poll. It never interrupts
// the minimum heartbeat interval or an offline retry backoff, and it carries
// no remote authority.
func (poller *Poller) Wake() error {
	if poller == nil || poller.wake == nil {
		return ErrInvalidConfig
	}
	if !poller.enabled {
		return ErrPollingDisabled
	}
	select {
	case poller.wake <- struct{}{}:
	default:
	}
	return nil
}

func (poller *Poller) Run(ctx context.Context) error {
	if poller == nil || poller.cycle == nil {
		return ErrInvalidConfig
	}
	if !poller.enabled {
		return ErrPollingDisabled
	}
	if !poller.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	defer poller.running.Store(false)
	// A wake left by an earlier run cannot authorize an early exchange after
	// restart; the last successful exchange still enforces the minimum gap.
	poller.discardWake()
	if !poller.lastSuccess.IsZero() {
		if remaining := MinimumHeartbeatInterval - poller.now().Sub(poller.lastSuccess); remaining > 0 {
			if err := poller.wait(ctx, remaining); err != nil {
				return err
			}
		}
	}

	backoff := poller.initialBackoff
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := poller.cycle.Exchange(ctx)
		if err == nil {
			backoff = poller.initialBackoff
			poller.lastSuccess = poller.now()
			if err := poller.wait(ctx, MinimumHeartbeatInterval); err != nil {
				return err
			}
			if remainder := poller.pollInterval - MinimumHeartbeatInterval; remainder > 0 {
				if err := poller.waitForWake(ctx, remainder, poller.wake); err != nil {
					return err
				}
			} else {
				poller.discardWake()
			}
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !retryable(err) {
			return err
		}
		delay, jitterErr := poller.jitter.duration(backoff)
		if jitterErr != nil {
			return ErrInvalidConfig
		}
		if err := poller.wait(ctx, delay); err != nil {
			return err
		}
		backoff = growBackoff(backoff, poller.maxBackoff)
	}
}

func (poller *Poller) discardWake() {
	select {
	case <-poller.wake:
	default:
	}
}

type retryableError interface {
	Retryable() bool
	// NoExchangeAttempted is an explicit adapter proof that the error occurred
	// before any request or semantic effect. A network timeout, HTTP 5xx, or
	// exhausted exact replay is not such proof even if it is Retryable.
	NoExchangeAttempted() bool
}

func retryable(err error) bool {
	// Require a direct trusted cycle result. Unwrapping a joined or wrapped
	// error could pick out a pre-exchange marker while hiding reconciliation.
	candidate, ok := err.(retryableError)
	return ok && candidate.Retryable() && candidate.NoExchangeAttempted()
}

func (source *jitterSource) duration(cap time.Duration) (time.Duration, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	// SplitMix64 is not used for secrets; it expands the construction-time
	// entropy into deterministic full-jitter values without any blocking I/O in
	// Run, so cancellation remains bounded even on retry paths.
	source.state += 0x9e3779b97f4a7c15
	value := source.state
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	value ^= value >> 31
	bound := uint64(cap)
	if bound == math.MaxUint64 {
		return time.Duration(value), nil
	}
	return time.Duration(value % (bound + 1)), nil
}

func growBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum || current > maximum/2 {
		return maximum
	}
	return current * 2
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func waitContextOrWake(ctx context.Context, duration time.Duration, wake <-chan struct{}) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wake:
		return nil
	case <-timer.C:
		return nil
	}
}

// PollCountUpperBound returns the scheduled immediate exchanges across a
// horizon without local wake. It deliberately excludes an attach-time
// exchange and exact in-process retries, rounds a partial interval up, and
// fails closed on overflow. Use PollCountUpperBoundWithWake when local wake
// can shorten the configured interval.
func PollCountUpperBound(workers uint64, horizon, interval time.Duration) (uint64, error) {
	if workers == 0 || horizon <= 0 || interval < MinimumHeartbeatInterval {
		return 0, ErrInvalidConfig
	}
	perWorker := uint64(horizon / interval)
	if horizon%interval != 0 {
		perWorker++
	}
	if perWorker != 0 && workers > math.MaxUint64/perWorker {
		return 0, ErrInvalidConfig
	}
	return workers * perWorker, nil
}

// PollCountUpperBoundWithWake budgets the fastest permitted local wake
// cadence. A longer configured interval cannot be used as the cost ceiling
// once a buffered wake may shorten it to MinimumHeartbeatInterval.
func PollCountUpperBoundWithWake(workers uint64, horizon, interval time.Duration) (uint64, error) {
	if interval < MinimumHeartbeatInterval {
		return 0, ErrInvalidConfig
	}
	return PollCountUpperBound(workers, horizon, MinimumHeartbeatInterval)
}
