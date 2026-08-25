package attachedworkertransport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

func TestPollerFeatureGatePreventsExchange(t *testing.T) {
	var calls atomic.Int32
	poller, err := NewPoller(Config{
		Enabled: false, PollInterval: MinimumHeartbeatInterval, InitialBackoff: time.Second, MaxBackoff: time.Minute,
	}, cycleFunc(func(context.Context) error { calls.Add(1); return nil }))
	if err != nil {
		t.Fatal(err)
	}
	if err := poller.Run(context.Background()); !errors.Is(err, ErrPollingDisabled) || calls.Load() != 0 {
		t.Fatalf("disabled run err=%v calls=%d", err, calls.Load())
	}
}

func TestPollerUsesDeterministicFullJitterAndWaitsAfterCompletion(t *testing.T) {
	random := new(bytes.Buffer)
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], 5)
	random.Write(encoded[:])
	outcomes := []error{temporaryError{}, temporaryError{}, nil, temporaryError{}, errors.New("stop")}
	index := 0
	poller, err := NewPoller(Config{
		Enabled: true, PollInterval: MinimumHeartbeatInterval,
		InitialBackoff: 10 * time.Nanosecond, MaxBackoff: 40 * time.Nanosecond, Random: random,
	}, cycleFunc(func(context.Context) error {
		err := outcomes[index]
		index++
		return err
	}))
	if err != nil {
		t.Fatal(err)
	}
	var waits []time.Duration
	poller.wait = func(_ context.Context, duration time.Duration) error {
		waits = append(waits, duration)
		return nil
	}
	err = poller.Run(context.Background())
	if err == nil || err.Error() != "stop" {
		t.Fatalf("terminal error=%v", err)
	}
	// The final retry must be bounded by 10ns, proving success resets the
	// backoff cap instead of retaining the prior 40ns cap.
	want := []time.Duration{time.Nanosecond, 19 * time.Nanosecond, MinimumHeartbeatInterval, 5 * time.Nanosecond}
	if len(waits) != len(want) {
		t.Fatalf("waits=%v", waits)
	}
	for index := range want {
		if waits[index] != want[index] {
			t.Fatalf("waits=%v want=%v", waits, want)
		}
	}
}

func TestPollerCancellationAndSingleflight(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	poller, err := NewPoller(Config{
		Enabled: true, PollInterval: MinimumHeartbeatInterval, InitialBackoff: time.Second, MaxBackoff: time.Minute,
	}, cycleFunc(func(ctx context.Context) error {
		close(entered)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return nil
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- poller.Run(ctx) }()
	<-entered
	if err := poller.Run(context.Background()); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("concurrent run=%v", err)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled run=%v", err)
	}
	close(release)
}

func TestPollerCancellationStopsTimerWithoutCatchup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	poller, err := NewPoller(Config{
		Enabled: true, PollInterval: time.Hour, InitialBackoff: time.Second, MaxBackoff: time.Minute,
	}, cycleFunc(func(context.Context) error { calls.Add(1); return nil }))
	if err != nil {
		t.Fatal(err)
	}
	poller.wait = func(ctx context.Context, _ time.Duration) error {
		cancel()
		<-ctx.Done()
		return ctx.Err()
	}
	if err := poller.Run(ctx); !errors.Is(err, context.Canceled) || calls.Load() != 1 {
		t.Fatalf("run err=%v calls=%d", err, calls.Load())
	}
}

func TestPollerRejectsSubminimumIntervalAndEntropyFailureAtConstruction(t *testing.T) {
	cycle := cycleFunc(func(context.Context) error { return nil })
	if _, err := NewPoller(Config{
		Enabled: true, PollInterval: MinimumHeartbeatInterval - time.Nanosecond,
		InitialBackoff: time.Second, MaxBackoff: time.Minute,
	}, cycle); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("subminimum poll interval error=%v", err)
	}
	if _, err := NewPoller(Config{
		Enabled: true, PollInterval: MinimumHeartbeatInterval,
		InitialBackoff: time.Second, MaxBackoff: time.Minute, Random: bytes.NewReader(nil),
	}, cycle); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("entropy failure error=%v", err)
	}
}

func TestPollCountUpperBound(t *testing.T) {
	month := 30 * 24 * time.Hour
	for _, test := range []struct {
		workers uint64
		want    uint64
	}{{1, 2_880}, {100, 288_000}, {1_000, 2_880_000}, {10_000, 28_800_000}} {
		got, err := PollCountUpperBound(test.workers, month, MinimumHeartbeatInterval)
		if err != nil || got != test.want {
			t.Fatalf("workers=%d got=%d want=%d err=%v", test.workers, got, test.want, err)
		}
	}
	got, err := PollCountUpperBound(2, 31*time.Minute, MinimumHeartbeatInterval)
	if err != nil || got != 6 {
		t.Fatalf("rounded partial interval got=%d err=%v", got, err)
	}
	if _, err := PollCountUpperBound(1, time.Hour, MinimumHeartbeatInterval-time.Nanosecond); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("subminimum interval error=%v", err)
	}
	if _, err := PollCountUpperBound(math.MaxUint64, time.Hour, MinimumHeartbeatInterval); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("overflow error=%v", err)
	}
}

type cycleFunc func(context.Context) error

func (function cycleFunc) Exchange(ctx context.Context) error { return function(ctx) }

type temporaryError struct{}

func (temporaryError) Error() string   { return "temporary" }
func (temporaryError) Retryable() bool { return true }
