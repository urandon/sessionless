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
	if err := poller.Wake(); !errors.Is(err, ErrPollingDisabled) {
		t.Fatalf("disabled wake err=%v", err)
	}
}

func TestPollerWakeCoalescesOnlyAfterMinimumInterval(t *testing.T) {
	var calls int
	poller, err := NewPoller(Config{
		Enabled: true, PollInterval: time.Hour, InitialBackoff: time.Second, MaxBackoff: time.Minute,
	}, cycleFunc(func(context.Context) error {
		calls++
		if calls == 2 {
			return errors.New("stop")
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	var events []string
	poller.wait = func(_ context.Context, duration time.Duration) error {
		events = append(events, "minimum")
		if duration != MinimumHeartbeatInterval || calls != 1 {
			t.Fatalf("minimum wait duration=%s calls=%d", duration, calls)
		}
		for range 3 {
			if err := poller.Wake(); err != nil {
				t.Fatalf("wake err=%v", err)
			}
		}
		return nil
	}
	poller.waitForWake = func(_ context.Context, duration time.Duration, wake <-chan struct{}) error {
		events = append(events, "wake")
		if duration != time.Hour-MinimumHeartbeatInterval || calls != 1 {
			t.Fatalf("wake wait duration=%s calls=%d", duration, calls)
		}
		select {
		case <-wake:
		default:
			t.Fatal("coalesced wake was not retained until the minimum interval")
		}
		select {
		case <-wake:
			t.Fatal("multiple wakes produced more than one early poll")
		default:
		}
		return nil
	}
	if err := poller.Run(context.Background()); err == nil || err.Error() != "stop" {
		t.Fatalf("run err=%v", err)
	}
	if calls != 2 || len(events) != 2 || events[0] != "minimum" || events[1] != "wake" {
		t.Fatalf("calls=%d events=%v", calls, events)
	}
}

func TestPollerWakeInterruptsOnlyPostMinimumRemainder(t *testing.T) {
	var calls atomic.Int32
	poller, err := NewPoller(Config{
		Enabled: true, PollInterval: time.Hour, InitialBackoff: time.Second, MaxBackoff: time.Minute,
	}, cycleFunc(func(context.Context) error {
		if calls.Add(1) == 1 {
			return nil
		}
		return errors.New("stop")
	}))
	if err != nil {
		t.Fatal(err)
	}
	minimumDone := make(chan struct{})
	poller.wait = func(_ context.Context, duration time.Duration) error {
		if duration != MinimumHeartbeatInterval {
			t.Errorf("minimum duration=%s", duration)
		}
		close(minimumDone)
		return nil
	}
	remainderEntered := make(chan struct{})
	poller.waitForWake = func(ctx context.Context, duration time.Duration, wake <-chan struct{}) error {
		if duration != time.Hour-MinimumHeartbeatInterval {
			t.Errorf("remainder duration=%s", duration)
		}
		close(remainderEntered)
		return waitContextOrWake(ctx, duration, wake)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- poller.Run(ctx) }()
	select {
	case <-remainderEntered:
	case <-ctx.Done():
		t.Fatalf("remainder was not entered: %v", ctx.Err())
	}
	select {
	case <-minimumDone:
	default:
		t.Fatal("remainder began before minimum interval")
	}
	if err := poller.Wake(); err != nil {
		t.Fatalf("wake err=%v", err)
	}
	select {
	case err := <-result:
		if err == nil || err.Error() != "stop" || calls.Load() != 2 {
			t.Fatalf("run err=%v calls=%d", err, calls.Load())
		}
	case <-ctx.Done():
		t.Fatalf("wake did not finish bounded remainder: %v", ctx.Err())
	}
}

func TestPollerCancellationInterruptsPostMinimumRemainder(t *testing.T) {
	var calls atomic.Int32
	poller, err := NewPoller(Config{
		Enabled: true, PollInterval: time.Hour, InitialBackoff: time.Second, MaxBackoff: time.Minute,
	}, cycleFunc(func(context.Context) error { calls.Add(1); return nil }))
	if err != nil {
		t.Fatal(err)
	}
	poller.wait = func(context.Context, time.Duration) error { return nil }
	remainderEntered := make(chan struct{})
	poller.waitForWake = func(ctx context.Context, duration time.Duration, wake <-chan struct{}) error {
		close(remainderEntered)
		return waitContextOrWake(ctx, duration, wake)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- poller.Run(ctx) }()
	select {
	case <-remainderEntered:
	case <-ctx.Done():
		t.Fatalf("remainder was not entered: %v", ctx.Err())
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) || calls.Load() != 1 {
			t.Fatalf("cancelled run err=%v calls=%d", err, calls.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled remainder did not exit")
	}
}

func TestPollerWakeDoesNotInterruptOfflineBackoff(t *testing.T) {
	var calls int
	poller, err := NewPoller(Config{
		Enabled: true, PollInterval: time.Hour, InitialBackoff: time.Second, MaxBackoff: time.Minute,
	}, cycleFunc(func(context.Context) error {
		calls++
		switch calls {
		case 1:
			return temporaryError{}
		case 2:
			return nil
		default:
			return errors.New("stop")
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	var waits []time.Duration
	poller.wait = func(_ context.Context, duration time.Duration) error {
		waits = append(waits, duration)
		if calls == 1 {
			if err := poller.Wake(); err != nil {
				t.Fatalf("offline wake err=%v", err)
			}
		}
		return nil
	}
	poller.waitForWake = func(_ context.Context, _ time.Duration, wake <-chan struct{}) error {
		if calls != 2 {
			t.Fatalf("wake observed during offline retry calls=%d", calls)
		}
		select {
		case <-wake:
			return nil
		default:
			t.Fatal("wake missing after successful minimum wait")
			return nil
		}
	}
	if err := poller.Run(context.Background()); err == nil || err.Error() != "stop" {
		t.Fatalf("run err=%v", err)
	}
	if calls != 3 || len(waits) != 2 || waits[0] > time.Second || waits[1] != MinimumHeartbeatInterval {
		t.Fatalf("calls=%d waits=%v", calls, waits)
	}
}

func TestPollerRestartDiscardsStaleWake(t *testing.T) {
	var calls int
	poller, err := NewPoller(Config{
		Enabled: true, PollInterval: time.Hour, InitialBackoff: time.Second, MaxBackoff: time.Minute,
	}, cycleFunc(func(context.Context) error {
		calls++
		if calls == 1 {
			return terminalNoEffectError{}
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := poller.Run(context.Background()); err == nil || err.Error() != "stop" {
		t.Fatalf("first run err=%v", err)
	}
	if err := poller.Wake(); err != nil {
		t.Fatalf("stale wake err=%v", err)
	}
	poller.wait = func(context.Context, time.Duration) error { return nil }
	poller.waitForWake = func(_ context.Context, _ time.Duration, wake <-chan struct{}) error {
		select {
		case <-wake:
			t.Fatal("stale wake crossed poller restart")
		default:
		}
		return context.Canceled
	}
	if err := poller.Run(context.Background()); !errors.Is(err, context.Canceled) || calls != 2 {
		t.Fatalf("restart err=%v calls=%d", err, calls)
	}
}

func TestPollerRestartAfterSuccessRetainsMinimumGap(t *testing.T) {
	var calls int
	now := time.Unix(1_700_000_000, 0)
	poller, err := NewPoller(Config{
		Enabled: true, PollInterval: MinimumHeartbeatInterval, InitialBackoff: time.Second, MaxBackoff: time.Minute,
	}, cycleFunc(func(context.Context) error {
		calls++
		if calls == 1 {
			return nil
		}
		return errors.New("stop")
	}))
	if err != nil {
		t.Fatal(err)
	}
	poller.now = func() time.Time { return now }
	firstCtx, firstCancel := context.WithCancel(context.Background())
	poller.wait = func(ctx context.Context, duration time.Duration) error {
		if duration != MinimumHeartbeatInterval {
			t.Fatalf("first wait=%s", duration)
		}
		firstCancel()
		return ctx.Err()
	}
	if err := poller.Run(firstCtx); !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("first run err=%v calls=%d", err, calls)
	}
	now = now.Add(5 * time.Minute)
	if err := poller.Wake(); err != nil {
		t.Fatalf("restart wake err=%v", err)
	}
	poller.wait = func(_ context.Context, duration time.Duration) error {
		if duration != 10*time.Minute || calls != 1 {
			t.Fatalf("restart minimum=%s calls=%d", duration, calls)
		}
		now = now.Add(duration)
		return nil
	}
	if err := poller.Run(context.Background()); err == nil || err.Error() != "stop" || calls != 2 {
		t.Fatalf("restart err=%v calls=%d", err, calls)
	}
}

func TestPollerNeverRetriesAmbiguousOrPostExchangeUnavailable(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "retryable transport outcome without no-effect proof", err: unsafeTemporaryError{}},
		{name: "joined reconciliation ambiguity", err: errors.Join(temporaryError{}, errors.New("reconciliation required"))},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			poller, err := NewPoller(Config{
				Enabled: true, PollInterval: MinimumHeartbeatInterval,
				InitialBackoff: time.Second, MaxBackoff: time.Minute,
			}, cycleFunc(func(context.Context) error { calls++; return test.err }))
			if err != nil {
				t.Fatal(err)
			}
			poller.wait = func(context.Context, time.Duration) error {
				t.Fatal("ambiguous exchange entered retry backoff")
				return nil
			}
			if got := poller.Run(context.Background()); got != test.err || calls != 1 {
				t.Fatalf("run err=%v calls=%d", got, calls)
			}
			if got := poller.Run(context.Background()); !errors.Is(got, ErrReconciliationRequired) || calls != 1 {
				t.Fatalf("ambiguous restart err=%v calls=%d", got, calls)
			}
		})
	}
}

func TestPollerStepGatesSerialDaemonSourceCallsAndWake(t *testing.T) {
	var calls int
	now := time.Unix(1_700_000_000, 0)
	poller, err := NewPoller(Config{
		Enabled: true, PollInterval: time.Hour, InitialBackoff: time.Second, MaxBackoff: time.Minute,
	}, cycleFunc(func(context.Context) error { calls++; return nil }))
	if err != nil {
		t.Fatal(err)
	}
	poller.now = func() time.Time { return now }
	if err := poller.Step(context.Background()); err != nil || calls != 1 {
		t.Fatalf("first step err=%v calls=%d", err, calls)
	}
	if err := poller.Wake(); err != nil {
		t.Fatal(err)
	}
	var waits []time.Duration
	poller.wait = func(_ context.Context, duration time.Duration) error {
		if calls != 1 {
			t.Fatalf("second network exchange began before minimum wait: calls=%d", calls)
		}
		waits = append(waits, duration)
		now = now.Add(duration)
		return nil
	}
	poller.waitForWake = func(_ context.Context, duration time.Duration, wake <-chan struct{}) error {
		waits = append(waits, duration)
		select {
		case <-wake:
			return nil
		default:
			t.Fatal("local wake missing after minimum wait")
			return nil
		}
	}
	if err := poller.Step(context.Background()); err != nil || calls != 2 {
		t.Fatalf("second step err=%v calls=%d", err, calls)
	}
	if len(waits) != 2 || waits[0] != MinimumHeartbeatInterval || waits[1] != time.Hour-MinimumHeartbeatInterval {
		t.Fatalf("step waits=%v", waits)
	}
}

func TestPollerStepSingleflightWhileCadenceWaits(t *testing.T) {
	var calls atomic.Int32
	poller, err := NewPoller(Config{
		Enabled: true, PollInterval: MinimumHeartbeatInterval,
		InitialBackoff: time.Second, MaxBackoff: time.Minute,
	}, cycleFunc(func(context.Context) error { calls.Add(1); return nil }))
	if err != nil {
		t.Fatal(err)
	}
	if err := poller.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	enteredWait := make(chan struct{})
	poller.wait = func(ctx context.Context, duration time.Duration) error {
		if duration <= 0 || duration > MinimumHeartbeatInterval {
			t.Errorf("cadence wait=%s", duration)
		}
		close(enteredWait)
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- poller.Step(ctx) }()
	<-enteredWait
	if err := poller.Step(context.Background()); !errors.Is(err, ErrAlreadyRunning) || calls.Load() != 1 {
		t.Fatalf("concurrent step err=%v calls=%d", err, calls.Load())
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) || calls.Load() != 1 {
		t.Fatalf("cancelled step err=%v calls=%d", err, calls.Load())
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
	withoutWake, err := PollCountUpperBound(1, 24*time.Hour, time.Hour)
	if err != nil || withoutWake != 24 {
		t.Fatalf("scheduled count=%d err=%v", withoutWake, err)
	}
	withWake, err := PollCountUpperBoundWithWake(1, 24*time.Hour, time.Hour)
	if err != nil || withWake != 96 {
		t.Fatalf("wake cost ceiling=%d err=%v", withWake, err)
	}
	if _, err := PollCountUpperBoundWithWake(1, time.Hour, MinimumHeartbeatInterval-time.Nanosecond); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("wake subminimum interval err=%v", err)
	}
}

type cycleFunc func(context.Context) error

func (function cycleFunc) Exchange(ctx context.Context) error { return function(ctx) }

type temporaryError struct{}

func (temporaryError) Error() string             { return "temporary" }
func (temporaryError) Retryable() bool           { return true }
func (temporaryError) NoExchangeAttempted() bool { return true }

type unsafeTemporaryError struct{}

func (unsafeTemporaryError) Error() string   { return "post-exchange unavailable" }
func (unsafeTemporaryError) Retryable() bool { return true }

type terminalNoEffectError struct{}

func (terminalNoEffectError) Error() string             { return "stop" }
func (terminalNoEffectError) NoExchangeAttempted() bool { return true }
