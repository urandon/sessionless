package localoperator

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// A test-only context models the permitted interval between the retained
// deadline being reached and asynchronous cancellation becoming observable.
// Done/Err stay undispatched deliberately; there is no production clock seam,
// timer, key, source authentication or installation/runtime proof here.
type undispatchedSourceDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c undispatchedSourceDeadlineContext) Deadline() (time.Time, bool) {
	return c.deadline, true
}

func TestSourceAliveRetainedDeadlineBeforeCancellationDispatch(t *testing.T) {
	for _, mode := range []string{"installation-boundary", "installation-after", "shorter-parent-boundary", "shorter-parent-after"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				deadline := time.Now().Add(5 * time.Second)
				configurationEnd := deadline
				if mode == "shorter-parent-boundary" || mode == "shorter-parent-after" {
					configurationEnd = deadline.Add(time.Minute)
				}
				parent, cancel := context.WithCancel(t.Context())
				t.Cleanup(cancel)
				retained := undispatchedSourceDeadlineContext{Context: parent, deadline: deadline}
				originalKey := [32]byte{1} // synthetic private unit-fixture bytes only
				source := &Source{ctx: retained, cancel: cancel, key: &sourceKey{bytes: originalKey}, config: domain.ConfiguredInferenceProfileV1{ExpiresAt: configurationEnd}}
				source.self = source
				t.Cleanup(func() { _ = source.Close() })
				time.Sleep(time.Until(deadline) - time.Microsecond)
				source.mu.Lock()
				before := source.aliveLocked()
				untouched := source.key.bytes == originalKey && !source.closed
				source.mu.Unlock()
				if !before || !untouched || retained.Err() != nil {
					t.Fatal("source expired before the shorter retained deadline")
				}
				time.Sleep(time.Microsecond)
				if mode == "installation-after" || mode == "shorter-parent-after" {
					time.Sleep(time.Microsecond)
				}
				if time.Now().Before(deadline) || retained.Err() != nil {
					t.Fatal("causal setup did not reach the deadline with Err still nil")
				}
				select {
				case <-retained.Done():
					t.Fatal("test accidentally waited for cancellation dispatch")
				default:
				}
				source.mu.Lock()
				alive := source.aliveLocked()
				closed, cleared := source.closed, source.key.bytes == ([32]byte{})
				again := source.aliveLocked()
				source.mu.Unlock()
				if alive || again || !closed || !cleared {
					t.Fatalf("%s depended on delayed cancellation: alive=%t again=%t closed=%t cleared=%t", mode, alive, again, closed, cleared)
				}
				if retained.Err() != nil {
					t.Fatal("denial was attributed to dispatched context error instead of original deadline")
				}
			})
		})
	}
}
